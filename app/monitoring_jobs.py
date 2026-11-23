"""Live monitoring workers, schedulers, client control, and caches.

# noqa: SIZE_OK — these coupled background jobs share queue identity and timing contracts.
"""

import asyncio
import datetime
import logging
import random
from dataclasses import replace

import redis

from . import (
    DanmakuCounts,
    danmaku_metrics,
    rounded_concurrency,
    bilibili_gateway,
    event_ingestion,
    room_config,
    room_lock_events,
    room_uid_cache,
    runtime_state,
)
from .blivedm.clients.ws_base import InitError
from .config import ATTENTION_DAILY_ROOM_SLEEP_SECONDS
from .live_client import AuthenticatedLiveClient
from .live_startup import StartupRooms, status_batches
from .metrics_runtime import checkpoint_session, record_concurrency, restore_session, retry_failed_buckets, start_session
from .models import (
    Attention,
    LiveSession,
    LiveSession15mStats,
    RoomStatsMonthly,
)

logger = logging.getLogger(__name__)
STARTUP_AUTH_WAIT_SECONDS = 60


def _now() -> datetime.datetime:
    return datetime.datetime.now().astimezone().replace(tzinfo=None)


def _next_daily_target(now: datetime.datetime, hour: int, minute: int) -> datetime.datetime:
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target + datetime.timedelta(days=1) if now >= target else target


async def _sleep_until(target: datetime.datetime) -> None:
    while True:
        remaining = (target - _now()).total_seconds()
        if remaining <= 0:
            return
        await asyncio.sleep(remaining)


async def start_client(
    room_id: int, *, daily_reconnect_from: AuthenticatedLiveClient | None = None,
) -> None:
    """Start and register one BLive client using the canonical handler."""
    from .logging_setup import api_log_context

    client = AuthenticatedLiveClient(room_id, session=runtime_state.aiohttp_session)
    if daily_reconnect_from is not None:
        client.inherit_daily_reconnect(daily_reconnect_from)
    client.set_handler(event_ingestion.MyHandler())
    # A room added by HTTP must not tag its long-lived gift/WS tasks as API work.
    with api_log_context(None):
        client.start()
    runtime_state.ROOM_CLIENTS[room_id] = client
    runtime_state.LAST_RECONNECT.setdefault(room_id, _now() - datetime.timedelta(days=random.random() * 3.0))
    logger.info("[connect] 已启动房间连接 room_id=%s", room_id)


async def reconnect_one(room_id: int) -> None:
    if runtime_state.LAST_STATUS.get(room_id, 0) == 1:
        logger.info("[reconnect] 房间 %s 已在播，跳过重连", room_id)
        return
    client = runtime_state.ROOM_CLIENTS.get(room_id)
    if client is None:
        return
    with client.daily_reconnect(STARTUP_AUTH_WAIT_SECONDS):
        try:
            await client.stop_and_close()
        except asyncio.CancelledError:
            if asyncio.current_task().cancelling():
                raise
            logger.debug("[reconnect] room=%s stop_and_close 触发取消（预期），忽略", room_id)
        except Exception as exc:
            logger.exception("[reconnect] stop_and_close 异常 room=%s: %s", room_id, type(exc).__name__, exc_info=False)
        await asyncio.sleep(3)
        if runtime_state.ROOM_CLIENTS.get(room_id) is not client:
            return
        await start_client(room_id, daily_reconnect_from=client)
    runtime_state.LAST_RECONNECT[room_id] = _now()
    logger.info("[reconnect] 房间 %s 已启动计划重连", room_id)


async def _wait_for_first_connection(room_id: int, start_lock: asyncio.Lock) -> None:
    try:
        while room_id in room_config.get_room_ids():
            client = runtime_state.ROOM_CLIENTS.get(room_id)
            if client is None:
                return
            try:
                await client.wait_connected()
            except InitError:
                logger.warning("[connect] 房间初始化失败，重试 room_id=%s", room_id)
                await asyncio.sleep(3)
                async with start_lock:
                    if room_id in room_config.get_room_ids() and runtime_state.ROOM_CLIENTS.get(room_id) is client:
                        await start_client(room_id)
                        await asyncio.sleep(3)
            else:
                if runtime_state.ROOM_CLIENTS.get(room_id) is client:
                    logger.info("[connect] 房间鉴权成功 room_id=%s", room_id)
                    return
    finally:
        runtime_state.ROOMS_AWAITING_FIRST_AUTH.discard(room_id)


async def _discover_startup_uids(startup: StartupRooms) -> None:
    while True:
        await init_uids_and_attention_once(max_rounds=1)
        startup.uid_sweep_done = True
        startup.changed.set()
        if all(room in runtime_state.ROOM_UIDS for room in room_config.get_room_ids()):
            return
        await asyncio.sleep(3)


async def run_clients_loop() -> None:
    startup = StartupRooms()
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(_discover_startup_uids(startup))
        tasks.create_task(monitor_all_rooms_status(startup))
        await startup.connect(start_client, _wait_for_first_connection, STARTUP_AUTH_WAIT_SECONDS)


def init_concurrency_cache(
    room_id: int,
    session_id: int,
    start_time: datetime.datetime | None = None,
) -> None:
    runtime_state.CONCURRENCY_CACHE[room_id] = {"session_id": int(session_id), "total": 0, "samples": 0, "max": 0, "last": 0}
    start_session(session_id, room_id, start_time or _now())


def update_concurrency_cache(room_id: int, session_id: int, count: int) -> None:
    cache = runtime_state.CONCURRENCY_CACHE.get(room_id)
    if not cache or cache.get("session_id") != session_id:
        init_concurrency_cache(room_id, session_id)
        cache = runtime_state.CONCURRENCY_CACHE[room_id]
    cache["total"] = int(cache.get("total", 0)) + int(count)
    cache["samples"] = int(cache.get("samples", 0)) + 1
    cache["max"] = max(int(cache.get("max", 0)), int(count))
    cache["last"] = int(count)
    record_concurrency(session_id, _now(), int(count))


def finalize_concurrency_cache(room_id: int, session_id: int | None) -> tuple[float | None, int | None]:
    cache = runtime_state.CONCURRENCY_CACHE.get(room_id)
    if not cache or session_id is None or cache.get("session_id") != session_id:
        return None, None
    samples = int(cache.get("samples", 0))
    return (rounded_concurrency(int(cache.get("total", 0)) / samples) if samples else None,
            int(cache.get("max", 0)) if samples else None)


def flush_pending_danmaku_for_room(
    room_id: int,
    session_id: int | None = None,
    event_time: datetime.datetime | None = None,
) -> None:
    pending = runtime_state.DANMAKU_PENDING.get(room_id)
    if pending is None:
        return
    total = sum(counts.total for counts in pending.sessions.values())

    def persist(items, key, scope, writer):
        try:
            counts = danmaku_metrics.prepare(pending, key, scope, items[key])
        except redis.RedisError as exc:
            logger.warning("[Danmaku] 保留待重试批次 scope=%s error=%s", scope.key, type(exc).__name__)
            original = items[key]
            if original.total and writer(key, replace(original, audience=danmaku_metrics.Audience(None, None))):
                # The legacy totals are durable. Retry only the sender snapshot.
                items[key] = DanmakuCounts()
            return
        if writer(key, counts):
            del items[key]
            pending.results.pop(key, None)

    for month in tuple(sorted(pending.monthly)):
        scope = danmaku_metrics.Scope(f"month:{month}:{room_id}", danmaku_metrics.month_start(month))
        persist(pending.monthly, month, scope, lambda key, counts: RoomStatsMonthly.add_danmaku_counts(room_id, key, counts))
    for pending_id in tuple(sorted(pending.sessions)):
        # The original bucket retains the parent start even after the session closes.
        start = pending.session_starts.get(pending_id, datetime.datetime.min)
        persist(pending.sessions, pending_id, danmaku_metrics.Scope(f"session:{pending_id}", start),
                LiveSession.add_danmaku_by_id)
        if pending_id not in pending.sessions:
            pending.session_starts.pop(pending_id, None)
    for target in tuple(pending.buckets):
        scope = danmaku_metrics.Scope(f"bucket:{target.session_id}:{target.bucket_index}", target.start_time, ranked=False)
        persist(pending.buckets, target, scope, LiveSession15mStats.add_danmaku_counts)
    if pending.is_empty():
        runtime_state.DANMAKU_PENDING.pop(room_id, None)
    logger.debug("[Danmaku] room_id=%s 下播/停用即时落库 +%s", room_id, total)


async def capture_end_snapshot(snapshot: runtime_state.SessionEndSnapshot) -> None:
    """Start all end reads immediately, independently of the periodic queues."""
    uid = runtime_state.ROOM_UIDS.get(snapshot.room_id)

    async def capture(field, fetch, *args):
        try:
            value = await fetch(*args)
            setattr(snapshot, field, value)
            if value is None:
                logger.warning("[EndSnapshot] room_id=%s field=%s 采集失败，保留缺失值", snapshot.room_id, field)
        except Exception as exc:
            logger.error("[EndSnapshot] room_id=%s field=%s 采集失败 error_type=%s", snapshot.room_id, field, type(exc).__name__)

    reads = [capture("attention", bilibili_gateway.fetch_room_attention, snapshot.room_id)]
    if uid is not None:
        reads.extend((
            capture("guard_counts", bilibili_gateway.fetch_guard_counts, uid, snapshot.room_id),
            capture("fans_count", bilibili_gateway.fetch_fans_count, uid, snapshot.room_id),
        ))
    else:
        logger.warning("[EndSnapshot] room_id=%s 未找到 uid，守护与粉丝团快照缺失", snapshot.room_id)
    await asyncio.gather(*reads)


async def attention_worker() -> None:
    while True:
        room_id, session_id, phase, target_date = await runtime_state.ATTENTION_QUEUE.get()
        try:
            if phase == "start" and session_id and runtime_state.CURRENT_SESSIONS.get(room_id) != session_id:
                continue
            attention = await bilibili_gateway.refresh_room_attention(room_id)
            if attention is None:
                continue
            if phase == "start" and session_id:
                LiveSession.update_start_attention(session_id, attention)
            elif phase == "end" and session_id:
                LiveSession.update_end_attention(session_id, attention)
            else:
                Attention.upsert_daily(room_id, target_date, attention)
        except Exception as exc:
            logger.exception("[Attention] room_id=%s phase=%s 处理失败: %s", room_id, phase, type(exc).__name__, exc_info=False)
        finally:
            runtime_state.ATTENTION_QUEUE.task_done()


async def daily_attention_worker() -> None:
    while True:
        room_id, target_date = await runtime_state.DAILY_ATTENTION_QUEUE.get()
        try:
            if target_date != _now().date():
                continue
            attention = await bilibili_gateway.refresh_room_attention(room_id)
            if attention is not None:
                Attention.upsert_daily(room_id, target_date, attention)
        finally:
            runtime_state.DAILY_ATTENTION_QUEUE.task_done()


async def guard_fans_worker() -> None:
    while True:
        room_id, session_id, phase = await runtime_state.GUARD_FANS_QUEUE.get()
        try:
            uid = runtime_state.ROOM_UIDS.get(room_id)
            if uid is None:
                logger.warning("[Guard/Fans] room_id=%s 未找到 uid，跳过", room_id)
                continue
            guard_values = await bilibili_gateway.fetch_guard_counts(uid, room_id)
            if guard_values is not None:
                guard_1, guard_2, guard_3 = guard_values
                runtime_state.GUARD_COUNTS[room_id] = {"guard_1": guard_1, "guard_2": guard_2, "guard_3": guard_3}
                if session_id and phase == "start":
                    LiveSession.update_start_counts(session_id, guard_1=guard_1, guard_2=guard_2, guard_3=guard_3)
                elif session_id and phase == "end":
                    LiveSession.update_end_counts(session_id, guard_1=guard_1, guard_2=guard_2, guard_3=guard_3)
            await asyncio.sleep(1.0)
            fans = await bilibili_gateway.fetch_fans_count(uid, room_id)
            if fans is not None:
                runtime_state.FANS_COUNT[room_id] = fans
                if session_id and phase == "start":
                    LiveSession.update_start_counts(session_id, fans_count=fans)
                elif session_id and phase == "end":
                    LiveSession.update_end_counts(session_id, fans_count=fans)
            await asyncio.sleep(1.0)
        finally:
            runtime_state.GUARD_FANS_QUEUE.task_done()


async def daily_guard_worker() -> None:
    while True:
        room_id, target_date = await runtime_state.DAILY_GUARD_QUEUE.get()
        try:
            uid = runtime_state.ROOM_UIDS.get(room_id)
            if uid is None:
                logger.warning("[Guard] room_id=%s 未找到 uid，跳过每日快照", room_id)
                continue
            values = await bilibili_gateway.fetch_guard_counts(uid, room_id)
            if values is not None:
                runtime_state.GUARD_COUNTS[room_id] = dict(zip(("guard_1", "guard_2", "guard_3"), values))
                Attention.upsert_daily_guards(room_id, target_date, values)
            await asyncio.sleep(1.0)
        finally:
            runtime_state.DAILY_GUARD_QUEUE.task_done()


async def daily_fans_worker() -> None:
    while True:
        room_id, target_date = await runtime_state.DAILY_FANS_QUEUE.get()
        try:
            uid = runtime_state.ROOM_UIDS.get(room_id)
            if uid is None:
                logger.warning("[Fans] room_id=%s 未找到 uid，跳过每日快照", room_id)
                continue
            fans = await bilibili_gateway.fetch_fans_count(uid, room_id)
            if fans is not None:
                runtime_state.FANS_COUNT[room_id] = fans
                Attention.upsert_daily_fans(room_id, target_date, fans)
            await asyncio.sleep(1.0)
        finally:
            runtime_state.DAILY_FANS_QUEUE.task_done()


async def danmaku_flush_scheduler() -> None:
    while True:
        await asyncio.sleep(60)
        retry_failed_buckets()
        now = _now()
        for room, session_id in tuple(runtime_state.CURRENT_SESSIONS.items()):
            if runtime_state.LAST_STATUS.get(room) == 1:
                checkpoint_session(session_id, now)
        for room_id in tuple(runtime_state.DANMAKU_PENDING):
            flush_pending_danmaku_for_room(room_id, runtime_state.CURRENT_SESSIONS.get(room_id))


async def refresh_attention_scheduler() -> None:
    while True:
        logger.info("[RoomInfo] 开始粉丝数刷新任务")
        for room_id in room_config.get_room_ids():
            await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=False)
            await asyncio.sleep(0.3)
        logger.info("[RoomInfo] 本轮粉丝数刷新任务完成")
        await asyncio.sleep(3 * 3600)


async def _daily_queue_scheduler(hour: int, minute: int, queue: asyncio.Queue[tuple[int, datetime.date]]) -> None:
    while True:
        target = _next_daily_target(_now(), hour, minute)
        await _sleep_until(target)
        for room_id in room_config.get_room_ids():
            await queue.put((room_id, target.date()))
            await asyncio.sleep(ATTENTION_DAILY_ROOM_SLEEP_SECONDS)


async def attention_daily_scheduler() -> None:
    await _daily_queue_scheduler(6, 30, runtime_state.DAILY_ATTENTION_QUEUE)


async def guard_daily_scheduler() -> None:
    await _daily_queue_scheduler(6, 40, runtime_state.DAILY_GUARD_QUEUE)


async def fans_daily_scheduler() -> None:
    await _daily_queue_scheduler(6, 50, runtime_state.DAILY_FANS_QUEUE)


async def guard_fans_refresh_scheduler() -> None:
    while not runtime_state.ROOM_UIDS:
        logger.info("[Guard/Fans] 等待 UID 初始化...")
        await asyncio.sleep(1)
    while True:
        for room_id in room_config.get_room_ids():
            if runtime_state.LAST_STATUS.get(room_id, 0) != 1:
                await runtime_state.GUARD_FANS_QUEUE.put((room_id, None, None))
                await asyncio.sleep(0.1)
        await asyncio.sleep(3600)


async def concurrency_poll_scheduler() -> None:
    while not runtime_state.ROOM_UIDS:
        logger.info("[Concurrency] 等待 UID 初始化...")
        await asyncio.sleep(1)
    while True:
        for room_id in room_config.get_room_ids():
            if runtime_state.LAST_STATUS.get(room_id, 0) != 1:
                continue
            uid = runtime_state.ROOM_UIDS.get(room_id)
            session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
            if uid is None or session_id is None:
                continue
            count = await bilibili_gateway.fetch_contribution_count(uid, room_id)
            if (count is not None and runtime_state.CURRENT_SESSIONS.get(room_id) == session_id
                    and runtime_state.LAST_STATUS.get(room_id, 0) == 1):
                update_concurrency_cache(room_id, session_id, count)
            await asyncio.sleep(0.1)
        await asyncio.sleep(15)


def load_cached_room_uids() -> None:
    cached_uids = room_uid_cache.load()
    cached_rooms = [room_id for room_id in room_config.get_room_ids() if room_id in cached_uids]
    for room_id in cached_rooms:
        runtime_state.ROOM_UIDS.setdefault(room_id, cached_uids[room_id])
    logger.info("[RoomUID] 已从本地缓存读取 %s 个房间的 UID", len(cached_rooms))


async def init_uids_and_attention_once(max_rounds: int = 5) -> None:
    for _ in range(max_rounds):
        missing = [room_id for room_id in room_config.get_room_ids() if room_id not in runtime_state.ROOM_UIDS]
        if not missing:
            return
        for room_id in missing:
            await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=True)
            await asyncio.sleep(0.3)


async def init_uid_and_attention_for_room(room_id: int, max_rounds: int = 3) -> None:
    if room_id in runtime_state.ROOM_UIDS:
        return
    cached_uid = room_uid_cache.load().get(room_id)
    if cached_uid is not None:
        runtime_state.ROOM_UIDS[room_id] = cached_uid
        await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=False)
        return
    for _ in range(max_rounds):
        if room_id in runtime_state.ROOM_UIDS:
            return
        await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=True)
        await asyncio.sleep(0.3)


async def bili_ticket_scheduler() -> None:
    while runtime_state.aiohttp_session is None:
        await asyncio.sleep(1)
    try:
        await bilibili_gateway.ensure_bili_ticket(force=True)
    except Exception as exc:
        logger.exception("[bili_ticket] 首次获取失败: %s", type(exc).__name__, exc_info=False)
    while True:
        target = _next_daily_target(_now(), 5, 0)
        await asyncio.sleep(max(60.0, (target - _now()).total_seconds()))
        try:
            await bilibili_gateway.ensure_bili_ticket(force=True)
        except Exception as exc:
            logger.exception("[bili_ticket] 定时刷新失败: %s", type(exc).__name__, exc_info=False)


async def reconnect_scheduler() -> None:
    while True:
        target = _next_daily_target(_now(), 6, 0)
        await asyncio.sleep(max(1.0, (target - _now()).total_seconds()))
        for room_id in room_config.get_room_ids():
            if room_id in runtime_state.ROOM_CLIENTS and runtime_state.LAST_STATUS.get(room_id, 0) != 1:
                try:
                    await reconnect_one(room_id)
                except Exception as exc:
                    logger.exception("[reconnect] 房间 %s 重连失败: %s", room_id, type(exc).__name__, exc_info=False)
                await asyncio.sleep(3 + random.uniform(0.5, 2.0))


def _apply_room_status(room_id: int, info: dict, now: datetime.datetime) -> bool:
    """Finish the lifecycle transition before an initial WebSocket can receive events."""
    from . import room_lifecycle

    locked_until = runtime_state.LOCKED_ROOM_UNTIL.get(room_id)
    if locked_until is not None:
        if now.timestamp() < locked_until:
            return True
        runtime_state.LOCKED_ROOM_UNTIL.pop(room_id, None)
    status = int(info["live_status"])
    if status == 2:
        status = 0
    previous = runtime_state.LAST_STATUS.get(room_id, 0)
    if status == 1 and previous != 1:
        start_raw = info.get("live_time", 0)
        try:
            start = datetime.datetime.fromtimestamp(int(start_raw)).astimezone().replace(tzinfo=None) if str(start_raw).isdigit() and int(start_raw) > 0 else now
        except (ValueError, OSError, OverflowError):
            start = now
        forced_end = runtime_state.FORCED_OFFLINE_AT.get(room_id)
        if forced_end is not None and start <= forced_end:
            return True
        open_session = None
        if room_id not in runtime_state.PENDING_SESSION_ENDS or room_id not in runtime_state.CURRENT_SESSIONS:
            open_session = LiveSession.find_open_session(room_id, strict=True)
            if open_session is not None:
                restore_session(open_session[0], room_id, open_session[1])
                runtime_state.CURRENT_SESSIONS[room_id] = open_session[0]
                room_lifecycle.finish_expired_live_sessions(now, lifecycle_dependencies())
        forced_end = runtime_state.FORCED_OFFLINE_AT.get(room_id)
        if forced_end is not None and start <= forced_end:
            return True
        runtime_state.FORCED_OFFLINE_AT.pop(room_id, None)
        session_id = room_lifecycle.resume_interrupted_session(room_id, start, now)
        if session_id is None:
            if open_session and runtime_state.CURRENT_SESSIONS.get(room_id) == open_session[0]:
                runtime_state.STREAM_STARTS.setdefault(room_id, open_session[1])
                # Refresh the live display without replacing the original start snapshot.
                runtime_state.GUARD_FANS_QUEUE.put_nowait((room_id, None, None))
            else:
                session_id = LiveSession.start_session(room_id, start, info.get("title") or "")
                if not session_id:
                    return False
                runtime_state.STREAM_STARTS[room_id] = start
                runtime_state.CURRENT_SESSIONS[room_id] = session_id
                init_concurrency_cache(room_id, session_id, start)
                runtime_state.GUARD_FANS_QUEUE.put_nowait((room_id, session_id, "start"))
                runtime_state.ATTENTION_QUEUE.put_nowait((room_id, session_id, "start", now.date()))
        runtime_state.LIVE_INFO.setdefault(room_id, {}).update({"live_time": start.strftime("%Y-%m-%d %H:%M:%S"), "title": info.get("title") or ""})
    elif status != 1:
        if previous == 1:
            room_lifecycle.defer_live_session_finish(room_id, now, lifecycle_dependencies())
        runtime_state.LIVE_INFO.setdefault(room_id, {}).update({"live_time": "0000-00-00 00:00:00", "title": ""})
    runtime_state.LAST_STATUS[room_id] = status
    return True


async def monitor_all_rooms_status(startup: StartupRooms | None = None) -> None:
    from . import room_lifecycle

    while True:
        if startup is not None and startup.finished:
            startup = None
        if runtime_state.aiohttp_session is not None:
            try:
                room_lifecycle.finish_expired_live_sessions(_now(), lifecycle_dependencies())
            except Exception:
                logger.exception("[LiveStatus] 结算待结束场次失败")
            polled_uids = set(runtime_state.ROOM_UIDS.values()) if startup is not None else set()
            async for batch in status_batches():
                configured = set(room_config.get_room_ids())
                now = _now()
                for room, (uid, info) in batch.items():
                    if room not in configured or runtime_state.ROOM_UIDS.get(room) != uid:
                        continue
                    newly_eligible = startup is not None and (
                        startup.observed.pop(room, None) != uid or runtime_state.LAST_STATUS.get(room) != 1
                    )
                    try:
                        # An omitted UID can connect as a non-live candidate. It
                        # never causes a fabricated offline or online transition.
                        if info is not None and not _apply_room_status(room, info, now):
                            continue
                    except Exception as exc:
                        logger.exception("[LiveStatus] 房间状态处理失败 room_id=%s error=%s", room, type(exc).__name__, exc_info=False)
                        continue
                    if startup is not None:
                        startup.observed[room] = uid
                        if newly_eligible and runtime_state.LAST_STATUS.get(room) == 1 and room not in runtime_state.ROOM_CLIENTS:
                            # The next room can require synchronous DB recovery. Give
                            # this live room its connection slot before doing that work.
                            startup.changed.set()
                            await asyncio.sleep(0)
                            configured = set(room_config.get_room_ids())
                if startup is not None:
                    startup.changed.set()
            if startup is not None:
                startup.checked_uids = polled_uids
                startup.changed.set()
        await asyncio.sleep(3)


def _record_stream_segment(room_id: int, end_dt: datetime.datetime) -> str | None:
    start = runtime_state.STREAM_STARTS.get(room_id)
    session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
    if session_id is not None and runtime_state.INVALID_DURATION_SESSIONS.get(room_id) == session_id:
        LiveSession.invalidate_duration(session_id)
    if session_id is not None:
        LiveSession.record_duration(session_id, start, end_dt)
    if start is None:
        return None
    runtime_state.STREAM_STARTS.pop(room_id, None)
    flush_pending_danmaku_for_room(room_id, runtime_state.CURRENT_SESSIONS.get(room_id), end_dt)
    total = int((end_dt - start).total_seconds())
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def lifecycle_dependencies():
    from . import room_lifecycle
    return room_lifecycle.LifecycleDependencies(_record_stream_segment, flush_pending_danmaku_for_room, finalize_concurrency_cache, _now, init_uid_and_attention_for_room, start_client, capture_end_snapshot)


room_lock_events.configure(lifecycle_dependencies)
