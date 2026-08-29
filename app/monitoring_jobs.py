"""Live monitoring workers, schedulers, client control, and caches.

# noqa: SIZE_OK — these coupled background jobs share queue identity and timing contracts.
"""

import asyncio
import datetime
import logging
import random
from typing import Optional

import aiohttp
from sqlalchemy.exc import SQLAlchemyError

from . import bilibili_gateway, blivedm, event_ingestion, room_config, runtime_state
from .config import ATTENTION_DAILY_ROOM_SLEEP_SECONDS
from .database import Session
from .metrics_runtime import record_concurrency, record_danmaku, start_session
from .models import Attention, LiveSession, RoomInfo, RoomLiveStats


def _now() -> datetime.datetime:
    return datetime.datetime.now()


def _next_daily_target(now: datetime.datetime, hour: int, minute: int) -> datetime.datetime:
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target + datetime.timedelta(days=1) if now >= target else target


async def _sleep_until(target: datetime.datetime) -> None:
    while True:
        remaining = (target - _now()).total_seconds()
        if remaining <= 0:
            return
        await asyncio.sleep(remaining)


async def start_client(room_id: int) -> None:
    """Start and register one BLive client using the canonical handler."""
    client = blivedm.BLiveClient(room_id, session=runtime_state.aiohttp_session)
    client.set_handler(event_ingestion.MyHandler())
    client.start()
    runtime_state.ROOM_CLIENTS[room_id] = client
    runtime_state.LAST_RECONNECT.setdefault(room_id, _now() - datetime.timedelta(days=random.random() * 3.0))
    logging.info("[connect] 已连接房间 %s", room_id)


async def reconnect_one(room_id: int) -> None:
    if runtime_state.LAST_STATUS.get(room_id, 0) == 1:
        logging.info("[reconnect] 房间 %s 已在播，跳过重连", room_id)
        return
    client = runtime_state.ROOM_CLIENTS.get(room_id)
    if client is not None:
        try:
            await client.stop_and_close()
        except asyncio.CancelledError:
            logging.debug("[reconnect] room=%s stop_and_close 触发取消（预期），忽略", room_id)
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.warning("[reconnect] stop_and_close 异常 room=%s: %s", room_id, exc)
    await asyncio.sleep(3)
    await start_client(room_id)
    runtime_state.LAST_RECONNECT[room_id] = _now()
    logging.info("[reconnect] 房间 %s 重连完成", room_id)


async def run_clients_loop() -> None:
    for room_id in room_config.get_room_ids():
        await start_client(room_id)
        await asyncio.sleep(3)


def init_concurrency_cache(
    room_id: int,
    session_id: int,
    start_time: Optional[datetime.datetime] = None,
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


def finalize_concurrency_cache(room_id: int, session_id: Optional[int]) -> tuple[Optional[float], Optional[int]]:
    cache = runtime_state.CONCURRENCY_CACHE.get(room_id)
    if not cache or session_id is None or cache.get("session_id") != session_id:
        return None, None
    samples = int(cache.get("samples", 0))
    return ((int(cache.get("total", 0)) / samples) if samples else None, int(cache.get("max", 0)) if samples else None)


def flush_pending_danmaku_for_room(
    room_id: int,
    session_id: Optional[int] = None,
    event_time: Optional[datetime.datetime] = None,
) -> None:
    pending = int(runtime_state.DANMAKU_PENDING.pop(room_id, 0) or 0)
    if pending <= 0:
        return
    if session_id:
        record_danmaku(session_id, event_time or _now(), pending)
        LiveSession.add_danmaku_by_id(session_id, pending)
    else:
        LiveSession.add_danmaku_by_room_open(room_id, pending)
    logging.debug("[Danmaku] room_id=%s 下播/停用即时落库 +%s", room_id, pending)


def _read_room_attention(room_id: int) -> int:
    session = Session()
    try:
        return int(session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar() or 0)
    except SQLAlchemyError as exc:
        logging.error("[Attention] 读取 room_id=%s 当前粉丝数失败: %s", room_id, exc)
        return 0
    finally:
        session.close()


async def attention_worker() -> None:
    while True:
        room_id, session_id, phase, target_date = await runtime_state.ATTENTION_QUEUE.get()
        try:
            await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=False)
            attention = _read_room_attention(room_id)
            if phase == "start" and session_id:
                LiveSession.update_start_attention(session_id, attention)
            elif phase == "end" and session_id:
                LiveSession.update_end_attention(session_id, attention)
            else:
                Attention.upsert_daily(room_id, target_date, attention)
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("[Attention] room_id=%s phase=%s 处理失败: %s", room_id, phase, exc)
        finally:
            runtime_state.ATTENTION_QUEUE.task_done()


async def daily_attention_worker() -> None:
    while True:
        room_id, target_date = await runtime_state.DAILY_ATTENTION_QUEUE.get()
        try:
            await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=False)
            Attention.upsert_daily(room_id, target_date, _read_room_attention(room_id))
        finally:
            runtime_state.DAILY_ATTENTION_QUEUE.task_done()


async def guard_fans_worker() -> None:
    while True:
        room_id, session_id, phase = await runtime_state.GUARD_FANS_QUEUE.get()
        try:
            uid = runtime_state.ROOM_UIDS.get(room_id)
            if uid is None:
                logging.warning("[Guard/Fans] room_id=%s 未找到 uid，跳过", room_id)
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
                logging.warning("[Guard] room_id=%s 未找到 uid，跳过每日快照", room_id)
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
                logging.warning("[Fans] room_id=%s 未找到 uid，跳过每日快照", room_id)
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
        if not runtime_state.DANMAKU_PENDING:
            continue
        for room_id in tuple(runtime_state.DANMAKU_PENDING):
            flush_pending_danmaku_for_room(room_id, runtime_state.CURRENT_SESSIONS.get(room_id))


async def refresh_attention_scheduler() -> None:
    while True:
        await asyncio.sleep(3 * 3600)
        logging.info("[RoomInfo] 开始 3 小时粉丝数刷新任务")
        for room_id in room_config.get_room_ids():
            await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=False)
            await asyncio.sleep(0.3)
        logging.info("[RoomInfo] 本轮粉丝数刷新任务完成")


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
        logging.info("[Guard/Fans] 等待 UID 初始化...")
        await asyncio.sleep(1)
    while True:
        for room_id in room_config.get_room_ids():
            if runtime_state.LAST_STATUS.get(room_id, 0) != 1:
                await runtime_state.GUARD_FANS_QUEUE.put((room_id, None, None))
                await asyncio.sleep(0.1)
        await asyncio.sleep(3600)


async def concurrency_poll_scheduler() -> None:
    while not runtime_state.ROOM_UIDS:
        logging.info("[Concurrency] 等待 UID 初始化...")
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
            if count is not None:
                update_concurrency_cache(room_id, session_id, count)
            await asyncio.sleep(0.1)
        await asyncio.sleep(15)


async def init_uids_and_attention_once(max_rounds: int = 5) -> None:
    for _ in range(max_rounds):
        missing = [room_id for room_id in room_config.get_room_ids() if room_id not in runtime_state.ROOM_UIDS]
        if not missing:
            return
        for room_id in missing:
            await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=True)
            await asyncio.sleep(0.3)


async def init_uid_and_attention_for_room(room_id: int, max_rounds: int = 3) -> None:
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
    except Exception as exc:  # noqa: BROAD_EXCEPT_OK
        logging.error("[bili_ticket] 首次获取失败: %s", exc)
    while True:
        target = _next_daily_target(_now(), 5, 0)
        await asyncio.sleep(max(60.0, (target - _now()).total_seconds()))
        try:
            await bilibili_gateway.ensure_bili_ticket(force=True)
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("[bili_ticket] 定时刷新失败: %s", exc)


async def reconnect_scheduler() -> None:
    while True:
        target = _next_daily_target(_now(), 6, 0)
        await asyncio.sleep(max(1.0, (target - _now()).total_seconds()))
        for room_id in room_config.get_room_ids():
            if runtime_state.LAST_STATUS.get(room_id, 0) != 1:
                try:
                    await reconnect_one(room_id)
                except asyncio.CancelledError:
                    logging.debug("[reconnect] room=%s 被取消（预期），略过", room_id)
                except Exception as exc:  # noqa: BROAD_EXCEPT_OK
                    logging.error("[reconnect] 房间 %s 重连失败: %s", room_id, exc)
                await asyncio.sleep(3 + random.uniform(0.5, 2.0))


async def monitor_all_rooms_status() -> None:
    """Poll live state while preserving the three-minute lifecycle grace flow."""
    from . import room_lifecycle
    while not runtime_state.ROOM_UIDS:
        await asyncio.sleep(1)
    while True:
        session = runtime_state.aiohttp_session
        if session is None:
            await asyncio.sleep(3)
            continue
        try:
            async with session.get(bilibili_gateway.LIVE_STATUS_API, params=[("uids[]", str(uid)) for uid in runtime_state.ROOM_UIDS.values()], timeout=aiohttp.ClientTimeout(total=10), headers={"User-Agent": bilibili_gateway.USER_AGENT, "Referer": "https://live.bilibili.com"}) as response:
                if response.status != 200:
                    await asyncio.sleep(3)
                    continue
                payload = await response.json(content_type=None)
            now = _now()
            room_lifecycle.finish_expired_live_sessions(now, lifecycle_dependencies())
            data = payload.get("data") or {}
            for room_id in room_config.get_room_ids():
                uid = runtime_state.ROOM_UIDS.get(room_id)
                info = data.get(str(uid)) if uid is not None else None
                previous = runtime_state.LAST_STATUS.get(room_id, 0)
                if not info or "live_status" not in info:
                    continue
                status = 0 if int(info.get("live_status", 0)) == 2 else int(info.get("live_status", 0))
                runtime_state.LAST_STATUS[room_id] = status
                if status == 1 and previous == 0:
                    start_raw = info.get("live_time", 0)
                    try:
                        start = datetime.datetime.fromtimestamp(int(start_raw)) if str(start_raw).isdigit() else now
                    except (ValueError, OSError, OverflowError):
                        start = now
                    session_id = room_lifecycle.resume_interrupted_session(room_id, start, now)
                    if session_id is None:
                        runtime_state.STREAM_STARTS[room_id] = start
                        session_id = LiveSession.start_session(room_id, start, info.get("title") or "")
                        if session_id:
                            runtime_state.CURRENT_SESSIONS[room_id] = session_id
                            init_concurrency_cache(room_id, session_id, start)
                            runtime_state.GUARD_FANS_QUEUE.put_nowait((room_id, session_id, "start"))
                            runtime_state.ATTENTION_QUEUE.put_nowait((room_id, session_id, "start", now.date()))
                    runtime_state.LIVE_INFO.setdefault(room_id, {}).update({"live_time": start.strftime("%Y-%m-%d %H:%M:%S"), "title": info.get("title") or ""})
                elif status != 1:
                    if previous == 1:
                        room_lifecycle.defer_live_session_finish(room_id, now, lifecycle_dependencies())
                    runtime_state.LIVE_INFO.setdefault(room_id, {}).update({"live_time": "0000-00-00 00:00:00", "title": ""})
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("[LiveStatus] get_status_info_by_uids 调用异常: %s", exc)
        await asyncio.sleep(3)


def _record_stream_segment(room_id: int, end_dt: datetime.datetime) -> Optional[str]:
    start = runtime_state.STREAM_STARTS.pop(room_id, None)
    if start is None:
        return None
    current = start
    while current < end_dt:
        boundary = end_dt if current.date() == end_dt.date() else datetime.datetime.combine(current.date() + datetime.timedelta(days=1), datetime.time.min)
        seconds = int((min(end_dt, boundary) - current).total_seconds())
        if seconds > 0:
            RoomLiveStats.add_duration(room_id, current.date(), seconds)
        current = min(end_dt, boundary)
    flush_pending_danmaku_for_room(room_id, runtime_state.CURRENT_SESSIONS.get(room_id), end_dt)
    total = int((end_dt - start).total_seconds())
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def lifecycle_dependencies():
    from . import room_lifecycle
    return room_lifecycle.LifecycleDependencies(_record_stream_segment, flush_pending_danmaku_for_room, finalize_concurrency_cache, _now, init_uid_and_attention_for_room, start_client)
