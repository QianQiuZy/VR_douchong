"""Room client and live-session lifecycle operations."""

import asyncio
import datetime
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from .models import LiveSession, RoomInfo
from . import room_config, runtime_state


LIVE_SESSION_GRACE_SECONDS = 180


@dataclass(frozen=True)
class LifecycleDependencies:
    record_stream_segment: Callable[[int, datetime.datetime], Optional[str]]
    flush_pending_danmaku: Callable[[int, Optional[int]], None]
    finalize_concurrency: Callable[[int, Optional[int]], tuple[Optional[float], Optional[int]]]
    now: Callable[[], datetime.datetime]
    initialize_room: Callable[[int], Awaitable[None]]
    start_client: Callable[[int], Awaitable[None]]


def finish_live_session(
    room_id: int,
    end_dt: datetime.datetime,
    dependencies: LifecycleDependencies,
) -> Optional[str]:
    duration_str = dependencies.record_stream_segment(room_id, end_dt)
    runtime_state.PENDING_SESSION_ENDS.pop(room_id, None)
    session_id = runtime_state.CURRENT_SESSIONS.pop(room_id, None)
    dependencies.flush_pending_danmaku(room_id, session_id)
    average, maximum = dependencies.finalize_concurrency(room_id, session_id)
    LiveSession.close_session_by_id(session_id, end_dt)
    LiveSession.update_concurrency_by_id(session_id, avg_concurrency=average, max_concurrency=maximum)
    runtime_state.CONCURRENCY_CACHE.pop(room_id, None)
    if session_id:
        runtime_state.GUARD_FANS_QUEUE.put_nowait((room_id, session_id, "end"))
        runtime_state.ATTENTION_QUEUE.put_nowait((room_id, session_id, "end", dependencies.now().date()))
    return duration_str


def defer_live_session_finish(room_id: int, end_dt: datetime.datetime, dependencies: LifecycleDependencies) -> Optional[str]:
    duration_str = dependencies.record_stream_segment(room_id, end_dt)
    if duration_str is not None:
        runtime_state.PENDING_SESSION_ENDS[room_id] = end_dt
    return duration_str


def resume_interrupted_session(room_id: int, start_dt: datetime.datetime, now: datetime.datetime) -> Optional[int]:
    interrupted_at = runtime_state.PENDING_SESSION_ENDS.get(room_id)
    if interrupted_at is None or (now - interrupted_at).total_seconds() > LIVE_SESSION_GRACE_SECONDS:
        return None
    session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
    if session_id is None:
        return None
    runtime_state.PENDING_SESSION_ENDS.pop(room_id, None)
    runtime_state.STREAM_STARTS[room_id] = start_dt if interrupted_at < start_dt <= now else now
    return session_id


def finish_expired_live_sessions(now: datetime.datetime, dependencies: LifecycleDependencies) -> None:
    expired = [
        (room_id, end_dt)
        for room_id, end_dt in runtime_state.PENDING_SESSION_ENDS.items()
        if (now - end_dt).total_seconds() > LIVE_SESSION_GRACE_SECONDS
    ]
    for room_id, end_dt in expired:
        session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
        finish_live_session(room_id, end_dt, dependencies)
        logging.info("[%s] 下播宽限期结束，已确认关闭 session_id=%s", room_id, session_id)


def ensure_room_state(room_id: int) -> None:
    runtime_state.LAST_STATUS.setdefault(room_id, 0)
    runtime_state.LIVE_INFO.setdefault(room_id, {"live_time": "0000-00-00 00:00:00", "title": ""})
    runtime_state.FANS_COUNT.setdefault(room_id, 0)
    runtime_state.GUARD_COUNTS.setdefault(room_id, {"guard_1": 0, "guard_2": 0, "guard_3": 0})


async def add_room_async(room_id: int, anchor_name: str, dependencies: LifecycleDependencies) -> tuple[bool, str]:
    if not room_config.add_room(room_id, anchor_name):
        return False, "房间已存在"
    ensure_room_state(room_id)
    RoomInfo.upsert(room_id, anchor_name=anchor_name)
    await dependencies.initialize_room(room_id)
    if runtime_state.aiohttp_session is None:
        return False, "aiohttp_session 未初始化"
    await dependencies.start_client(room_id)
    if runtime_state.LAST_STATUS.get(room_id, 0) != 1:
        runtime_state.GUARD_FANS_QUEUE.put_nowait((room_id, None, None))
    return True, "房间已添加并启动任务"


async def delete_room_async(room_id: int, dependencies: LifecycleDependencies) -> tuple[bool, str]:
    if not room_config.delete_room(room_id):
        return False, "房间不存在"
    client = runtime_state.ROOM_CLIENTS.pop(room_id, None)
    if client is not None:
        try:
            await client.stop_and_close()
        except asyncio.CancelledError:
            logging.debug("[delete] room=%s stop_and_close 触发取消（预期）", room_id)
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.warning("[delete] room=%s stop_and_close 异常: %s", room_id, exc)
    if room_id in runtime_state.STREAM_STARTS or room_id in runtime_state.PENDING_SESSION_ENDS:
        end_dt = runtime_state.PENDING_SESSION_ENDS.get(room_id) or dependencies.now()
        dependencies.record_stream_segment(room_id, end_dt)
        runtime_state.PENDING_SESSION_ENDS.pop(room_id, None)
        session_id = runtime_state.CURRENT_SESSIONS.pop(room_id, None)
        dependencies.flush_pending_danmaku(room_id, session_id)
        LiveSession.close_session_by_id(session_id, end_dt)
        average, maximum = dependencies.finalize_concurrency(room_id, session_id)
        LiveSession.update_concurrency_by_id(session_id, avg_concurrency=average, max_concurrency=maximum)
        runtime_state.CONCURRENCY_CACHE.pop(room_id, None)
    for state_map in (
        runtime_state.ROOM_UIDS,
        runtime_state.LAST_STATUS,
        runtime_state.LIVE_INFO,
        runtime_state.LAST_RECONNECT,
        runtime_state.DANMAKU_PENDING,
        runtime_state.FANS_COUNT,
        runtime_state.GUARD_COUNTS,
        runtime_state.CONCURRENCY_CACHE,
        runtime_state.LOCKED_ROOM_UNTIL,
        runtime_state.PENDING_SESSION_ENDS,
    ):
        state_map.pop(room_id, None)
    return True, "房间已删除并停止任务"
