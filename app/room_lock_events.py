"""Websocket forced-offline events and session lifecycle handoff."""

import datetime
import logging
from collections.abc import Callable, Mapping

from . import blivedm, room_lifecycle, runtime_state
from .blivedm.clients.ws_base import WebSocketClientBase
from .models import LiveSession

logger = logging.getLogger(__name__)
_lifecycle_dependencies: Callable[[], room_lifecycle.LifecycleDependencies] | None = None


def configure(dependencies: Callable[[], room_lifecycle.LifecycleDependencies]) -> None:
    global _lifecycle_dependencies
    _lifecycle_dependencies = dependencies


def _invalidate_duration(room_id: int, session_id: int, event_time: datetime.datetime) -> bool:
    previous = runtime_state.INVALID_DURATION_SESSIONS.get(room_id)
    runtime_state.INVALID_DURATION_SESSIONS[room_id] = session_id
    if LiveSession.invalidate_duration(session_id, event_time) is False:
        if previous is None:
            runtime_state.INVALID_DURATION_SESSIONS.pop(room_id, None)
        else:
            runtime_state.INVALID_DURATION_SESSIONS[room_id] = previous
        return False
    return True


def handle_warning(
    _handler: blivedm.BaseHandler,
    client: WebSocketClientBase,
    command: Mapping[str, str | int | bool],
) -> None:
    try:
        room_id = int(command["roomid"])
        event_time = datetime.datetime.fromtimestamp(
            int(command["send_time"]) // 1000, tz=datetime.timezone.utc
        ).astimezone().replace(tzinfo=None)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        logger.warning("[LiveWarning] invalid event room_id=%s", client.room_id)
        return
    if room_id != client.room_id:
        return
    pending_end = runtime_state.PENDING_SESSION_ENDS.get(room_id)
    if pending_end is not None and event_time > pending_end:
        return
    start_time = runtime_state.STREAM_STARTS.get(room_id)
    if start_time is not None and start_time > event_time:
        return
    session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
    if session_id is None:
        open_session = LiveSession.find_open_session(room_id)
        if open_session is None:
            return
        session_id, start_time = open_session
        if start_time > event_time:
            return
        runtime_state.CURRENT_SESSIONS[room_id] = session_id
        runtime_state.STREAM_STARTS.setdefault(room_id, start_time)
    if not _invalidate_duration(room_id, session_id, event_time):
        return
    logger.info("[LiveWarning] broadcast duration invalid room_id=%s session_id=%s", room_id, session_id)


def handle_room_lock(
    _handler: blivedm.BaseHandler,
    client: WebSocketClientBase,
    command: Mapping[str, str | int | bool],
) -> None:
    try:
        room_id = int(command["room_id"])
        end_time = datetime.datetime.fromtimestamp(
            int(command["send_time"]) // 1000, tz=datetime.timezone.utc
        ).astimezone().replace(tzinfo=None)
        expires = datetime.datetime.fromisoformat(str(command["expire"])).replace(
            tzinfo=datetime.timezone(datetime.timedelta(hours=8))
        ).astimezone().replace(tzinfo=None)
        if room_id != client.room_id or expires <= end_time:
            return
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        logger.warning("[RoomLock] invalid event room_id=%s", client.room_id)
        return
    start_time = runtime_state.STREAM_STARTS.get(room_id)
    if start_time is not None and start_time > end_time:
        return
    session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
    if session_id is None:
        open_session = LiveSession.find_open_session(room_id)
        if open_session is not None:
            session_id, start_time = open_session
            if start_time > end_time:
                return
            runtime_state.CURRENT_SESSIONS[room_id] = session_id
            runtime_state.STREAM_STARTS.setdefault(room_id, start_time)
    if session_id is not None:
        if _lifecycle_dependencies is None:
            raise RuntimeError("room lock lifecycle dependencies are not configured")
        if not _invalidate_duration(room_id, session_id, end_time):
            return
    runtime_state.LOCKED_ROOM_UNTIL[room_id] = int(expires.timestamp())
    runtime_state.FORCED_OFFLINE_AT[room_id] = end_time
    runtime_state.LAST_STATUS[room_id] = 0
    runtime_state.LIVE_INFO.setdefault(room_id, {}).update(
        {"live_time": "0000-00-00 00:00:00", "title": ""}
    )
    if session_id is not None:
        room_lifecycle.defer_live_session_finish(room_id, end_time, _lifecycle_dependencies())
    logger.info("[RoomLock] forced offline room_id=%s session_id=%s end_time=%s expires=%s", room_id, session_id, end_time, expires)


def handle_cut_off(
    _handler: blivedm.BaseHandler,
    client: WebSocketClientBase,
    command: Mapping[str, str | int | bool],
) -> None:
    try:
        room_id = int(command["room_id"])
        end_time = datetime.datetime.fromtimestamp(
            int(command["send_time"]) // 1000, tz=datetime.timezone.utc
        ).astimezone().replace(tzinfo=None)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        logger.warning("[CutOff] invalid event room_id=%s", client.room_id)
        return
    if room_id != client.room_id:
        return
    pending_end = runtime_state.PENDING_SESSION_ENDS.get(room_id)
    if pending_end is not None and end_time > pending_end:
        return
    start_time = runtime_state.STREAM_STARTS.get(room_id)
    if start_time is not None and start_time > end_time:
        return
    if _lifecycle_dependencies is None:
        raise RuntimeError("room lock lifecycle dependencies are not configured")
    dependencies = _lifecycle_dependencies()
    session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
    if session_id is None:
        open_session = LiveSession.find_open_session(room_id)
        if open_session is not None:
            session_id, start_time = open_session
            if start_time > end_time:
                return
            runtime_state.CURRENT_SESSIONS[room_id] = session_id
            runtime_state.STREAM_STARTS.setdefault(room_id, start_time)
    if session_id is not None and not _invalidate_duration(room_id, session_id, end_time):
        return
    runtime_state.FORCED_OFFLINE_AT[room_id] = end_time
    runtime_state.LAST_STATUS[room_id] = 0
    runtime_state.LIVE_INFO.setdefault(room_id, {}).update(
        {"live_time": "0000-00-00 00:00:00", "title": ""}
    )
    if session_id is not None:
        room_lifecycle.defer_live_session_finish(room_id, end_time, dependencies)
    logger.info("[CutOff] offline with grace room_id=%s session_id=%s end_time=%s", room_id, session_id, end_time)
