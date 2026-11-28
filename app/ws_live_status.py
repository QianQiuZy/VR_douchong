"""Normal broadcast transitions from an authenticated room socket."""

import datetime
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import NamedTuple

from . import runtime_state
from .live_startup import reported_start

logger = logging.getLogger(__name__)


class BroadcastEvent(NamedTuple):
    live: bool
    stamp: datetime.datetime | None
    key: str
    received: datetime.datetime
    connection: int


@dataclass(eq=False)
class RoomStatusFeed:
    revision: int = 0
    connection: int = 0
    synchronized: bool = False
    live_key: str = ""
    last_end: datetime.datetime | None = None
    pending: deque[BroadcastEvent] = field(default_factory=deque)

    def reset_connection(self) -> None:
        # A transition may have happened during the gap. Invalidate in-flight HTTP too.
        self.revision += 1
        self.connection += 1
        self.synchronized = False


def poll_token(room):
    client = runtime_state.ROOM_CLIENTS.get(room)
    feed = getattr(client, "status_feed", None)
    return client, feed.revision if feed is not None else 0


def owns_status(client) -> bool:
    return getattr(client, "status_authenticated", False) and client.status_feed.synchronized


def receive(_handler, client, command):
    from . import monitoring_jobs

    room = getattr(client, "tmp_room_id", client.room_id)
    if runtime_state.ROOM_CLIENTS.get(room) is not client or not client.status_authenticated:
        return
    if str(command.get("roomid")) not in (str(room), str(client.room_id)):
        return
    now = monitoring_jobs._now()
    live = command["cmd"].split(":", 1)[0] == "LIVE"
    if live:
        stamp = reported_start(command, now)
    else:
        try:
            stamp = datetime.datetime.fromtimestamp(int(command["send_time"]) / 1000)
            if stamp.year < 2000 or stamp > now:
                stamp = now
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            stamp = now
    feed = client.status_feed
    feed.revision += 1
    feed.pending.append(BroadcastEvent(live, stamp, command.get("live_key") or "", now, feed.connection))
    drain(room, feed)


def drain(room: int, feed: RoomStatusFeed) -> None:
    """Apply before later gift frames, retry failed writes in order on the next poll."""
    while feed.pending:
        try:
            if not _apply_event(room, feed, feed.pending[0]):
                return
        except Exception as exc:
            logger.error("[LiveStatus] WS 场次处理失败 room_id=%s error=%s", room, type(exc).__name__)
            return
        feed.pending.popleft()


def _apply_event(room: int, feed: RoomStatusFeed, event: BroadcastEvent) -> bool:
    from .monitoring_jobs import _apply_room_status, _record_stream_segment

    live, stamp, key, received, connection = event
    if runtime_state.LOCKED_ROOM_UNTIL.get(room, 0) > received.timestamp():
        return True
    segment = runtime_state.STREAM_STARTS.get(room)
    active = runtime_state.LAST_STATUS.get(room) == 1
    same_key = not key or not feed.live_key or key == feed.live_key
    if live:
        ended = runtime_state.FORCED_OFFLINE_AT.get(room)
        if ended and (stamp is None or stamp <= ended):
            return True
        if stamp is not None:
            display = runtime_state.LIVE_INFO.get(room, {}).get("live_time", "")
            if ((feed.last_end and stamp <= feed.last_end) or (segment and stamp < segment)
                    or (active and segment and stamp.strftime("%Y-%m-%d %H:%M:%S") < display)):
                return True
            # A different broadcast must obtain its own title from the batch endpoint.
            same = stamp == segment or (active and not segment and same_key)
            title = runtime_state.LIVE_INFO.get(room, {}).get("title", "") if same else ""
            info = {"live_status": 1, "live_time": int(stamp.timestamp()), "title": title}
            if not _apply_room_status(room, info, received):
                return False
        elif not active and key and key == feed.live_key and feed.last_end:
            return True
        elif not active or not same_key:
            # The push-stream variant proves live status but has no broadcast start.
            # Stop crediting a superseded segment at the last trustworthy observation.
            runtime_state.SESSION_ATTRIBUTION_PAUSED.add(room)
            sid = runtime_state.CURRENT_SESSIONS.get(room)
            if active and segment and sid is not None:
                last_seen = max(segment, runtime_state.LAST_LIVE_OBSERVATIONS.get(sid, segment))
                _record_stream_segment(room, last_seen)
                runtime_state.PENDING_SESSION_ENDS[room] = last_seen
            runtime_state.LAST_STATUS[room] = 1
            runtime_state.LIVE_INFO[room] = {"live_time": "0000-00-00 00:00:00", "title": ""}
        elif segment and key and key == feed.live_key:
            # A matching WS broadcast identity supersedes a pending HTTP verification.
            runtime_state.SESSION_ATTRIBUTION_PAUSED.discard(room)
        if key:
            feed.live_key = key
    else:
        if (segment and stamp < segment) or (feed.last_end and stamp <= feed.last_end):
            return True
        if not _apply_room_status(room, {"live_status": 0}, stamp):
            return False
        if active:
            feed.last_end = stamp
        elif feed.last_end is None:
            feed.last_end = (runtime_state.PENDING_SESSION_ENDS.get(room)
                             or runtime_state.FORCED_OFFLINE_AT.get(room) or stamp)
    if connection == feed.connection:
        feed.synchronized = True
    return True
