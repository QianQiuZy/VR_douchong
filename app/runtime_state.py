"""Canonical mutable runtime state and queue singletons."""

from __future__ import annotations

import asyncio
import datetime
import threading
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiohttp

from . import PendingDanmaku
from . import config
if TYPE_CHECKING:
    from .live_client import AuthenticatedLiveClient

# Entry timestamps use Shanghai time explicitly, independently of the host TZ.
SHANGHAI = datetime.timezone(datetime.timedelta(hours=8), name="Asia/Shanghai")
DEFAULT_ROOMS_JSON_PATH = config.DEFAULT_CONFIG_DIR / "rooms.json"
ROOMS_JSON_PATH = str(config.ROOMS_JSON_PATH)
ROOM_CONFIG_LOCK = threading.Lock()
ROOM_IDS: list[int] = []
ROOM_ANCHORS: dict[int, str] = {}
ROOM_UIDS: dict[int, int] = {}
STARTUP_ATTENTION_READY: asyncio.Event | None = None
ROOMS_AWAITING_FIRST_AUTH: set[int] = set()


def can_refresh_attention(room_id: int) -> bool:
    return (STARTUP_ATTENTION_READY is None or STARTUP_ATTENTION_READY.is_set()) and (
        room_id not in ROOMS_AWAITING_FIRST_AUTH
    )

aiohttp_session: aiohttp.ClientSession | None = None
CURRENT_SESSIONS: dict[int, int] = {}
ROOM_CLIENTS: dict[int, AuthenticatedLiveClient] = {}
LAST_RECONNECT: dict[int, datetime.datetime] = {}
RECONNECT_DAILY_STATE = {"date": None, "done": set()}

LAST_STATUS: dict[int, int] = {}
STREAM_STARTS: dict[int, datetime.datetime] = {}
# Last observation of an active broadcast via the room socket or HTTP fallback.
LAST_LIVE_OBSERVATIONS: dict[int, datetime.datetime] = {}
LIVE_INFO: dict[int, dict[str, str]] = {}
# Open rows awaiting a first explicit status, and rooms whose events cannot
# currently be attributed to a confirmed broadcast.
RECOVERING_ROOMS: set[int] = set()
SESSION_ATTRIBUTION_PAUSED: set[int] = set()
RECOVERED_LIVE_AT: dict[int, datetime.datetime] = {}
RECENT_LIVE_AT: dict[int, deque[datetime.datetime]] = {}


def observe_live(room_id: int, session_id: int, now: datetime.datetime) -> None:
    LAST_LIVE_OBSERVATIONS[session_id] = now
    if room_id not in RECENT_LIVE_AT:
        RECENT_LIVE_AT[room_id] = deque(maxlen=64)
    RECENT_LIVE_AT[room_id].append(now)


def event_session(room_id: int) -> int | None:
    return None if room_id in SESSION_ATTRIBUTION_PAUSED else CURRENT_SESSIONS.get(room_id)


PENDING_SESSION_ENDS: dict[int, datetime.datetime] = {}


@dataclass
class SessionEndSnapshot:
    """One interruption's values, collected before its grace period expires."""

    room_id: int
    session_id: int
    guard_counts: tuple[int, int, int] | None = None
    fans_count: int | None = None
    attention: int | None = None
    confirmed: bool = False
    task: asyncio.Task[None] | None = None


SESSION_END_SNAPSHOTS: dict[int, SessionEndSnapshot] = {}
INVALID_DURATION_SESSIONS: dict[int, int] = {}
FORCED_OFFLINE_AT: dict[int, datetime.datetime] = {}
DANMAKU_RECOVERY_ROOMS: set[int] = set()
DANMAKU_PENDING: dict[int, PendingDanmaku] = {}
FANS_COUNT: dict[int, int] = {}
GUARD_COUNTS: dict[int, dict[str, int]] = {}
CONCURRENCY_CACHE: dict[int, dict[str, int]] = {}
LOCKED_ROOM_UNTIL: dict[int, int] = {}

GUARD_FANS_QUEUE: asyncio.Queue[tuple[int, int | None, str | None]] = asyncio.Queue()
ATTENTION_QUEUE: asyncio.Queue[tuple[int, int | None, str | None, datetime.date]] = asyncio.Queue()
DAILY_ATTENTION_QUEUE: asyncio.Queue[tuple[int, datetime.date]] = asyncio.Queue()
DAILY_GUARD_QUEUE: asyncio.Queue[tuple[int, datetime.date]] = asyncio.Queue()
DAILY_FANS_QUEUE: asyncio.Queue[tuple[int, datetime.date]] = asyncio.Queue()

MAIN_LOOP: asyncio.AbstractEventLoop | None = None
