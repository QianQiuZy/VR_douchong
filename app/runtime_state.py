"""Canonical mutable runtime state and queue singletons."""

import asyncio
import datetime
import os
import threading
from pathlib import Path
from typing import Optional

import aiohttp
from . import blivedm


DEFAULT_ROOMS_JSON_PATH = Path(__file__).resolve().parents[1] / "rooms.json"
ROOMS_JSON_PATH = os.getenv("ROOMS_JSON_PATH", str(DEFAULT_ROOMS_JSON_PATH))
ROOM_CONFIG_LOCK = threading.Lock()
ROOM_IDS: list[int] = []
ROOM_ANCHORS: dict[int, str] = {}
ROOM_UIDS: dict[int, int] = {}

aiohttp_session: Optional[aiohttp.ClientSession] = None
CURRENT_SESSIONS: dict[int, int] = {}
ROOM_CLIENTS: dict[int, blivedm.BLiveClient] = {}
LAST_RECONNECT: dict[int, datetime.datetime] = {}
RECONNECT_DAILY_STATE = {"date": None, "done": set()}

LAST_STATUS: dict[int, int] = {}
STREAM_STARTS: dict[int, datetime.datetime] = {}
LIVE_INFO: dict[int, dict[str, str]] = {}
PENDING_SESSION_ENDS: dict[int, datetime.datetime] = {}
DANMAKU_PENDING: dict[int, int] = {}
FANS_COUNT: dict[int, int] = {}
GUARD_COUNTS: dict[int, dict[str, int]] = {}
CONCURRENCY_CACHE: dict[int, dict[str, int]] = {}
LOCKED_ROOM_UNTIL: dict[int, int] = {}

GUARD_FANS_QUEUE: asyncio.Queue[tuple[int, Optional[int], Optional[str]]] = asyncio.Queue()
ATTENTION_QUEUE: asyncio.Queue[tuple[int, Optional[int], Optional[str], datetime.date]] = asyncio.Queue()
DAILY_ATTENTION_QUEUE: asyncio.Queue[tuple[int, datetime.date]] = asyncio.Queue()
DAILY_GUARD_QUEUE: asyncio.Queue[tuple[int, datetime.date]] = asyncio.Queue()
DAILY_FANS_QUEUE: asyncio.Queue[tuple[int, datetime.date]] = asyncio.Queue()

MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None
