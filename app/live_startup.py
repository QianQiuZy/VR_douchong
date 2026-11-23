"""Schedule first connections from the same status observations used by the collector."""

import asyncio
import datetime
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine

import aiohttp

from . import bilibili_gateway, room_config, runtime_state

logger = logging.getLogger(__name__)


async def status_batches() -> AsyncIterator[dict[int, tuple[int, dict | None]]]:
    """Yield successful batches immediately. A failed batch is retried on the next poll."""
    rooms = {room: runtime_state.ROOM_UIDS[room] for room in room_config.get_room_ids()
             if room in runtime_state.ROOM_UIDS}
    uids = list(dict.fromkeys(rooms.values()))
    for offset in range(0, len(uids), 100):
        batch = uids[offset:offset + 100]
        try:
            async with runtime_state.aiohttp_session.get(
                bilibili_gateway.LIVE_STATUS_API,
                params=[("uids[]", str(uid)) for uid in batch],
                headers={"User-Agent": bilibili_gateway.USER_AGENT, "Referer": "https://live.bilibili.com"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP {response.status}")
                payload = await response.json(content_type=None)
            if not isinstance(payload, dict) or payload.get("code", 0) != 0 or not isinstance(payload.get("data"), dict):
                raise ValueError("invalid live status response")
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            logger.warning("[LiveStatus] 本批状态暂不可用 uids=%s error=%s", len(batch), type(exc).__name__)
            continue
        requested = set(batch)
        yield {room: (uid, payload["data"].get(str(uid))) for room, uid in rooms.items() if uid in requested}


def reported_start(info: dict, now: datetime.datetime) -> datetime.datetime | None:
    raw = info.get("live_time")
    try:
        if str(raw).isdigit() and int(raw) > 0:
            start = datetime.datetime.fromtimestamp(int(raw))
        else:
            # get_info supplies a Shanghai wall-clock string; the batch endpoint
            # normally supplies epoch seconds.
            start = datetime.datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S")
            start = start.replace(tzinfo=runtime_state.SHANGHAI).astimezone().replace(tzinfo=None)
        return start if 946684800 <= start.timestamp() and start <= now else None
    except (ValueError, OverflowError, OSError):
        return None


class LiveIdentityChecks:
    """Use a separate room detail request only for ambiguous live responses."""

    def __init__(self):
        self.candidates = {}
        self.next_lookup = {}
        self.slots = asyncio.Semaphore(4)

    def needs_lookup(self, room, uid, info, now):
        key = (room, uid)
        start = reported_start(info, now)
        live = int(info["live_status"]) == 1
        previous = runtime_state.LIVE_INFO.get(room, {}).get("live_time")
        changed = (runtime_state.LAST_STATUS.get(room) == 1 and start is not None and previous
                   and previous != start.strftime("%Y-%m-%d %H:%M:%S"))
        if not live or (start is not None and not changed):
            self.candidates.pop(key, None)
            self.next_lookup.pop(key, None)
            return False
        if changed and self.candidates.get(key) == start:
            return False  # Two consecutive batch observations agree.
        self.candidates[key] = start
        return True

    async def lookup(self, room, uid, info):
        key = (room, uid)
        clock = asyncio.get_running_loop().time()
        if self.next_lookup.get(key, 0) > clock or self.slots.locked():
            return None
        self.next_lookup[key] = clock + 30
        async with self.slots:
            detail = await bilibili_gateway._fetch_room_info(room)
        if not detail or str(detail.get("uid")) != str(uid):
            return None
        if str(room) not in (str(detail.get("room_id")), str(detail.get("short_id"))):
            return None
        status = str(detail.get("live_status"))
        if info is None and status in {"0", "2"}:
            return {"live_status": 0}
        if status != "1":
            return None
        now = datetime.datetime.now()
        confirmed = reported_start(detail, now)
        if confirmed is None:
            return None
        return {**(info or {}), "live_status": 1, "live_time": int(confirmed.timestamp()),
                "title": detail.get("title") or (info or {}).get("title", "")}


class StartupRooms:
    def __init__(self) -> None:
        self.observed: dict[int, int] = {}
        self.changed = asyncio.Event()
        self.finished = False
        self.checked_uids: set[int] = set()
        self.uid_sweep_done = False

    async def connect(
        self, start_client: Callable[[int], Awaitable[None]],
        supervise: Callable[[int, asyncio.Lock], Coroutine], auth_timeout: float,
    ) -> None:
        pending = dict.fromkeys(room_config.get_room_ids())
        runtime_state.ROOMS_AWAITING_FIRST_AUTH.update(pending)
        lock = asyncio.Lock()
        loop = asyncio.get_running_loop()
        deadlines: dict[int, float] = {}
        try:
            async with asyncio.TaskGroup() as tasks:
                while True:
                    async with lock:
                        self.changed.clear()
                        configured = set(room_config.get_room_ids())
                        for room in pending.keys() - configured:
                            del pending[room]
                            runtime_state.ROOMS_AWAITING_FIRST_AUTH.discard(room)
                        now = loop.time()
                        deadlines = {room: until for room, until in deadlines.items()
                                     if room in configured and room in runtime_state.ROOMS_AWAITING_FIRST_AUTH and until > now}
                        eligible = [room for room in pending if room in self.observed
                                    and self.observed[room] == runtime_state.ROOM_UIDS.get(room)]
                        room = next((room for room in eligible if runtime_state.LAST_STATUS.get(room) == 1), None)
                        if room is None and self.checked_uids and eligible and not any(
                            runtime_state.LAST_STATUS.get(r) == 1 for r in deadlines
                        ):
                            room = eligible[0]
                        if room is not None:
                            if room not in runtime_state.ROOM_CLIENTS:
                                await start_client(room)
                            del pending[room]
                            task = tasks.create_task(supervise(room, lock))
                            task.add_done_callback(lambda _: self.changed.set())
                            deadlines[room] = loop.time() + auth_timeout
                            await asyncio.sleep(3)
                            continue
                    deferred = self.uid_sweep_done and self.observed and all(
                        room not in runtime_state.ROOM_UIDS or runtime_state.ROOM_UIDS[room] in self.checked_uids
                        for room in pending
                    )
                    if not deadlines and not eligible and (not pending or deferred):
                        if runtime_state.STARTUP_ATTENTION_READY is not None:
                            runtime_state.STARTUP_ATTENTION_READY.set()
                        if not pending:
                            self.finished = True
                            return
                    timeout = min(deadlines.values()) - now if deadlines else None
                    try:
                        await asyncio.wait_for(self.changed.wait(), timeout=timeout)
                    except TimeoutError:
                        pass
        finally:
            runtime_state.ROOMS_AWAITING_FIRST_AUTH.difference_update(pending)
