"""Schedule first connections from the same status observations used by the collector."""

import asyncio
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
