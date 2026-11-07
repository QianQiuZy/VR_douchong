"""Process/event-loop scoped admission for danmaku configuration requests only."""

import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger('blivedm')


def get_request_policy():
    loop = asyncio.get_event_loop()
    policy = getattr(loop, '_blivedm_danmaku_request_policy', None)
    if policy is None:
        policy = DanmakuRequestPolicy()
        loop._blivedm_danmaku_request_policy = policy
    return policy


class DanmakuRequestPolicy:
    MIN_INTERVAL = 3.0
    COOLDOWN = 60.0
    FAILURE_WINDOW = 30.0
    FAILURE_ROOMS = 3

    def __init__(self):
        self._lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._open_until = 0.0
        self._failures = {}

    def _now(self):
        return asyncio.get_running_loop().time()

    async def _sleep(self, seconds):
        await asyncio.sleep(seconds)

    @asynccontextmanager
    async def request(self):
        # Hold admission through the response: one half-open probe, no stale outcomes.
        async with self._lock:
            while True:
                delay = max(self._next_request_at, self._open_until) - self._now()
                if delay <= 0:
                    break
                await self._sleep(delay)
            if self._open_until:
                logger.info('getDanmuInfo circuit half-open, probing one request')
            yield

    def started(self):
        # Called immediately before HTTP, after any WBI refresh has completed.
        self._next_request_at = self._now() + self.MIN_INTERVAL

    def success(self):
        if self._open_until:
            logger.info('getDanmuInfo circuit closed, resuming paced requests')
        self._open_until = 0.0
        self._failures.clear()

    def rejected(self, room_id, refreshed=False, gaia=False):
        now = self._now()
        self._failures = {room: at for room, at in self._failures.items()
                          if now - at <= self.FAILURE_WINDOW}
        self._failures[room_id] = now
        if self._open_until or refreshed or gaia or len(self._failures) >= self.FAILURE_ROOMS:
            self.failed_probe()

    def failed_probe(self):
        self._open_until = self._now() + self.COOLDOWN
        logger.warning('getDanmuInfo circuit open, cooldown=%d seconds', self.COOLDOWN)

    def other_failure(self):
        if self._open_until:
            self.failed_probe()
