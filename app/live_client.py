import asyncio

import aiohttp

from .blivedm import BLiveClient
from .blivedm.clients.ws_base import HeaderTuple, InitError, Operation


class AuthenticatedLiveClient(BLiveClient):
    """Expose first successful danmaku authentication, not merely task creation."""

    def __init__(self, room_id: int, *, session: aiohttp.ClientSession | None = None) -> None:
        super().__init__(room_id, session=session)
        self._first_connection = asyncio.Event()
        self._authenticated = False

    async def wait_connected(self) -> None:
        await self._first_connection.wait()
        if not self._authenticated:
            raise InitError("Client stopped before its first authentication")

    async def _parse_business_message(self, header: HeaderTuple, body: bytes) -> None:
        await super()._parse_business_message(header, body)
        if header.operation == Operation.AUTH_REPLY:
            self._authenticated = True
            self._first_connection.set()

    async def _network_coroutine_wrapper(self) -> None:
        try:
            await super()._network_coroutine_wrapper()
        finally:
            self._first_connection.set()
