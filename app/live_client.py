import asyncio
import time

import aiohttp

from . import health_events
from .blivedm import BLiveClient
from .blivedm.clients.ws_base import HeaderTuple, InitError, Operation


class AuthenticatedLiveClient(BLiveClient):
    """Expose first successful danmaku authentication, not merely task creation."""

    def __init__(self, room_id: int, *, session: aiohttp.ClientSession | None = None) -> None:
        super().__init__(room_id, session=session)
        self._first_connection = asyncio.Event()
        self._authenticated = False
        self._health_transport_open = False
        self._health_authenticated = False
        self._health_stop_requested = False
        self._health_connected_at = None
        self._health_disconnected_at = None
        self._health_last_message_at = None

    def start(self) -> None:
        if not self.is_running:
            self._health_stop_requested = False
        super().start()

    def stop(self) -> None:
        self._health_stop_requested = True
        super().stop()

    async def _on_ws_connect(self) -> None:
        self._health_transport_open = True
        self._health_authenticated = False
        await super()._on_ws_connect()

    async def _on_ws_close(self) -> None:
        if self._health_transport_open:
            self._health_disconnected_at = time.time()
            health_events.events.record(
                "room_planned_close" if self._health_stop_requested else "room_disconnect",
                "websocket", room_id=self.room_id,
                reason="stop_requested" if self._health_stop_requested else "transport_closed",
            )
        self._health_transport_open = False
        self._health_authenticated = False
        await super()._on_ws_close()

    async def _on_ws_message(self, message) -> None:
        self._health_last_message_at = time.time()
        await super()._on_ws_message(message)

    def health_snapshot(self) -> dict:
        socket = self._websocket
        open_socket = socket is not None and not socket.closed
        if open_socket and self._health_authenticated:
            status = "connected"
        elif open_socket:
            status = "authenticating"
        else:
            status = "connecting" if self.is_running else "stopped"
        return {
            "status": status, "running": self.is_running,
            "authenticated": bool(open_socket and self._health_authenticated),
            "connected_at": health_events.timestamp(self._health_connected_at),
            "last_disconnected_at": health_events.timestamp(self._health_disconnected_at),
            "last_message_at": health_events.timestamp(self._health_last_message_at),
            "disconnected_for_seconds": (
                round(max(0, time.time() - self._health_disconnected_at), 3)
                if not open_socket and self._health_disconnected_at is not None else None
            ),
        }

    async def wait_connected(self) -> None:
        await self._first_connection.wait()
        if not self._authenticated:
            raise InitError("Client stopped before its first authentication")

    async def _parse_business_message(self, header: HeaderTuple, body: bytes) -> None:
        await super()._parse_business_message(header, body)
        if header.operation == Operation.AUTH_REPLY:
            self._authenticated = True
            self._health_authenticated = True
            self._health_connected_at = time.time()
            self._first_connection.set()

    async def _network_coroutine_wrapper(self) -> None:
        try:
            await super()._network_coroutine_wrapper()
        finally:
            self._first_connection.set()
