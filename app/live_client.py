import asyncio
import time
from contextlib import contextmanager
from typing import Self

import aiohttp

from . import health_events
from .blivedm import BLiveClient
from .blivedm.clients.ws_base import HeaderTuple, InitError, Operation
from .ws_live_status import RoomStatusFeed


class AuthenticatedLiveClient(BLiveClient):
    """Expose first successful danmaku authentication, not merely task creation."""

    def __init__(self, room_id: int, *, session: aiohttp.ClientSession | None = None) -> None:
        super().__init__(room_id, session=session)
        self.status_feed = RoomStatusFeed()
        self._first_connection = asyncio.Event()
        self._authenticated = False
        self._health_transport_open = False
        self._health_authenticated = False
        self._health_stop_requested = False
        self._health_connected_at = None
        self._health_disconnected_at = None
        self._health_disconnected_since = None
        self._health_last_message_at = None
        self._daily_reconnect_until: float | None = None

    @contextmanager
    def daily_reconnect(self, timeout: float):
        self._daily_reconnect_until = time.monotonic() + timeout
        try:
            yield
        finally:
            self._daily_reconnect_until = None

    def inherit_daily_reconnect(self, previous: Self) -> None:
        # Carry the last disconnect across clients, but start with no active outage.
        self._daily_reconnect_until = previous._daily_reconnect_until
        self._health_disconnected_at = previous._health_disconnected_at
        self.status_feed = previous.status_feed
        self.status_feed.reset_connection()

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
        self._health_disconnected_since = None
        await super()._on_ws_connect()

    async def _on_ws_close(self) -> None:
        self.status_feed.reset_connection()
        planned_close = self._health_stop_requested and self._daily_reconnect_until is not None
        if not planned_close:
            # The first replacement attempt failed, possibly before a socket opened.
            self._daily_reconnect_until = None
        if self._health_transport_open:
            if not planned_close:
                self._health_disconnected_at = self._health_disconnected_since = time.time()
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
        if self._daily_reconnect_until is not None and time.monotonic() < self._daily_reconnect_until:
            status = "daily-reconnecting"
        elif open_socket and self._health_authenticated:
            status = "connected"
        elif open_socket:
            status = "authenticating"
        else:
            status = "connecting" if self.is_running else "stopped"
        disconnected_for = None
        if not open_socket and status != "daily-reconnecting" and self._health_disconnected_since is not None:
            disconnected_for = round(max(0, time.time() - self._health_disconnected_since), 3)
        return {
            "status": status, "running": self.is_running,
            "authenticated": self.status_authenticated,
            "connected_at": health_events.timestamp(self._health_connected_at),
            "last_disconnected_at": health_events.timestamp(self._health_disconnected_at),
            "last_message_at": health_events.timestamp(self._health_last_message_at),
            "disconnected_for_seconds": disconnected_for,
        }

    @property
    def status_authenticated(self) -> bool:
        return self._health_authenticated and self._websocket is not None and not self._websocket.closed

    async def wait_connected(self) -> None:
        await self._first_connection.wait()
        if not self._authenticated:
            raise InitError("Client stopped before its first authentication")

    async def _parse_business_message(self, header: HeaderTuple, body: bytes) -> None:
        await super()._parse_business_message(header, body)
        if header.operation == Operation.AUTH_REPLY:
            self.status_feed.reset_connection()
            self._daily_reconnect_until = None
            self._authenticated = True
            self._health_authenticated = True
            self._health_connected_at = time.time()
            self._first_connection.set()

    async def _network_coroutine_wrapper(self) -> None:
        try:
            await super()._network_coroutine_wrapper()
        finally:
            self._first_connection.set()
