"""Real local HTTP/WS paths and rolling, process-local history semantics."""

import asyncio
import json

import aiohttp
import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from app import bilibili_gateway, health_events, runtime_state
from app.health_events import HealthEvents, HealthTrackingMiddleware
from app.live_client import AuthenticatedLiveClient


def test_window_expires_and_recent_sample_cap_does_not_truncate_counts():
    clock = [10.0]
    events = HealthEvents(clock=lambda: clock[0], wall_clock=lambda: 1700000000 + clock[0])
    for _ in range(250):
        events.record("room_disconnect", "websocket", room_id=42)
    snapshot = events.snapshot()
    assert snapshot["totals"]["room_disconnect"] == 250
    assert len(snapshot["recent_events"]) == 200
    assert snapshot["full_window"] is False
    clock[0] += 3599
    assert events.snapshot()["totals"]["room_disconnect"] == 250
    clock[0] += 1
    snapshot = events.snapshot()
    assert snapshot["full_window"] is True
    assert snapshot["totals"]["room_disconnect"] == 0
    assert snapshot["recent_events"] == []


def test_local_api_counts_cache_errors_429_and_incomplete_response_once():
    app = FastAPI()

    @app.get("/gift")
    def response(status: int):
        return JSONResponse({}, status_code=status)

    @app.get("/gift/sc")
    def broken():
        raise RuntimeError("synthetic-sensitive-detail")

    app.add_middleware(HealthTrackingMiddleware)
    with TestClient(app, raise_server_exceptions=False) as client:
        for status in (200, 400, 401, 404, 429, 503):
            assert client.get("/gift", params={"status": status}).status_code == status
        assert client.get("/gift/sc").status_code == 500
        client.get("/health/check")
        client.get("/unrecognized-sensitive-path")
    result = health_events.events.snapshot()
    assert result["totals"]["local_api"] == 2
    assert result["totals"]["rate_limited"] == 1
    assert "synthetic-sensitive-detail" not in json.dumps(result)
    assert "unrecognized-sensitive-path" not in json.dumps(result)


def test_client_auth_disconnect_and_planned_stop_are_separate(monkeypatch):
    from app.blivedm.clients.ws_base import HeaderTuple, Operation

    async def scenario():
        bilibili_gateway.init_session()
        async with runtime_state.aiohttp_session as session:
            client = AuthenticatedLiveClient(42, session=session)
            client._room_id = 42

            async def noop():
                pass

            monkeypatch.setattr(client, "_send_auth", noop)
            monkeypatch.setattr(client, "_send_heartbeat", noop)
            assert client.health_snapshot()["status"] == "stopped"
            await client._on_ws_close()  # An initialization failure is not a disconnect.
            assert health_events.events.snapshot()["totals"]["room_disconnect"] == 0
            class Socket:
                closed = False

                async def send_bytes(self, packet):
                    pass

            client._websocket = Socket()
            await client._on_ws_connect()
            assert client.health_snapshot()["status"] == "authenticating"
            body = b'{"code":0}'
            await client._parse_business_message(
                HeaderTuple(16 + len(body), 16, 1, Operation.AUTH_REPLY, 1), body,
            )
            assert client.health_snapshot()["status"] == "connected"
            await client._on_ws_close()
            await client._on_ws_close()  # Retry callbacks must not double count.
            assert client.health_snapshot()["authenticated"] is False
            await client._on_ws_connect()
            client.stop()
            await client._on_ws_close()
            result = health_events.events.snapshot()
            assert result["totals"]["room_disconnect"] == 1
            assert result["totals"]["room_planned_close"] == 1

    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    monkeypatch.setattr(bilibili_gateway, "BILI_COOKIES_BASE", {})
    anyio.run(scenario)


def test_real_websocket_close_updates_room_state_and_history(monkeypatch):
    from app.blivedm.clients.ws_base import Operation

    async def scenario():
        drop = asyncio.Event()
        async def websocket(request):
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            await socket.send_bytes(AuthenticatedLiveClient._make_packet({"code": 0}, Operation.AUTH_REPLY))
            await drop.wait()
            await socket.close()
            return socket

        app = web.Application()
        app.router.add_get("/ws", websocket)
        async with TestServer(app) as server:
            bilibili_gateway.init_session()
            async with runtime_state.aiohttp_session as session:
                client = AuthenticatedLiveClient(42, session=session)
                client._room_id = 42
                async def initialized(retry):
                    pass
                async def send_auth():
                    pass
                monkeypatch.setattr(client, "_on_before_ws_connect", initialized)
                monkeypatch.setattr(client, "_send_auth", send_auth)
                monkeypatch.setattr(client, "_get_ws_url", lambda retry: str(server.make_url("/ws")))
                client.set_reconnect_policy(lambda retry, total: 3600)
                client.start()
                try:
                    async with asyncio.timeout(2):
                        await client.wait_connected()
                        assert client.health_snapshot()["status"] == "connected"
                        assert client.health_snapshot()["last_message_at"] is not None
                        drop.set()
                        while not health_events.events.snapshot()["totals"]["room_disconnect"]:
                            await asyncio.sleep(0.001)
                    snapshot = client.health_snapshot()
                    assert snapshot["status"] == "connecting"
                    assert snapshot["last_disconnected_at"] is not None
                    assert snapshot["authenticated"] is False
                finally:
                    drop.set()
                    await client.stop_and_close()

    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    monkeypatch.setattr(bilibili_gateway, "BILI_COOKIES_BASE", {})
    anyio.run(scenario)


def test_failed_websocket_handshake_is_cleared_when_next_handshake_succeeds(monkeypatch):
    async def scenario():
        calls = []
        async def handshake(request):
            calls.append(True)
            if len(calls) == 1:
                return web.Response(status=503)
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            async for _ in socket:
                pass
            return socket
        app = web.Application()
        app.router.add_get("/ws", handshake)
        async with TestServer(app) as server:
            bilibili_gateway.init_session()
            async with runtime_state.aiohttp_session as session:
                with pytest.raises(aiohttp.WSServerHandshakeError):
                    await session.ws_connect(server.make_url("/ws"))
                assert health_events.events.snapshot()["upstream_endpoints"]["websocket"]["last_result"] == "failed"
                async with session.ws_connect(server.make_url("/ws")):
                    snapshot = health_events.events.snapshot()
                    assert snapshot["upstream_endpoints"]["websocket"]["last_result"] == "ok"
                    assert snapshot["totals"]["upstream_api"] == 1
    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    monkeypatch.setattr(bilibili_gateway, "BILI_COOKIES_BASE", {})
    anyio.run(scenario)


@pytest.mark.parametrize("mode", ["ok", "http", "business", "json", "body_timeout", "queue_timeout"])
def test_upstream_observation_includes_body_business_errors_and_queue_time(monkeypatch, mode):
    async def handler(request):
        if mode == "http":
            return web.Response(status=503)
        if mode == "business":
            return web.json_response({"code": -101, "message": "synthetic-secret"})
        if mode == "json":
            return web.Response(text="synthetic-secret")
        if mode == "body_timeout":
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(request)
            await asyncio.sleep(0.2)
            return response
        return web.json_response({"code": 0})

    async def scenario():
        app = web.Application()
        app.router.add_get("/room/v1/Room/get_info", handler)
        async with TestServer(app) as server:
            bilibili_gateway.init_session()
            async with runtime_state.aiohttp_session as session:
                socket = None
                if mode == "queue_timeout":
                    # A local long-lived response acquires the only slot.
                    session.connector._limit = 1
                    async def hold(request):
                        response = web.StreamResponse()
                        await response.prepare(request)
                        await asyncio.sleep(0.2)
                        return response
                    hold_app = web.Application()
                    hold_app.router.add_get("/hold", hold)
                    async with TestServer(hold_app) as hold_server:
                        socket = await session.get(hold_server.make_url("/hold"))
                        try:
                            with pytest.raises(TimeoutError):
                                await session.get(server.make_url("/room/v1/Room/get_info"), timeout=aiohttp.ClientTimeout(total=0.05))
                        finally:
                            socket.close()
                else:
                    try:
                        async with session.get(
                            server.make_url("/room/v1/Room/get_info"),
                            params={"room_id": 42, "token": "synthetic-secret"},
                            timeout=aiohttp.ClientTimeout(total=0.05 if mode == "body_timeout" else 2),
                        ) as response:
                            await response.json(content_type=None)
                    except (ValueError, TimeoutError):
                        assert mode in {"http", "json", "body_timeout"}
        result = health_events.events.snapshot()
        assert result["totals"]["upstream_api"] == int(mode != "ok")
        assert "synthetic-secret" not in json.dumps(result)
        if mode == "queue_timeout":
            assert result["recent_events"][0]["queue_ms"] >= 40
        if mode == "business":
            assert result["recent_events"][0]["business_code"] == -101

    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    monkeypatch.setattr(bilibili_gateway, "BILI_COOKIES_BASE", {})
    anyio.run(scenario)
