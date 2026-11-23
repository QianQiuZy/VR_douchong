import asyncio

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from app import health_check, health_events, monitoring_jobs, runtime_state
from app.blivedm.clients.ws_base import Operation
from app.live_client import AuthenticatedLiveClient


def test_scheduled_handoff_preserves_last_real_disconnect_until_next_drop(monkeypatch):
    async def scenario():
        sockets = asyncio.Queue()

        async def websocket(request):
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            await socket.receive()  # Wait for the client's AUTH packet.
            sockets.put_nowait(socket)
            async for _ in socket:
                pass
            return socket

        async def init_room(client):
            client._room_id = 42
            return True

        async def assert_planned(last_disconnect):
            snapshot = await health_check.collector_snapshot()
            assert snapshot["status"] == "ok"
            rooms = snapshot["rooms"]
            assert rooms["daily_reconnecting"] == rooms["not_connected"] == 1
            assert rooms["connected"] == 0
            room = rooms["items"][0]
            assert room["status"] == "daily-reconnecting"
            assert room["last_disconnected_at"] == last_disconnect
            assert room["disconnected_for_seconds"] is None

        app = web.Application()
        app.router.add_get("/ws", websocket)
        async with TestServer(app) as server, aiohttp.ClientSession() as session:
            monkeypatch.setattr(runtime_state, "aiohttp_session", session)
            monkeypatch.setattr(runtime_state, "ROOM_IDS", [42])
            monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {})
            monkeypatch.setattr(runtime_state, "LAST_STATUS", {})
            monkeypatch.setattr(runtime_state, "LAST_RECONNECT", {})
            monkeypatch.setattr(AuthenticatedLiveClient, "init_room", init_room)
            monkeypatch.setattr(AuthenticatedLiveClient, "_get_ws_url", lambda *_: server.make_url("/ws"))
            auth = AuthenticatedLiveClient._make_packet({"code": 0}, Operation.AUTH_REPLY)

            await monitoring_jobs.start_client(42)
            old = runtime_state.ROOM_CLIENTS[42]
            old.set_reconnect_policy(lambda *_: 0)
            try:
                async with asyncio.timeout(10):
                    socket = await sockets.get()
                    await socket.send_bytes(auth)
                    await old.wait_connected()
                    await socket.close()
                    socket = await sockets.get()
                    await socket.send_bytes(auth)
                    while old.health_snapshot()["status"] != "connected":
                        await asyncio.sleep(0.001)
                    last_disconnect = old.health_snapshot()["last_disconnected_at"]
                    assert last_disconnect is not None

                    rotation = asyncio.create_task(monitoring_jobs.reconnect_one(42))
                    try:
                        await old.join()
                        await assert_planned(last_disconnect)  # Old client stopped in the 3 s gap.
                    finally:
                        await rotation
                    replacement = runtime_state.ROOM_CLIENTS[42]
                    assert replacement is not old
                    await assert_planned(last_disconnect)
                    socket = await sockets.get()
                    await assert_planned(last_disconnect)  # Replacement still awaiting AUTH_REPLY.
                    with monkeypatch.context() as mixed:
                        mixed.setattr(runtime_state, "ROOM_IDS", [42, 43])
                        assert (await health_check.collector_snapshot())["status"] == "degraded"
                    assert health_events.events.snapshot()["totals"]["room_planned_close"] == 1
                    assert health_events.events.snapshot()["totals"]["room_disconnect"] == 1

                    await socket.send_bytes(auth)
                    await replacement.wait_connected()
                    assert replacement.health_snapshot()["status"] == "connected"
                    assert replacement.health_snapshot()["last_disconnected_at"] == last_disconnect
                    await socket.close()
                    while replacement.health_snapshot()["disconnected_for_seconds"] is None:
                        await asyncio.sleep(0.001)
                    assert replacement.health_snapshot()["last_disconnected_at"] != last_disconnect
                    assert (await health_check.collector_snapshot())["status"] == "degraded"
                    assert health_events.events.snapshot()["totals"]["room_disconnect"] == 2
            finally:
                await runtime_state.ROOM_CLIENTS[42].stop_and_close()

    asyncio.run(scenario())


def test_failed_and_overdue_first_attempts_leave_daily_reconnect(monkeypatch):
    async def scenario():
        async with aiohttp.ClientSession() as session:
            old = AuthenticatedLiveClient(42, session=session)
            replacement = AuthenticatedLiveClient(42, session=session)
            with old.daily_reconnect(60):
                replacement.inherit_daily_reconnect(old)
            assert replacement.health_snapshot()["status"] == "daily-reconnecting"
            assert replacement.health_snapshot()["last_disconnected_at"] is None

            async def fail_init():
                return False

            monkeypatch.setattr(replacement, "init_room", fail_init)
            replacement.start()
            await replacement.join()
            assert replacement.health_snapshot()["status"] == "stopped"
            assert replacement.health_snapshot()["last_disconnected_at"] is None
            assert health_events.events.snapshot()["totals"]["room_disconnect"] == 0

            with old.daily_reconnect(0):
                replacement.inherit_daily_reconnect(old)
            replacement.start()
            assert replacement.health_snapshot()["status"] == "connecting"
            await replacement.join()

    asyncio.run(scenario())
