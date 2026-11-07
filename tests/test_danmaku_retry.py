"""Exercise -352 recovery through HTTP initialization and WS authentication."""

import asyncio
from collections import deque

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from app.blivedm.clients import web as live_web
from app.blivedm.clients.ws_base import Operation
from app.live_client import AuthenticatedLiveClient


def virtual_admission(client):
    policy = client._danmaku_policy
    clock = [0.0]
    policy._now = lambda: clock[0]

    async def advance(delay):
        clock[0] += delay

    policy._sleep = advance
    return policy


def make_server(monkeypatch, codes, calls):
    responses = deque(codes)

    async def nav(request):
        calls.append("nav")
        return web.json_response({"data": {"wbi_img": {
            "img_url": "https://example.invalid/" + "a" * 32 + ".png",
            "sub_url": "https://example.invalid/" + "b" * 32 + ".png",
        }}})

    async def room(request):
        return web.json_response({"code": 0, "data": {"room_id": 10, "uid": 20}})

    async def danmu(request):
        calls.append("danmu")
        assert "w_rid" in request.query
        code = responses.popleft()
        if code:
            return web.json_response({"code": code, "message": str(code)})
        return web.json_response({"code": 0, "data": {
            "host_list": [{"host": "chat.example.invalid", "wss_port": 443}],
            "token": "synthetic-token",
        }})

    async def websocket(request):
        calls.append("ws")
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_bytes()
        await ws.send_bytes(AuthenticatedLiveClient._make_packet({"code": 0}, Operation.AUTH_REPLY))
        async for _ in ws:
            pass
        return ws

    app = web.Application()
    app.router.add_get("/nav", nav)
    app.router.add_get("/room", room)
    app.router.add_get("/danmu", danmu)
    app.router.add_get("/ws", websocket)
    monkeypatch.setattr(live_web.BLiveClient, "_get_buvid", lambda self: "synthetic-buvid")
    return TestServer(app)


def configure_urls(monkeypatch, server):
    monkeypatch.setattr(live_web, "WBI_INIT_URL", str(server.make_url("/nav")))
    monkeypatch.setattr(live_web, "ROOM_INIT_URL", str(server.make_url("/room")))
    monkeypatch.setattr(live_web, "DANMAKU_SERVER_CONF_URL", str(server.make_url("/danmu")))


def test_352_backoff_caps_at_48_and_recovers_to_authenticated_connection(monkeypatch):
    async def scenario():
        calls, delays = [], []
        original_sleep = asyncio.sleep

        async def sleep(delay):
            delays.append(delay)
            await original_sleep(0)

        async with make_server(monkeypatch, [-352] * 7 + [0, -352, 0], calls) as server:
            configure_urls(monkeypatch, server)
            monkeypatch.setattr(live_web.asyncio, "sleep", sleep)
            async with aiohttp.ClientSession() as session:
                client = AuthenticatedLiveClient(10, session=session)
                virtual_admission(client)
                client._uid = 0
                monkeypatch.setattr(client, "_get_ws_url", lambda retry: str(server.make_url("/ws")))
                client.start()
                try:
                    await asyncio.wait_for(client.wait_connected(), timeout=3)
                    assert client.is_running
                    assert delays == [3, 6, 12, 24, 48, 48, 48]
                    assert calls == ["nav", "danmu", "nav"] + ["danmu"] * 7 + ["ws"]
                    assert client._host_server_token == "synthetic-token"
                    # A later initialization begins a new backoff at three seconds.
                    assert await client._init_host_server()
                    assert delays[-1] == 3
                    assert calls[-3:] == ["danmu", "nav", "danmu"]
                finally:
                    await client.stop_and_close()

    asyncio.run(scenario())


@pytest.mark.parametrize("cancel_delay", [3, 48])
def test_stop_during_352_backoff_cancels_further_requests(monkeypatch, cancel_delay):
    async def scenario():
        calls, delays = [], []
        waiting = asyncio.Event()
        original_sleep = asyncio.sleep

        async def sleep(delay):
            delays.append(delay)
            if delay == cancel_delay:
                waiting.set()
                await asyncio.Event().wait()
            await original_sleep(0)

        async with make_server(monkeypatch, [-352] * 5, calls) as server:
            configure_urls(monkeypatch, server)
            monkeypatch.setattr(live_web.asyncio, "sleep", sleep)
            async with aiohttp.ClientSession() as session:
                client = AuthenticatedLiveClient(10, session=session)
                virtual_admission(client)
                client._uid = 0
                client.start()
                try:
                    await asyncio.wait_for(waiting.wait(), timeout=3)
                    assert client.is_running
                finally:
                    await asyncio.wait_for(client.stop_and_close(), timeout=3)
                calls_at_stop = list(calls)
                await original_sleep(0)
                assert calls == calls_at_stop
                assert not client.is_running
                assert "ws" not in calls
                assert delays == ([3] if cancel_delay == 3 else [3, 6, 12, 24, 48])

    asyncio.run(scenario())


def test_other_danmaku_error_keeps_existing_failure_behavior(monkeypatch):
    async def scenario():
        calls = []
        async with make_server(monkeypatch, [-101], calls) as server:
            configure_urls(monkeypatch, server)
            async with aiohttp.ClientSession() as session:
                client = AuthenticatedLiveClient(10, session=session)
                virtual_admission(client)
                client._room_id = 10
                assert not await client._init_host_server()
                assert calls == ["nav", "danmu"]

    asyncio.run(scenario())
