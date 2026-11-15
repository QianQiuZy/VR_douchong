"""Long-lived room sockets must leave shared-session HTTP requests usable."""

import asyncio
from contextlib import AsyncExitStack

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from app import bilibili_gateway, runtime_state


@pytest.mark.parametrize("room_count", [99, 100, 101, 169])
def test_http_works_after_room_websockets_exceed_default_pool_limit(monkeypatch, room_count):
    monkeypatch.setattr(bilibili_gateway, "BILI_COOKIES_BASE", {})
    monkeypatch.setattr(runtime_state, "aiohttp_session", None)

    async def websocket(request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        async for _ in socket:
            pass
        return socket

    async def status(request):
        response = web.json_response({"code": 0})
        # Force a fresh HTTP connection next time: cached keepalive connections
        # can temporarily conceal an exhausted connector, as after a restart.
        response.force_close()
        return response

    async def scenario():
        ws_app = web.Application()
        ws_app.router.add_get("/ws", websocket)
        http_app = web.Application()
        http_app.router.add_get("/status", status)
        async with AsyncExitStack() as stack:
            ws_server = await stack.enter_async_context(TestServer(ws_app))
            http_server = await stack.enter_async_context(TestServer(http_app))
            bilibili_gateway.init_session()
            session = await stack.enter_async_context(runtime_state.aiohttp_session)
            sockets = []

            async def check_http():
                async with session.get(http_server.make_url("/status")) as response:
                    assert response.status == 200
                    assert await response.json() == {"code": 0}

            try:
                # Exercise both startup and adding more rooms to a running
                # session; a capacity computed only at startup is insufficient.
                async with asyncio.timeout(10):
                    for _ in range(99):
                        sockets.append(await session.ws_connect(ws_server.make_url("/ws")))
                    await check_http()
                    for _ in range(99, room_count):
                        sockets.append(await session.ws_connect(ws_server.make_url("/ws")))
                    for _ in range(2):
                        await check_http()
            finally:
                await asyncio.gather(*(socket.close() for socket in sockets))

    anyio.run(scenario)
