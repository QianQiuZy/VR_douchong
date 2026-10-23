"""Exercise the installed HTTP Brotli backend without Bilibili connections."""

import json

import anyio
import brotli
from aiohttp import web
from aiohttp.test_utils import TestServer

from app import bilibili_gateway, runtime_state
from app.blivedm.clients import web as live_web
from app.live_client import AuthenticatedLiveClient


def test_shared_session_initializes_danmaku_from_brotli_response(monkeypatch):
    monkeypatch.setattr(bilibili_gateway, "BILI_COOKIES_BASE", {})
    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    payload = {
        "code": 0,
        "data": {
            "host_list": [{"host": "chat.example.invalid", "wss_port": 443}],
            "token": "synthetic-test-token",
        },
    }
    body = brotli.compress(json.dumps(payload).encode())

    async def respond(request):
        assert "br" in request.headers["Accept-Encoding"].split(", ")
        return web.Response(
            body=body, content_type="application/json", headers={"Content-Encoding": "br"}
        )

    async def scenario():
        application = web.Application()
        application.router.add_get("/danmu", respond)
        async with TestServer(application) as server:
            monkeypatch.setattr(
                live_web, "DANMAKU_SERVER_CONF_URL", str(server.make_url("/danmu"))
            )
            bilibili_gateway.init_session()
            async with runtime_state.aiohttp_session as session:
                client = AuthenticatedLiveClient(30103697, session=session)
                client._room_id = 30103697
                client._wbi_signer._wbi_key = "synthetic-wbi-key"
                assert await client._init_host_server()
                assert client._host_server_list == payload["data"]["host_list"]
                assert client._host_server_token == payload["data"]["token"]

    anyio.run(scenario)
