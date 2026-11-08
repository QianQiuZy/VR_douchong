"""Guard API failures must not become zero counts or duplicate Cookie emails."""

import asyncio
from email import message_from_string

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from app import bilibili_gateway, gift, runtime_state


@pytest.mark.parametrize(
    "payload, expected, alerts_expected",
    [
        ({"code": -101, "message": "账号未登录", "ttl": 1}, None, 1),
        ({"code": -352, "message": "风控"}, None, 0),
        ({"code": -400, "message": "请求错误"}, None, 0),
        ({"message": "missing code"}, None, 0),
        ({"code": 0, "data": {"guard_num_3": "12", "guard_num_2": 2,
                               "guard_num_1": 1}}, (12, 2, 1), 0),
        ({"code": 0, "data": {"guard_num_3": 0, "guard_num_2": 0,
                               "guard_num_1": 0}}, (0, 0, 0), 0),
    ],
)
def test_guard_business_response(monkeypatch, payload, expected, alerts_expected):
    alerts = []
    monkeypatch.setattr(bilibili_gateway, "_cookie_invalid_alert", alerts.append)

    async def respond(request):
        assert request.query == {"ruid": "123", "platform": "pc"}
        return web.json_response(payload)

    async def scenario():
        application = web.Application()
        application.router.add_get("/guard", respond)
        async with TestServer(application) as server, aiohttp.ClientSession() as session:
            monkeypatch.setattr(bilibili_gateway, "GUARD_API", str(server.make_url("/guard")))
            monkeypatch.setattr(runtime_state, "aiohttp_session", session)
            assert await bilibili_gateway.fetch_guard_counts(123, 456) == expected

    asyncio.run(scenario())
    assert len(alerts) == alerts_expected
    if alerts:
        assert "code=-101" in alerts[0]
        assert "账号未登录" in alerts[0]
        assert "room_id=456" in alerts[0]


@pytest.mark.parametrize("guard_first", [True, False])
def test_guard_and_gift_share_one_email(monkeypatch, guard_first):
    messages = []
    monkeypatch.setattr(gift, "_cookie_alert_sent", False)
    # Preserve runtime wiring: this is the callback installed by app.gift.
    assert bilibili_gateway._cookie_invalid_alert is gift.send_cookie_invalid_email_async

    class InlineThread:
        def __init__(self, target, daemon):
            self.target = target

        def start(self):
            self.target()

    class SMTP:
        def __init__(self, *args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def starttls(self):
            pass

        def login(self, *args):
            pass

        def sendmail(self, sender, recipients, message):
            messages.append(message_from_string(message))

    monkeypatch.setattr(gift.threading, "Thread", InlineThread)
    monkeypatch.setattr(gift.smtplib, "SMTP", SMTP)

    async def respond(request):
        return web.json_response({"code": -101, "message": "账号未登录", "ttl": 1})

    async def scenario():
        application = web.Application()
        application.router.add_get("/guard", respond)
        async with TestServer(application) as server, aiohttp.ClientSession() as session:
            monkeypatch.setattr(bilibili_gateway, "GUARD_API", str(server.make_url("/guard")))
            monkeypatch.setattr(runtime_state, "aiohttp_session", session)
            if not guard_first:
                gift.send_cookie_invalid_email_async("[456] 礼物 uid=0")
            assert await bilibili_gateway.fetch_guard_counts(123, 456) is None
            assert await bilibili_gateway.fetch_guard_counts(123, 789) is None
            gift.send_cookie_invalid_email_async("[456] 礼物 uid=0")

    asyncio.run(scenario())
    assert gift.COOKIE_ALERT_SENT
    assert len(messages) == 1
    body = messages[0].get_payload(decode=True).decode("utf-8")
    assert "GUARD_API code=-101" in body
    assert "uid=0" in body
    assert ("room_id=456" if guard_first else "[456] 礼物 uid=0") in body
