import asyncio
from types import SimpleNamespace

import pytest

from app import bilibili_gateway, event_ingestion, monitoring_jobs, runtime_state


class _FailedResponse:
    status = 500

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _FailedSession:
    def get(self, *args, **kwargs):
        return _FailedResponse()


def test_extracted_handler_replays_gift_guard_sc_notice_and_danmaku(monkeypatch):
    calls = []
    client = SimpleNamespace(room_id=301)
    runtime_state.CURRENT_SESSIONS[301] = 44
    runtime_state.LAST_STATUS[301] = 1
    monkeypatch.setattr(event_ingestion.RoomStatsMonthly, "add_amounts", lambda *args, **kwargs: calls.append(("monthly", kwargs)))
    monkeypatch.setattr(event_ingestion.LiveSession, "add_values_by_id", lambda *args, **kwargs: calls.append(("session", kwargs)))
    monkeypatch.setattr(event_ingestion.SuperChatLog, "log_sc", lambda *args, **kwargs: calls.append(("sc", kwargs)))
    handler = event_ingestion.MyHandler()
    handler.__getattribute__("_on_gift")(client, SimpleNamespace(total_price=1000, total_coin=1000, gift_name="gift", num=1, uname="u", uid=1))
    handler.__getattribute__("_on_user_toast_v2")(client, SimpleNamespace(price=1000, num=1, guard_level=3, username="u", uid=1))
    handler.__getattribute__("_on_super_chat")(client, SimpleNamespace(price=30, uname="u", uid=1, message="sc", time=1_700_000_000))
    handler.__getattribute__("_on_common_notice_danmaku")(client, SimpleNamespace(content_segments=[SimpleNamespace(text="u"), SimpleNamespace(text="干杯之旅")], content_text="u 干杯之旅"))
    handler.__getattribute__("_on_danmaku")(client, SimpleNamespace(is_mirror=False))
    assert runtime_state.DANMAKU_PENDING[301] == 1
    assert [name for name, _ in calls] == ["monthly", "session", "monthly", "session", "monthly", "session", "sc", "monthly", "session"]


def test_extracted_handler_ignores_malformed_and_unknown_events():
    client = SimpleNamespace(room_id=302)
    handler = event_ingestion.MyHandler()
    handler.__getattribute__("_on_common_notice_danmaku")(client, SimpleNamespace(content_segments=[], content_text=""))
    handler.__getattribute__("_on_common_notice_danmaku")(client, SimpleNamespace(content_segments=[SimpleNamespace(text="u"), SimpleNamespace(text="unknown")], content_text="u unknown"))
    handler.__getattribute__("_on_danmaku")(SimpleNamespace(room_id=None), SimpleNamespace(is_mirror=False))


def test_gateway_returns_false_for_failed_http_response(monkeypatch):
    monkeypatch.setattr(runtime_state, "aiohttp_session", _FailedSession())
    assert asyncio.run(bilibili_gateway.fetch_room_info_and_update(303, update_uid=False)) is False


def test_guard_worker_cancellation_cleans_queue(monkeypatch):
    async def guard_counts(uid, room):
        return 1, 2, 3

    async def cancelled_sleep(seconds):
        raise asyncio.CancelledError

    runtime_state.ROOM_UIDS[304] = 99
    runtime_state.GUARD_FANS_QUEUE.put_nowait((304, None, None))
    monkeypatch.setattr(monitoring_jobs.bilibili_gateway, "fetch_guard_counts", guard_counts)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", cancelled_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.guard_fans_worker())
    assert runtime_state.GUARD_FANS_QUEUE.empty()
