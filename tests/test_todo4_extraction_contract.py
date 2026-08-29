import asyncio
import importlib
from types import SimpleNamespace

import pytest

event_ingestion = importlib.import_module("app.event_ingestion")
monitoring_jobs = importlib.import_module("app.monitoring_jobs")


def test_my_handler_records_gift_and_common_notice_with_existing_repository_calls(monkeypatch):
    # Given: a live-room client and repository methods that record their arguments.
    calls = []
    client = SimpleNamespace(room_id=100)
    event_ingestion.runtime_state.CURRENT_SESSIONS[100] = 9
    monkeypatch.setattr(
        event_ingestion.RoomStatsMonthly,
        "add_amounts",
        lambda room_id, month, **values: calls.append(("monthly", room_id, values)),
    )
    monkeypatch.setattr(
        event_ingestion.LiveSession,
        "add_values_by_id",
        lambda session_id, **values: calls.append(("session", session_id, values)),
    )

    # When: the handler replays a normal gift then a mapped common-notice gift.
    handler = event_ingestion.MyHandler()
    handler._on_gift(
        client,
        SimpleNamespace(
            total_price=2000,
            total_coin=2000,
            gift_name="test",
            num=2,
            uname="viewer",
            uid=1,
        ),
    )
    handler._on_common_notice_danmaku(
        client,
        SimpleNamespace(
            content_segments=[SimpleNamespace(text="viewer"), SimpleNamespace(text="干杯之旅")],
            content_text="viewer 干杯之旅",
        ),
    )

    # Then: both paths retain their original RMB conversion and repository field names.
    assert calls == [
        ("monthly", 100, {"gift": 2.0}),
        ("session", 9, {"gift": 2.0}),
        ("monthly", 100, {"gift": 10.0}),
        ("session", 9, {"gift": 10.0}),
    ]


def test_danmaku_scheduler_propagates_cancellation_without_flushing(monkeypatch):
    # Given: a scheduler whose first scheduled sleep is cancelled.
    async def cancelled_sleep(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", cancelled_sleep)
    monitoring_jobs.runtime_state.DANMAKU_PENDING.clear()

    # When / Then: cancellation exits the background job immediately and cleanly.
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.danmaku_flush_scheduler())
