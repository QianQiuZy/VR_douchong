from types import SimpleNamespace

from app import event_ingestion


def test_gift_and_common_notice_convert_coins_to_rmb(monkeypatch):
    # Given: a live-room client and repository methods that record their arguments.
    calls = []
    client = SimpleNamespace(room_id=100)
    monkeypatch.setattr(event_ingestion.runtime_state, "CURRENT_SESSIONS", {100: 9})
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
    monkeypatch.setattr(event_ingestion.RoomLiveStats, "add_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(event_ingestion, "register_payer", lambda *args, **kwargs: None)

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
