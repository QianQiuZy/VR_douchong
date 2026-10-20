"""Real National Day blind package: 2026-10-03 XML, uid=4941873, ts=851.436.

The recorder emits two XML gift rows with the same raw. The fixture contains
that raw once: two awards, five boxes, 35 RMB gift value, 45 RMB cost, -10 profit.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from app import event_ingestion as ingestion
from app.blivedm.models.web import SendGiftV2Message


@pytest.fixture
def blind_package_raw():
    return json.loads(
        (Path(__file__).parent / "fixtures" / "send_gift_v2_blind_package.json").read_text(
            encoding="utf-8",
        ),
    )


def test_real_blind_package_preserves_both_awards(blind_package_raw):
    messages = SendGiftV2Message.from_command(blind_package_raw)
    assert [(m.gift_id, m.gift_name, m.num) for m in messages] == [
        (35926, "故邑呈笺", 2), (35925, "渡口衔风", 3),
    ]
    assert all(m.uid == 4941873 and m.is_blind_box is True for m in messages)
    assert [m.total_price for m in messages] == [20000, 15000]
    assert [m.total_coin for m in messages] == [10000, 5000]
    assert [m.blind_cost_coin for m in messages] == [18000, 27000]
    assert len({m.tid for m in messages}) == 2
    assert sum(m.num for m in messages) == 5
    assert sum(m.total_price for m in messages) == 35000
    assert sum(m.total_price - m.blind_cost_coin for m in messages) == -10000


@pytest.mark.parametrize("session_id", [44, None])
def test_real_blind_package_persists_positive_and_negative_profit(
    blind_package_raw, monkeypatch, session_id,
):
    room_id = 1820703922
    monkeypatch.setattr(ingestion.runtime_state, "CURRENT_SESSIONS", {room_id: session_id})
    monkeypatch.setattr(ingestion.MyHandler, "_resolve_session", lambda *args: session_id)
    monthly = Mock()
    blind_monthly = Mock()
    session = Mock()
    daily = Mock()
    bucket = Mock()
    whale = Mock()
    monkeypatch.setattr(ingestion.RoomStatsMonthly, "add_amounts", monthly)
    monkeypatch.setattr(ingestion.RoomBlindBoxMonthly, "add_amounts", blind_monthly)
    session_method = "add_values_by_id" if session_id else "add_values_by_room_open"
    monkeypatch.setattr(ingestion.LiveSession, session_method, session)
    monkeypatch.setattr(ingestion.RoomLiveStats, "add_metrics", daily)
    monkeypatch.setattr(ingestion, "record_payment", bucket)
    monkeypatch.setattr(ingestion, "record_whale_revenue", whale)
    monkeypatch.setattr(ingestion, "register_payer", Mock(return_value=None))

    ingestion.MyHandler().handle(
        SimpleNamespace(room_id=room_id), {"cmd": "SEND_GIFT_V2", "data": blind_package_raw},
    )

    assert monthly.call_args_list == [
        call(room_id, "202610", gift=value) for value in (20.0, 15.0)
    ]
    assert blind_monthly.call_args_list == [
        call(room_id, "202610", count=2, profit=20),
        call(room_id, "202610", count=3, profit=-120),
    ]
    session_key = session_id or room_id
    assert session.call_args_list == [
        item for value, count, profit in ((20.0, 2, 20), (15.0, 3, -120))
        for item in (
            call(session_key, gift=value),
            call(session_key, blind_box_count=count, blind_box_profit=profit),
        )
    ]
    assert daily.call_count == whale.call_count == 2
    assert sum(c.kwargs["gift"] for c in daily.call_args_list) == 35.0
    assert sum(c.args[3] for c in whale.call_args_list) == 35000
    assert len({c.args[4] for c in whale.call_args_list}) == 2
    if session_id:
        assert bucket.call_count == 4
        assert sum(c.kwargs.get("gift", 0) for c in bucket.call_args_list) == 35.0
        assert sum(c.kwargs.get("blind_box_count", 0) for c in bucket.call_args_list) == 5
        assert [c.kwargs["blind_box_profit"] for c in bucket.call_args_list
                if "blind_box_profit" in c.kwargs] == [20, -120]
    else:
        bucket.assert_not_called()
