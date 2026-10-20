"""Regression for the 2026-10-21 cotton-candy blind-box award."""

import base64
import datetime
from types import SimpleNamespace

import pytest

from app import event_ingestion as ingestion
from app.blivedm.models.web import SendGiftV2Message


def _varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def _integer(number, value):
    return _varint(number << 3) + _varint(value)


def _blob(number, value):
    value = value.encode() if isinstance(value, str) else value
    return _varint((number << 3) | 2) + _varint(len(value)) + value


def _packet(num, price, total_coin, blind_price):
    # Encode independently of the production protobuf models.
    blind = _integer(2, 32251) + _blob(3, "心动盲盒") + _integer(6, blind_price)
    gift = b"".join([
        _integer(1, 32126), _blob(2, "棉花糖"), _integer(3, num),
        _integer(5, price), _integer(6, total_coin), _integer(7, 15000),
        _blob(8, "gold"), _blob(9, "4830033891890976256"),
        _integer(10, 1792514248),
    ])
    payload = _integer(1, 50269896) + _blob(2, "viewer") + _blob(9, blind) + _blob(10, gift)
    return {"pb": base64.b64encode(payload).decode()}


@pytest.mark.parametrize(
    "num,price,total_coin,blind_price,cost,profit",
    [
        (1, 9000, 9000, 15000, 15000, -60),
        (1, 15000, 15000, 15000, 15000, 0),
        (2, 9000, 9000, 15000, 30000, -120),
        (1, 9000, 15000, 0, 15000, -60),
        (1, 9000, 0, 0, None, 0),
    ],
)
@pytest.mark.parametrize("session_id", [44, None])
def test_blind_box_writes_cost_based_profit_without_changing_revenue(
    monkeypatch, caplog, num, price, total_coin, blind_price, cost, profit, session_id,
):
    raw = _packet(num, price, total_coin, blind_price)
    message, = SendGiftV2Message.from_command(raw)
    assert message.is_blind_box is True
    assert message.blind_cost_coin == cost
    assert (message.total_price, message.total_coin) == (price * num, total_coin)

    calls = []

    def capture(name):
        def record(*args, **kwargs):
            calls.append((name, args, kwargs))
        return record

    room_id = 1820703922
    monkeypatch.setattr(ingestion.runtime_state, "CURRENT_SESSIONS", {room_id: session_id})
    for target, attribute, name in [
        (ingestion.RoomStatsMonthly, "add_amounts", "monthly"),
        (ingestion.RoomBlindBoxMonthly, "add_amounts", "blind_monthly"),
        (ingestion.LiveSession, "add_values_by_id", "session"),
        (ingestion.LiveSession, "add_values_by_room_open", "open_session"),
        (ingestion.RoomLiveStats, "add_metrics", "daily"),
        (ingestion, "record_payment", "bucket"),
        (ingestion, "record_whale_revenue", "whale"),
        (ingestion, "register_payer", "payer"),
    ]:
        monkeypatch.setattr(target, attribute, capture(name))
    monkeypatch.setattr(ingestion.MyHandler, "_resolve_session", lambda *args: session_id)

    ingestion.MyHandler().handle(
        SimpleNamespace(room_id=room_id), {"cmd": "SEND_GIFT_V2", "data": raw},
    )

    value = price * num / 1000
    when = datetime.datetime(2026, 10, 21, 0, 37, 28)
    assert ("monthly", (room_id, "202610"), {"gift": value}) in calls
    assert ("blind_monthly", (room_id, "202610"), {"count": num, "profit": profit}) in calls
    session_target = "session" if session_id else "open_session"
    session_key = session_id or room_id
    assert (session_target, (session_key,), {"gift": value}) in calls
    assert (session_target, (session_key,), {"blind_box_count": num, "blind_box_profit": profit}) in calls
    assert ("whale", (room_id, "202610", 50269896, price * num,
                      "gift:4830033891890976256", "gift"), {}) in calls
    daily, = [call for call in calls if call[0] == "daily"]
    assert daily[1] == (room_id, when.date())
    assert daily[2]["gift"] == value
    if session_id:
        assert ("bucket", (session_id, when), {"gift": value, "payer_added": False}) in calls
        assert ("bucket", (session_id, when), {"blind_box_count": num, "blind_box_profit": profit}) in calls
    else:
        assert not any(call[0] == "bucket" for call in calls)
    if cost is None:
        assert "盲盒成本未知" in caplog.text
