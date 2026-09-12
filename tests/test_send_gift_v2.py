import base64
import datetime
from typing import override

import pytest

from app import event_ingestion, runtime_state
from app.blivedm.clients.ws_base import WebSocketClientBase
from app.blivedm.handlers import BaseHandler
from app.blivedm.models.web import (
    BlindGift,
    GiftData,
    GiftEffect,
    GiftMessage,
    MedalInfo,
    SendGiftV2,
    SendGiftV2Command,
    SendGiftV2Message,
)
from app.models import LiveSession, RoomBlindBoxMonthly, RoomLiveStats, RoomStatsMonthly


def _field10(*gifts: GiftData) -> str:
    payload = b"".join(
        bytes([0x52, len(gift.dumps())]) + gift.dumps()
        for gift in gifts
    )
    return base64.b64encode(payload).decode("ascii")


def test_complete_send_gift_v2_log_preserves_each_gift_record():
    data: SendGiftV2Command = {
        "pb": _field10(
            GiftData(gift_id=101, gift_name="first", num=2, total_coin=2000),
            GiftData(gift_id=202, gift_name="second", num=3, total_coin=3000),
        )
    }

    messages = SendGiftV2Message.from_command(data)

    assert [(message.gift_id, message.gift_name, message.num) for message in messages] == [
        (101, "first", 2),
        (202, "second", 3),
    ]


def test_send_gift_v2_fills_outer_and_nested_fields_for_each_record():
    proto = SendGiftV2(
        uid=7,
        uname="viewer",
        face="face-url",
        medal=MedalInfo(anchor_uid=9, medal_level=5, medal_name="medal", guard_level=3),
        blind=BlindGift(original_gift_id=1, original_gift_name="box"),
        gift=[
            GiftData(
                gift_id=101,
                gift_name="first",
                num=2,
                price=100,
                total_coin=200,
                timestamp=1_700_000_000,
                tid="transaction-1",
                effect=GiftEffect(img_basic="gift-url"),
            )
        ],
    )
    data: SendGiftV2Command = {
        "pb": base64.b64encode(proto.dumps()).decode("ascii")
    }

    message = SendGiftV2Message.from_command(data)[0]

    assert message == GiftMessage(
        gift_name="first",
        num=2,
        uname="viewer",
        face="face-url",
        guard_level=3,
        uid=7,
        timestamp=1_700_000_000,
        gift_id=101,
        gift_img_basic="gift-url",
        price=100,
        total_coin=200,
        total_price=200,
        tid="transaction-1",
        medal_level=5,
        medal_name="medal",
        medal_ruid=9,
    )


def test_send_gift_v2_handler_emits_each_decoded_gift():
    received: list[tuple[int | None, int]] = []
    client = object.__new__(WebSocketClientBase)
    client.__dict__["_room_id"] = 31368705

    class RecordingHandler(BaseHandler):
        @override
        def _on_gift(self, client: WebSocketClientBase, message: GiftMessage) -> None:
            received.append((client.room_id, message.gift_id))

    handler = RecordingHandler()
    BaseHandler.handle(
        handler,
        client,
        {
            "cmd": "SEND_GIFT_V2",
            "data": {
                "pb": _field10(
                    GiftData(gift_id=101, gift_name="first", num=2),
                    GiftData(gift_id=202, gift_name="second", num=3),
                )
            },
        },
    )

    assert received == [(31368705, 101), (31368705, 202)]


def test_send_gift_v2_handler_persists_every_blind_box_record(monkeypatch: pytest.MonkeyPatch):
    proto = SendGiftV2(
        uid=7,
        uname="viewer",
        blind=BlindGift(original_gift_id=1, original_gift_name="box"),
        gift=[
            GiftData(gift_id=101, gift_name="first", num=2, price=100, total_coin=100),
            GiftData(gift_id=202, gift_name="second", num=3, price=200, total_coin=200),
        ],
    )
    command = {
        "cmd": "SEND_GIFT_V2",
        "data": {"pb": base64.b64encode(proto.dumps()).decode("ascii")},
    }
    monthly_gifts: list[float] = []
    session_gifts: list[float] = []
    blind_box_writes: list[tuple[int, int]] = []
    payment_writes: list[tuple[str, float | int, float | int]] = []

    def current_month() -> str:
        return "202609"

    def calculate_profit(total_price: int, total_coin: int) -> int:
        return total_price - total_coin

    def ignore_cookie_alert(_message: str) -> None:
        return None

    def record_monthly(
        _room_id: int | None,
        _month: str,
        **metrics: float,
    ) -> None:
        monthly_gifts.append(metrics["gift"])

    def record_session(
        _session_id: int,
        **metrics: float,
    ) -> None:
        if "gift" in metrics:
            session_gifts.append(metrics["gift"])

    def record_blind_box(
        _room_id: int | None,
        _month: str,
        **metrics: int,
    ) -> None:
        blind_box_writes.append((metrics["count"], metrics["profit"]))

    def ignore_live_metrics(
        _room_id: int | None,
        _date: datetime.date,
        **_metrics: float | None,
    ) -> None:
        return None

    def ignore_registration(
        *_args: int | str | datetime.date | None,
        **_kwargs: bool,
    ) -> None:
        return None

    def record_runtime_payment(
        _session_id: int,
        _event_time: datetime.datetime,
        **metrics: float | bool,
    ) -> None:
        if "gift" in metrics:
            payment_writes.append(("gift", metrics["gift"], 0))
        if "blind_box_count" in metrics or "blind_box_profit" in metrics:
            payment_writes.append(
                ("blind", metrics["blind_box_count"], metrics["blind_box_profit"])
            )

    monkeypatch.setattr(
        event_ingestion,
        "_dependencies",
        event_ingestion.EventDependencies(current_month, calculate_profit, ignore_cookie_alert),
    )
    monkeypatch.setitem(runtime_state.CURRENT_SESSIONS, 31368705, 44)
    monkeypatch.setattr(RoomStatsMonthly, "add_amounts", record_monthly)
    monkeypatch.setattr(LiveSession, "add_values_by_id", record_session)
    monkeypatch.setattr(RoomBlindBoxMonthly, "add_amounts", record_blind_box)
    monkeypatch.setattr(RoomLiveStats, "add_metrics", ignore_live_metrics)
    monkeypatch.setattr(event_ingestion, "register_payer", ignore_registration)
    monkeypatch.setattr(event_ingestion, "record_payment", record_runtime_payment)

    client = object.__new__(WebSocketClientBase)
    client.__dict__["_room_id"] = 31368705
    handler: BaseHandler = event_ingestion.MyHandler()
    BaseHandler.handle(handler, client, command)

    assert monthly_gifts == [0.2, 0.6]
    assert session_gifts == [0.2, 0.6]
    assert blind_box_writes == [(2, 100), (3, 400)]
    assert payment_writes == [
        ("gift", 0.2, 0),
        ("blind", 2, 100),
        ("gift", 0.6, 0),
        ("blind", 3, 400),
    ]
