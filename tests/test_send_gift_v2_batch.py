import base64

import pytest

from app import event_ingestion
from app.blivedm.clients.ws_base import WebSocketClientBase
from app.blivedm.models.web import GiftData, GiftMessage, SendGiftV2, SendGiftV2Message


@pytest.mark.parametrize("quantity", [2, 10, 100, 520])
def test_v2_batch_uses_unit_price_times_quantity(quantity):
    # Given: V2 represents the paid coin field as a unit amount for a batch.
    proto = SendGiftV2(gift=[GiftData(gift_id=1, gift_name="batch", num=quantity, price=100000, total_coin=100000)])

    # When: the actual protobuf decoder normalizes the gift.
    message = SendGiftV2Message.from_command({"pb": base64.b64encode(proto.dumps()).decode("ascii")})[0]

    # Then: both ordinary-gift total fields represent the whole batch.
    assert message.total_price == quantity * 100000
    assert message.total_coin == quantity * 100000


def test_v2_single_gift_preserves_supplied_total_coin():
    # Given: a single discounted gift whose unit list price differs from paid coins.
    proto = SendGiftV2(gift=[GiftData(gift_id=1, gift_name="single", num=1, price=100000, total_coin=80000)])

    # When: the V2 decoder processes it.
    message = SendGiftV2Message.from_command({"pb": base64.b64encode(proto.dumps()).decode("ascii")})[0]

    # Then: the explicit single-gift amount is unchanged.
    assert (message.total_price, message.total_coin) == (80000, 80000)


def test_v2_free_batch_remains_free_even_with_a_nominal_unit_price():
    # Given: a free promotion has a list price but zero paid coins.
    proto = SendGiftV2(gift=[GiftData(gift_id=1, gift_name="free", num=10, price=100, total_coin=0)])

    # When: the V2 batch rule is applied.
    message = SendGiftV2Message.from_command({"pb": base64.b64encode(proto.dumps()).decode("ascii")})[0]

    # Then: quantity normalization cannot manufacture revenue for a free gift.
    assert (message.total_price, message.total_coin) == (0, 0)


def test_v2_normal_batch_does_not_trigger_blind_box_accounting(monkeypatch):
    # Given: a normal V2 batch and the actual websocket ingestion handler.
    proto = SendGiftV2(gift=[GiftData(gift_id=1, gift_name="batch", num=10, price=100, total_coin=100)])
    handler = event_ingestion.MyHandler()
    values = []
    blind_calls = []
    monkeypatch.setattr(handler, "_record_gift", lambda *args, **kwargs: values.append(args[3]))
    monkeypatch.setattr(handler, "_record_blind_box", lambda *args, **kwargs: blind_calls.append(args))
    client = WebSocketClientBase.__new__(WebSocketClientBase)
    client._room_id = 1

    # When: the command enters the actual dispatcher and gift callback.
    handler.handle(client, {"cmd": "SEND_GIFT_V2", "data": {"pb": base64.b64encode(proto.dumps()).decode("ascii")}})

    # Then: the corrected total is recorded once without a false blind-box side effect.
    assert values == [1000]
    assert blind_calls == []


def test_legacy_send_gift_preserves_existing_amount_semantics():
    # Given: a legacy JSON gift with a deliberately distinct explicit total amount.
    payload = {
        "giftName": "legacy", "num": 10, "uname": "fixture", "uid": 1,
        "face": "", "guard_level": 0, "timestamp": 1, "giftId": 1,
        "giftType": 0, "gift_info": {"img_basic": ""}, "action": "赠送",
        "price": 100, "rnd": "", "coin_type": "gold", "total_coin": 700, "tid": "",
    }

    # When: the legacy decoder handles it, not SEND_GIFT_V2.
    message = GiftMessage.from_command(payload)

    # Then: V2's batch rule does not leak into the shipped legacy interpretation.
    assert (message.total_price, message.total_coin) == (700, 700)
