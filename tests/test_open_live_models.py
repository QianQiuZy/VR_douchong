import pytest

from app.blivedm.models import open_live


@pytest.mark.parametrize("union_id", [None, "", "test-union-id"])
def test_room_entry_preserves_open_id_with_optional_union_id(union_id):
    data = {
        "room_id": 123,
        "uface": "",
        "uname": "test-user",
        "open_id": "test-open-id",
        "timestamp": 1,
    }
    if union_id is not None:
        data["union_id"] = union_id

    message = open_live.RoomEnterMessage.from_command(data)

    assert message.open_id == "test-open-id"
    assert message.union_id == (union_id or "")


@pytest.mark.parametrize("blind_gift", [None, {"blind_gift_id": 42, "status": True}])
def test_open_live_gift_parses_blind_box_and_nested_anchor(blind_gift):
    data = {
        "room_id": 123,
        "open_id": "test-open-id",
        "union_id": "test-union-id",
        "uname": "test-user",
        "uface": "",
        "gift_id": 7,
        "gift_name": "test-gift",
        "gift_num": 2,
        "price": 100,
        "r_price": 80,
        "paid": True,
        "fans_medal_level": 0,
        "fans_medal_name": "",
        "fans_medal_wearing_status": False,
        "guard_level": 0,
        "timestamp": 1,
        "anchor_info": {
            "uid": 9,
            "open_id": "anchor-open-id",
            "union_id": "anchor-union-id",
            "uname": "test-anchor",
            "uface": "",
        },
        "msg_id": "test-message",
        "gift_icon": "",
    }
    if blind_gift is not None:
        data["blind_gift"] = blind_gift

    message = open_live.GiftMessage.from_command(data)

    assert (message.open_id, message.union_id) == ("test-open-id", "test-union-id")
    assert message.anchor_info.union_id == "anchor-union-id"
    assert (message.price, message.r_price, message.gift_num) == (100, 80, 2)
    assert isinstance(message.blind_gift, open_live.BlindGift)
    assert message.blind_gift.blind_gift_id == (42 if blind_gift else 0)
    assert message.blind_gift.status is (blind_gift is not None)

    # Older payloads omit union IDs and the optional blind/combo fields.
    data.pop("union_id")
    data["anchor_info"].pop("union_id")
    legacy = open_live.GiftMessage.from_command(data)
    assert legacy.union_id == legacy.anchor_info.union_id == ""
    assert legacy.open_id == "test-open-id"
