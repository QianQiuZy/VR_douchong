"""Portable protocol regressions; no application, database or network required."""

import dataclasses
import unittest
from types import SimpleNamespace

from app.blivedm import __version__
from app.blivedm.handlers import BaseHandler
from app.blivedm.models.web import PopularityRedPocketV2NewMessage


class RedPocketV2Tests(unittest.TestCase):
    def payload(self, **changes):
        return {"lot_id": 28994501, "start_time": 1794740533,
                "rp_type": 2, "price": 9805, **changes}

    def test_raw_fields_are_preserved(self):
        data = self.payload(
            current_time=1794740534, wait_num=1, wait_num_v2=2,
            uid=123, uname="sender", action="送出", num=1,
            gift_id=0, gift_name="上舰红包", name_color="",
            medal_info={"guard_level": 3}, wealth_level=1, group_medal=None,
            is_mystery=False, sender_info={"uid": 123}, gift_icon="",
        )
        self.assertEqual(dataclasses.asdict(PopularityRedPocketV2NewMessage.from_command(data)), data)

    def test_all_guard_price_tiers(self):
        for price, count in ((9805, 5), (19610, 10), (39220, 20), (98050, 50)):
            with self.subTest(price=price):
                message = PopularityRedPocketV2NewMessage.from_command(self.payload(price=price))
                self.assertEqual(message.pocket_type, "guard")
                self.assertEqual(message.guard_num, count)
                self.assertEqual(message.price, price)
                self.assertIsNone(message.gift_num)
                self.assertIsNone(message.battery_num)

    def test_gift_and_battery_use_original_price(self):
        for kind, name in ((0, "gift"), (3, "battery")):
            with self.subTest(kind=kind):
                message = PopularityRedPocketV2NewMessage.from_command(self.payload(rp_type=kind, price=100))
                self.assertEqual(message.pocket_type, name)
                self.assertEqual(getattr(message, name + "_num"), 100)
                self.assertIsNone(message.guard_num)

    def test_unknown_types_and_guard_prices_are_preserved_without_inventing_count(self):
        unknown = PopularityRedPocketV2NewMessage.from_command(self.payload(rp_type=99))
        self.assertEqual(unknown.rp_type, 99)
        self.assertIsNone(unknown.pocket_type)
        message = PopularityRedPocketV2NewMessage.from_command(self.payload(price=123))
        self.assertEqual(message.pocket_type, "guard")
        self.assertIsNone(message.guard_num)

    def test_invalid_core_fields_are_rejected(self):
        for data in (None, [], {}, self.payload(lot_id=0), self.payload(start_time=True),
                     self.payload(price=-1), self.payload(rp_type="2")):
            with self.subTest(data=data), self.assertRaises(ValueError):
                PopularityRedPocketV2NewMessage.from_command(data)

    def test_base_handler_dispatches_typed_message_and_supports_cmd_suffix(self):
        class Handler(BaseHandler):
            def _on_red_pocket_v2_new(self, client, message):
                self.received.append(message)

        handler = Handler()
        handler.received = []
        client = SimpleNamespace(room_id=42)
        handler.handle(client, {"cmd": "POPULARITY_RED_POCKET_V2_NEW:variant", "data": self.payload()})
        self.assertEqual(len(handler.received), 1)
        self.assertIsInstance(handler.received[0], PopularityRedPocketV2NewMessage)
        self.assertEqual(handler.received[0].guard_num, 5)
        with self.assertLogs("blivedm", level="WARNING") as logs:
            handler.handle(client, {"cmd": "POPULARITY_RED_POCKET_V2_NEW", "data": {}})
        self.assertEqual(len(handler.received), 1)
        self.assertNotIn("token", " ".join(logs.output).lower())

    def test_default_callback_is_safe_for_other_library_users(self):
        BaseHandler().handle(SimpleNamespace(room_id=42), {
            "cmd": "POPULARITY_RED_POCKET_V2_NEW", "data": self.payload(),
        })

    def test_library_version(self):
        self.assertEqual(__version__, "1.0.4-VR_douchong")


if __name__ == "__main__":
    unittest.main()
