"""Regression coverage for Web room notification dispatch."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.blivedm import handlers
from app.blivedm.models.web import RoomEventMessage


class RoomEventTests(unittest.TestCase):
    def setUp(self):
        self.handler = handlers.BaseHandler()
        self.client = SimpleNamespace(room_id=42)

    def test_dispatch_preserves_payload_and_envelope(self):
        cases = [
            ({"cmd": "ROOM_CHANGE:variant", "data": {"roomid": "42", "title": "example"},
              "send_time": 120, "msg_id": "message-1"}, 42),
            ({"cmd": "LIVE_MULTI_VIEW_EVENT_CHANGE", "data": [{"view_id": 1}]}, None),
            ({"cmd": "ROOM_SKIN_MSG", "roomid": "42", "skin_id": 1}, 42),
            ({"cmd": "SYS_MSG", "msg": "example"}, None),
        ]
        for command, room_id in cases:
            with self.subTest(cmd=command["cmd"]), patch.object(self.handler, "_on_room_event") as receive:
                self.handler.handle(self.client, command)
                receive.assert_called_once()
                client, message = receive.call_args.args
                self.assertIs(client, self.client)
                self.assertIsInstance(message, RoomEventMessage)
                self.assertEqual(message.cmd, command["cmd"].split(":")[0])
                self.assertEqual(message.room_id, room_id)
                self.assertEqual(message.msg_id, command.get("msg_id"))
                self.assertEqual(message.send_time, command.get("send_time"))
                self.assertIs(message.raw, command)
                if "data" in command:
                    self.assertIs(message.data, command["data"])
                else:
                    self.assertEqual(message.data, {k: v for k, v in command.items() if k != "cmd"})

    def test_gift_combo_is_silent_and_does_not_record_a_gift(self):
        with patch.object(handlers.logger, "warning") as warning, patch.object(self.handler, "_on_gift") as gift:
            self.handler.handle(self.client, {"cmd": "GIFT_COMBO", "data": {}})
            warning.assert_not_called()
            gift.assert_not_called()

    def test_unrecognized_command_still_warns_once(self):
        name = "TEST_UNRECOGNIZED_ROOM_EVENT"
        self.addCleanup(handlers.logged_unknown_cmds.discard, name)
        with patch.object(handlers.logger, "warning") as warning:
            for _ in range(2):
                self.handler.handle(self.client, {"cmd": name, "data": {}})
            warning.assert_called_once()
            self.assertIn(name, warning.call_args.args)
