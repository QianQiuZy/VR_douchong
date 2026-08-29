"""Characterization: rooms.json load / save round-trip.

`rooms.json` is a two-key object with:
  - "room_ids": sorted deduplicated list[int]
  - "room_anchors": dict[str(room_id), str(anchor_name)] (str keys)

`load_rooms_config()` populates ``ROOM_IDS`` / ``ROOM_ANCHORS`` at import
and `save_rooms_config()` writes the same shape back.  Todo 3 must not
alter the key names, ordering, or the string-key convention on
``room_anchors``.
"""

from __future__ import annotations

import json


class TestRoomsJsonRoundTrip:
    def test_save_produces_frozen_top_level_keys(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        gift_module.ROOM_IDS.extend([888888, 777777])
        gift_module.ROOM_ANCHORS[888888] = "AnchorTest_A"
        gift_module.ROOM_ANCHORS[777777] = "AnchorTest_B"
        gift_module.save_rooms_config()

        data = json.loads(isolated_rooms_json.read_text(encoding="utf-8"))
        assert set(data.keys()) == {"room_ids", "room_anchors"}

    def test_save_sorts_and_deduplicates_room_ids(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        gift_module.ROOM_IDS.extend([100, 200, 100, 50, 200])
        gift_module.save_rooms_config()

        data = json.loads(isolated_rooms_json.read_text(encoding="utf-8"))
        assert data["room_ids"] == [50, 100, 200]

    def test_save_encodes_anchor_keys_as_strings(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        gift_module.ROOM_IDS.append(12345)
        gift_module.ROOM_ANCHORS[12345] = "AnchorFoo"
        gift_module.save_rooms_config()

        data = json.loads(isolated_rooms_json.read_text(encoding="utf-8"))
        assert data["room_anchors"] == {"12345": "AnchorFoo"}
        for key in data["room_anchors"]:
            assert isinstance(key, str)

    def test_save_preserves_non_ascii_anchor_names(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        gift_module.ROOM_IDS.append(80397)
        gift_module.ROOM_ANCHORS[80397] = "阿梓从小就很可爱"
        gift_module.save_rooms_config()

        raw = isolated_rooms_json.read_text(encoding="utf-8")
        assert "阿梓从小就很可爱" in raw
        data = json.loads(raw)
        assert data["room_anchors"]["80397"] == "阿梓从小就很可爱"

    def test_load_populates_normalized_int_keys_in_room_anchors(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        isolated_rooms_json.write_text(
            json.dumps(
                {
                    "room_ids": [42, 43],
                    "room_anchors": {"42": "Anchor42", "43": "Anchor43"},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        gift_module.load_rooms_config()

        assert gift_module.ROOM_IDS == [42, 43]
        assert set(gift_module.ROOM_ANCHORS.keys()) == {42, 43}
        assert gift_module.ROOM_ANCHORS[42] == "Anchor42"
        assert gift_module.ROOM_ANCHORS[43] == "Anchor43"

    def test_load_fills_missing_anchor_with_empty_string(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        isolated_rooms_json.write_text(
            json.dumps({"room_ids": [1, 2, 3], "room_anchors": {"2": "Two"}}),
            encoding="utf-8",
        )
        gift_module.load_rooms_config()

        assert gift_module.ROOM_ANCHORS[1] == ""
        assert gift_module.ROOM_ANCHORS[2] == "Two"
        assert gift_module.ROOM_ANCHORS[3] == ""

    def test_load_skips_non_integer_room_ids(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        isolated_rooms_json.write_text(
            json.dumps({"room_ids": [7, "not-an-int", None, 9], "room_anchors": {}}),
            encoding="utf-8",
        )
        gift_module.load_rooms_config()

        assert gift_module.ROOM_IDS == [7, 9]

    def test_load_treats_malformed_top_level_types_as_no_op(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        isolated_rooms_json.write_text(
            json.dumps({"room_ids": "not-a-list", "room_anchors": {}}),
            encoding="utf-8",
        )
        gift_module.ROOM_IDS.append(999)
        gift_module.ROOM_ANCHORS[999] = "Sentinel"
        gift_module.load_rooms_config()

        assert gift_module.ROOM_IDS == [999]
        assert gift_module.ROOM_ANCHORS == {999: "Sentinel"}

    def test_load_treats_malformed_anchors_as_no_op(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        isolated_rooms_json.write_text(
            json.dumps({"room_ids": [1], "room_anchors": "not-a-dict"}),
            encoding="utf-8",
        )
        gift_module.ROOM_IDS.append(42)
        gift_module.load_rooms_config()

        assert gift_module.ROOM_IDS == [42]

    def test_missing_file_is_silently_ignored(
        self, tmp_path, gift_module, monkeypatch, clean_room_registry
    ):
        missing_path = tmp_path / "does-not-exist.json"
        monkeypatch.setattr(gift_module, "ROOMS_JSON_PATH", str(missing_path))
        gift_module.load_rooms_config()

        assert gift_module.ROOM_IDS == []
        assert gift_module.ROOM_ANCHORS == {}

    def test_round_trip_preserves_data_end_to_end(
        self, gift_module, isolated_rooms_json, clean_room_registry
    ):
        original = {
            "room_ids": [11, 22, 33],
            "room_anchors": {"11": "AnchorOne", "22": "AnchorTwo", "33": "AnchorThree"},
        }
        isolated_rooms_json.write_text(json.dumps(original, ensure_ascii=False), encoding="utf-8")
        gift_module.load_rooms_config()
        gift_module.save_rooms_config()

        round_tripped = json.loads(isolated_rooms_json.read_text(encoding="utf-8"))
        assert round_tripped == original


class TestGetRoomAnchorsThreadSafety:
    def test_get_room_ids_returns_independent_copy(
        self, gift_module, clean_room_registry
    ):
        gift_module.ROOM_IDS.extend([1, 2, 3])
        snapshot = gift_module.get_room_ids()
        snapshot.append(4)
        assert 4 not in gift_module.ROOM_IDS

    def test_get_room_anchors_returns_independent_copy(
        self, gift_module, clean_room_registry
    ):
        gift_module.ROOM_ANCHORS[1] = "Anchor1"
        snapshot = gift_module.get_room_anchors()
        snapshot[2] = "MutantAnchor"
        assert 2 not in gift_module.ROOM_ANCHORS
