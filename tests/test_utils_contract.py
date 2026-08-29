"""Characterization: pure helpers gift.py exposes.

Extraction (Todo 2 onwards) moves these into a maintenance / persistence
module.  Any move must preserve exact string outputs, boundary conditions
and rounding behaviour.  All tests run with zero DB and zero network.
"""

from __future__ import annotations

import datetime

import pytest


class TestMonthHelpers:
    def test_month_str_from_datetime_returns_yyyymm(self, gift_module):
        assert gift_module.month_str(datetime.datetime(2026, 1, 15, 12, 34)) == "202601"
        assert gift_module.month_str(datetime.datetime(2025, 12, 31, 23, 59)) == "202512"

    def test_month_str_default_is_now_in_yyyymm(self, gift_module):
        result = gift_module.month_str()
        assert len(result) == 6
        assert result.isdigit()
        year, month = int(result[:4]), int(result[4:])
        assert 2000 <= year <= 9999
        assert 1 <= month <= 12

    def test_month_range_january_boundary(self, gift_module):
        start, end = gift_module.month_range("202601")
        assert start == datetime.date(2026, 1, 1)
        assert end == datetime.date(2026, 2, 1)

    def test_month_range_december_wraps_to_next_year(self, gift_module):
        start, end = gift_module.month_range("202612")
        assert start == datetime.date(2026, 12, 1)
        assert end == datetime.date(2027, 1, 1)

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("202601", "202601"),
            ("2026-01", "202601"),
            ("202612", "202612"),
            ("2026-12", "202612"),
        ],
    )
    def test_normalize_month_code_accepts_both_formats(self, gift_module, raw, expected):
        assert gift_module.normalize_month_code(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            "",
            None,
            "2026",
            "20261",
            "2026013",
            "2026-1",
            "2026-013",
            "abcdef",
            "202600",
            "202613",
            "2026-00",
            "2026-13",
        ],
    )
    def test_normalize_month_code_rejects_malformed_input(self, gift_module, raw):
        assert gift_module.normalize_month_code(raw) is None

    def test_is_current_month_matches_month_str_today(self, gift_module):
        assert gift_module.is_current_month(gift_module.month_str()) is True
        assert gift_module.is_current_month("190001") is False


class TestArchiveTableNaming:
    """Suffix rules Todo 5 must NOT change."""

    def test_super_chat_log_suffix_format(self, gift_module):
        assert gift_module.sc_log_table_name("202601") == "super_chat_log_202601"

    def test_live_session_suffix_format(self, gift_module):
        assert gift_module.live_session_table_name("202601") == "live_session_202601"

    def test_room_live_stats_suffix_format(self, gift_module):
        assert gift_module.room_live_stats_table_name("202601") == "room_live_stats_202601"

    def test_attention_suffix_format(self, gift_module):
        assert gift_module.attention_table_name("202601") == "attention_202601"


class TestSecondsFormatting:
    def test_seconds_to_hms_zero(self, gift_module):
        assert gift_module._seconds_to_hms(0) == "00:00:00"

    def test_seconds_to_hms_single_hour(self, gift_module):
        assert gift_module._seconds_to_hms(3600) == "01:00:00"

    def test_seconds_to_hms_mixed(self, gift_module):
        assert gift_module._seconds_to_hms(3661) == "01:01:01"

    def test_seconds_to_hms_two_digit_hours(self, gift_module):
        assert gift_module._seconds_to_hms(36000) == "10:00:00"


class TestProfitConversions:
    """Blind-box profit uses 0.1 RMB granularity (tenths).  Freeze this."""

    def test_profit_to_tenths_rounds_half_up(self, gift_module):
        # profit_coin = 15000 - 5000 = 10000; /1000 = 10.0 -> 100 tenths
        assert gift_module._profit_to_tenths(15000, 5000) == 100

    def test_profit_to_tenths_negative_result_when_coin_exceeds_price(self, gift_module):
        assert gift_module._profit_to_tenths(1000, 5000) == -40

    def test_profit_to_tenths_zero(self, gift_module):
        assert gift_module._profit_to_tenths(0, 0) == 0

    def test_profit_display_none_returns_zero_float(self, gift_module):
        assert gift_module._profit_display(None) == 0.0

    def test_profit_display_int_treated_as_tenths(self, gift_module):
        assert gift_module._profit_display(100) == 10.0
        assert gift_module._profit_display(-40) == -4.0

    def test_profit_display_float_passes_through_with_half_up(self, gift_module):
        assert gift_module._profit_display(10.25) == 10.3
        assert gift_module._profit_display(10.24) == 10.2


class TestRoomAnchorHelpers:
    def test_get_room_ids_returns_a_copy(self, gift_module):
        ids = gift_module.get_room_ids()
        ids.append(999_999_999)
        # Mutation must NOT affect module state.
        assert 999_999_999 not in gift_module.ROOM_IDS

    def test_get_room_anchors_returns_a_copy(self, gift_module):
        anchors = gift_module.get_room_anchors()
        anchors[999_999_999] = "MutantAnchor"
        assert 999_999_999 not in gift_module.ROOM_ANCHORS

    def test_get_room_anchor_name_falls_back_to_empty_string(self, gift_module):
        assert gift_module.get_room_anchor_name(999_999_999) == ""

    def test_get_room_anchor_name_returns_configured_value(self, gift_module):
        assert gift_module.get_room_anchor_name(111111) == "TestAnchorAlpha"
