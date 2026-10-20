"""Archive CLI dispatch, month validation, and protection of the current month."""

from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest


def _load_migrate_module(gift_module):
    """Import app.migrate_sc_archive fresh under conftest.py's stubbed app.gift."""
    # Ensure gift is already loaded via conftest; migrate imports from it.
    assert "app.gift" in sys.modules and sys.modules["app.gift"] is gift_module
    if "app.migrate_sc_archive" in sys.modules:
        del sys.modules["app.migrate_sc_archive"]
    return importlib.import_module("app.migrate_sc_archive")


class TestMigrateArchiveCallShape:
    """When main() runs it must invoke exactly the three archive functions."""

    def test_all_month_variant_calls_each_archive_once_with_none(
        self, gift_module, monkeypatch, capsys
    ):
        from app import archive_service

        def forbidden_attention(*_args, **_kwargs):
            pytest.fail("the CLI must leave attention archiving to the scheduler")

        monkeypatch.setattr(gift_module, "archive_attention", forbidden_attention)
        monkeypatch.setattr(archive_service, "archive_attention", forbidden_attention)
        migrate = _load_migrate_module(gift_module)
        calls = []
        monkeypatch.setattr(migrate, "archive_super_chat_log", lambda m: calls.append(("sc", m)) or 1)
        monkeypatch.setattr(migrate, "archive_live_session", lambda m: calls.append(("session", m)) or 2)
        monkeypatch.setattr(migrate, "archive_room_live_stats", lambda m: calls.append(("daily", m)) or 3)

        monkeypatch.setattr(sys, "argv", ["app.migrate_sc_archive"])
        migrate.main()

        assert calls == [("sc", None), ("session", None), ("daily", None)]

    def test_specified_month_is_normalised_before_dispatch(
        self, gift_module, monkeypatch
    ):
        migrate = _load_migrate_module(gift_module)
        sc_calls: list[str | None] = []
        ls_calls: list[str | None] = []
        rls_calls: list[str | None] = []
        monkeypatch.setattr(migrate, "archive_super_chat_log", lambda m: sc_calls.append(m) or 0)
        monkeypatch.setattr(migrate, "archive_live_session", lambda m: ls_calls.append(m) or 0)
        monkeypatch.setattr(migrate, "archive_room_live_stats", lambda m: rls_calls.append(m) or 0)

        monkeypatch.setattr(sys, "argv", ["app.migrate_sc_archive", "--month", "2024-01"])
        migrate.main()

        assert sc_calls == ["202401"]
        assert ls_calls == ["202401"]
        assert rls_calls == ["202401"]

    def test_yyyymm_direct_form_is_accepted(self, gift_module, monkeypatch):
        migrate = _load_migrate_module(gift_module)
        seen: list[str | None] = []
        monkeypatch.setattr(migrate, "archive_super_chat_log", lambda m: seen.append(m) or 0)
        monkeypatch.setattr(migrate, "archive_live_session", lambda m: seen.append(m) or 0)
        monkeypatch.setattr(migrate, "archive_room_live_stats", lambda m: seen.append(m) or 0)

        monkeypatch.setattr(sys, "argv", ["app.migrate_sc_archive", "--month", "202401"])
        migrate.main()

        assert seen == ["202401", "202401", "202401"]

    def test_invalid_month_raises_system_exit(self, gift_module, monkeypatch):
        migrate = _load_migrate_module(gift_module)
        monkeypatch.setattr(migrate, "archive_super_chat_log", lambda m: 0)
        monkeypatch.setattr(migrate, "archive_live_session", lambda m: 0)
        monkeypatch.setattr(migrate, "archive_room_live_stats", lambda m: 0)

        monkeypatch.setattr(sys, "argv", ["app.migrate_sc_archive", "--month", "not-a-month"])
        with pytest.raises(SystemExit) as excinfo:
            migrate.main()
        assert excinfo.value.code == "month 参数格式不正确，应为 YYYYMM 或 YYYY-MM"

    def test_current_month_target_short_circuits_each_archive_to_zero(
        self, gift_module, monkeypatch
    ):
        migrate = _load_migrate_module(gift_module)
        current = gift_module.month_str()

        counts: list[int] = []
        with patch.object(migrate, "archive_super_chat_log", wraps=migrate.archive_super_chat_log) as sc, \
             patch.object(migrate, "archive_live_session", wraps=migrate.archive_live_session) as ls, \
             patch.object(migrate, "archive_room_live_stats", wraps=migrate.archive_room_live_stats) as rls:
            monkeypatch.setattr(sys, "argv", ["app.migrate_sc_archive", "--month", current])
            migrate.main()
            counts.extend([sc.call_count, ls.call_count, rls.call_count])
        # Each archive is called exactly once with the current month; each
        # returns 0 because is_current_month() short-circuits inside app.gift.
        assert counts == [1, 1, 1]


class TestArchiveFunctionsSkipCurrentMonth:
    """Freeze the "skip current month" contract that each archive relies on."""

    def test_archive_super_chat_log_returns_zero_for_current_month(self, gift_module):
        assert gift_module.archive_super_chat_log(gift_module.month_str()) == 0

    def test_archive_live_session_returns_zero_for_current_month(self, gift_module):
        assert gift_module.archive_live_session(gift_module.month_str()) == 0

    def test_archive_room_live_stats_returns_zero_for_current_month(self, gift_module):
        assert gift_module.archive_room_live_stats(gift_module.month_str()) == 0

    def test_archive_attention_returns_zero_for_current_month(self, gift_module):
        # archive_attention exists but the CLI must not call it - included
        # here to prove the module-scope behaviour is symmetric.
        assert gift_module.archive_attention(gift_module.month_str()) == 0

    def test_archive_super_chat_log_returns_zero_for_malformed_month(self, gift_module):
        assert gift_module.archive_super_chat_log("not-a-month") == 0

    def test_archive_live_session_returns_zero_for_malformed_month(self, gift_module):
        assert gift_module.archive_live_session("not-a-month") == 0

    def test_archive_room_live_stats_returns_zero_for_malformed_month(self, gift_module):
        assert gift_module.archive_room_live_stats("not-a-month") == 0

    def test_archive_attention_returns_zero_for_malformed_month(self, gift_module):
        assert gift_module.archive_attention("not-a-month") == 0
