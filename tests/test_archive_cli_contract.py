"""Characterization: app.migrate_sc_archive CLI shape.

The CLI wraps app.gift's archive functions and must:

  1. Import EXACTLY these four symbols from gift:
     archive_super_chat_log, archive_live_session, archive_room_live_stats,
     normalize_month_code.  (NOT ``archive_attention`` - that stays on the
     scheduler per Todo 5's freeze.)
  2. Call EXACTLY three archive functions when invoked (SC, LiveSession,
     RoomLiveStats), in that documented order.
  3. Accept ``--month YYYYMM`` and ``--month YYYY-MM`` and pipe the result
     through ``normalize_month_code``; reject anything else with
     ``SystemExit`` and a Chinese error message.
  4. Not crash when no ``--month`` is supplied (archives everything older
     than the current month).
"""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATE_PATH = REPO_ROOT / "app" / "migrate_sc_archive.py"


def _load_migrate_module(gift_module):
    """Import app.migrate_sc_archive fresh under conftest.py's stubbed app.gift."""
    # Ensure gift is already loaded via conftest; migrate imports from it.
    assert "app.gift" in sys.modules and sys.modules["app.gift"] is gift_module
    if "app.migrate_sc_archive" in sys.modules:
        del sys.modules["app.migrate_sc_archive"]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    return importlib.import_module("app.migrate_sc_archive")


class TestMigrateImportsExactlyThreeArchiveFunctions:
    """Static AST check - what does app.migrate_sc_archive actually import?"""

    def test_migrate_imports_only_the_four_frozen_names(self):
        source = MIGRATE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports_from_gift = [
            {alias.name for alias in node.names}
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "app.gift"
        ]
        assert imports_from_gift == [
            {
                "archive_live_session",
                "archive_room_live_stats",
                "archive_super_chat_log",
                "normalize_month_code",
            }
        ]

    def test_migrate_does_not_import_archive_attention(self):
        source = MIGRATE_PATH.read_text(encoding="utf-8")
        assert "archive_attention" not in source, (
            "app.migrate_sc_archive must not touch archive_attention; "
            "that function belongs to the scheduler (Todo 5 freeze)."
        )


class TestMigrateArchiveCallShape:
    """When main() runs it must invoke exactly the three archive functions."""

    def test_all_month_variant_calls_each_archive_once_with_none(
        self, gift_module, monkeypatch, capsys
    ):
        migrate = _load_migrate_module(gift_module)
        sc_calls: list[str | None] = []
        ls_calls: list[str | None] = []
        rls_calls: list[str | None] = []
        monkeypatch.setattr(migrate, "archive_super_chat_log", lambda m: sc_calls.append(m) or 1)
        monkeypatch.setattr(migrate, "archive_live_session", lambda m: ls_calls.append(m) or 2)
        monkeypatch.setattr(migrate, "archive_room_live_stats", lambda m: rls_calls.append(m) or 3)

        monkeypatch.setattr(sys, "argv", ["app.migrate_sc_archive"])
        migrate.main()

        assert sc_calls == [None]
        assert ls_calls == [None]
        assert rls_calls == [None]

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
