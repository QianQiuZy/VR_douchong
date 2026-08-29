"""Characterization: SQLAlchemy metadata snapshot.

The extraction plan freezes every table name, column name, column type
category, nullability, default, primary-key, and index name.  This module
asserts the exact metadata gift.py wires up, so any Todo 2 move must
produce an equal snapshot (extraction is behaviour-preserving).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    Integer,
    String,
)


def _column_snapshot(column) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    """Reduce a SQLAlchemy Column to a stable, comparable snapshot."""
    default = column.default.arg if column.default is not None else None
    return {
        "name": column.name,
        "type": type(column.type).__name__,
        "length": getattr(column.type, "length", None),
        "nullable": column.nullable,
        "primary_key": column.primary_key,
        "autoincrement": column.autoincrement is True,
        "default": default,
        "index": column.index or False,
    }


def _table_snapshot(table) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return {
        "name": table.name,
        "columns": [_column_snapshot(col) for col in table.columns],
        "primary_key": sorted(col.name for col in table.primary_key.columns),
        "indexes": sorted(
            (
                {"name": idx.name, "columns": [c.name for c in idx.columns], "unique": idx.unique}
                for idx in table.indexes
                if idx.name is not None
            ),
            key=lambda index: index["name"],
        ),
    }


class TestSchemaTablesAndColumns:
    def test_metadata_contains_exactly_the_seven_tables(self, gift_module):
        assert sorted(gift_module.Base.metadata.tables.keys()) == [
            "attention",
            "live_session",
            "room_blind_box_monthly",
            "room_info",
            "room_live_stats",
            "room_stats_monthly",
            "super_chat_log",
        ]

    def test_room_info_columns(self, gift_module):
        table = gift_module.Base.metadata.tables["room_info"]
        assert [_column_snapshot(c) for c in table.columns] == [
            {
                "name": "room_id",
                "type": Integer.__name__,
                "length": None,
                "nullable": False,
                "primary_key": True,
                "autoincrement": False,
                "default": None,
                "index": False,
            },
            {
                "name": "anchor_name",
                "type": String.__name__,
                "length": 100,
                "nullable": False,
                "primary_key": False,
                "autoincrement": False,
                "default": None,
                "index": False,
            },
            {
                "name": "attention",
                "type": Integer.__name__,
                "length": None,
                "nullable": False,
                "primary_key": False,
                "autoincrement": False,
                "default": 0,
                "index": False,
            },
        ]

    def test_attention_columns_and_indexes(self, gift_module):
        table = gift_module.Base.metadata.tables["attention"]
        columns = {c.name: _column_snapshot(c) for c in table.columns}
        assert set(columns) == {
            "room_id",
            "date",
            "attention",
            "guard_1",
            "guard_2",
            "guard_3",
            "fans_count",
        }
        assert columns["room_id"]["type"] == Integer.__name__
        assert columns["date"]["type"] == Date.__name__
        for metric in ("attention", "guard_1", "guard_2", "guard_3", "fans_count"):
            assert columns[metric]["type"] == Integer.__name__
            assert columns[metric]["default"] == 0
            assert columns[metric]["nullable"] is False
        assert sorted(idx.name for idx in table.indexes) == ["idx_attention_date"]
        assert {"room_id", "date"} == {c.name for c in table.primary_key.columns}

    def test_room_stats_monthly_columns_and_indexes(self, gift_module):
        table = gift_module.Base.metadata.tables["room_stats_monthly"]
        columns = {c.name: _column_snapshot(c) for c in table.columns}
        assert set(columns) == {"room_id", "month", "gift", "guard", "super_chat"}
        assert columns["month"]["type"] == String.__name__
        assert columns["month"]["length"] == 6
        for metric in ("gift", "guard", "super_chat"):
            assert columns[metric]["type"] == Float.__name__
            assert columns[metric]["default"] == 0.0
        assert {"room_id", "month"} == {c.name for c in table.primary_key.columns}
        assert sorted(idx.name for idx in table.indexes) == ["idx_rsm_month"]

    def test_room_blind_box_monthly_has_autoincrement_id(self, gift_module):
        table = gift_module.Base.metadata.tables["room_blind_box_monthly"]
        columns = {c.name: _column_snapshot(c) for c in table.columns}
        assert columns["id"]["primary_key"] is True
        assert columns["id"]["autoincrement"] is True
        for metric in ("blind_box_count", "blind_box_profit"):
            assert columns[metric]["type"] == Integer.__name__
            assert columns[metric]["default"] == 0
        index_specs = {(idx.name, tuple(c.name for c in idx.columns), idx.unique) for idx in table.indexes}
        assert index_specs == {
            ("idx_rbbm_month", ("month",), False),
            ("idx_rbbm_room_month", ("room_id", "month"), True),
        }

    def test_room_live_stats_columns_pk_indexes(self, gift_module):
        table = gift_module.Base.metadata.tables["room_live_stats"]
        columns = {c.name: _column_snapshot(c) for c in table.columns}
        assert set(columns) == {"room_id", "date", "duration"}
        assert columns["duration"]["type"] == Integer.__name__
        assert columns["duration"]["default"] == 0
        assert {"room_id", "date"} == {c.name for c in table.primary_key.columns}
        assert sorted(idx.name for idx in table.indexes) == ["idx_rls_date"]

    def test_live_session_columns_and_indexes(self, gift_module):
        table = gift_module.Base.metadata.tables["live_session"]
        columns = {c.name: _column_snapshot(c) for c in table.columns}
        # Full column set the plan freezes.
        expected_names = {
            "id",
            "room_id",
            "start_time",
            "end_time",
            "title",
            "gift",
            "guard",
            "super_chat",
            "month",
            "blind_box_count",
            "blind_box_profit",
            "danmaku_count",
            "start_guard_1",
            "start_guard_2",
            "start_guard_3",
            "start_fans_count",
            "start_attention",
            "end_guard_1",
            "end_guard_2",
            "end_guard_3",
            "end_fans_count",
            "end_attention",
            "avg_concurrency",
            "max_concurrency",
        }
        assert set(columns) == expected_names
        assert columns["id"]["primary_key"] is True
        assert columns["id"]["autoincrement"] is True
        assert columns["room_id"]["nullable"] is False
        assert columns["start_time"]["type"] == DateTime.__name__
        assert columns["end_time"]["type"] == DateTime.__name__
        assert columns["end_time"]["nullable"] is True
        assert columns["title"]["type"] == String.__name__
        assert columns["title"]["length"] == 255
        assert columns["title"]["default"] == ""
        assert columns["month"]["type"] == String.__name__
        assert columns["month"]["length"] == 6
        # start_* / end_* snapshots are nullable Integers.
        for name in (
            "start_guard_1",
            "start_guard_2",
            "start_guard_3",
            "start_fans_count",
            "start_attention",
            "end_guard_1",
            "end_guard_2",
            "end_guard_3",
            "end_fans_count",
            "end_attention",
        ):
            assert columns[name]["type"] == Integer.__name__
            assert columns[name]["nullable"] is True
        assert columns["avg_concurrency"]["type"] == Float.__name__
        assert columns["avg_concurrency"]["nullable"] is True
        assert columns["max_concurrency"]["type"] == Integer.__name__
        assert columns["max_concurrency"]["nullable"] is True
        index_specs = {(idx.name, tuple(c.name for c in idx.columns)) for idx in table.indexes}
        assert index_specs == {
            ("idx_ls_room_month", ("room_id", "month")),
            ("idx_ls_room_month_start", ("room_id", "month", "start_time")),
            ("ix_live_session_room_id", ("room_id",)),
            ("ix_live_session_month", ("month",)),
        }

    def test_super_chat_log_columns_and_indexes(self, gift_module):
        table = gift_module.Base.metadata.tables["super_chat_log"]
        columns = {c.name: _column_snapshot(c) for c in table.columns}
        assert set(columns) == {"id", "room_id", "uname", "uid", "send_time", "price", "message"}
        assert columns["id"]["primary_key"] is True
        assert columns["id"]["autoincrement"] is True
        assert columns["uname"]["type"] == String.__name__
        assert columns["uname"]["length"] == 100
        assert columns["uid"]["type"] == BigInteger.__name__
        assert columns["message"]["type"] == String.__name__
        assert columns["message"]["length"] == 500
        assert columns["price"]["type"] == Float.__name__
        index_specs = {(idx.name, tuple(c.name for c in idx.columns)) for idx in table.indexes}
        assert index_specs == {
            ("idx_scl_room_time", ("room_id", "send_time")),
            ("idx_scl_uid_time", ("uid", "send_time")),
            ("ix_super_chat_log_room_id", ("room_id",)),
            ("ix_super_chat_log_send_time", ("send_time",)),
        }
