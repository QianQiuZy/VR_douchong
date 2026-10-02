"""A single budgeted MySQL read connection; never uses the collector's QueuePool."""

from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field

import pymysql
from pymysql.constants import COMMAND

from . import config
from .api_cache_store import CacheStore
from .repositories.tables import month_range, month_str, normalize_month_code

BASE_TABLES = ("room_info", "room_stats_monthly", "room_blind_box_monthly")
MONTH_TABLES = (
    "room_live_stats",
    "attention",
    "live_session",
    "live_session_15m_stats",
    "super_chat_log",
)
TABLE_PATTERN = re.compile(
    r"(?:room_live_stats|attention|live_session|live_session_15m_stats|super_chat_log)(?:_[0-9]{6})?\Z"
)


class BudgetConnection(pymysql.connections.Connection):
    """Account actual COM_QUERY packets, including connect settings and transactions."""

    def __init__(self, store: CacheStore, stop: threading.Event):
        self.cache_store = store
        self.cache_stop = stop
        super().__init__(
            host=config.DB_CONFIG["host"],
            port=config.DB_CONFIG["port"],
            user=config.DB_CONFIG["user"],
            password=config.DB_CONFIG["password"],
            database=config.DB_CONFIG["db"],
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=3,
            read_timeout=5,
            write_timeout=5,
        )

    def _execute_command(self, command, sql):
        if command == COMMAND.COM_QUERY:
            self.cache_store.take_sql(self.cache_stop)
        return super()._execute_command(command, sql)


@dataclass
class Snapshot:
    month: str
    stamp: float
    base: dict[str, list[dict]]
    tables: dict[str, list[dict]]
    months: set[str] = field(default_factory=set)


class SnapshotReader:
    def __init__(
        self, store: CacheStore, stop: threading.Event, connect=BudgetConnection
    ):
        self.store = store
        self.stop = stop
        self.connect = connect
        self.connection = None
        self.metadata: set[str] = set()
        self.base: dict[str, list[dict]] = {}
        self.sc: dict[int, dict] = {}
        self.sc_revisions: dict[int, tuple[int, int, int]] = {}
        self.buckets: dict[int, list[dict]] = {}
        self.current_month: str | None = None
        self.hot_months: set[str] = set()

    def query(self, sql: str, params=None) -> list[dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    @contextmanager
    def transaction(self):
        if self.connection is None:
            self.connection = self.connect(self.store, self.stop)
            self.query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        try:
            self.query("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            yield
            self.connection.commit()
        except BaseException:
            # Closing a failed transaction rolls it back without an unbudgeted SQL packet.
            self.close()
            raise

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def discover(self) -> None:
        rows = self.query(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s",
            (config.DB_CONFIG["db"],),
        )
        self.metadata = {row["TABLE_NAME"] for row in rows}
        # Old, unarchived hot rows may be the only evidence that a month exists.
        rows = self.query(
            "SELECT month AS month FROM live_session UNION "
            "SELECT month FROM live_session_15m_stats UNION "
            "SELECT DATE_FORMAT(date, '%Y%m') FROM room_live_stats UNION "
            "SELECT DATE_FORMAT(date, '%Y%m') FROM attention UNION "
            "SELECT DATE_FORMAT(send_time, '%Y%m') FROM super_chat_log"
        )
        self.hot_months = {
            row["month"] for row in rows if normalize_month_code(row["month"])
        }

    def read_table(self, prefix: str, month: str) -> list[dict]:
        table = (
            f"{prefix}_{month}"
            if month != month_str() and f"{prefix}_{month}" in self.metadata
            else prefix
        )
        if not TABLE_PATTERN.fullmatch(table):
            raise ValueError("invalid cache table")
        start, end = month_range(month)
        if prefix in {"live_session", "live_session_15m_stats"}:
            return self.query(f"SELECT * FROM `{table}` WHERE month=%s", (month,))
        name = "send_time" if prefix == "super_chat_log" else "date"
        return self.query(
            f"SELECT * FROM `{table}` WHERE `{name}` >= %s AND `{name}` < %s",
            (start, end),
        )

    def known_months(self) -> set[str]:
        months = {month_str()} | self.hot_months
        for table in self.metadata:
            if TABLE_PATTERN.fullmatch(table):
                value = normalize_month_code(table.rsplit("_", 1)[-1])
                if value:
                    months.add(value)
        for rows in self.base.values():
            months.update(
                row["month"] for row in rows if normalize_month_code(row.get("month"))
            )
        return months

    def current(self, *, discover: bool = False) -> Snapshot:
        month = month_str()
        stamp = self.store.clock()
        # Do not mutate incremental state until the complete read transaction commits.
        with self.transaction():
            if discover or not self.metadata:
                self.discover()
            base = {
                table: self.query(f"SELECT * FROM `{table}`") for table in BASE_TABLES
            }
            tables = {
                table: self.read_table(table, month) for table in MONTH_TABLES[:3]
            }
            # Late danmaku retries can change closed buckets without changing the
            # parent row. Read buckets in one batch rather than silently freezing
            # their values; unchanged response bodies are reused by PayloadBuilder.
            buckets = self.read_table("live_session_15m_stats", month)

            # Counts catch late commits/deletions; fingerprints catch in-place corrections.
            # Only changed rooms' SC lists are re-read, not the whole monthly SC payload.
            start, end = month_range(month)
            counts = self.query(
                "SELECT room_id, COUNT(*) AS count, MAX(id) AS last_id, "
                "BIT_XOR(CAST(CONV(SUBSTRING(SHA2(JSON_ARRAY("
                "id, send_time, uname, uid, price, message), 256), 1, 16), "
                "16, 10) AS UNSIGNED)) AS revision "
                "FROM super_chat_log WHERE send_time >= %s AND send_time < %s GROUP BY room_id",
                (start, end),
            )
            old_sc = self.sc if self.current_month == month else {}
            old_counts = self.sc_revisions if self.current_month == month else {}
            new_counts = {
                int(row["room_id"]): (
                    int(row["count"]),
                    int(row["last_id"]),
                    int(row["revision"]),
                )
                for row in counts
            }
            changed_rooms = {
                rid
                for rid in old_counts.keys() | new_counts.keys()
                if old_counts.get(rid) != new_counts.get(rid)
            }
            if self.current_month != month or discover:
                sc_rows = self.read_table("super_chat_log", month)
            elif changed_rooms:
                placeholders = ",".join(["%s"] * len(changed_rooms))
                sc_rows = self.query(
                    "SELECT * FROM super_chat_log WHERE send_time >= %s AND send_time < %s "
                    f"AND room_id IN ({placeholders})",
                    (start, end, *sorted(changed_rooms)),
                )
            else:
                sc_rows = []
        self.base = base
        self.buckets = {}
        for row in buckets:
            self.buckets.setdefault(int(row["session_id"]), []).append(row)
        self.sc = (
            {}
            if discover
            else {
                sid: row
                for sid, row in old_sc.items()
                if int(row["room_id"]) not in changed_rooms
            }
        )
        self.sc.update({int(row["id"]): row for row in sc_rows})
        self.sc_revisions = new_counts
        self.current_month = month
        tables["live_session_15m_stats"] = [
            row for rows in self.buckets.values() for row in rows
        ]
        tables["super_chat_log"] = list(self.sc.values())
        return Snapshot(month, stamp, base, tables, self.known_months())

    def history(self, month: str) -> Snapshot:
        stamp = self.store.clock()
        with self.transaction():
            tables = {table: self.read_table(table, month) for table in MONTH_TABLES}
        return Snapshot(month, stamp, self.base, tables, self.known_months())
