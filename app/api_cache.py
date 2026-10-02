"""Background cache lifecycle; hot snapshots take priority over historical repair."""

from __future__ import annotations

import threading
import time

import pymysql
import redis

from . import config
from .api_cache_payloads import PayloadBuilder
from .api_cache_source import SnapshotReader
from .api_cache_store import CacheStore, CacheUnavailable
from .repositories.tables import month_str, normalize_month_code


class ApiCache:
    def __init__(self, store=None, reader_factory=SnapshotReader):
        self.store = store if store is not None else CacheStore()
        self.stop = threading.Event()
        self.reader = reader_factory(self.store, self.stop)
        self.builder = PayloadBuilder()
        self.thread: threading.Thread | None = None
        self.history_due: dict[str, float] = {}
        self.history_summaries: dict[str, bytes] = {}
        self.last_hot = float("-inf")
        self.last_discovery = float("-inf")
        self.monthly_rows: dict[str, list[dict]] | None = None
        self.metrics = {
            "refreshes": 0,
            "failures": 0,
            "last_duration_seconds": 0.0,
            "snapshot_age_seconds": 0.0,
            "ready": False,
        }

    def start(self) -> None:
        self.thread = threading.Thread(
            target=self.run, name="api-cache-builder", daemon=True
        )
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                self.store.faults.record(
                    "shutdown-timeout", CacheUnavailable("builder did not stop")
                )
                return  # Do not close a connection still being used by another thread.
        self.store.client.close()

    def refresh_hot(self, *, discover: bool = False) -> set[str]:
        started = time.monotonic()
        snapshot = self.reader.current(discover=discover)
        self.store.publish(snapshot.month, self.builder.build(snapshot), snapshot.stamp)
        monthly_rows: dict[str, list[dict]] = {}
        for table in ("room_stats_monthly", "room_blind_box_monthly"):
            for row in snapshot.base[table]:
                monthly_rows.setdefault(row["month"], []).append(row)
        if self.monthly_rows is not None:
            for month in monthly_rows.keys() | self.monthly_rows.keys():
                if month != snapshot.month and monthly_rows.get(
                    month
                ) != self.monthly_rows.get(month):
                    self.history_due[month] = 0
        self.monthly_rows = monthly_rows
        # Reuse historical daily aggregates; latest room names, attention, monthly
        # totals and whale dirty state are refreshed without re-reading old SC tables.
        for month, (aggregates, rooms) in self.builder.history_summary_inputs.items():
            if month == snapshot.month:
                continue
            body = self.builder.summary_values(month, snapshot.base, aggregates, rooms)[
                "/gift/by_month"
            ]
            if self.history_summaries.get(month) != body:
                if not self.store.update_summary(month, body):
                    self.history_due[month] = 0
                    continue
                self.history_summaries[month] = body
        self.last_hot = time.monotonic()
        if discover:
            self.last_discovery = self.last_hot
        self.metrics.update(
            refreshes=self.metrics["refreshes"] + 1,
            last_duration_seconds=self.last_hot - started,
            snapshot_age_seconds=self.store.clock() - snapshot.stamp,
        )
        if self.metrics["snapshot_age_seconds"] > self.store.max_age:
            self.store.faults.record(
                "refresh-overdue",
                CacheUnavailable("snapshot exceeded freshness deadline"),
            )
        return snapshot.months

    def refresh_history(self, month: str) -> None:
        snapshot = self.reader.history(month)
        values = self.builder.build(snapshot)
        self.store.publish(month, values, snapshot.stamp)
        self.history_summaries[month] = values["/gift/by_month"]
        self.history_due[month] = time.monotonic() + config.API_CACHE_HISTORY_SECONDS

    def prewarm(self) -> None:
        self.store.begin_prewarm()
        months = self.refresh_hot(discover=True)
        pending = set(months) - {month_str()}
        while pending and not self.stop.is_set():
            month = min(pending)
            self.refresh_history(month)
            pending.remove(month)
            if time.monotonic() - self.last_hot >= config.API_CACHE_REFRESH_SECONDS:
                months = self.refresh_hot()
                pending.update(months - set(self.history_due) - {month_str()})
        if self.stop.is_set():
            raise CacheUnavailable("prewarm stopped")
        # A fresh hot generation must exist when the entire known domain becomes ready.
        self.refresh_hot()
        self.store.ready()
        self.metrics["ready"] = True

    def cycle(self) -> None:
        now = time.monotonic()
        self.store.renew()
        if now - self.last_hot >= config.API_CACHE_REFRESH_SECONDS:
            months = self.refresh_hot(discover=now - self.last_discovery >= 60)
            for month in months - {month_str()}:
                self.history_due.setdefault(month, 0)
            self.store.ready()
        dirty = self.store.client.smembers(self.store.key("dirty-months"))
        if dirty:
            # Archive tables may have been created since the last discovery.
            self.refresh_hot(discover=True)
            for value in dirty:
                month = value.decode()
                if month == "*":
                    self.history_due.update(
                        {m: 0 for m in self.reader.known_months() if m != month_str()}
                    )
                elif normalize_month_code(month) and month != month_str():
                    self.history_due[month] = 0
            self.store.client.srem(self.store.key("dirty-months"), *dirty)
        due = [
            (stamp, month)
            for month, stamp in self.history_due.items()
            if month != month_str() and stamp <= time.monotonic()
        ]
        if due:
            self.refresh_history(min(due)[1])

    def run(self) -> None:
        leader = False
        try:
            while not self.stop.is_set():
                try:
                    if not leader:
                        leader = self.store.acquire()
                        if leader:
                            self.prewarm()
                    else:
                        self.cycle()
                except (
                    redis.RedisError,
                    pymysql.MySQLError,
                    CacheUnavailable,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as exc:
                    self.metrics["failures"] += 1
                    self.store.faults.record("refresh", exc)
                    self.reader.close()
                    if leader:
                        try:
                            self.store.release()
                        except redis.RedisError as release_error:
                            self.store.faults.record("release", release_error)
                    leader = False
                    self.metrics["ready"] = False
                    self.stop.wait(2)
                self.stop.wait(0.2 if leader else 1)
        finally:
            self.reader.close()
            if leader:
                try:
                    self.store.release()
                except redis.RedisError as exc:
                    self.store.faults.record("release", exc)


def invalidate_history(month: str | None = None) -> None:
    """Archive jobs notify the builder without any API-side MySQL queries."""
    if not config.API_CACHE_ENABLED:
        return
    from . import api_app

    service = getattr(api_app.app.state, "api_cache", None)
    if service is None:
        return
    try:
        value = normalize_month_code(month) if month else "*"
        if value:
            service.store.client.sadd(service.store.key("dirty-months"), value)
    except redis.RedisError as exc:
        service.store.faults.record("history-invalidation", exc)
