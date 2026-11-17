"""Background cache lifecycle; hot snapshots take priority over historical repair."""

from __future__ import annotations

import threading
import time

import pymysql
import redis

from . import config
from .api_cache_payloads import PayloadBuilder
from .api_cache_source import Snapshot, SnapshotReader
from .api_cache_store import CacheStore, CacheUnavailable
from .logging_setup import api_log_context
from .repositories.tables import month_str, normalize_month_code

_pending_history: set[str] = set()
_pending_history_lock = threading.Lock()


def _flush_history_notifications(store) -> None:
    """Retry Redis failures without losing committed changes in this process."""
    with _pending_history_lock:
        pending = set(_pending_history)
        _pending_history.clear()
    for month in pending:
        try:
            store.mark_history_changed(month)
        except redis.RedisError as exc:
            with _pending_history_lock:
                _pending_history.add(month)
            store.faults.record("history-invalidation", exc)


class ApiCache:
    def __init__(self, store=None, reader_factory=SnapshotReader):
        self.store = store if store is not None else CacheStore()
        self.stop = threading.Event()
        self.reader = reader_factory(self.store, self.stop)
        self.builder = PayloadBuilder()
        self.thread: threading.Thread | None = None
        self.history_due: dict[str, float] = {}
        self.history_summaries: dict[str, bytes] = {}
        self.year_bodies: dict[str, dict[str, bytes]] = {}
        self.hot_snapshot: Snapshot | None = None
        self.last_hot = float("-inf")
        self.last_discovery = float("-inf")
        self.last_history_check = float("-inf")
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
        previous_month = self.hot_snapshot.month if self.hot_snapshot else None
        self.store.publish(snapshot.month, self.builder.build(snapshot), snapshot.stamp)
        if previous_month is not None and previous_month < snapshot.month:
            # Updating the old month's summary below is not a full historical read.
            # Refresh its closing rows and other payloads once, even without a dirty mark.
            self.history_due.setdefault(previous_month, 0)
        monthly_rows: dict[str, list[dict]] = {}
        for table in ("room_stats_monthly", "room_blind_box_monthly"):
            for row in snapshot.base[table]:
                monthly_rows.setdefault(row["month"], []).append(row)
        if self.monthly_rows is not None:
            for month in monthly_rows.keys() | self.monthly_rows.keys():
                if month != snapshot.month and monthly_rows.get(
                    month
                ) != self.monthly_rows.get(month):
                    self.store.mark_history_changed(month)
        self.monthly_rows = monthly_rows
        # Reuse historical daily aggregates; latest room names, attention, monthly
        # totals and whale dirty state are refreshed without re-reading old SC tables.
        for month, (aggregates, rooms) in self.builder.summary_inputs.items():
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
        self.hot_snapshot = snapshot
        self.refresh_years()
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

    def refresh_years(self, years: set[str] | None = None) -> None:
        """Publish annual summaries using retained month inputs; never call the reader."""
        snapshot = self.hot_snapshot
        if snapshot is None:
            return
        current = month_str()
        known = snapshot.months | set(self.builder.summary_inputs)
        candidates = years if years is not None else {month[:4] for month in known} | {current[:4]}
        for year in sorted(candidates):
            if len(year) != 4 or not year.isascii() or not year.isdecimal() or not "0001" <= year <= current[:4]:
                continue
            cutoff = current if year == current[:4] else year + "12"
            expected = {month for month in known if month[:4] == year and month <= cutoff}
            # A newly discovered archive must be warmed before its year's publication.
            if expected - self.builder.summary_inputs.keys():
                self.store.expect_year(year)
                continue
            values = self.builder.year_values(year, snapshot.base)
            if (
                year == current[:4]
                or self.year_bodies.get(year) != values
                or not self.store.year_exists(year)
            ):
                self.store.publish_year(year, values, snapshot.stamp)
                self.year_bodies[year] = values

    def verify_historical_years(self) -> None:
        """Repair damaged annual generations on the history poll, without SQL."""
        if self.hot_snapshot is None:
            return
        current_year = month_str()[:4]
        for year, values in self.year_bodies.items():
            if year < current_year and not self.store.year_is_intact(year, values):
                self.store.publish_year(year, values, self.hot_snapshot.stamp)

    def refresh_history(self, month: str) -> None:
        revision = self.store.history_revision(month)
        snapshot = self.reader.history(month)
        values = self.builder.build(snapshot)
        self.store.publish(month, values, snapshot.stamp)
        self.history_summaries[month] = values["/gift/by_month"]
        self.refresh_years({month[:4]})
        self.store.acknowledge_history(month, revision)
        self.history_due.pop(month, None)

    def prewarm(self) -> None:
        self.store.begin_prewarm()
        self.history_due.clear()
        self.history_summaries.clear()
        self.year_bodies.clear()
        self.hot_snapshot = None
        self.builder = PayloadBuilder()
        self.monthly_rows = None
        _flush_history_notifications(self.store)
        wildcard_revision = self.store.history_revision("*")
        months = self.refresh_hot(discover=True)
        months.update(
            value.decode() for value in self.store.client.smembers(self.store.key("dirty-months"))
            if normalize_month_code(value.decode())
        )
        pending = set(months) - {month_str()}
        while not self.stop.is_set():
            if pending:
                month = min(pending)
                self.refresh_history(month)
                pending.remove(month)
            if not pending or time.monotonic() - self.last_hot >= config.API_CACHE_REFRESH_SECONDS:
                # Finish with a fresh hot generation; if it crossed a month boundary,
                # drain the resulting historical work before marking the cache ready.
                months = self.refresh_hot()
                pending.update(months - self.history_summaries.keys())
                pending.update(self.history_due)
                pending.discard(month_str())
                if not pending:
                    break
        if self.stop.is_set():
            raise CacheUnavailable("prewarm stopped")
        # Startup read every known month, so the legacy wildcard is also covered.
        self.store.acknowledge_history("*", wildcard_revision)
        self.last_history_check = time.monotonic()
        self.store.ready()
        self.metrics["ready"] = True

    def cycle(self) -> None:
        now = time.monotonic()
        self.store.renew()
        if now - self.last_hot >= config.API_CACHE_REFRESH_SECONDS:
            months = self.refresh_hot(discover=now - self.last_discovery >= 60)
            for month in months - {month_str()}:
                if month not in self.history_summaries:
                    self.history_due.setdefault(month, 0)
            self.store.ready()
        _flush_history_notifications(self.store)
        if time.monotonic() - self.last_history_check >= config.API_CACHE_HISTORY_SECONDS:
            dirty = self.store.client.smembers(self.store.key("dirty-months"))
            if dirty:
                # Redis names the changed months; discovery sees newly created archives.
                self.refresh_hot(discover=True)
                for value in dirty:
                    month = value.decode()
                    if month == "*":
                        revision = self.store.history_revision("*")
                        for known in self.reader.known_months() - {month_str()}:
                            self.store.mark_history_changed(known)
                            self.history_due.setdefault(known, 0)
                        self.store.acknowledge_history("*", revision)
                    elif normalize_month_code(month) and month != month_str():
                        self.history_due.setdefault(month, 0)
            self.verify_historical_years()
            self.last_history_check = time.monotonic()
        due = [
            (stamp, month)
            for month, stamp in self.history_due.items()
            if month != month_str() and stamp <= time.monotonic()
        ]
        if due:
            self.refresh_history(min(due)[1])

    def run(self) -> None:
        with api_log_context():
            self._run()

    def _run(self) -> None:
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
    """Call after commit; DB2 retains months until their refresh succeeds."""
    if not config.API_CACHE_ENABLED:
        return
    value = normalize_month_code(month) if month else "*"
    if not value or value == month_str():
        return
    with _pending_history_lock:
        _pending_history.add(value)
    from . import api_app

    service = getattr(api_app.app.state, "api_cache", None)
    if service is None:
        return
    _flush_history_notifications(service.store)
