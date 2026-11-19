"""Opt-in disposable MySQL 5.7.44 / Redis 8.0.5 tests; never use production env.

VR_CACHE_TEST_MYSQL_PORT and VR_CACHE_TEST_REDIS_PORT must point to explicitly
created loopback test containers. The database name is fixed to vr_cache_test.
"""

from __future__ import annotations

import datetime
import gzip
import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import redis
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app import api_app, config, gift, runtime_state, whale_metrics
from app.api_cache import ApiCache
from app.api_cache_http import LIVE_SESSIONS_WITH_STATS_CACHE_PATH, CacheMiddleware
from app.api_cache_source import BudgetConnection, SnapshotReader
from app.api_cache_store import CacheStore, CacheUnavailable
from app.database import Base
from app.models import (
    Attention,
    LiveSession,
    LiveSession15mStats,
    RoomEntryLog,
    RoomInfo,
    RoomLiveStats,
    RoomStatsMonthly,
    SuperChatLog,
)
from app.repositories.tables import month_range, month_str


@pytest.fixture
def services(monkeypatch):
    mysql_port = os.getenv("VR_CACHE_TEST_MYSQL_PORT")
    redis_port = os.getenv("VR_CACHE_TEST_REDIS_PORT")
    if not mysql_port or not redis_port:
        pytest.skip(
            "requires explicitly provisioned disposable MySQL/Redis test containers"
        )
    if not mysql_port.isdigit() or not redis_port.isdigit():
        pytest.fail("test ports must be numeric loopback ports")
    engine = create_engine(
        f"mysql+pymysql://root:vr_cache_test@127.0.0.1:{int(mysql_port)}/vr_cache_test"
    )
    from conftest import _ORIG_CREATE_ALL, _ORIG_INSPECT

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT VERSION()")).startswith("5.7.44")
    client = redis.Redis(host="127.0.0.1", port=int(redis_port), db=2)
    assert client.info("server")["redis_version"] == "8.0.5"
    # Fixed test database only; no global Redis flush and no production DSN.
    Base.metadata.drop_all(engine)
    _ORIG_CREATE_ALL(Base.metadata, engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(gift, "Session", Session)
    monkeypatch.setattr(api_app, "engine", engine)
    monkeypatch.setattr(api_app, "inspect", _ORIG_INSPECT)
    monkeypatch.setattr(
        config,
        "DB_CONFIG",
        {
            "host": "127.0.0.1",
            "port": int(mysql_port),
            "user": "root",
            "password": "vr_cache_test",
            "db": "vr_cache_test",
        },
    )
    monkeypatch.setattr(
        whale_metrics,
        "_client",
        redis.Redis(host="127.0.0.1", port=int(redis_port), db=1),
    )
    prefix = "integration:api:" + uuid.uuid4().hex
    store = CacheStore(client, prefix=prefix)
    assert store.acquire()
    start_date, _ = month_range(month_str())
    start = datetime.datetime.combine(start_date, datetime.time.min)
    with Session.begin() as session:
        session.add_all(
            [
                RoomInfo(
                    room_id=111111, anchor_name="Integration Anchor", attention=12345
                ),
                RoomStatsMonthly(
                    room_id=111111,
                    month=month_str(),
                    gift=12.5,
                    payer_count=2,
                    danmaku_count=9,
                ),
                RoomStatsMonthly(room_id=111111, month="190001", gift=1.5),
                RoomLiveStats(
                    room_id=111111,
                    date=start_date,
                    duration=7200,
                    gift=12.5,
                    steel_coin_count=3,
                ),
                Attention(
                    room_id=111111,
                    date=start_date,
                    attention=12345,
                    guard_1=4,
                    fans_count=5,
                ),
                LiveSession(
                    id=1,
                    room_id=111111,
                    month=month_str(),
                    start_time=start,
                    gift=12.5,
                    payer_count=2,
                ),
                LiveSession15mStats(
                    room_id=111111,
                    month=month_str(),
                    session_id=1,
                    bucket_index=0,
                    start_time=start,
                    end_time=start + datetime.timedelta(minutes=15),
                    gift=12.5,
                ),
                SuperChatLog(
                    id=10,
                    room_id=111111,
                    uid=123,
                    uname="test",
                    price=30,
                    message="test SC",
                    send_time=start + datetime.timedelta(seconds=1),
                ),
            ]
        )
    monkeypatch.setitem(runtime_state.LAST_STATUS, 111111, 1)
    monkeypatch.setitem(runtime_state.CURRENT_SESSIONS, 111111, 1)
    monkeypatch.setitem(
        runtime_state.CONCURRENCY_CACHE,
        111111,
        {"session_id": 1, "samples": 2, "total": 30, "max": 20, "last": 20},
    )
    yield store, engine, Session, start
    store.release()
    # Exact prefix ownership; leave any other logical DBs and services untouched.
    owned = list(client.scan_iter(match=store.key("*")))
    if owned:
        client.delete(*owned)
    client.close()
    Base.metadata.drop_all(engine)
    engine.dispose()


def test_mysql_redis_prewarm_matches_all_five_public_payloads_and_sql_budget(
    services, monkeypatch
):
    store, engine, _Session, _start = services
    statements = []

    class ObservedConnection(BudgetConnection):
        def _execute_command(self, command, sql):
            result = super()._execute_command(command, sql)
            if command == 3:
                statements.append(time.monotonic())
            return result

    cache = ApiCache(
        store, lambda target, stop: SnapshotReader(target, stop, ObservedConnection)
    )
    cache.prewarm()
    paths = [
        "/gift",
        "/gift/by_month",
        "/gift/live_sessions?room_id=111111",
        "/gift/attention?room_id=111111",
        "/gift/sc?room_id=111111",
        "/gift/by_month?month=190001",
        "/gift/sc?room_id=111111&month=190001",
    ]
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    with TestClient(api_app.app) as legacy:
        original = [legacy.get(path).json() for path in paths]
    app = FastAPI()
    app.router.routes.extend(api_app.app.router.routes)
    app.state.api_cache = cache
    app.add_middleware(CacheMiddleware, owner=app)
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    monkeypatch.setattr(
        gift, "Session", lambda: pytest.fail("cache HTTP reads must have zero SQL")
    )
    before = len(statements)
    with TestClient(app, client=("203.0.113.1", 123)) as client:
        cached = [client.get(path).json() for path in paths]
    assert cached == original
    assert len(statements) == before
    assert (
        max(
            sum(0 <= stamp - earlier < 1 for earlier in statements)
            for stamp in statements
        )
        <= 9
    )
    assert engine.pool.checkedout() == 0
    cache.reader.close()


def test_real_incremental_sc_catches_lower_id_late_commit_and_closed_bucket_update(
    services,
):
    store, _engine, Session, start = services
    reader = SnapshotReader(store, threading.Event())
    reader.current(discover=True)
    with Session.begin() as session:
        session.add(
            SuperChatLog(
                id=5,
                room_id=111111,
                uid=456,
                uname="late",
                price=30,
                message="lower ID commits later",
                send_time=start,
            )
        )
        row = session.get(LiveSession, 1)
        row.end_time = start + datetime.timedelta(hours=1)
    updated = reader.current()
    assert {row["id"] for row in updated.tables["super_chat_log"]} == {5, 10}
    with Session.begin() as session:
        session.get(LiveSession15mStats, (1, 0)).normal_danmaku_count = 42
        session.get(SuperChatLog, 10).message = "corrected without changing count or ID"
    updated = reader.current()
    assert updated.tables["live_session_15m_stats"][0]["normal_danmaku_count"] == 42
    assert (
        next(
            row["message"]
            for row in updated.tables["super_chat_log"]
            if row["id"] == 10
        )
        == "corrected without changing count or ID"
    )
    reader.close()


def test_real_annual_cache_totals_and_history_changes_add_no_http_sql(services, monkeypatch):
    from app.repositories import monthly

    store, _engine, Session, _start = services
    monkeypatch.setattr(monthly, "Session", Session)
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    with Session.begin() as session:
        session.add_all([
            RoomStatsMonthly(room_id=111111, month="200101", gift=10.1, danmaku_count=10),
            RoomStatsMonthly(room_id=111111, month="200102", gift=20.2, danmaku_count=20),
            RoomLiveStats(room_id=111111, date=datetime.date(2001, 1, 1), duration=7200),
            RoomLiveStats(room_id=111111, date=datetime.date(2001, 2, 1), duration=10800),
        ])
    statements = []
    history_reads = []

    class Connection(BudgetConnection):
        def _execute_command(self, command, sql):
            result = super()._execute_command(command, sql)
            if command == 3:
                statements.append(time.monotonic())
            return result

    class Reader(SnapshotReader):
        def history(self, month):
            history_reads.append(month)
            return super().history(month)

    cache = ApiCache(store, lambda target, stop: Reader(target, stop, Connection))
    monkeypatch.setattr(api_app.app.state, "api_cache", cache, raising=False)
    app = FastAPI()
    app.router.routes.extend(api_app.app.router.routes)
    app.state.api_cache = cache
    app.add_middleware(CacheMiddleware, owner=app)
    try:
        cache.prewarm()
        history_reads.clear()
        before = len(statements)
        monkeypatch.setattr(gift, "Session", lambda: pytest.fail("annual HTTP cannot read MySQL"))
        with TestClient(app, client=("203.0.113.27", 123)) as client:
            result = client.get("/gift/by_month?year=2001")
            assert result.status_code == 200
            row = next(row for row in result.json() if row["room_id"] == 111111)
            assert row["gift"] == 30.3 and row["live_duration"] == "05:00:00"
            assert row["effective_days"] == 2 and row["danmaku"]["total"] == 30
            assert row["payer_count"] is row["whale_dependency"] is row["steel_coin_count"] is None
            assert row["year"] == "2001" and "month" not in row
            assert len(statements) == before
            cache.refresh_hot()
            assert history_reads == []
            monthly.add_room_stats_amounts(RoomStatsMonthly, 111111, "200101", gift=5)
            cache.last_history_check -= 301
            cache.cycle()
            assert history_reads == ["200101"]
            before = len(statements)
            row = next(row for row in client.get("/gift/by_month?year=2001").json() if row["room_id"] == 111111)
            assert row["gift"] == 35.3 and len(statements) == before
        assert max(sum(0 <= stamp - earlier < 1 for earlier in statements) for stamp in statements) <= 9
    finally:
        cache.reader.close()


@pytest.fixture
def crossmonth_sessions(services, monkeypatch):
    from conftest import _ORIG_INSPECT
    from app import archive_service
    from app.repositories import live_session_snapshots, live_sessions, session_15m, tables

    store, engine, Session, _start = services
    for module in (archive_service, live_sessions, live_session_snapshots, session_15m):
        monkeypatch.setattr(module, "Session", Session)
    for module in (archive_service, tables):
        monkeypatch.setattr(module, "engine", engine)
        monkeypatch.setattr(module, "inspect", _ORIG_INSPECT)
    monkeypatch.setattr(session_15m, "inspect", _ORIG_INSPECT)
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    cache = ApiCache(store)
    monkeypatch.setattr(api_app.app.state, "api_cache", cache, raising=False)
    with Session.begin() as session:
        session.add(RoomStatsMonthly(room_id=111111, month="200101", gift=10))
        for ident, title, end, gift_amount in (
            (11, "archived", datetime.datetime(2001, 1, 31, 23, 0), 1),
            (12, "cross-month", None, 9),
        ):
            start = datetime.datetime(2001, 1, 31, 22, 0 if ident == 11 else 30)
            session.add(LiveSession(
                id=ident, room_id=111111, month="200101", start_time=start,
                end_time=end, title=title, gift=gift_amount, normal_danmaku_count=42,
            ))
            session.add(LiveSession15mStats(
                session_id=ident, bucket_index=0, room_id=111111, month="200101",
                start_time=start, end_time=start + datetime.timedelta(minutes=15),
                gift=gift_amount, normal_danmaku_count=42,
            ))
    yield cache, engine, Session
    cache.reader.close()
    with engine.begin() as connection:
        for prefix in ("live_session", "live_session_15m_stats"):
            connection.execute(text(f"DROP TABLE IF EXISTS `{prefix}_200101`"))


def test_real_crossmonth_session_stays_cached_before_and_after_late_archive(crossmonth_sessions):
    from app import archive_service
    from app.repositories import live_session_snapshots, live_sessions

    cache, engine, _Session = crossmonth_sessions
    # Another session is already archived while the cross-month stream stays hot.
    assert archive_service.archive_live_session("200101", session_ids=(11,)) == 1
    cache.prewarm()

    def payload():
        body = cache.store.read("200101", LIVE_SESSIONS_WITH_STATS_CACHE_PATH, 111111)
        return json.loads(gzip.decompress(body))

    rows = payload()["sessions"]
    assert {row["title"] for row in rows} == {"archived", "cross-month"}
    live = next(row for row in rows if row["title"] == "cross-month")
    assert live["end_time"] is None and live["stats_15m"][0]["normal_danmaku_count"] == 42
    end = datetime.datetime(2001, 2, 1, 0, 30)
    live_sessions.close_session_by_id(LiveSession, 12, end)
    live_session_snapshots.update_end_counts(LiveSession, 12, guard_1=7)
    cache.last_history_check -= 301
    cache.cycle()
    before_archive = payload()
    ended = next(row for row in before_archive["sessions"] if row["title"] == "cross-month")
    assert ended["end_time"] == "2001-02-01 00:30:00" and ended["end_guard_1"] == 7
    previous = cache.store.client.get(cache.store.key("month:200101"))
    assert archive_service.archive_live_session("200101", session_ids=(12,)) == 1
    assert cache.store.client.smembers(cache.store.key("dirty-months")) == {b"200101"}
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM live_session WHERE month='200101'")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM live_session_15m_stats WHERE month='200101'")) == 0
    cache.last_history_check -= 301
    cache.cycle()
    assert payload() == before_archive
    assert cache.store.client.get(cache.store.key("month:200101")) != previous
    assert not cache.store.client.smembers(cache.store.key("dirty-months"))


@pytest.mark.parametrize("archive_parent", [False, True])
def test_real_late_danmaku_updates_archived_bucket_without_shadowing_totals(
    crossmonth_sessions, archive_parent,
):
    from app import DanmakuBucketTarget, DanmakuCounts, archive_service

    cache, engine, _session_factory = crossmonth_sessions
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE live_session_15m_stats SET guard=3, payer_count=4, danmaku_count=100 "
            "WHERE session_id=11 AND bucket_index=0"
        ))
    archive = archive_service.archive_live_session if archive_parent else archive_service.archive_live_session_15m_stats
    assert archive("200101", session_ids=(11,)) == 1
    cache.prewarm()
    start = datetime.datetime(2001, 1, 31, 22)
    target = DanmakuBucketTarget(
        session_id=11, room_id=111111, month="200101", bucket_index=0,
        start_time=start, end_time=start + datetime.timedelta(minutes=15),
    )
    assert LiveSession15mStats.add_danmaku_counts(target, DanmakuCounts(total=2, normal=2))
    assert LiveSession15mStats.add_danmaku_counts(target, DanmakuCounts(total=3, normal=3))
    # A full stats replay replaces metrics but must not reset archived danmaku.
    assert LiveSession15mStats.upsert(
        session_id=11, room_id=111111, month="200101", bucket_index=0,
        start_time=start, end_time=target.end_time, gift=6, guard=3, super_chat=0,
        blind_box_count=0, blind_box_profit=0, danmaku_count=0,
        captain_danmaku_count=0, admiral_danmaku_count=0, governor_danmaku_count=0,
        normal_danmaku_count=0, avg_concurrency=None, max_concurrency=None,
        sample_count=0, payer_count=4,
    )
    if not archive_parent:
        # The parent can finish archiving without recreating or losing its bucket.
        assert archive_service.archive_live_session("200101", session_ids=(11,)) == 1
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM live_session_15m_stats WHERE session_id=11")) == 0
        stored = connection.execute(text(
            "SELECT gift, guard, payer_count, danmaku_count, normal_danmaku_count "
            "FROM live_session_15m_stats_200101 WHERE session_id=11 AND bucket_index=0"
        )).one()
        assert tuple(stored) == (6, 3, 4, 105, 47)
    cache.last_history_check -= 301
    cache.cycle()
    body = cache.store.read("200101", LIVE_SESSIONS_WITH_STATS_CACHE_PATH, 111111)
    payload = json.loads(gzip.decompress(body))
    bucket = next(row for row in payload["sessions"] if row["title"] == "archived")["stats_15m"][0]
    assert (bucket["gift"], bucket["guard"], bucket["danmaku_count"], bucket["normal_danmaku_count"]) == (6, 3, 105, 47)


def test_real_late_bucket_writer_serializes_with_archive_copy_delete(crossmonth_sessions):
    from sqlalchemy import event

    from app import DanmakuBucketTarget, DanmakuCounts, archive_service

    _cache, engine, _session_factory = crossmonth_sessions
    copying = threading.Event()
    writing = threading.Event()
    allow_delete = threading.Event()

    def gate(_conn, _cursor, statement, _parameters, _context, _executemany):
        if statement.startswith("DELETE stats FROM `live_session_15m_stats`"):
            copying.set()
            assert allow_delete.wait(5)
        elif statement.startswith("SELECT id FROM live_session WHERE id = "):
            writing.set()

    start = datetime.datetime(2001, 1, 31, 22)
    target = DanmakuBucketTarget(
        session_id=11, room_id=111111, month="200101", bucket_index=0,
        start_time=start, end_time=start + datetime.timedelta(minutes=15),
    )
    event.listen(engine, "before_cursor_execute", gate)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            archive = pool.submit(archive_service.archive_live_session_15m_stats, "200101", session_ids=(11,))
            try:
                assert copying.wait(5)
                writer = pool.submit(LiveSession15mStats.add_danmaku_counts, target, DanmakuCounts(total=9, normal=9))
                assert writing.wait(5)
                assert not writer.done()
            finally:
                allow_delete.set()
            assert archive.result(timeout=5) == 1
            assert writer.result(timeout=5)
    finally:
        event.remove(engine, "before_cursor_execute", gate)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM live_session_15m_stats WHERE session_id=11")) == 0
        assert connection.scalar(text(
            "SELECT normal_danmaku_count FROM live_session_15m_stats_200101 WHERE session_id=11 AND bucket_index=0"
        )) == 51


@pytest.mark.parametrize("bucket_gift", [0, 1])
def test_real_archive_keeps_parent_corrections_and_identical_duplicate_buckets(
    crossmonth_sessions, bucket_gift,
):
    from app import archive_service

    _cache, engine, _session_factory = crossmonth_sessions
    with engine.begin() as connection:
        connection.execute(text("UPDATE live_session_15m_stats SET gift=:gift WHERE session_id=11"), {"gift": bucket_gift})
        for prefix, identity in (("live_session", "id"), ("live_session_15m_stats", "session_id")):
            archive = prefix + "_200101"
            connection.execute(text(f"CREATE TABLE `{archive}` LIKE `{prefix}`"))
            connection.execute(text(f"INSERT INTO `{archive}` SELECT * FROM `{prefix}` WHERE `{identity}`=11"))
        connection.execute(text("UPDATE live_session SET gift=9 WHERE id=11"))
    assert archive_service.archive_live_session("200101", session_ids=(11,)) == 1
    with engine.connect() as connection:
        for prefix, identity in (("live_session", "id"), ("live_session_15m_stats", "session_id")):
            assert connection.scalar(text(f"SELECT COUNT(*) FROM `{prefix}` WHERE `{identity}`=11")) == 0
            expected = 9 if prefix == "live_session" else bucket_gift
            assert connection.scalar(text(f"SELECT gift FROM `{prefix}_200101` WHERE `{identity}`=11")) == expected


@pytest.mark.parametrize("conflict", ["legacy_stub", "full_correction", "null_optional"])
def test_real_archive_preserves_both_copies_of_ambiguous_existing_buckets(
    crossmonth_sessions, monkeypatch, caplog, conflict,
):
    from app import DanmakuBucketTarget, DanmakuCounts, archive_service
    from app.repositories import session_15m

    _cache, engine, _session_factory = crossmonth_sessions
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE live_session_15m_stats SET gift=100, guard=30, payer_count=4, "
            "danmaku_count=100 WHERE session_id=11 AND bucket_index=0"
        ))
    assert archive_service.archive_live_session_15m_stats("200101", session_ids=(11,)) == 1
    if conflict == "legacy_stub":
        start = datetime.datetime(2001, 1, 31, 22)
        target = DanmakuBucketTarget(11, 111111, "200101", 0, start, start + datetime.timedelta(minutes=15))
        # Exercise the old writer's INSERT path: no lookup of an archived bucket.
        with monkeypatch.context() as old_writer:
            old_writer.setattr(session_15m, "_find_archived_bucket", lambda *_args: None)
            assert LiveSession15mStats.add_danmaku_counts(target, DanmakuCounts(total=2, normal=2))
    else:
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO live_session_15m_stats SELECT * FROM live_session_15m_stats_200101"
            ))
            column, value = ("gift", 9) if conflict == "full_correction" else ("avg_concurrency", 0)
            connection.execute(text(
                f"UPDATE live_session_15m_stats SET `{column}`=:value WHERE session_id=11"
            ), {"value": value})
    # A conflict must also preserve other eligible rows in this month's batch.
    with engine.begin() as connection:
        connection.execute(text("UPDATE live_session SET end_time='2001-01-31 23:00:00' WHERE id=12"))

    def saved_rows():
        with engine.connect() as connection:
            return {
                table: [dict(row) for row in connection.execute(text(
                    f"SELECT * FROM `{table}` ORDER BY 1, 2"
                )).mappings()]
                for table in ("live_session", "live_session_15m_stats", "live_session_15m_stats_200101")
            }

    before = saved_rows()
    for _ in range(2):
        assert archive_service.archive_live_session("200101") == 0
        assert saved_rows() == before
    assert "保留两侧并暂缓本月归档" in caplog.text


@pytest.mark.parametrize("archive_parent", [False, True])
@pytest.mark.parametrize("legacy_columns", [False, True])
def test_real_readonly_overlap_audit_includes_orphan_hot_buckets(
    crossmonth_sessions, monkeypatch, archive_parent, legacy_columns,
):
    from app import DanmakuBucketTarget, DanmakuCounts, archive_service
    from app.repositories import session_15m

    _cache, engine, _session_factory = crossmonth_sessions
    archive = archive_service.archive_live_session if archive_parent else archive_service.archive_live_session_15m_stats
    assert archive("200101", session_ids=(11,)) == 1
    if legacy_columns:
        with engine.begin() as connection:
            connection.execute(text(
                "ALTER TABLE live_session_15m_stats_200101 DROP COLUMN payer_count, DROP COLUMN danmaku_count"
            ))
    start = datetime.datetime(2001, 1, 31, 22)
    target = DanmakuBucketTarget(11, 111111, "200101", 0, start, start + datetime.timedelta(minutes=15))
    with monkeypatch.context() as old_writer:
        old_writer.setattr(session_15m, "_find_archived_bucket", lambda *_args: None)
        assert LiveSession15mStats.add_danmaku_counts(target, DanmakuCounts(total=2, normal=2))
    generator = Path("scripts/audit_session_bucket_overlaps.sql").read_text(encoding="utf-8")
    with engine.connect() as connection:
        connection.exec_driver_sql("START TRANSACTION READ ONLY")
        queries = list(connection.execute(text(generator)).scalars())
        assert len(queries) == 1
        rows = list(connection.execute(text(queries[0])).mappings())
        assert len(rows) == 1
        row = rows[0]
        assert (row["session_id"], row["bucket_index"]) == (11, 0)
        assert row["hot_gift"] == 0 and row["archived_gift"] == 1
        assert (row["hot_parent_end_time"] is None) == archive_parent
        assert (row["archived_payer_count"] is None) == legacy_columns
        assert (row["archived_danmaku_count"] is None) == legacy_columns
        connection.rollback()


def test_real_session_merge_tolerates_legacy_columns_and_deduplicates_hot_corrections(crossmonth_sessions):
    from conftest import _ORIG_INSPECT

    cache, engine, _Session = crossmonth_sessions
    # Populate discovery first, then create archives with older column sets.
    cache.reader.current(discover=True)
    with engine.begin() as connection:
        for prefix, identity in (("live_session", "id"), ("live_session_15m_stats", "session_id")):
            archive = prefix + "_200101"
            connection.execute(text(f"CREATE TABLE `{archive}` LIKE `{prefix}`"))
            connection.execute(text(f"INSERT INTO `{archive}` SELECT * FROM `{prefix}` WHERE month='200101'"))
            connection.execute(text(f"DELETE FROM `{prefix}` WHERE `{identity}`=11"))
            connection.execute(text(f"UPDATE `{archive}` SET gift=1 WHERE `{identity}`=12"))
            connection.execute(text(f"ALTER TABLE `{archive}` DROP COLUMN normal_danmaku_count"))
        archive = "live_session_15m_stats_200101"
        columns = [column["name"] for column in _ORIG_INSPECT(engine).get_columns(archive)]
        names = ", ".join(f"`{column}`" for column in columns)
        expressions = ", ".join("1" if column == "bucket_index" else f"`{column}`" for column in columns)
        connection.execute(text(
            f"INSERT INTO `{archive}` ({names}) SELECT {expressions} FROM `{archive}` "
            "WHERE session_id=12 AND bucket_index=0"
        ))
    cache.prewarm()
    body = cache.store.read("200101", LIVE_SESSIONS_WITH_STATS_CACHE_PATH, 111111)
    rows = json.loads(gzip.decompress(body))["sessions"]
    assert len(rows) == 2
    old = next(row for row in rows if row["title"] == "archived")
    hot = next(row for row in rows if row["title"] == "cross-month")
    assert old["normal_danmaku_count"] is None
    assert old["stats_15m"][0]["normal_danmaku_count"] is None
    assert hot["gift"] == 9 and hot["normal_danmaku_count"] == 42
    assert [bucket["bucket_index"] for bucket in hot["stats_15m"]] == [0, 1]
    assert hot["stats_15m"][0]["gift"] == 9
    assert hot["stats_15m"][0]["normal_danmaku_count"] == 42


@pytest.mark.parametrize("prefix", ["live_session", "live_session_15m_stats"])
@pytest.mark.parametrize("create_during_read", [False, True])
def test_real_session_copy_delete_committing_during_read_has_no_gap_or_duplicates(
    crossmonth_sessions, monkeypatch, prefix, create_during_read
):
    cache, engine, _Session = crossmonth_sessions
    reader = cache.reader
    archive = prefix + "_200101"
    if not create_during_read:
        with engine.begin() as connection:
            connection.execute(text(f"CREATE TABLE `{archive}` LIKE `{prefix}`"))
    original = reader.query
    moved = []

    def query(sql, params=None):
        rows = original(sql, params)
        if sql == f"SELECT * FROM `{prefix}` WHERE month=%s" and not moved:
            with engine.begin() as connection:
                if create_during_read:
                    connection.execute(text(f"CREATE TABLE `{archive}` LIKE `{prefix}`"))
                connection.execute(text(f"INSERT INTO `{archive}` SELECT * FROM `{prefix}` WHERE month='200101'"))
                connection.execute(text(f"DELETE FROM `{prefix}` WHERE month='200101'"))
            moved.append(True)
        return rows

    monkeypatch.setattr(reader, "query", query)
    rows = reader.history("200101").tables[prefix]
    identity = "id" if prefix == "live_session" else "session_id"
    assert len(rows) == 2 and {row[identity] for row in rows} == {11, 12}
    assert moved == [True]
    # The next snapshot sees the committed rows on the archive side as well.
    rows = reader.history("200101").tables[prefix]
    assert len(rows) == 2 and {row[identity] for row in rows} == {11, 12}


def test_real_db2_history_changes_skip_unchanged_months_and_keep_concurrent_write(
    services, monkeypatch
):
    from conftest import _ORIG_INSPECT
    from app.repositories import super_chat, tables

    store, engine, Session, _start = services
    monkeypatch.setattr(super_chat, "Session", Session)
    monkeypatch.setattr(tables, "engine", engine)
    monkeypatch.setattr(tables, "inspect", _ORIG_INSPECT)
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    cache = ApiCache(store)
    monkeypatch.setattr(api_app.app.state, "api_cache", cache, raising=False)
    archive = "super_chat_log_200101"
    with Session.begin() as session:
        session.add_all([
            RoomStatsMonthly(room_id=111111, month="200101", gift=0),
            RoomStatsMonthly(room_id=111111, month="200102", gift=0),
        ])
    calls = []
    original = cache.reader.history
    concurrent = [False]

    def write(message):
        super_chat.log_super_chat(
            SuperChatLog, 111111, "test", 123, 30, message,
            datetime.datetime(2001, 1, 2),
        )

    def observed(month):
        calls.append(month)
        value = original(month)
        if concurrent[0]:
            concurrent[0] = False
            write("concurrent")
        return value

    try:
        cache.prewarm()
        monkeypatch.setattr(cache.reader, "history", observed)
        # An elapsed five-minute interval with no changes reads no history.
        cache.last_history_check -= 301
        cache.cycle()
        assert calls == []
        write("first")
        assert store.client.smembers(store.key("dirty-months")) == {b"200101"}
        assert store.client.zscore(store.key("history-changed-at"), "200101")
        assert store.client.connection_pool.connection_kwargs["db"] == 2
        concurrent[0] = True
        cache.last_history_check -= 301
        cache.cycle()
        assert calls == ["200101"]
        assert store.client.smembers(store.key("dirty-months")) == {b"200101"}
        cache.last_history_check -= 301
        cache.cycle()
        assert calls == ["200101", "200101"]
        assert not store.client.smembers(store.key("dirty-months"))
        payload = json.loads(gzip.decompress(store.read("200101", "/gift/sc", 111111)))
        assert {row["message"] for row in payload["list"]} == {"first", "concurrent"}
        cache.last_history_check -= 301
        cache.cycle()
        assert calls == ["200101", "200101"]
    finally:
        cache.reader.close()
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS `{archive}`"))


def test_real_entry_writes_archive_rollback_late_duplicates_and_cached_queries(services, monkeypatch):
    from sqlalchemy import event
    from sqlalchemy.exc import SQLAlchemyError

    from app.event_ingestion import now_local
    from app.repositories import entries

    store, engine, Session, _start = services
    monkeypatch.setattr(entries, "engine", engine)
    monkeypatch.setattr(entries, "Session", Session)
    monkeypatch.setattr(entries, "ARCHIVE_BATCH_SIZE", 2)
    recent = now_local().replace(microsecond=123000) - datetime.timedelta(minutes=1)
    old = datetime.datetime.fromisoformat("2001-01-02T00:00:00")
    old_rows = [{"room_id": 111111, "uid": 7, "event_time": old + datetime.timedelta(seconds=index)} for index in range(3)]
    rows = [{"room_id": 111111, "uid": 7, "event_time": recent}, *old_rows]
    archive = "room_entry_log_200101"
    cache = ApiCache(store)
    try:
        entries.save_entries([*rows, rows[0]])
        with Session() as session:
            assert session.query(RoomEntryLog).count() == 4
            assert session.get(RoomEntryLog, (111111, recent, 7)).event_time.microsecond == 123000

        def fail_delete(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("DELETE FROM room_entry_log"):
                raise SQLAlchemyError("synthetic archive interruption")

        event.listen(engine, "before_cursor_execute", fail_delete)
        try:
            assert entries.archive_entries("200101") == 0
        finally:
            event.remove(engine, "before_cursor_execute", fail_delete)
        with Session() as session:
            assert session.query(RoomEntryLog).count() == 4
            assert session.scalar(text(f"SELECT COUNT(*) FROM `{archive}`")) == 0
        assert entries.archive_entries("200101") == 3
        assert entries.archive_entries("200101") == 0
        # A replay can live in the hot table while its original is already archived.
        entries.save_entries([old_rows[0]])
        cache.prewarm()
        assert len(cache.reader.history("200101").tables["room_entry_log"]) == 3
        monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
        app = FastAPI()
        app.router.routes.extend(api_app.app.router.routes)
        app.state.api_cache = cache
        app.add_middleware(CacheMiddleware, owner=app)
        with TestClient(app, client=("203.0.113.46", 123)) as client:
            room = client.get("/gift/entry?room_id=111111")
            assert room.status_code == 200
            assert room.json() == {"mode": "room", "room_id": 111111, "month": month_str(), "items": [{"uid": 7, "event_time": recent.isoformat() + "+08:00"}]}
            history = client.get("/gift/entry?uid=7&month=2001-01")
            assert history.status_code == 200 and len(history.json()["items"]) == 3
            for window in (1, 2):
                response = client.get(f"/gift/entry?cache={window}")
                assert response.status_code == 200 and len(response.json()["items"]) == 1
            unavailable = client.get("/gift/entry?uid=7&month=200102")
            assert unavailable.status_code == 200 and unavailable.json()["status"] == "unavailable"
        assert entries.archive_entries("200101") == 1
        with Session() as session:
            assert session.query(RoomEntryLog).count() == 1
            assert session.scalar(text(f"SELECT COUNT(*) FROM `{archive}`")) == 3
    finally:
        cache.reader.close()
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS `{archive}`"))


def test_real_entry_hour_window_merges_previous_archive_and_current_hot_rows(services, monkeypatch):
    from app.repositories import entries

    store, engine, Session, start = services
    monkeypatch.setattr(entries, "engine", engine)
    monkeypatch.setattr(entries, "Session", Session)
    previous_time = start - datetime.timedelta(minutes=30)
    current_time = start + datetime.timedelta(minutes=10)
    previous_month = previous_time.strftime("%Y%m")
    archive = f"room_entry_log_{previous_month}"
    reader = SnapshotReader(store, threading.Event())
    try:
        entries.save_entries([
            {"room_id": 111111, "uid": 7, "event_time": previous_time},
            {"room_id": 222222, "uid": 7, "event_time": current_time},
        ])
        assert entries.archive_entries(previous_month) == 1
        with reader.transaction():
            reader.query("SELECT room_id FROM room_info")
            rows = reader.read_entries(start - datetime.timedelta(minutes=40), start + datetime.timedelta(minutes=20))
        assert {(row["room_id"], row["event_time"]) for row in rows} == {(111111, previous_time), (222222, current_time)}
    finally:
        reader.close()
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS `{archive}`"))


def test_real_lua_two_ips_and_parallel_workers_share_per_ip_limit(services):
    store, _engine, _Session, _start = services
    with ThreadPoolExecutor(max_workers=20) as executor:
        values = list(executor.map(lambda _: store.allow_ip("203.0.113.8"), range(60)))
    assert sum(values) == 20
    assert sum(store.allow_ip("203.0.113.9") for _ in range(20)) == 20


def test_real_atomic_generations_and_readiness_expiry(services):
    store, _engine, _Session, _start = services
    cache = ApiCache(store)
    cache.prewarm()
    current = month_str()
    generation = store.client.get(store.key(f"month:{current}"))
    payload = store.read(current, "/gift")
    assert isinstance(json.loads(gzip.decompress(payload)), list)
    store.client.hset(generation, "_snapshot", str(store.clock() - 31))
    with pytest.raises(CacheUnavailable):
        store.read(current, "/gift")
    cache.refresh_hot()
    assert store.read(current, "/gift") == payload
    assert 0 < store.client.ttl(generation) <= 5
    store.client.delete(store.key("ready"))
    with pytest.raises(CacheUnavailable):
        store.read(current, "/gift")
    cache.reader.close()


def test_real_room_visibility_keeps_mysql_writes_and_complete_snapshots(services, monkeypatch):
    from app import room_visibility
    from app.repositories import entries, monthly

    store, engine, Session, _start = services
    policy = room_visibility.RoomVisibilityStore(store.client, prefix=store.prefix)
    monkeypatch.setattr(room_visibility, "_store", policy)
    monkeypatch.setattr(entries, "engine", engine)
    monkeypatch.setattr(entries, "Session", Session)
    monkeypatch.setattr(monthly, "Session", Session)
    recent = datetime.datetime.now() - datetime.timedelta(minutes=1)
    entries.save_entries([
        {"room_id": room_id, "uid": 7, "event_time": recent}
        for room_id in (111111, 222222)
    ])
    cache = ApiCache(store)
    cache.prewarm()
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    app = FastAPI()
    app.router.routes.extend(api_app.app.router.routes)
    app.state.api_cache = cache
    app.add_middleware(CacheMiddleware, owner=app)
    app.add_middleware(room_visibility.RoomVisibilityMiddleware)
    auth = {"X-API-Key": api_app.API_SECRET}
    try:
        with TestClient(app, client=("203.0.113.71", 123)) as client:
            response = client.post("/delete/room/temporary", json={"room_id": 111111}, headers=auth)
            assert response.status_code == 200 and response.json()["hidden"] is True
            assert store.client.ttl(policy.key) == -1
            assert room_visibility.RoomVisibilityStore(store.client, prefix=store.prefix).hidden_rooms() == {111111}
            for path in ("/gift", "/gift/by_month?month=190001"):
                assert all(row["room_id"] != 111111 for row in client.get(path).json())
            for path in ("live_sessions", "attention", "sc", "entry"):
                response = client.get(f"/gift/{path}?room_id=111111")
                assert response.status_code == 404 and response.json() == {"error": "房间不存在"}
            assert [row["room_id"] for row in client.get("/gift/entry?uid=7").json()["items"]] == [222222]

            # Use the real collector repository writer while publication is blocked.
            RoomStatsMonthly.add_amounts(111111, month_str(), gift=2.0)
            entries.save_entries([{"room_id": 111111, "uid": 7, "event_time": recent + datetime.timedelta(seconds=1)}])
            with Session() as session:
                assert float(session.get(RoomStatsMonthly, (111111, month_str())).gift) == 14.5
                assert session.get(LiveSession, 1).end_time is None
                assert session.query(RoomEntryLog).filter_by(room_id=111111).count() == 2
            cache.refresh_hot()
            raw = json.loads(gzip.decompress(store.read(month_str(), "/gift")))
            assert next(row for row in raw if row["room_id"] == 111111)["gift"] == 14.5
            assert all(row["room_id"] != 111111 for row in client.get("/gift").json())
            response = client.post("/restore/room/temporary", json={"room_id": 111111}, headers=auth)
            assert response.status_code == 200 and response.json()["hidden"] is False
            restored = next(row for row in client.get("/gift").json() if row["room_id"] == 111111)
            assert restored["gift"] == 14.5
            assert len(client.get("/gift/entry?room_id=111111").json()["items"]) == 2
    finally:
        cache.reader.close()
