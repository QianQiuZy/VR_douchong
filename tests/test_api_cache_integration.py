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

import pytest
import redis
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app import api_app, config, gift, runtime_state, whale_metrics
from app.api_cache import ApiCache
from app.api_cache_http import CacheMiddleware
from app.api_cache_source import BudgetConnection, SnapshotReader
from app.api_cache_store import CacheStore, CacheUnavailable
from app.database import Base
from app.models import (
    Attention,
    LiveSession,
    LiveSession15mStats,
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
