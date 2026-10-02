from __future__ import annotations

import asyncio
import gzip
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import fakeredis
import pytest
import redis
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pymysql.constants import COMMAND
from starlette.requests import Request

from app import api_app, config, gift, room_config, runtime_state
from app.api_cache import ApiCache, invalidate_history
from app.api_cache_http import CacheMiddleware, accepts_gzip, client_ip
from app.api_cache_payloads import PayloadBuilder
from app.api_cache_source import (
    BASE_TABLES,
    MONTH_TABLES,
    BudgetConnection,
    Snapshot,
    SnapshotReader,
)
from app.api_cache_store import (
    ApiSqlScope,
    CacheStore,
    CacheUnavailable,
    FaultLog,
    api_sql_scope,
    db2_url,
    encode,
)
from app.database import ApiBudgetConnection
from app.repositories.tables import month_str


@pytest.fixture(autouse=True)
def isolate_whale_metrics(monkeypatch):
    monkeypatch.setattr(
        api_app,
        "_whale_dependency_for_room",
        lambda *_: {
            "status": "unavailable",
            "source": "redis",
            "top1": None,
            "top5": None,
            "top10": None,
            "top1_percent": None,
        },
    )


@pytest.fixture
def store(monkeypatch):
    client = fakeredis.FakeRedis()

    # fakeredis does not implement MEMORY USAGE; production Redis is covered by
    # the opt-in integration suite with the actual command and the same Lua scripts.
    def memory_usage(key):
        return sum(len(k) + len(v) for k, v in client.hgetall(key).items())

    monkeypatch.setattr(client, "memory_usage", memory_usage)
    result = CacheStore(client, prefix="test:api")
    assert result.acquire()
    yield result
    client.close()


def snapshot(store, month=None):
    return Snapshot(
        month or month_str(),
        store.clock(),
        {table: [] for table in BASE_TABLES},
        {table: [] for table in MONTH_TABLES},
        {month_str(), "190001"},
    )


def prepare(store, value=None):
    value = value or snapshot(store)
    store.publish(value.month, PayloadBuilder().build(value), value.stamp)
    store.ready()


def app_client(store, monkeypatch, ip="203.0.113.1"):
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    app = FastAPI()
    app.router.routes.extend(api_app.app.router.routes)
    app.state.api_cache = SimpleNamespace(store=store)
    app.add_middleware(CacheMiddleware, owner=app)
    return TestClient(app, client=(ip, 12345))


@pytest.mark.parametrize(
    "url",
    [
        "redis://localhost:6379/0?db=1",
        "redis://localhost/9",
        "rediss://localhost/1?db=0&socket_timeout=1",
    ],
)
def test_redis_always_selects_db2(url):
    target = db2_url(url)
    assert "/2" in target
    assert "db=" not in target
    if "socket_timeout" in url:
        assert "socket_timeout=1" in target


def test_get_cache_never_uses_sql_or_report_gate(store, monkeypatch):
    prepare(store)

    def forbidden():
        pytest.fail("cache hit must never open an ORM session")

    monkeypatch.setattr(gift, "Session", forbidden)
    gate = threading.BoundedSemaphore(1)
    gate.acquire()
    monkeypatch.setattr(api_app, "_report_gate", gate)
    with app_client(store, monkeypatch) as client:
        for path in (
            "/gift",
            "/gift/by_month",
            "/gift/live_sessions?room_id=111111",
            "/gift/attention?room_id=111111",
            "/gift/sc?room_id=111111",
        ):
            response = client.get(path)
            assert response.status_code == 200
            assert response.headers["content-encoding"] == "gzip"
            assert response.headers["vary"] == "Accept-Encoding"
        response = client.get(
            "/gift", headers={"Accept-Encoding": "gzip;q=0, identity"}
        )
        assert response.status_code == 200
        assert "content-encoding" not in response.headers
    gate.release()


def test_per_ip_limit_is_shared_across_routes_and_clients(store, monkeypatch):
    prepare(store)
    with (
        app_client(store, monkeypatch) as first,
        app_client(store, monkeypatch) as same,
    ):
        responses = [first.get("/gift") for _ in range(10)] + [
            same.get("/gift/by_month") for _ in range(10)
        ]
        assert all(response.status_code == 200 for response in responses)
        denied = same.get("/gift/sc?room_id=1")
        assert denied.status_code == 429
        assert denied.headers["retry-after"] == "1"
        assert denied.headers["cache-control"] == "no-store"
    with app_client(store, monkeypatch, ip="203.0.113.2") as second:
        assert second.get("/gift").status_code == 200


def test_window_is_atomic_under_concurrent_admission(store):
    with ThreadPoolExecutor(max_workers=30) as executor:
        allowed = list(executor.map(lambda _: store.allow_ip("203.0.113.8"), range(60)))
    assert sum(allowed) == 20


def test_window_expires_and_rejected_requests_do_not_add_members(store):
    for _ in range(20):
        assert store.allow_ip("203.0.113.9")
    key = store.key("ip:203.0.113.9")
    assert not store.allow_ip("203.0.113.9")
    assert store.client.zcard(key) == 20
    seconds, micros = store.client.time()
    now = seconds * 1_000_000 + micros
    store.client.zadd(
        key, {member: now - 1_000_001 for member in store.client.zrange(key, 0, -1)}
    )
    assert store.allow_ip("203.0.113.9")
    assert store.client.zcard(key) == 1


@pytest.mark.parametrize(
    ("peer", "headers", "expected"),
    [
        (
            "127.0.0.1",
            {"eo-connecting-ip": "203.0.113.7", "x-forwarded-for": "fake,1.2.3.4"},
            "203.0.113.7",
        ),
        ("::1", {"x-real-ip": "2001:0db8::1"}, "2001:db8::1"),
        (
            "127.0.0.1",
            {"eo-connecting-ip": "bad", "x-real-ip": "::ffff:203.0.113.7"},
            "203.0.113.7",
        ),
        (
            "203.0.113.8",
            {"eo-connecting-ip": "203.0.113.7", "x-real-ip": "1.2.3.4"},
            "203.0.113.8",
        ),
    ],
)
def test_ip_selection_trusts_only_local_proxy(peer, headers, expected):
    request = Request(
        {
            "type": "http",
            "client": (peer, 1234),
            "headers": [
                (key.encode(), value.encode()) for key, value in headers.items()
            ],
        }
    )
    assert client_ip(request) == expected


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("gzip", True),
        ("GZIP;q=1", True),
        ("gzip;q=0,*;q=1", False),
        ("br,*;q=0.5", True),
        ("gzip;q=nan", False),
        ("gzip;q=oops", False),
        ("identity", False),
    ],
)
def test_gzip_negotiation(header, expected):
    assert accepts_gzip(header) is expected


@pytest.mark.parametrize(
    "failure", ["missing", "stale", "redis", "corrupt", "known-field"]
)
def test_faults_return_503_without_sql_fallback(store, monkeypatch, failure):
    value = snapshot(store)
    if failure == "stale":
        value.stamp -= 31
    if failure != "missing":
        prepare(store, value)
    if failure in {"corrupt", "known-field"}:
        generation = store.client.get(store.key(f"month:{value.month}"))
        if failure == "corrupt":
            store.client.hset(generation, "/gift", b"not-gzip")
        else:
            store.client.hdel(generation, "/gift")
    if failure == "redis":
        monkeypatch.setattr(
            store,
            "read",
            lambda *_: (_ for _ in ()).throw(redis.ConnectionError("unavailable")),
        )
    monkeypatch.setattr(gift, "Session", lambda: pytest.fail("no SQL fallback"))
    with app_client(store, monkeypatch) as client:
        response = client.get("/gift")
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert response.headers["cache-control"] == "no-store"


def test_parameter_errors_preserve_old_response_contract(store, monkeypatch):
    with app_client(store, monkeypatch) as client:
        cases = [
            ("/gift/sc", "room_id 参数必填"),
            ("/gift/sc?room_id=-1", "room_id 必须为正整数"),
            ("/gift/live_sessions?room_id=no", "room_id 参数无效"),
            ("/gift/attention?room_id=0", "room_id 必填且需为正整数"),
            ("/gift/by_month?month=000001", "month 参数无效，支持 YYYYMM 或 YYYY-MM"),
            (
                "/gift/sc?room_id=1&month=999912",
                "month 格式不正确，应为 YYYYMM 或 YYYY-MM",
            ),
        ]
        for path, error in cases:
            response = client.get(path)
            assert response.status_code == 400
            assert response.json() == {"error": error}


def test_unknown_valid_domains_use_templates_without_creating_keys(store, monkeypatch):
    prepare(store)
    before = set(store.client.scan_iter(match=store.key("generation:*")))
    with app_client(store, monkeypatch) as client:
        summary = client.get("/gift/by_month?month=1800-01")
        assert summary.status_code == 200
        assert all(
            row["month"] == "180001" and row["gift"] == 0 for row in summary.json()
        )
        for path, field in (
            ("/gift/sc", "list"),
            ("/gift/attention", "attention"),
            ("/gift/live_sessions", "sessions"),
        ):
            response = client.get(
                path, params={"month": "180001", "room_id": "987654321"}
            )
            assert response.json() == {
                "room_id": 987654321,
                "month": "180001",
                field: [],
            }
            response = client.get(path, params={"room_id": "987654321"})
            assert response.json()[field] == []
    assert set(store.client.scan_iter(match=store.key("generation:*"))) == before


def test_management_authentication_is_preserved_and_rate_limited(store, monkeypatch):
    with app_client(store, monkeypatch) as client:
        assert client.post("/add/room", json={}).status_code == 401
        for _ in range(19):
            assert client.post("/delete/room", json={}).status_code == 401
        assert client.post("/add/room", json={}).status_code == 429


def test_incomplete_staging_is_invisible_and_lost_leader_cannot_publish(store):
    value = snapshot(store)
    values = PayloadBuilder().build(value)
    store.publish(value.month, values, value.stamp)
    with pytest.raises(CacheUnavailable):
        store.read(value.month, "/gift")
    store.ready()
    body = store.read(value.month, "/gift")
    store.client.set(store.lease_key, "another-owner")
    with pytest.raises(CacheUnavailable):
        store.publish(value.month, {"/gift": encode(["wrong"])}, value.stamp)
    assert store.read(value.month, "/gift") == body


def test_generations_retire_without_touching_other_dbs_or_keys(store):
    value = snapshot(store)
    prepare(store, value)
    previous = store.client.get(store.key(f"month:{value.month}"))
    store.client.set("unrelated", "keep")
    store.publish(value.month, PayloadBuilder().build(value), value.stamp)
    assert 0 < store.client.ttl(previous) <= 5
    assert store.client.get("unrelated") == b"keep"


def test_memory_budget_stops_publication_without_replacing_good_generation(store):
    prepare(store)
    previous = store.client.get(store.key(f"month:{month_str()}"))
    store.max_bytes = 1
    with pytest.raises(CacheUnavailable):
        store.publish(month_str(), {"/gift": encode([])}, store.clock())
    assert store.client.get(store.key(f"month:{month_str()}")) == previous


def test_sql_packets_and_transactions_share_budget_but_ping_is_not_sql(monkeypatch):
    packets = []
    store = SimpleNamespace(take_sql=lambda _stop: packets.append("budget"))
    conn = object.__new__(BudgetConnection)
    conn.cache_store = store
    conn.cache_stop = threading.Event()
    monkeypatch.setattr(
        "pymysql.connections.Connection._execute_command",
        lambda _self, command, sql: packets.append(sql),
    )
    for sql in ("SELECT 1", "COMMIT", "ROLLBACK", "SET NAMES utf8mb4"):
        conn._execute_command(COMMAND.COM_QUERY, sql)
    conn._execute_command(COMMAND.COM_PING, b"")
    assert packets == [
        "budget",
        "SELECT 1",
        "budget",
        "COMMIT",
        "budget",
        "ROLLBACK",
        "budget",
        "SET NAMES utf8mb4",
        b"",
    ]


def test_sql_budget_waits_and_cancellation_does_not_issue_packets(store):
    stop = threading.Event()
    for _ in range(9):
        store.take_sql(stop)
    stop.set()
    with pytest.raises(CacheUnavailable):
        store.take_sql(stop)
    assert store.client.zcard(store.key("sql-budget")) == 9


def test_prewarm_covers_all_months_before_ready_and_success_is_silent(store, caplog):
    class Reader:
        def __init__(self, target, _stop):
            self.store = target
            self.calls = []

        def current(self, **_kwargs):
            self.calls.append(month_str())
            return snapshot(self.store)

        def history(self, month):
            self.calls.append(month)
            assert not self.store.client.exists(self.store.key("ready"))
            return snapshot(self.store, month)

    cache = ApiCache(store, Reader)
    with caplog.at_level("DEBUG"):
        cache.prewarm()
    assert cache.reader.calls == [month_str(), "190001", month_str()]
    assert cache.metrics["ready"]
    assert store.read("190001", "/gift/by_month")
    assert not [
        record for record in caplog.records if record.name.startswith("app.api_cache")
    ]


def test_fault_logs_are_sanitized_and_rate_limited(caplog):
    log = FaultLog()
    for _ in range(10):
        log.record("refresh", ValueError("password=secret raw_uid=123"))
    assert len(caplog.records) == 1
    assert "ValueError" in caplog.text
    assert "secret" not in caplog.text and "raw_uid" not in caplog.text


def test_unchanged_sc_payload_is_reused(store, monkeypatch):
    builder = PayloadBuilder()
    value = snapshot(store)
    first = builder.build(value)
    second = builder.build(value)
    rid = room_config.get_room_ids()[0]
    assert first[f"/gift/sc:{rid}"] is second[f"/gift/sc:{rid}"]


def test_hot_payload_preserves_runtime_fields_and_historical_nulls(store, monkeypatch):
    rid = room_config.get_room_ids()[0]
    value = snapshot(store)
    value.base["room_stats_monthly"] = [
        {"room_id": rid, "month": value.month, "gift": 12.5, "danmaku_count": 7}
    ]
    monkeypatch.setitem(runtime_state.LAST_STATUS, rid, 1)
    monkeypatch.setitem(runtime_state.CURRENT_SESSIONS, rid, 123)
    monkeypatch.setitem(
        runtime_state.CONCURRENCY_CACHE,
        rid,
        {"session_id": 123, "samples": 2, "last": 42},
    )
    monkeypatch.setitem(
        runtime_state.LIVE_INFO,
        rid,
        {"title": "测试", "live_time": "2026-10-03 01:00:00"},
    )
    monkeypatch.setattr(
        api_app, "_whale_dependency_for_room", lambda *_: {"status": "unavailable"}
    )
    bodies = PayloadBuilder().build(value)
    current = next(
        row
        for row in json.loads(gzip.decompress(bodies["/gift"]))
        if row["room_id"] == rid
    )
    assert current["gift"] == 12.5 and current["current_concurrency"] == 42
    assert current["title"] == "测试" and current["danmaku"]["total"] == 7
    monthly = json.loads(gzip.decompress(bodies["/gift/by_month"]))
    assert all("current_concurrency" not in row for row in monthly)
    historical = PayloadBuilder().build(snapshot(store, "190001"))
    rows = json.loads(gzip.decompress(historical["/gift/by_month"]))
    assert all(row["guard_1"] is None and row["status"] == 0 for row in rows)


def test_invalid_month_never_becomes_a_sql_identifier(store):
    reader = SnapshotReader(store, threading.Event())
    reader.metadata.add("super_chat_log_bad`month")
    with pytest.raises((ValueError, TypeError)):
        reader.read_table("super_chat_log", "bad`month")


def test_management_sql_scope_does_not_budget_collector_or_retained_child_context(
    monkeypatch,
):
    calls = []
    target = SimpleNamespace(take_sql=lambda _stop, **kwargs: calls.append(kwargs))
    connection = object.__new__(ApiBudgetConnection)
    monkeypatch.setattr(
        "pymysql.connections.Connection._execute_command", lambda *_: None
    )
    connection._execute_command(COMMAND.COM_QUERY, "SELECT 1")
    scope = ApiSqlScope(target)
    token = api_sql_scope.set(scope)
    try:
        connection._execute_command(COMMAND.COM_QUERY, "COMMIT")
        scope.active = False
        connection._execute_command(
            COMMAND.COM_QUERY, "INSERT INTO live_session VALUES (...)"
        )
    finally:
        api_sql_scope.reset(token)
    assert calls == [{"require_lease": False}]


def test_different_cache_instances_share_sql_budget_not_per_ip_budget(store):
    stop = threading.Event()
    for _ in range(9):
        store.take_sql(stop)
    other = CacheStore(store.client, prefix=store.prefix)
    timer = threading.Timer(0.01, stop.set)
    timer.start()
    try:
        with pytest.raises(CacheUnavailable):
            other.take_sql(stop, require_lease=False)
    finally:
        timer.join()
    assert store.client.zcard(store.key("sql-budget")) == 9


def test_lost_owner_cannot_clear_readiness_or_publish_ready(store):
    prepare(store)
    ready = store.client.get(store.key("ready"))
    store.client.set(store.lease_key, "new-owner")
    with pytest.raises(CacheUnavailable):
        store.begin_prewarm()
    with pytest.raises(CacheUnavailable):
        store.ready()
    assert store.client.get(store.key("ready")) == ready


def test_archive_notification_is_coalesced_and_offline_mode_has_no_redis(
    store, monkeypatch
):
    monkeypatch.setattr(
        api_app.app.state, "api_cache", SimpleNamespace(store=store), raising=False
    )
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    invalidate_history("1900-01")
    invalidate_history("190001")
    invalidate_history("bad")
    assert store.client.smembers(store.key("dirty-months")) == {b"190001"}
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    invalidate_history()
    assert store.client.smembers(store.key("dirty-months")) == {b"190001"}


def test_dirty_history_is_refreshed_after_hot_priority(store, monkeypatch):
    cache = ApiCache(store)
    calls = []
    cache.last_hot = float("-inf")
    cache.history_due["190001"] = 0

    def hot(**kwargs):
        calls.append(("hot", kwargs))
        cache.last_hot = 1e30
        return {month_str(), "190001"}

    monkeypatch.setattr(cache, "refresh_hot", hot)
    monkeypatch.setattr(
        cache, "refresh_history", lambda month: calls.append(("history", month))
    )
    cache.cycle()
    assert calls[0][0] == "hot"
    assert calls[-1] == ("history", "190001")


def test_background_failure_is_logged_and_releases_leader(store, monkeypatch, caplog):
    cache = ApiCache(store)
    monkeypatch.setattr(store, "acquire", lambda: True)

    def broken():
        cache.stop.set()
        raise ValueError("secret: must not be logged")

    monkeypatch.setattr(cache, "prewarm", broken)
    cache.run()
    assert cache.metrics["failures"] == 1
    assert not store.client.exists(store.lease_key)
    assert "ValueError" in caplog.text and "secret" not in caplog.text


def test_lifespan_starts_and_closes_builder_without_blocking_collector(monkeypatch):
    calls = []
    fake = SimpleNamespace(
        start=lambda: calls.append("start"), close=lambda: calls.append("close")
    )
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    monkeypatch.setattr("app.api_cache.ApiCache", lambda: fake)

    async def exercise():
        async with api_app.cache_lifespan(api_app.app):
            assert api_app.app.state.api_cache is fake
            assert calls == ["start"]
        assert api_app.app.state.api_cache is None

    asyncio.run(exercise())
    assert calls == ["start", "close"]


def test_inflight_capacity_is_not_a_global_requests_per_second_limit(
    store, monkeypatch
):
    prepare(store)
    monkeypatch.setattr(config, "API_CACHE_MAX_INFLIGHT", 1)
    with app_client(store, monkeypatch) as client:
        assert client.get("/gift").status_code == 200
        gate = client.app.middleware_stack.app.gate
        gate.acquire()
        try:
            response = client.get("/gift")
            assert response.status_code == 503
            assert response.headers["retry-after"] == "1"
        finally:
            gate.release()
        # Sequential requests within the same second still use the per-IP quota,
        # not a one-request-per-second global limit.
        assert client.get("/gift").status_code == 200


def test_cache_failure_releases_inflight_permit(store, monkeypatch):
    monkeypatch.setattr(config, "API_CACHE_MAX_INFLIGHT", 1)
    with app_client(store, monkeypatch) as client:
        assert client.get("/gift").status_code == 503
        prepare(store)
        assert client.get("/gift").status_code == 200
