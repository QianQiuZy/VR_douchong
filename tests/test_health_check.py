"""Public health transport, dependency failures, staleness and thread bridge."""

import asyncio
import json
import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from yarl import URL

from app import api_app, config, health_check, health_events, runtime_state


@pytest.mark.parametrize("healthy", [True, False])
def test_public_health_bypasses_redis_cache_visibility_and_auth(monkeypatch, healthy):
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    monkeypatch.setattr(api_app, "API_SECRET", "synthetic-secret")
    monkeypatch.setattr(health_check.service, "latest", {
        "status": "ok" if healthy else "degraded", "ok": healthy,
        "last_hour": health_events.events.snapshot(),
    })
    monkeypatch.setattr(health_check.service, "sampled", time.monotonic())
    monkeypatch.setattr(api_app.app.state, "api_cache", SimpleNamespace(
        metrics={"ready": True, "snapshot_age_seconds": 0}, last_hot=time.monotonic(),
    ), raising=False)

    def forbidden():
        raise AssertionError("health must not require working visibility Redis")

    monkeypatch.setattr(api_app.room_visibility.get_store(), "hidden_rooms", forbidden)
    # Exercise the route without starting the unrelated cache builder.
    client = TestClient(api_app.app)
    try:
        response = client.get("/health/check")
        assert response.status_code == (200 if healthy else 503)
        assert response.json()["ok"] is healthy
        assert response.headers["cache-control"] == "no-store"
        assert "synthetic-secret" not in response.text
    finally:
        client.close()


def test_health_transport_without_lifespan_uses_only_published_snapshot(monkeypatch):
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    monkeypatch.setattr(health_check.service, "latest", {"status": "ok", "ok": True})
    monkeypatch.setattr(health_check.service, "sampled", time.monotonic())
    with TestClient(api_app.app) as client:
        response = client.get("/health/check")
        assert response.status_code == 200
        assert response.json()["ok"] is True
        assert response.headers["cache-control"] == "no-store"
        assert health_events.events.snapshot()["totals"]["local_api"] == 0


def test_real_lifespan_samples_once_and_serves_public_diagnostics(monkeypatch):
    service = health_check.HealthService()
    calls, closed = [], []
    def database():
        calls.append("database")
    def ping():
        calls.append("redis")
        return True
    monkeypatch.setattr(health_check, "service", service)
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    monkeypatch.setattr(runtime_state, "MAIN_LOOP", None)
    monkeypatch.setattr(health_check, "probe_database", database)
    monkeypatch.setattr(health_check, "redis_clients", lambda: {
        role: SimpleNamespace(ping=ping, close=lambda: closed.append(True))
        for role in ("metrics", "whale_db1", "api_db2", "danmaku_users_db3", "danmaku_rank_db4")
    })
    with TestClient(api_app.app) as client:
        deadline = time.monotonic() + 2
        while service.snapshot()["status"] == "starting" and time.monotonic() < deadline:
            time.sleep(0.001)
        for _ in range(3):
            response = client.get("/health/check")
            assert response.status_code == 503  # The collector was deliberately not started.
            result = response.json()
            assert result["dependencies"]["database"]["status"] == "ok"
            assert result["dependencies"]["redis"]["api_db2"]["status"] == "ok"
            assert result["collector"]["loop_responsive"] is False
            assert result["last_hour"]["full_window"] is False
    assert calls.count("database") == 1
    assert calls.count("redis") == 5
    assert len(closed) == 5
    assert not service.thread.is_alive()


def test_starting_stale_and_unavailable_cache_never_claim_healthy(monkeypatch):
    service = health_check.HealthService()
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    assert service.snapshot()["status"] == "starting"
    service.latest = {"status": "ok", "ok": True}
    service.sampled = time.monotonic() - 46
    assert service.snapshot()["status"] == "stale"
    assert service.snapshot()["ok"] is False
    service.sampled = time.monotonic()
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    assert service.snapshot()["status"] == "degraded"
    cache = SimpleNamespace(last_hot=time.monotonic() - 5,
                            metrics={"ready": True, "snapshot_age_seconds": 29})
    assert service.snapshot(cache)["api_cache"]["status"] == "unavailable"


@pytest.mark.parametrize("failure", [None, "database", "redis", "pool", "collector", "http_pool"])
def test_collect_reports_dependency_faults_without_secret_exception_text(monkeypatch, failure):
    def database():
        if failure == "database":
            raise OSError("synthetic-secret")

    def ping():
        if failure == "redis":
            raise ConnectionError("synthetic-secret")
        return True

    monkeypatch.setattr(health_check, "probe_database", database)
    monkeypatch.setattr(health_check, "pool_snapshot", lambda: {
        "status": "exhausted" if failure == "pool" else "ok",
    })
    monkeypatch.setattr(health_check, "probe_collector", lambda: {
        "status": "unavailable" if failure == "collector" else "ok",
        "http_pool": {"status": "exhausted" if failure == "http_pool" else "ok"},
    })
    clients = {role: SimpleNamespace(ping=ping) for role in ("metrics", "whale_db1", "api_db2", "danmaku_users_db3", "danmaku_rank_db4")}
    result = health_check.HealthService().collect(clients)
    assert result["ok"] == (failure is None)
    assert "synthetic-secret" not in json.dumps(result)
    assert result["last_hour"]["full_window"] is False


@pytest.mark.parametrize("column,status", [(("YES",), "ok"), (("NO",), "schema_mismatch"), (None, "schema_mismatch")])
def test_database_probe_uses_bounded_dedicated_read_only_connection(monkeypatch, column, status):
    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query):
            calls.append(query)

        def fetchone(self):
            return column

    class Connection(Cursor):
        def cursor(self):
            return Cursor()

    def connect(**kwargs):
        assert all(kwargs[key] == 2 for key in ("connect_timeout", "read_timeout", "write_timeout"))
        return Connection()

    monkeypatch.setattr(health_check.pymysql, "connect", connect)
    assert health_check.measured(health_check.probe_database)["status"] == status
    assert len(calls) == 1 and "TABLE_SCHEMA = DATABASE()" in calls[0] and "IS_NULLABLE" in calls[0]


def test_pool_reports_real_occupancy_and_exhaustion(monkeypatch):
    size = config.DB_POOL_SIZE
    capacity = size + config.DB_POOL_MAX_OVERFLOW
    pool = SimpleNamespace(size=lambda: size, checkedin=lambda: 0,
                           checkedout=lambda: capacity, overflow=lambda: config.DB_POOL_MAX_OVERFLOW)
    monkeypatch.setattr(health_check.database, "engine", SimpleNamespace(pool=pool))
    snapshot = health_check.pool_snapshot()
    assert snapshot["status"] == "exhausted"
    assert snapshot["checked_out"] == snapshot["capacity"] == capacity


def test_collector_bridge_reads_live_state_on_own_loop(monkeypatch):
    started = threading.Event()
    loop = asyncio.new_event_loop()

    def run():
        asyncio.set_event_loop(loop)
        loop.call_soon(started.set)
        loop.run_forever()

    thread = threading.Thread(target=run)
    monkeypatch.setattr(runtime_state, "MAIN_LOOP", loop)
    monkeypatch.setattr(runtime_state, "ROOM_IDS", [42, 43])

    def client_snapshot():
        assert asyncio.get_running_loop() is loop
        return {"status": "connected", "authenticated": True}

    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {42: SimpleNamespace(health_snapshot=client_snapshot)})
    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    thread.start()
    assert started.wait(timeout=2)
    try:
        snapshot = health_check.probe_collector()
        assert snapshot["rooms"]["connected"] == 1
        assert snapshot["rooms"]["not_connected"] == 1
        assert snapshot["rooms"]["items"][1]["status"] == "not_started"
        assert snapshot["http_pool"]["status"] == "uninitialized"
        assert snapshot["status"] == "degraded"
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        loop.close()


def test_monitor_publishes_and_closes_single_background_worker(monkeypatch):
    service = health_check.HealthService()
    sampled = threading.Event()
    closed = []
    monkeypatch.setattr(health_check, "redis_clients", lambda: {
        "metrics": SimpleNamespace(close=lambda: closed.append(True)),
    })

    def collect(clients):
        sampled.set()
        return {"ok": True, "status": "ok"}

    monkeypatch.setattr(service, "collect", collect)
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    service.start()
    thread = service.thread
    service.start()
    assert service.thread is thread
    assert sampled.wait(timeout=2)
    service.close()
    assert not thread.is_alive()
    assert closed == [True]
    assert service.snapshot()["ok"] is True


def test_collector_without_running_loop_is_unavailable(monkeypatch):
    monkeypatch.setattr(runtime_state, "MAIN_LOOP", None)
    assert health_check.probe_collector()["status"] == "unavailable"


def test_unresponsive_collector_bridge_cancels_pending_snapshot(monkeypatch):
    pending = Future()
    def submit(coroutine, loop):
        coroutine.close()
        return pending
    monkeypatch.setattr(runtime_state, "MAIN_LOOP", SimpleNamespace(
        is_closed=lambda: False, is_running=lambda: True,
    ))
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", submit)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        health_check.probe_collector()
    assert pending.cancelled()
    assert time.monotonic() - started < 3


def test_latest_upstream_failure_degrades_health_until_success(monkeypatch):
    monkeypatch.setattr(health_check, "probe_database", lambda: None)
    monkeypatch.setattr(health_check, "pool_snapshot", lambda: {"status": "ok"})
    monkeypatch.setattr(health_check, "probe_collector", lambda: {
        "status": "ok", "http_pool": {"status": "ok"},
    })
    clients = {role: SimpleNamespace(ping=lambda: True) for role in ("metrics", "whale_db1", "api_db2", "danmaku_users_db3", "danmaku_rank_db4")}
    url = URL("https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids")
    health_events.events.upstream_result(health_events.observation(url), reason="TimeoutError")
    service = health_check.HealthService()
    snapshot = service.collect(clients)
    assert snapshot["ok"] is False
    assert snapshot["upstream_api"]["failing_endpoints"] == ["live_status"]
    health_events.events.upstream_result(health_events.observation(url))
    snapshot = service.collect(clients)
    assert snapshot["ok"] is True
    assert snapshot["last_hour"]["totals"]["upstream_api"] == 1


def test_redis_roles_have_timeouts_and_no_probe_retries(monkeypatch):
    calls = []
    def create(url, **kwargs):
        calls.append((url, kwargs))
        return object()
    monkeypatch.setattr(config, "REDIS_URL", "redis://localhost:6379/4?db=4")
    monkeypatch.setattr(health_check.redis.Redis, "from_url", create)
    assert set(health_check.redis_clients()) == {"metrics", "whale_db1", "api_db2", "danmaku_users_db3", "danmaku_rank_db4"}
    assert "/1" in calls[1][0] and "db=" not in calls[1][0]
    assert "/2" in calls[2][0] and "db=" not in calls[2][0]
    assert all(options["socket_timeout"] == options["socket_connect_timeout"] == 2 for _, options in calls)
