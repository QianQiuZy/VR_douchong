from __future__ import annotations

import asyncio
import datetime
import gzip
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from types import SimpleNamespace

import fakeredis
import pymysql
import pytest
import redis
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pymysql.constants import COMMAND
from starlette.requests import Request

from app import api_app, config, gift, room_config, runtime_state, whale_metrics
from app.api_cache import ApiCache, invalidate_history
from app.api_cache_http import CacheMiddleware, accepts_gzip, client_ip
from app.api_cache_payloads import PayloadBuilder, year_summaries
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
    monkeypatch.setattr(api_app, "get_live_whale_metrics", lambda *_: None)
    monkeypatch.setattr(api_app, "get_whale_state", lambda *_: None)


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


@pytest.fixture
def annual_cache(store, monkeypatch):
    from app import api_cache

    rid = room_config.get_room_ids()[0]
    current = month_str()
    base = {
        "room_info": [{"room_id": rid, "anchor_name": "Annual Anchor", "attention": 12345}],
        "room_stats_monthly": [
            {"room_id": rid, "month": month, "gift": gift, "guard": 2, "super_chat": 3,
             "danmaku_count": 10, "captain_danmaku_count": 2, "normal_danmaku_count": normal,
             "emoticon_danmaku_count": 3}
            for month, gift, normal in (("201912", 999, 8), ("202001", 10.1, 8),
                                        ("202002", 20.2, None), (current, 4, 8))
        ],
        "room_blind_box_monthly": [
            {"room_id": rid, "month": "202001", "blind_box_count": 100, "blind_box_profit": -11},
            {"room_id": rid, "month": "202002", "blind_box_count": 200, "blind_box_profit": 22},
        ],
    }
    daily = {
        "201912": [{"room_id": rid, "date": datetime.date(2019, 12, 1), "duration": 7200}],
        "202001": [
            {"room_id": rid, "date": datetime.date(2020, 1, 1), "duration": 25 * 3600},
            {"room_id": rid, "date": datetime.date(2020, 1, 2), "duration": 3600},
        ],
        "202002": [{"room_id": rid, "date": datetime.date(2020, 2, 1), "duration": 7200}],
        current: [{"room_id": rid, "date": datetime.date.today(), "duration": 7200}],
    }
    clock = [1000.0]
    monkeypatch.setattr(api_cache.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(api_cache, "_pending_history", set())
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)

    class Reader:
        def __init__(self, target, _stop):
            self.store = target
            self.reads = []

        def known_months(self):
            return set(daily)

        def value(self, month):
            value = snapshot(self.store, month)
            value.months = self.known_months()
            value.base = deepcopy(base)
            value.tables["room_live_stats"] = deepcopy(daily[month])
            return value

        def current(self, **_kwargs):
            return self.value(month_str())

        def history(self, month):
            self.reads.append(month)
            return self.value(month)

    cache = ApiCache(store, Reader)
    monkeypatch.setattr(api_app.app.state, "api_cache", cache, raising=False)
    cache.prewarm()
    cache.reader.reads.clear()
    return SimpleNamespace(cache=cache, clock=clock, base=base, daily=daily, rid=rid)


def annual_room(store, year, rid):
    return next(row for row in json.loads(gzip.decompress(store.read_year(year))) if row["room_id"] == rid)


def test_annual_totals_duration_snapshot_fields_and_missing_danmaku(annual_cache):
    state = annual_cache
    row = annual_room(state.cache.store, "2020", state.rid)
    assert row["gift"] == 30.3
    assert row["guard"] == 4 and row["super_chat"] == 6
    assert row["live_duration"] == "28:00:00" and row["effective_days"] == 2
    assert row["blind_box_count"] == 300 and row["blind_box_profit"] == 1.1
    assert row["year"] == "2020" and "month" not in row
    assert row["whale_dependency"] is row["payer_count"] is row["steel_coin_count"] is None
    assert row["danmaku"] == {"total": 20, "emoticon": 6, "captain": 4, "normal": None, "admiral": None, "governor": None}
    assert row["attention"] == 12345 and row["status"] == 0 and row["title"] == ""
    assert row["live_time"] == "0000-00-00 00:00:00"
    assert all(row[field] is None for field in ("guard_1", "guard_2", "guard_3", "fans_count"))
    assert "current_concurrency" not in row


def test_current_year_uses_latest_runtime_and_never_includes_future_months(annual_cache, monkeypatch):
    from app import api_cache_payloads

    state = annual_cache
    monkeypatch.setattr(api_cache_payloads, "month_str", lambda: "202611")
    monkeypatch.setitem(runtime_state.LAST_STATUS, state.rid, 1)
    monkeypatch.setitem(runtime_state.GUARD_COUNTS, state.rid, {"guard_1": 12, "guard_2": 3, "guard_3": 1})
    monkeypatch.setitem(runtime_state.FANS_COUNT, state.rid, 99)
    monkeypatch.setitem(runtime_state.LIVE_INFO, state.rid, {"live_time": "2026-11-17 09:00:00", "title": "Live"})
    base = deepcopy(state.base)
    base["room_stats_monthly"] = [
        {"room_id": state.rid, "month": "202611", "gift": 5, "danmaku_count": 1},
        {"room_id": state.rid, "month": "202612", "gift": 999, "danmaku_count": 999},
    ]
    rows = year_summaries("2026", base, {
        "202611": ({state.rid: (7200, 1, 10)}, set()),
        "202612": ({state.rid: (999999, 20, 100)}, set()),
    })
    row = next(row for row in rows if row["room_id"] == state.rid)
    assert row["gift"] == 5 and row["live_duration"] == "02:00:00"
    assert row["danmaku"]["total"] == 1
    assert row["danmaku"]["emoticon"] is None
    assert (row["status"], row["title"], row["guard_1"], row["fans_count"]) == (1, "Live", 12, 99)


def test_annual_http_is_redis_only_and_retains_monthly_queries(annual_cache, monkeypatch):
    state = annual_cache
    monkeypatch.setattr(gift, "Session", lambda: pytest.fail("annual HTTP must not open SQL sessions"))
    monkeypatch.setattr(state.cache.reader, "history", lambda _: pytest.fail("HTTP must not read history"))
    with app_client(state.cache.store, monkeypatch) as client:
        for encoding in ("gzip", "identity"):
            result = client.get("/gift/by_month?year=2020", headers={"Accept-Encoding": encoding})
            assert result.status_code == 200 and result.headers["cache-control"] == "no-store"
            row = next(row for row in result.json() if row["room_id"] == state.rid)
            assert row["year"] == "2020" and row["gift"] == 30.3
            assert ("content-encoding" in result.headers) == (encoding == "gzip")
        monthly = client.get("/gift/by_month?month=202001")
        assert monthly.status_code == 200
        assert next(row for row in monthly.json() if row["room_id"] == state.rid)["gift"] == 10.1
        assert all(row["month"] == month_str() for row in client.get("/gift/by_month").json())
        before = set(state.cache.store.client.scan_iter(match=state.cache.store.key("generation:*")))
        empty = client.get("/gift/by_month?year=1800")
        assert empty.status_code == 200
        assert all(row["year"] == "1800" and row["gift"] == 0 and row["status"] == 0 for row in empty.json())
        assert set(state.cache.store.client.scan_iter(match=state.cache.store.key("generation:*"))) == before


@pytest.mark.parametrize("query", [
    "year=", "year=26", "year=202001", "year=202a", "year=0000", "year=9999",
    "year=２０２０", "year=2020&year=2020", "year=2020&month=202001", "year=2020&month=",
])
def test_annual_parameter_errors_never_fall_back_to_current_month(store, monkeypatch, query):
    with app_client(store, monkeypatch) as client:
        assert client.get("/gift/by_month?" + query).status_code == 400


def test_annual_hot_updates_do_not_read_history_and_history_change_rebuilds_its_year(annual_cache):
    state = annual_cache
    store = state.cache.store
    other_year = store.client.get(store.key("year:2019"))
    for _ in range(3):
        state.clock[0] += 11
        state.cache.cycle()
    assert state.cache.reader.reads == []
    assert store.client.get(store.key("year:2019")) == other_year
    next(row for row in state.base["room_stats_monthly"] if row["month"] == "202001")["gift"] = 20.1
    state.daily["202001"][0]["duration"] += 7200
    invalidate_history("202001")
    state.clock[0] += 301
    state.cache.cycle()
    row = annual_room(store, "2020", state.rid)
    assert state.cache.reader.reads == ["202001"]
    assert row["gift"] == 40.3 and row["live_duration"] == "30:00:00"
    assert store.client.get(store.key("year:2019")) == other_year
    assert not store.client.smembers(store.key("dirty-months"))


@pytest.mark.parametrize("damage", ["body", "checksum", "template", "stamp", "index"])
def test_historical_year_integrity_is_repaired_by_history_poll(annual_cache, damage):
    state = annual_cache
    cache = state.cache
    store = cache.store
    original_year = store.client.get(store.key("year:2020"))
    unchanged_year = store.client.get(store.key("year:2019"))
    if damage == "body":
        store.client.hset(original_year, "/gift/by_month", b"broken")
    elif damage == "checksum":
        store.client.hdel(original_year, "_hash:/gift/by_month")
    elif damage == "template":
        store.client.hdel(original_year, "_empty:/gift/by_month")
    elif damage == "stamp":
        store.client.hset(original_year, "_snapshot", "NaN")
    else:
        store.client.srem(store.key("years"), "2020")

    state.clock[0] += config.API_CACHE_REFRESH_SECONDS + 1
    cache.cycle()
    assert store.client.get(store.key("year:2020")) == original_year

    state.clock[0] += config.API_CACHE_HISTORY_SECONDS
    cache.cycle()
    assert store.client.get(store.key("year:2020")) != original_year
    assert store.client.get(store.key("year:2019")) == unchanged_year
    assert annual_room(store, "2020", state.rid)["gift"] == 30.3
    assert store.year_is_intact("2020", cache.year_bodies["2020"])
    assert cache.reader.reads == []


def test_failed_annual_publication_keeps_month_pending_for_retry(annual_cache, monkeypatch):
    state = annual_cache
    store = state.cache.store
    previous = store.client.get(store.key("year:2020"))
    state.daily["202001"][0]["duration"] += 7200
    invalidate_history("202001")
    original = store.publish_year

    def fail(year, *args):
        if year == "2020":
            raise CacheUnavailable("synthetic annual publication failure")
        return original(year, *args)

    monkeypatch.setattr(store, "publish_year", fail)
    state.clock[0] += 301
    with pytest.raises(CacheUnavailable):
        state.cache.cycle()
    assert store.client.get(store.key("year:2020")) == previous
    assert store.client.smembers(store.key("dirty-months")) == {b"202001"}
    monkeypatch.setattr(store, "publish_year", original)
    state.cache.cycle()
    assert annual_room(store, "2020", state.rid)["live_duration"] == "30:00:00"
    assert not store.client.smembers(store.key("dirty-months"))


def test_newly_discovered_year_is_unavailable_until_its_first_month_is_warmed(annual_cache):
    state = annual_cache
    state.base["room_stats_monthly"].append({"room_id": state.rid, "month": "201812", "gift": 5})
    state.daily["201812"] = [{"room_id": state.rid, "date": datetime.date(2018, 12, 1), "duration": 3600}]
    state.cache.refresh_hot(discover=True)
    assert state.cache.reader.reads == []
    with pytest.raises(CacheUnavailable):
        state.cache.store.read_year("2018")
    state.clock[0] += 11
    state.cache.cycle()
    assert state.cache.reader.reads == ["201812"]
    row = annual_room(state.cache.store, "2018", state.rid)
    assert row["gift"] == 5 and row["live_duration"] == "01:00:00"


@pytest.mark.parametrize("fault", ["stale", "missing", "corrupt", "not-ready"])
def test_annual_cache_failure_has_no_mysql_fallback(annual_cache, monkeypatch, fault):
    state = annual_cache
    store = state.cache.store
    year = month_str()[:4]
    generation = store.client.get(store.key(f"year:{year}"))
    if fault == "stale":
        store.client.hset(generation, "_snapshot", str(store.clock() - 31))
    elif fault == "missing":
        store.client.delete(generation)
    elif fault == "corrupt":
        store.client.hset(generation, "/gift/by_month", b"broken")
    else:
        store.client.delete(store.key("ready"))
    monkeypatch.setattr(gift, "Session", lambda: pytest.fail("no annual SQL fallback"))
    with app_client(store, monkeypatch) as client:
        result = client.get(f"/gift/by_month?year={year}")
        assert result.status_code == 503 and result.headers["retry-after"] == "1"


def test_year_rollover_keeps_last_december_in_the_completed_year(store, monkeypatch):
    from app import api_cache_payloads

    rid = room_config.get_room_ids()[0]
    builder = PayloadBuilder()
    value = snapshot(store, "202612")
    value.base["room_stats_monthly"] = [
        {"room_id": rid, "month": "202612", "gift": 5},
        {"room_id": rid, "month": "202701", "gift": 7},
    ]
    value.tables["room_live_stats"] = [{"room_id": rid, "date": datetime.date(2026, 12, 1), "duration": 7200}]
    monkeypatch.setattr(api_cache_payloads, "month_str", lambda: "202612")
    builder.build(value)
    value.month = "202701"
    value.tables["room_live_stats"] = [{"room_id": rid, "date": datetime.date(2027, 1, 1), "duration": 10800}]
    monkeypatch.setattr(api_cache_payloads, "month_str", lambda: "202701")
    builder.build(value)
    old = next(row for row in json.loads(gzip.decompress(builder.year_values("2026", value.base)["/gift/by_month"])) if row["room_id"] == rid)
    new = next(row for row in json.loads(gzip.decompress(builder.year_values("2027", value.base)["/gift/by_month"])) if row["room_id"] == rid)
    assert (old["gift"], old["live_duration"], old["guard_1"]) == (5, "02:00:00", None)
    assert (new["gift"], new["live_duration"]) == (7, "03:00:00")


@pytest.mark.parametrize("during_prewarm", [False, True])
def test_rollover_reloads_closed_month_even_when_its_base_totals_did_not_change(
    annual_cache, monkeypatch, during_prewarm
):
    from app import api_cache, api_cache_payloads
    from app.repositories import tables

    state = annual_cache
    cache = state.cache
    store = cache.store
    old = month_str()
    _, first_next_month = tables.month_range(old)
    new = first_next_month.strftime("%Y%m")
    state.daily[new] = []
    current = [old]
    for module in (api_cache, api_cache_payloads, tables):
        monkeypatch.setattr(module, "month_str", lambda: current[0])
    monkeypatch.setitem(globals(), "month_str", lambda: current[0])

    def advance_month():
        state.daily[old][0]["duration"] += 3600
        current[0] = new

    if during_prewarm:
        # Advance at the beginning of the final hot refresh, after all startup
        # historical reads have finished but before readiness is published.
        current_snapshot = cache.reader.current
        hot_calls = 0

        def cross_boundary(**kwargs):
            nonlocal hot_calls
            hot_calls += 1
            if hot_calls == 1:
                # The future month should not be discovered in the old snapshot.
                with monkeypatch.context() as patch:
                    patch.setattr(cache.reader, "known_months", lambda: set(state.daily) - {new})
                    return current_snapshot(**kwargs)
            if hot_calls == 2:
                advance_month()
            return current_snapshot(**kwargs)

        monkeypatch.setattr(cache.reader, "current", cross_boundary)
        cache.prewarm()
        assert cache.reader.reads.count(old) == 1
    else:
        advance_month()
        state.clock[0] += config.API_CACHE_REFRESH_SECONDS + 1
        cache.cycle()
        assert cache.reader.reads == [old]
    assert cache.history_due == {}
    assert state.clock[0] < 1000 + config.API_CACHE_HISTORY_SECONDS
    row = next(
        row for row in json.loads(gzip.decompress(store.read(old, "/gift/by_month")))
        if row["room_id"] == state.rid
    )
    assert row["live_duration"] == "03:00:00"
    state.clock[0] += config.API_CACHE_REFRESH_SECONDS + 1
    cache.cycle()
    assert cache.reader.reads.count(old) == 1


def test_annual_diagnostic_route_still_uses_cache_and_validates_year(annual_cache, monkeypatch):
    state = annual_cache
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    monkeypatch.setattr(gift, "Session", lambda: pytest.fail("annual diagnostic mode must not use SQL"))
    app = FastAPI()
    app.router.routes.extend(api_app.app.router.routes)
    app.state.api_cache = SimpleNamespace(store=state.cache.store)
    with TestClient(app) as client:
        result = client.get("/gift/by_month?year=2020")
        assert result.status_code == 200
        assert next(row for row in result.json() if row["room_id"] == state.rid)["gift"] == 30.3
        assert client.get("/gift/by_month?year=2020&month=202001").status_code == 400
        app.state.api_cache = None
        assert client.get("/gift/by_month?year=2020").status_code == 503


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


def test_cached_summaries_round_whale_ratios(store, monkeypatch):
    metrics = whale_metrics.calculate_whale_metrics({7: 7594}, 100000, 92406)
    monkeypatch.setattr(api_app, "get_live_whale_metrics", lambda *_: metrics)
    prepare(store)

    with app_client(store, monkeypatch) as client:
        for path in ("/gift", "/gift/by_month"):
            response = client.get(path)
            assert response.status_code == 200
            assert response.json()[0]["whale_dependency"] == {
                "status": "live",
                "source": "redis",
                "top1": 0.0759,
                "top5": 0.0759,
                "top10": 0.0759,
                "top1_percent": 0.0759,
            }


@pytest.mark.parametrize("historical", [False, True])
def test_cached_live_sessions_include_15m_stats_only_when_requested(store, monkeypatch, historical):
    value = snapshot(store, "190001" if historical else None)
    rid = room_config.get_room_ids()[0]
    value.tables["live_session"] = [
        {"id": 1, "room_id": rid, "start_time": datetime.datetime(1900, 1, 1), "end_time": None},
    ]
    value.tables["live_session_15m_stats"] = [
        {"session_id": 1, "room_id": rid, "bucket_index": 0, "danmaku_count": 42},
    ]
    prepare(store, value)
    params = {"room_id": rid, "month": value.month}

    with app_client(store, monkeypatch) as client:
        default = client.get("/gift/live_sessions", params=params)
        assert default.status_code == 200
        rows = default.json()["sessions"]
        assert len(rows) == 1 and "stats_15m" not in rows[0]

        for option in ("0", "false", "yes"):
            response = client.get("/gift/live_sessions", params={**params, "stats_15m": option})
            assert response.status_code == 200
            assert response.json() == default.json()

        for option in ("1", "true"):
            response = client.get("/gift/live_sessions", params={**params, "stats_15m": option})
            assert response.status_code == 200
            session = response.json()["sessions"][0]
            buckets = session.pop("stats_15m")
            assert session == rows[0]
            assert len(buckets) == 1 and buckets[0]["danmaku_count"] == 42


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
        for month in ("180001", month_str()):
            response = client.get(
                "/gift/live_sessions", params={"month": month, "room_id": 987654321, "stats_15m": 1}
            )
            assert response.status_code == 200
            assert response.json() == {"room_id": 987654321, "month": month, "sessions": []}
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
        {"room_id": rid, "month": value.month, "gift": 12.5, "danmaku_count": 7, "emoticon_danmaku_count": 2}
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
    assert current["danmaku"]["emoticon"] == 2
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


@pytest.mark.parametrize("prefix, keys", [
    ("live_session", ("id",)),
    ("live_session_15m_stats", ("session_id", "bucket_index")),
])
@pytest.mark.parametrize("archive_state", ["absent", "empty", "populated"])
def test_historical_sessions_merge_hot_rows_even_with_existing_archive(
    store, monkeypatch, prefix, keys, archive_state
):
    reader = SnapshotReader(store, threading.Event())
    archive = f"{prefix}_190001"
    hot = {keys[0]: 2, "month": "190001", "gift": 9, "normal_danmaku_count": 7}
    old = {keys[0]: 1, "month": "190001", "gift": 3}
    duplicate = {keys[0]: 2, "month": "190001", "gift": 1}
    if len(keys) == 2:
        for row in (hot, old, duplicate):
            row["bucket_index"] = 0
    calls = []

    def query(sql, params=None):
        calls.append(sql)
        if "information_schema" in sql:
            assert params[-1] == archive
            return [] if archive_state == "absent" else [{"TABLE_NAME": archive}]
        if f"`{archive}`" in sql:
            return [old, duplicate] if archive_state == "populated" else []
        assert sql == f"SELECT * FROM `{prefix}` WHERE month=%s"
        return [hot]

    monkeypatch.setattr(reader, "query", query)
    # Stale discovery must neither hide new archives nor force reads of deleted ones.
    reader.metadata = {archive} if archive_state == "absent" else set()
    rows = reader.read_table(prefix, "190001")
    by_id = {row[keys[0]]: row for row in rows}
    assert set(by_id) == ({1, 2} if archive_state == "populated" else {2})
    assert by_id[2]["gift"] == 9 and by_id[2]["normal_danmaku_count"] == 7
    if archive_state == "populated":
        assert by_id[1].get("normal_danmaku_count") is None
    assert calls[0] == f"SELECT * FROM `{prefix}` WHERE month=%s"
    assert (archive in reader.metadata) == (archive_state != "absent")


def test_historical_bucket_dedup_keeps_distinct_indexes_for_same_session(store, monkeypatch):
    reader = SnapshotReader(store, threading.Event())

    def query(sql, _params=None):
        if "information_schema" in sql:
            return [{"TABLE_NAME": "live_session_15m_stats_190001"}]
        return [{"session_id": 1, "bucket_index": 1 if "_190001`" in sql else 0}]

    monkeypatch.setattr(reader, "query", query)
    rows = reader.read_table("live_session_15m_stats", "190001")
    assert {(row["session_id"], row["bucket_index"]) for row in rows} == {(1, 0), (1, 1)}


@pytest.mark.parametrize("prefix", ["live_session", "live_session_15m_stats"])
def test_current_session_reads_remain_one_hot_query_and_reject_invalid_months(store, monkeypatch, prefix):
    reader = SnapshotReader(store, threading.Event())
    calls = []
    monkeypatch.setattr(reader, "query", lambda sql, params: calls.append((sql, params)) or [])
    assert reader.read_table(prefix, month_str()) == []
    assert calls == [(f"SELECT * FROM `{prefix}` WHERE month=%s", (month_str(),))]
    with pytest.raises(ValueError):
        reader.read_table(prefix, "190001`bad")
    assert len(calls) == 1


@pytest.mark.parametrize("error_code, failures, attempts", [(1412, 1, 2), (1412, 3, 3), (1205, 1, 1)])
def test_history_restarts_whole_snapshot_for_archive_ddl_but_bounds_retries(
    store, monkeypatch, error_code, failures, attempts
):
    connections = []
    closed = []
    committed = []
    reads = []
    stamps = []

    def connect(*_args):
        ident = len(connections)
        connection = SimpleNamespace(
            close=lambda: closed.append(ident), commit=lambda: committed.append(ident)
        )
        connections.append(connection)
        return connection

    def clock():
        stamps.append(len(stamps) + 1)
        return stamps[-1]

    reader = SnapshotReader(store, threading.Event(), connect)
    monkeypatch.setattr(reader, "query", lambda *_args: [])
    monkeypatch.setattr(store, "clock", clock)

    def read_table(prefix, _month):
        reads.append(prefix)
        if prefix == "live_session" and len(connections) <= failures:
            raise pymysql.OperationalError(error_code, "synthetic interrupted snapshot")
        return []

    monkeypatch.setattr(reader, "read_table", read_table)
    if error_code == 1412 and failures == 1:
        result = reader.history("190001")
        assert result.stamp == 2 and committed == [1]
    else:
        with pytest.raises(pymysql.OperationalError):
            reader.history("190001")
        assert committed == []
    assert len(connections) == len(stamps) == attempts
    assert reads.count("room_live_stats") == reads.count("attention") == attempts
    assert len(closed) == min(failures, attempts)


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


@pytest.fixture
def incremental_cache(store, monkeypatch):
    from app import api_cache

    clock = [1000.0]
    monkeypatch.setattr(api_cache.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(api_cache, "_pending_history", set())
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)

    class Reader:
        def __init__(self, target, _stop):
            self.store = target
            self.reads = []

        def known_months(self):
            return {month_str(), "190001", "190002"}

        def current(self, **_kwargs):
            value = snapshot(self.store)
            value.months = self.known_months()
            return value

        def history(self, month):
            self.reads.append(month)
            return snapshot(self.store, month)

    cache = ApiCache(store, Reader)
    monkeypatch.setattr(api_app.app.state, "api_cache", cache, raising=False)
    cache.prewarm()
    assert cache.reader.reads == ["190001", "190002"]
    cache.reader.reads.clear()
    return cache, clock


def test_history_poll_skips_unchanged_months_across_hot_and_five_minute_cycles(incremental_cache):
    cache, clock = incremental_cache
    for seconds in (11, 301, 11, 301):
        clock[0] += seconds
        cache.cycle()
    assert cache.reader.reads == []
    assert cache.history_due == {}


def test_only_changed_month_is_read_on_next_history_poll(incremental_cache):
    cache, clock = incremental_cache
    invalidate_history("190001")
    invalidate_history("190001")
    assert cache.store.client.zscore(cache.store.key("history-changed-at"), "190001")
    clock[0] += 299
    cache.cycle()
    assert cache.reader.reads == []
    clock[0] += 2
    cache.cycle()
    assert cache.reader.reads == ["190001"]
    assert not cache.store.client.smembers(cache.store.key("dirty-months"))
    assert cache.store.history_revision("190001") == b""
    assert cache.store.client.zscore(cache.store.key("history-changed-at"), "190001") is None
    clock[0] += 301
    cache.cycle()
    assert cache.reader.reads == ["190001"]


def test_old_pending_changes_survive_multiple_intervals_and_drain_one_month_per_cycle(incremental_cache):
    cache, clock = incremental_cache
    invalidate_history("190001")
    invalidate_history("190002")
    clock[0] += 1200
    cache.cycle()
    assert cache.reader.reads == ["190001"]
    assert cache.store.client.smembers(cache.store.key("dirty-months")) == {b"190002"}
    cache.cycle()
    assert cache.reader.reads == ["190001", "190002"]


def test_write_during_history_refresh_is_not_acknowledged_as_old_snapshot(incremental_cache, monkeypatch):
    cache, clock = incremental_cache
    invalidate_history("190001")
    original = cache.reader.history

    def concurrent_write(month):
        value = original(month)
        invalidate_history(month)
        return value

    monkeypatch.setattr(cache.reader, "history", concurrent_write)
    clock[0] += 301
    cache.cycle()
    assert cache.store.client.smembers(cache.store.key("dirty-months")) == {b"190001"}
    monkeypatch.setattr(cache.reader, "history", original)
    clock[0] += 301
    cache.cycle()
    assert cache.reader.reads == ["190001", "190001"]
    assert not cache.store.client.smembers(cache.store.key("dirty-months"))


@pytest.mark.parametrize("stage", ["read", "publish"])
def test_failed_history_refresh_retains_pending_month_and_previous_generation(
    incremental_cache, monkeypatch, stage
):
    cache, clock = incremental_cache
    invalidate_history("190001")
    previous = cache.store.client.get(cache.store.key("month:190001"))
    owner = cache.reader if stage == "read" else cache.store
    method = "history" if stage == "read" else "publish"
    original = getattr(owner, method)

    def fail(month, *args):
        if month == "190001":
            raise CacheUnavailable("synthetic interrupted refresh")
        return original(month, *args)

    monkeypatch.setattr(owner, method, fail)
    clock[0] += 301
    with pytest.raises(CacheUnavailable):
        cache.cycle()
    assert cache.store.client.get(cache.store.key("month:190001")) == previous
    assert cache.store.client.smembers(cache.store.key("dirty-months")) == {b"190001"}
    monkeypatch.setattr(owner, method, original)
    cache.cycle()
    assert not cache.store.client.smembers(cache.store.key("dirty-months"))


def test_redis_notification_failure_is_retried_without_blocking_committed_write(
    incremental_cache, monkeypatch
):
    from app import api_cache

    cache, _clock = incremental_cache
    original = cache.store.mark_history_changed

    def unavailable(_month):
        raise redis.ConnectionError("synthetic Redis outage")

    monkeypatch.setattr(cache.store, "mark_history_changed", unavailable)
    invalidate_history("190001")
    assert api_cache._pending_history == {"190001"}
    monkeypatch.setattr(cache.store, "mark_history_changed", original)
    cache.cycle()
    assert not api_cache._pending_history
    assert cache.store.client.smembers(cache.store.key("dirty-months")) == {b"190001"}


def test_restart_prewarm_still_reads_all_months_and_acknowledges_existing_changes(incremental_cache):
    cache, _clock = incremental_cache
    invalidate_history("190001")
    cache.prewarm()
    assert cache.reader.reads == ["190001", "190002"]
    assert not cache.store.client.smembers(cache.store.key("dirty-months"))


@pytest.mark.parametrize("month", ["190001", month_str()])
def test_active_session_payer_write_reuses_known_month_without_extra_sql(
    incremental_cache, monkeypatch, month
):
    from app import metrics_runtime
    from app.models import LiveSession
    from app.repositories import live_sessions

    cache, _clock = incremental_cache
    calls = []

    class Session:
        def execute(self, _statement):
            calls.append("update")

        def commit(self):
            assert not cache.store.client.smembers(cache.store.key("dirty-months"))
            calls.append("commit")

        def close(self):
            calls.append("close")

    monkeypatch.setattr(live_sessions, "Session", Session)
    monkeypatch.setattr(metrics_runtime, "session_month", lambda _session_id: month)
    live_sessions.set_payer_count(LiveSession, 123, 2)
    assert calls == ["update", "commit", "close"]
    assert cache.store.client.smembers(cache.store.key("dirty-months")) == (
        {b"190001"} if month == "190001" else set()
    )


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
        middleware = client.app.middleware_stack
        while not isinstance(middleware, CacheMiddleware):
            middleware = middleware.app
        gate = middleware.gate
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
