import datetime
import threading
from types import SimpleNamespace

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import api_app, config, event_ingestion, room_config
from app.api_cache_http import CacheMiddleware
from app.api_cache_payloads import PayloadBuilder
from app.api_cache_source import BASE_TABLES, MONTH_TABLES, Snapshot, SnapshotReader
from app.api_cache_store import CacheStore, encode
from app.blivedm.models.web import PopularityRedPocketV2NewMessage
from app.event_ingestion import now_local
from app.repositories.tables import month_str


@pytest.fixture
def cache(monkeypatch):
    client = fakeredis.FakeRedis()
    monkeypatch.setattr(
        client,
        "memory_usage",
        lambda key: sum(len(k) + len(v) for k, v in client.hgetall(key).items()),
    )
    store = CacheStore(client, prefix="test:entry")
    assert store.acquire()
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    monkeypatch.setattr(
        api_app, "_whale_dependency_for_room", lambda *_: {"status": "unavailable"}
    )
    app = FastAPI()
    app.router.routes.extend(api_app.app.router.routes)
    app.state.api_cache = SimpleNamespace(store=store)
    app.add_middleware(CacheMiddleware, owner=app)
    with TestClient(app, client=("203.0.113.42", 123)) as http:
        yield store, http
    client.close()


def publish(store, month=None, rows=None, recent=None):
    value = Snapshot(
        month or month_str(),
        store.clock(),
        {table: [] for table in BASE_TABLES},
        {table: [] for table in MONTH_TABLES},
    )
    value.tables["room_entry_log"] = rows or []
    value.recent_entries = recent or []
    store.publish(value.month, PayloadBuilder().build(value), value.stamp)
    store.ready()


def test_room_uid_and_history_minimal_shapes_and_zero_sql(cache, monkeypatch):
    store, client = cache
    when = now_local().replace(microsecond=0) - datetime.timedelta(minutes=1)
    rows = [
        {"room_id": 111111, "uid": 7, "event_time": when},
        {"room_id": 222222, "uid": 7, "event_time": when},
    ]
    publish(store, rows=rows)
    monkeypatch.setattr(
        api_app,
        "_default_Session",
        lambda: pytest.fail("entry HTTP must not read MySQL"),
    )
    room = client.get("/gift/entry?room_id=111111").json()
    stamp = when.replace(tzinfo=api_app.SHANGHAI).isoformat()
    assert room == {
        "mode": "room",
        "room_id": 111111,
        "month": month_str(),
        "items": [{"uid": 7, "event_time": stamp}],
    }
    uid = client.get("/gift/entry?uid=7").json()
    assert uid == {
        "mode": "uid",
        "uid": 7,
        "month": month_str(),
        "items": [
            {"room_id": 222222, "event_time": stamp},
            {"room_id": 111111, "event_time": stamp},
        ],
    }
    assert client.get("/gift/entry?uid=8").json()["items"] == []
    unavailable = client.get("/gift/entry?room_id=111111&month=1900-01")
    assert unavailable.status_code == 200
    assert unavailable.json() == {
        "mode": "room",
        "room_id": 111111,
        "month": "190001",
        "status": "unavailable",
        "items": [],
    }
    publish(
        store,
        "200101",
        [
            {
                "room_id": 111111,
                "uid": 7,
                "event_time": datetime.datetime.fromisoformat("2001-01-02T00:00:00"),
            }
        ],
    )
    historical = client.get("/gift/entry?uid=7&month=200101")
    assert historical.status_code == 200 and "status" not in historical.json()


def test_ten_minute_and_hour_windows_include_previous_month(cache, monkeypatch):
    store, client = cache
    current = now_local().replace(day=1, hour=0, minute=20, second=0, microsecond=0)
    monkeypatch.setattr(api_app, "_entry_now", lambda: current)
    recent = [
        {
            "room_id": 111111,
            "uid": seconds,
            "event_time": current - datetime.timedelta(seconds=seconds),
        }
        for seconds in (0, 599, 600, 601, 3599, 3600, 3601)
    ]
    publish(store, recent=recent)
    # UID zero is invalid; use a valid UID for the event exactly at the excluded end.
    recent[0]["uid"] = 1
    publish(store, recent=recent)
    ten = client.get("/gift/entry?cache=1")
    assert ten.status_code == 200
    assert set(ten.json()) == {"mode", "cache", "items", "red_pocket"}
    assert ten.json()["red_pocket"] == []
    assert [row["uid"] for row in ten.json()["items"]] == [599, 600]
    hour = client.get("/gift/entry?cache=2").json()
    assert [row["uid"] for row in hour["items"]] == [599, 600, 601, 3599, 3600]
    assert any(
        row["event_time"][:7] != current.strftime("%Y-%m") for row in hour["items"]
    )


@pytest.mark.parametrize(
    "query",
    [
        "",
        "room_id=1&uid=7",
        "room_id=1&cache=1",
        "uid=7&cache=1",
        "room_id=0",
        "uid=-1",
        "uid=18446744073709551616",
        "cache=0",
        "cache=3",
        "cache=1&month=202610",
        "uid=7&month=",
        "uid=7&month=bad",
        "uid=7&month=999812",
        "uid=7&uid=8",
        "room_id=abc",
    ],
)
def test_invalid_parameters_return_400(cache, query):
    store, client = cache
    publish(store)
    assert client.get("/gift/entry?" + query).status_code == 400


def test_entry_cache_missing_stale_corrupt_and_disabled_fail_closed(cache, monkeypatch):
    store, client = cache
    assert client.get("/gift/entry?uid=7").status_code == 503
    publish(store)
    generation = store.client.get(store.key(f"month:{month_str()}"))
    store.client.hset(generation, "_snapshot", str(store.clock() - 31))
    assert client.get("/gift/entry?uid=7").status_code == 503
    publish(store)
    generation = store.client.get(store.key(f"month:{month_str()}"))
    store.client.hset(generation, "/gift/entry", b"corrupt")
    assert client.get("/gift/entry?uid=7").status_code == 503
    publish(store)
    store.client.hdel(
        store.client.get(store.key(f"month:{month_str()}")), "/gift/entry/recent"
    )
    assert client.get("/gift/entry?cache=2").status_code == 503
    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    assert client.get("/gift/entry?uid=7").status_code == 503


def test_entry_shares_existing_rate_limit_and_supports_identity_encoding(cache):
    store, client = cache
    publish(store)
    for _ in range(19):
        assert store.allow_ip("203.0.113.42")
    response = client.get("/gift/entry?uid=7", headers={"Accept-Encoding": "identity"})
    assert response.status_code == 200 and "content-encoding" not in response.headers
    assert response.headers["cache-control"] == "no-store"
    assert client.get("/gift").status_code == 429


def test_entry_route_owns_validation_and_cache_response_without_middleware(cache):
    store, _ = cache
    publish(store)
    app = FastAPI()
    app.add_api_route(api_app.ENTRY_PATH, api_app.get_room_entries, methods=["GET"])
    app.state.api_cache = SimpleNamespace(store=store)
    with TestClient(app) as client:
        assert client.get("/gift/entry?uid=7&room_id=1").status_code == 400
        response = client.get("/gift/entry?uid=7")
        assert response.status_code == 200
        assert response.headers["content-encoding"] == "gzip"
        assert response.headers["vary"] == "Accept-Encoding"
        assert response.json() == {
            "mode": "uid",
            "uid": 7,
            "month": month_str(),
            "items": [],
        }


def test_entry_trailing_slash_preserves_cache_transport_behavior(cache):
    store, client = cache
    publish(store)
    response = client.get("/gift/entry/?cache=1", follow_redirects=False)
    assert response.status_code == 200
    assert response.json() == {"mode": "cache", "cache": 1, "items": [], "red_pocket": []}


def test_red_pockets_match_example_and_appear_only_in_cache_one(cache, monkeypatch):
    store, client = cache
    now = now_local().replace(microsecond=0)
    monkeypatch.setattr(event_ingestion, "now_local", lambda: now)
    monkeypatch.setattr(api_app, "_entry_now", lambda: now)
    monkeypatch.setattr(event_ingestion, "save_entries", lambda *_: pytest.fail("red pockets must not write SQL"))
    monkeypatch.setattr(api_app, "_default_Session", lambda: pytest.fail("red pockets must not query SQL"))
    publish(store)
    start = now - datetime.timedelta(seconds=60)
    handler = event_ingestion.MyHandler()
    for lot, kind, price in ((1, 2, 9805), (2, 3, 100), (3, 0, 20)):
        handler.handle(SimpleNamespace(room_id=111111), {
            "cmd": "POPULARITY_RED_POCKET_V2_NEW", "data": {
                "lot_id": lot, "rp_type": kind, "price": price,
                "start_time": int(start.replace(tzinfo=api_app.SHANGHAI).timestamp()),
            },
        })
    expected = [
        {"room_id": 111111, "pocket_type": "gift", "gift_num": 20, "event_time": start.replace(tzinfo=api_app.SHANGHAI).isoformat()},
        {"room_id": 111111, "pocket_type": "battery", "battery_num": 100, "event_time": start.replace(tzinfo=api_app.SHANGHAI).isoformat()},
        {"room_id": 111111, "pocket_type": "guard", "guard_num": 5, "event_time": start.replace(tzinfo=api_app.SHANGHAI).isoformat()},
    ]
    response = client.get("/gift/entry?cache=1", headers={"Accept-Encoding": "identity"})
    assert response.json() == {"mode": "cache", "cache": 1, "items": [], "red_pocket": expected}
    assert client.get("/gift/entry/?cache=1").json()["red_pocket"] == expected
    for query in ("cache=2", "room_id=111111", "uid=7", "room_id=111111&month=190001", "uid=7&month=190001"):
        assert "red_pocket" not in client.get("/gift/entry?" + query).json()
    for path in ("/gift", "/gift/by_month", "/gift/live_sessions?room_id=111111",
                 "/gift/attention?room_id=111111", "/gift/sc?room_id=111111"):
        response = client.get(path)
        assert response.status_code == 200
        assert "red_pocket" not in response.text
    import gzip
    import json
    assert "red_pocket" not in json.loads(gzip.decompress(store.read(month_str(), api_app.RECENT_PATH)))
    assert event_ingestion.entry_monitor.queue.empty()


def test_red_pocket_expiration_uses_start_time_not_receipt_or_entry_snapshot(cache, monkeypatch):
    store, client = cache
    now = [now_local().replace(microsecond=0)]
    monkeypatch.setattr(event_ingestion, "now_local", lambda: now[0])
    monkeypatch.setattr(api_app, "_entry_now", lambda: now[0])
    start = now[0] - datetime.timedelta(seconds=599)
    command = {"data": {"lot_id": 1, "rp_type": 3, "price": 100,
                        "start_time": int(start.replace(tzinfo=api_app.SHANGHAI).timestamp())}}
    message = PopularityRedPocketV2NewMessage.from_command(command["data"])
    assert event_ingestion.red_pocket_monitor.receive(SimpleNamespace(room_id=111111), message)
    publish(store)
    assert len(client.get("/gift/entry?cache=1").json()["red_pocket"]) == 1
    now[0] += datetime.timedelta(seconds=1)
    assert client.get("/gift/entry?cache=1").json()["red_pocket"] == []
    assert not event_ingestion.red_pocket_monitor.receive(SimpleNamespace(room_id=111111), message)


def test_entry_only_rooms_do_not_expand_existing_report_domains(cache):
    store, _ = cache
    publish(store, rows=[{"room_id": 999999, "uid": 7, "event_time": now_local()}])
    import gzip
    import json

    original = json.loads(gzip.decompress(store.read(month_str(), "/gift")))
    assert {row["room_id"] for row in original} == set(room_config.get_room_ids())


def test_entry_reader_unions_hot_and_archive_and_discovers_entry_only_months(
    cache, monkeypatch
):
    store, _ = cache
    reader = SnapshotReader(store, threading.Event())
    calls = []

    def query(sql, params):
        calls.append((sql, params))
        if "information_schema" in sql:
            return [{"TABLE_NAME": "room_entry_log_202609"}]
        return []

    monkeypatch.setattr(reader, "query", query)
    assert reader.read_table("room_entry_log", "202609") == []
    assert "room_entry_log_202609" in reader.metadata
    assert "202609" in reader.known_months()
    assert " UNION " in calls[-1][0]
    assert (
        "room_entry_log`" in calls[-1][0] and "room_entry_log_202609`" in calls[-1][0]
    )
    assert len(calls[-1][1]) == 4


def test_well_checksummed_malformed_entry_payload_returns_503(cache):
    store, client = cache
    values = PayloadBuilder().build(
        Snapshot(
            month_str(),
            store.clock(),
            {table: [] for table in BASE_TABLES},
            {table: [] for table in MONTH_TABLES},
        )
    )
    values["/gift/entry"] = encode({"items": [{"uid": 7}]})
    store.publish(month_str(), values, store.clock())
    store.ready()
    assert client.get("/gift/entry?uid=7").status_code == 503
