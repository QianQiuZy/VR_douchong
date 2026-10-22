"""Publication policy across cache generations, entry modes and legacy reports."""

from __future__ import annotations

import asyncio
import datetime
import gzip
import json
from types import SimpleNamespace
from pathlib import Path

import pytest
import redis
from fastapi.testclient import TestClient

from app import api_app, config, event_ingestion, gift, room_config, room_visibility, runtime_state
from app.api_cache_payloads import PayloadBuilder
from app.api_cache_source import BASE_TABLES, MONTH_TABLES, Snapshot
from app.api_cache_store import CacheStore, encode
from app.repositories.tables import month_str

HIDE = "/delete/room/temporary"
RESTORE = "/restore/room/temporary"
AUTH = {"X-API-Key": "test-api-secret-abc123"}
ROOM = 111111
OTHER = 222222


@pytest.fixture
def published(monkeypatch, isolated_room_visibility):
    client = isolated_room_visibility.client
    monkeypatch.setattr(
        client,
        "memory_usage",
        lambda key: sum(len(k) + len(v) for k, v in client.hgetall(key).items()),
    )
    store = CacheStore(client, prefix="test:reports")
    assert store.acquire()
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)
    monkeypatch.setattr(api_app.app.state, "api_cache", SimpleNamespace(store=store), raising=False)
    monkeypatch.setattr(api_app, "_whale_dependency_for_room", lambda *_: {"status": "unavailable"})
    monkeypatch.setattr(gift, "Session", lambda: pytest.fail("cached queries must not read MySQL"))

    def publish(month=None, gift_amount=12.5):
        month = month or month_str()
        when = (
            api_app._entry_now() - datetime.timedelta(minutes=1)
            if month == month_str()
            else datetime.datetime(1900, 1, 2)
        )
        snapshot = Snapshot(
            month, store.clock(), {t: [] for t in BASE_TABLES}, {t: [] for t in MONTH_TABLES}
        )
        for rid in (ROOM, OTHER):
            snapshot.base["room_info"].append({"room_id": rid, "anchor_name": f"Anchor{rid}"})
            snapshot.base["room_stats_monthly"].append(
                {"room_id": rid, "month": month, "gift": gift_amount}
            )
            snapshot.tables["live_session"].append(
                {"room_id": rid, "id": rid, "start_time": when, "end_time": None}
            )
            snapshot.tables["attention"].append(
                {"room_id": rid, "date": when.date(), "attention": 10}
            )
            snapshot.tables["super_chat_log"].append(
                {
                    "room_id": rid,
                    "id": rid,
                    "send_time": when,
                    "uname": "viewer",
                    "uid": 7,
                    "price": 30,
                    "message": "hello",
                }
            )
            snapshot.tables["room_entry_log"].append({"room_id": rid, "uid": 7, "event_time": when})
        snapshot.recent_entries = snapshot.tables["room_entry_log"]
        store.publish(month, PayloadBuilder().build(snapshot), snapshot.stamp)
        store.ready()

    publish()
    publish("190001")
    http = TestClient(api_app.app, client=("203.0.113.70", 123))
    yield SimpleNamespace(http=http, store=store, policy=isolated_room_visibility, publish=publish)
    http.close()


@pytest.mark.parametrize("path", [HIDE, RESTORE])
def test_post_payload_authentication_validation_and_idempotence(published, path):
    client = published.http
    assert client.post(path, json={"room_id": ROOM}).status_code == 401
    assert (
        client.post(path, json={"room_id": ROOM}, headers={"X-API-Key": "bad"}).status_code == 401
    )
    assert client.post(path + f"?room_id={ROOM}", headers=AUTH).status_code == 400
    assert client.get(path).status_code == 405
    if path == RESTORE:
        published.policy.set_hidden(ROOM, True)
    first = client.post(path, json={"room_id": ROOM}, headers=AUTH)
    assert first.status_code == 200
    assert first.json() == {
        "ok": True,
        "room_id": ROOM,
        "hidden": path == HIDE,
        "changed": True,
        "message": "房间已临时删除" if path == HIDE else "房间已恢复展示",
    }
    repeated = client.post(
        path,
        json={"room_id": str(ROOM)},
        headers={"Authorization": "Bearer test-api-secret-abc123"},
    )
    assert repeated.status_code == 200 and repeated.json()["changed"] is False


@pytest.mark.parametrize("value", [None, True, 0, -1, 1.5, "1.5", "abc", "", 2**64])
@pytest.mark.parametrize("path", [HIDE, RESTORE])
def test_invalid_room_ids_are_rejected(published, value, path):
    assert published.http.post(path, json={"room_id": value}, headers=AUTH).status_code == 400
    assert published.policy.hidden_rooms() == set()


@pytest.mark.parametrize("query", ["", "?month=190001", "?month=180001"])
@pytest.mark.parametrize("encoding", ["gzip", "identity"])
def test_summaries_hide_and_restore_without_rebuilding(published, query, encoding):
    path = "/gift/by_month" + query if query else "/gift"
    client = published.http
    headers = {"Accept-Encoding": encoding}
    original = client.get(path, headers=headers).json()
    assert {row["room_id"] for row in original} == {ROOM, OTHER}
    generation = published.store.client.get(published.store.key(f"month:{month_str()}"))
    before = published.store.client.hgetall(generation)
    assert client.post(HIDE, json={"room_id": ROOM}, headers=AUTH).status_code == 200
    response = client.get(path, headers=headers)
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    assert response.json() == [row for row in original if row["room_id"] != ROOM]
    assert int(response.headers["content-length"]) > 0
    assert (response.headers.get("content-encoding") == "gzip") == (encoding == "gzip")
    assert published.store.client.hgetall(generation) == before
    assert client.post(RESTORE, json={"room_id": ROOM}, headers=AUTH).status_code == 200
    assert client.get(path, headers=headers).json() == original


@pytest.mark.parametrize("path", ["live_sessions", "attention", "sc", "entry"])
@pytest.mark.parametrize("month", ["", "190001", "180001"])
def test_direct_queries_return_404_instead_of_empty_room_objects(published, path, month):
    client = published.http
    query = f"/gift/{path}?room_id={ROOM}" + (f"&month={month}" if month else "")
    original = client.get(query).json()
    assert client.post(HIDE, json={"room_id": ROOM}, headers=AUTH).status_code == 200
    response = client.get(query)
    assert response.status_code == 404 and response.json() == {"error": "房间不存在"}
    assert "room_id" not in response.json() and response.headers["cache-control"] == "no-store"
    assert client.get(query.replace(str(ROOM), str(OTHER))).status_code == 200
    assert client.post(RESTORE, json={"room_id": ROOM}, headers=AUTH).status_code == 200
    assert client.get(query).json() == original


@pytest.mark.parametrize("query", ["uid=7", "uid=7&month=190001", "cache=1", "cache=2"])
def test_entry_uid_and_recent_windows_filter_each_room(published, query):
    client = published.http
    path = "/gift/entry/?" + query
    original = client.get(path).json()
    assert {row["room_id"] for row in original["items"]} == {ROOM, OTHER}
    published.policy.set_hidden(ROOM, True)
    response = client.get(path)
    assert response.status_code == 200
    assert response.json()["items"] == [row for row in original["items"] if row["room_id"] == OTHER]
    published.policy.set_hidden(OTHER, True)
    assert client.get(path).json()["items"] == []
    published.policy.set_hidden(ROOM, False)
    assert {row["room_id"] for row in client.get(path).json()["items"]} == {ROOM}


def test_policy_survives_cache_prewarm_and_new_readers_and_is_namespaced(published):
    published.policy.set_hidden(ROOM, True)
    assert published.policy.client.ttl(published.policy.key) == -1
    published.store.begin_prewarm()
    published.publish(gift_amount=99)
    second = room_visibility.RoomVisibilityStore(published.policy.client, prefix="test:visibility")
    assert second.hidden_rooms() == {ROOM}
    assert (
        room_visibility.RoomVisibilityStore(
            published.policy.client, prefix="psp:api:v1"
        ).hidden_rooms()
        == set()
    )
    assert all(row["room_id"] != ROOM for row in published.http.get("/gift").json())
    published.http.post(RESTORE, json={"room_id": ROOM}, headers=AUTH)
    row = next(row for row in published.http.get("/gift").json() if row["room_id"] == ROOM)
    assert row["gift"] == 99
    # Historical-only/unconfigured room IDs can also be hidden; no config lookup.
    assert published.http.post(HIDE, json={"room_id": 999999}, headers=AUTH).status_code == 200


@pytest.mark.parametrize("enabled", [True, False])
def test_redis_faults_fail_closed_for_reads_and_management(published, monkeypatch, enabled):
    monkeypatch.setattr(config, "API_CACHE_ENABLED", enabled)

    def broken(*_):
        raise redis.ConnectionError("must not expose connection details")

    monkeypatch.setattr(published.policy.client, "smembers", broken)
    response = published.http.get("/gift")
    assert response.status_code == 503 and response.json() == {"error": "房间可见状态暂不可用"}
    monkeypatch.setattr(published.policy, "set_hidden", broken)
    for path in (HIDE, RESTORE):
        assert published.http.post(path, json={"room_id": ROOM}, headers=AUTH).status_code == 503


def test_corrupt_policy_is_not_treated_as_no_hidden_rooms(published):
    published.policy.client.sadd(published.policy.key, b"not-a-room")
    assert published.http.get("/gift").status_code == 503


def test_legacy_reports_are_also_filtered(monkeypatch, isolated_room_visibility):
    from test_routes_contract import _StubSession

    monkeypatch.setattr(config, "API_CACHE_ENABLED", False)
    monkeypatch.setattr(gift, "Session", lambda: _StubSession())
    monkeypatch.setattr(gift, "_room_ids_for_month", lambda *_, **__: [ROOM, OTHER])
    monkeypatch.setattr(gift, "sc_log_table_exists", lambda *_: False)
    monkeypatch.setattr(api_app, "_whale_dependency_for_room", lambda *_: {"status": "unavailable"})
    monkeypatch.setattr(api_app, "inspect", lambda *_: SimpleNamespace(has_table=lambda *_: False))
    monkeypatch.setattr(gift.RoomLiveStats, "month_aggregate_for_month", lambda *_, **__: {})
    monkeypatch.setattr(gift.RoomLiveStats, "month_steel_coin_for_month", lambda *_, **__: {})
    isolated_room_visibility.set_hidden(ROOM, True)
    client = TestClient(api_app.app)
    try:
        for path in ("/gift", "/gift/by_month", "/gift/by_month?month=190001"):
            response = client.get(path)
            assert response.status_code == 200
            assert [row["room_id"] for row in response.json()] == [OTHER]
        for path in ("live_sessions", "attention", "sc"):
            response = client.get(f"/gift/{path}?room_id={ROOM}")
            assert response.status_code == 404
    finally:
        client.close()


def test_hide_does_not_touch_collection_or_configuration(published, monkeypatch):
    calls = []
    client = SimpleNamespace(room_id=ROOM)
    monkeypatch.setattr(runtime_state, "CURRENT_SESSIONS", {ROOM: 9})
    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {ROOM: client})
    monkeypatch.setattr(
        event_ingestion.RoomStatsMonthly,
        "add_amounts",
        lambda *a, **kw: calls.append(("monthly", a, kw)),
    )
    monkeypatch.setattr(
        event_ingestion.LiveSession,
        "add_values_by_id",
        lambda *a, **kw: calls.append(("session", a, kw)),
    )
    monkeypatch.setattr(
        event_ingestion.RoomLiveStats,
        "add_metrics",
        lambda *a, **kw: calls.append(("daily", a, kw)),
    )
    monkeypatch.setattr(event_ingestion, "register_payer", lambda *a, **kw: None)
    monkeypatch.setattr(event_ingestion, "record_payment", lambda *a, **kw: None)
    ids = room_config.get_room_ids()
    config_bytes = Path(runtime_state.ROOMS_JSON_PATH).read_bytes()
    assert published.http.post(HIDE, json={"room_id": ROOM}, headers=AUTH).status_code == 200
    event_ingestion.MyHandler()._on_gift(
        client,
        SimpleNamespace(
            total_price=2000, total_coin=2000, gift_name="test", num=2, uname="viewer", uid=1
        ),
    )
    assert [name for name, *_ in calls] == ["monthly", "session", "daily"]
    assert calls[0][2] == {"gift": 2.0}
    assert runtime_state.CURRENT_SESSIONS == {ROOM: 9}
    assert runtime_state.ROOM_CLIENTS[ROOM] is client
    assert room_config.get_room_ids() == ids
    assert Path(runtime_state.ROOMS_JSON_PATH).read_bytes() == config_bytes


def test_new_posts_share_rate_limit_and_preserve_parameter_errors(published):
    client = published.http
    published.policy.set_hidden(ROOM, True)
    assert client.get("/gift/entry?room_id=111111&uid=7").status_code == 400
    for _ in range(19):
        assert client.post(HIDE, json={"room_id": ROOM}, headers=AUTH).status_code == 200
    assert client.post(RESTORE, json={"room_id": ROOM}, headers=AUTH).status_code == 429
    assert published.policy.hidden_rooms() == {ROOM}


def test_chunked_compressed_response_is_filtered_before_any_body_is_sent(isolated_room_visibility):
    isolated_room_visibility.set_hidden(ROOM, True)
    body = encode([{"room_id": ROOM}, {"room_id": OTHER}])

    async def inner(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json"), (b"content-encoding", b"gzip")],
            }
        )
        await send({"type": "http.response.body", "body": body[:10], "more_body": True})
        await send({"type": "http.response.body", "body": body[10:]})

    messages = []

    async def send(message):
        messages.append(message)

    asyncio.run(
        room_visibility.RoomVisibilityMiddleware(inner)(
            {"type": "http", "method": "GET", "path": "/gift"}, None, send
        )
    )
    assert messages[0]["status"] == 200
    assert json.loads(gzip.decompress(messages[1]["body"])) == [{"room_id": OTHER}]
