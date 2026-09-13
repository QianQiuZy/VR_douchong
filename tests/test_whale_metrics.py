from __future__ import annotations

from collections import defaultdict
from types import SimpleNamespace

from app import api_app, event_ingestion, runtime_state, whale_metrics
from app.whale_metrics import _redis_db1_url


class _FakeWhaleRedis:
    def __init__(self) -> None:
        self.sets: dict[str, set[str]] = defaultdict(set)
        self.hashes: dict[str, dict[str, int]] = defaultdict(dict)

    def eval(self, _script: str, key_count: int, *args: str) -> int:
        dedupe_key, uid_key, totals_key, *_state_key = args[:key_count]
        event_key, uid, amount, category, _ttl = args[key_count:]
        if event_key in self.sets[dedupe_key]:
            return 0
        self.sets[dedupe_key].add(event_key)
        amount_int = int(amount)
        if uid != "0":
            self.hashes[uid_key][uid] = self.hashes[uid_key].get(uid, 0) + amount_int
            self.hashes[totals_key]["attributed_revenue"] = (
                self.hashes[totals_key].get("attributed_revenue", 0) + amount_int
            )
        else:
            self.hashes[totals_key]["unattributed_revenue"] = (
                self.hashes[totals_key].get("unattributed_revenue", 0) + amount_int
            )
        self.hashes[totals_key]["total_revenue"] = (
            self.hashes[totals_key].get("total_revenue", 0) + amount_int
        )
        self.hashes[totals_key][category] = self.hashes[totals_key].get(category, 0) + amount_int
        self.hashes[totals_key]["event_count"] = self.hashes[totals_key].get("event_count", 0) + 1
        return 1

    def hgetall(self, key: str) -> dict[str, str]:
        return {name: str(value) for name, value in self.hashes.get(key, {}).items()}


def test_calculate_whale_metrics_uses_deterministic_top_groups() -> None:
    # Given: five payers with a total revenue denominator including one
    # unattributed event.
    payer_spend = {101: 5000, 102: 4000, 103: 3000, 104: 2000, 105: 1000}

    # When: monthly whale dependency is calculated.
    metrics = whale_metrics.calculate_whale_metrics(
        payer_spend,
        total_revenue=16000,
        unattributed_revenue=1000,
    )

    # Then: top groups use the total-income denominator and top 1% has one
    # payer for a month with fewer than 100 eligible payers.
    assert metrics.payer_count == 5
    assert metrics.top1_amount == 5000
    assert metrics.top5_amount == 15000
    assert metrics.top10_amount == 15000
    assert metrics.top1pct_count == 1
    assert metrics.top1pct_amount == 5000
    assert metrics.top1_ratio == 5000 / 16000
    assert metrics.top5_ratio == 15000 / 16000


def test_redis_url_forces_db_one_over_url_database() -> None:
    # Given: a Redis URL whose path and query select DB0.
    url = "redis://localhost:6379/0?db=0&socket_timeout=1"

    # When: the application normalizes its whale-cache URL.
    normalized = _redis_db1_url(url)

    # Then: both URL database selectors are replaced by DB1.
    assert normalized == "redis://localhost:6379/1?socket_timeout=1"


def test_calculate_whale_metrics_breaks_equal_amounts_by_uid() -> None:
    # Given: equal amounts whose UID order must make the selected top group
    # deterministic.
    payer_spend = {20: 1000, 10: 1000, 30: 500}

    # When: the top one payer is selected.
    metrics = whale_metrics.calculate_whale_metrics(
        payer_spend,
        total_revenue=2500,
        unattributed_revenue=0,
    )

    # Then: the lower UID wins the tie-break.
    assert metrics.top1_amount == 1000
    assert metrics.top1pct_amount == 1000


def test_calculate_whale_metrics_returns_null_ratios_for_zero_revenue() -> None:
    # Given: a Redis month with no positive revenue.
    # When: monthly dependency is calculated.
    metrics = whale_metrics.calculate_whale_metrics({}, total_revenue=0, unattributed_revenue=0)

    # Then: zero is preserved as a valid aggregate and ratios are undefined.
    assert metrics.payer_count == 0
    assert metrics.top1_ratio is None
    assert metrics.top1pct_ratio is None


def test_redis_month_cache_deduplicates_events_and_keeps_unknown_revenue(monkeypatch) -> None:
    # Given: an isolated Redis DB1-compatible fake and two event identities.
    client = _FakeWhaleRedis()
    monkeypatch.setattr(whale_metrics, "_client", client)

    # When: the same known-UID event is recorded twice alongside an unknown one.
    assert whale_metrics.record_whale_revenue(301, "202609", 7, 1000, "gift:1", "gift") is True
    assert whale_metrics.record_whale_revenue(301, "202609", 7, 1000, "gift:1", "gift") is False
    assert whale_metrics.record_whale_revenue(301, "202609", 0, 500, "gift:2", "gift") is True
    metrics = whale_metrics.get_live_whale_metrics(301, "202609")

    # Then: duplicate delivery does not inflate either payer or total revenue.
    assert metrics is not None
    assert metrics.total_revenue == 1500
    assert metrics.attributed_revenue == 1000
    assert metrics.unattributed_revenue == 500
    assert metrics.top1_amount == 1000


def test_current_route_exposes_live_whale_dependency(monkeypatch, gift_module) -> None:
    # Given: a current room whose Redis calculation is available.
    metrics = whale_metrics.calculate_whale_metrics({7: 7500, 8: 2500}, 10000, 0)
    monkeypatch.setattr(api_app, "get_live_whale_metrics", lambda _room_id, _month: metrics)
    monkeypatch.setattr(gift_module, "_room_ids_for_month", lambda _month, include_config=True: [301])
    monkeypatch.setattr(gift_module, "Session", lambda: _RouteSession())
    monkeypatch.setattr(
        gift_module.RoomLiveStats,
        "month_aggregate_for_month",
        classmethod(lambda _cls, _room_id, _month: (0, 0)),
    )
    monkeypatch.setattr(
        gift_module.RoomLiveStats,
        "month_steel_coin_for_month",
        classmethod(lambda _cls, _room_id, _month: 0),
    )

    # When: the public current-month route is requested.
    from fastapi.testclient import TestClient

    response = TestClient(gift_module.app).get("/gift")

    # Then: the route exposes the calculated live ratios without changing the
    # legacy aggregate fields.
    assert response.status_code == 200
    assert response.json()[0]["whale_dependency"] == {
        "status": "live",
        "source": "redis",
        "top1": 0.75,
        "top5": 1.0,
        "top10": 1.0,
        "top1_percent": 0.75,
    }


def test_historical_route_helper_prefers_archived_mysql_values(monkeypatch) -> None:
    # Given: an archived MySQL row and no usable live Redis calculation.
    row = SimpleNamespace(
        whale_status="archived",
        whale_top1_ratio=0.75,
        whale_top5_ratio=1.0,
        whale_top10_ratio=1.0,
        whale_top1pct_ratio=0.75,
    )
    monkeypatch.setattr(api_app, "get_live_whale_metrics", lambda _room_id, _month: None)
    monkeypatch.setattr(api_app, "get_whale_state", lambda _room_id, _month: "archived")

    # When: a historical room-month is resolved for the API.
    payload = api_app._whale_dependency_for_room(row, 301, "202608")

    # Then: the archived values are returned from MySQL.
    assert payload == {
        "status": "archived",
        "source": "mysql",
        "top1": 0.75,
        "top5": 1.0,
        "top10": 1.0,
        "top1_percent": 0.75,
    }


def test_gift_records_blind_box_actual_price_once(monkeypatch) -> None:
    # Given: a blind-box gift whose actual price differs from its nominal coin value.
    captured: list[tuple[object, ...]] = []
    client = SimpleNamespace(room_id=301)
    runtime_state.CURRENT_SESSIONS.pop(301, None)
    monkeypatch.setattr(event_ingestion, "record_whale_revenue", lambda *args: captured.append(args))
    monkeypatch.setattr(event_ingestion.RoomStatsMonthly, "add_amounts", lambda *args, **kwargs: None)
    monkeypatch.setattr(event_ingestion.LiveSession, "add_values_by_room_open", lambda *args, **kwargs: None)
    monkeypatch.setattr(event_ingestion.RoomLiveStats, "add_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(event_ingestion, "register_payer", lambda *args, **kwargs: None)

    # When: the gift event is consumed.
    event_ingestion.MyHandler().__getattribute__("_on_gift")(
        client,
        SimpleNamespace(
            total_price=2000,
            total_coin=1000,
            gift_name="blind-box",
            num=1,
            uname="viewer",
            uid=9,
            timestamp=1_700_000_000,
            tid="transaction-1",
        ),
    )

    # Then: the UID receives the actual paid price and no separate blind-box amount.
    assert captured == [(301, "202311", 9, 2000, "gift:transaction-1", "gift")]


class _RouteSession:
    def query(self, *_args):
        return _RouteQuery()

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _RouteQuery:
    def filter_by(self, **_kwargs):
        return self

    def filter(self, *_args, **_kwargs):
        return self

    def first(self):
        return None

    def scalar(self):
        return None
