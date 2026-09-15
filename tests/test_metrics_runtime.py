from __future__ import annotations

import datetime
from types import SimpleNamespace
from typing import Protocol

import pytest

from app import (
    bootstrap,
    event_ingestion,
    metrics_runtime,
    monitoring_jobs,
    redis_metrics,
    runtime_state,
)


class _DanmakuCountView(Protocol):
    total: int
    captain: int
    admiral: int
    governor: int
    normal: int


class _FakeRedis:
    def __init__(self) -> None:
        self.sets: dict[str, set[str]] = {}
        self.expirations: dict[str, int] = {}

    def sadd(self, key: str, member: str) -> int:
        values = self.sets.setdefault(key, set())
        before = len(values)
        values.add(member)
        return int(len(values) != before)

    def expire(self, key: str, seconds: int) -> bool:
        self.expirations[key] = seconds
        return True

    def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    def delete(self, key: str) -> int:
        return int(self.sets.pop(key, None) is not None)


@pytest.fixture(autouse=True)
def clear_runtime_buckets():
    with metrics_runtime._lock:
        metrics_runtime._buckets.clear()
    runtime_state.DANMAKU_PENDING.clear()
    yield
    with metrics_runtime._lock:
        metrics_runtime._buckets.clear()
    runtime_state.DANMAKU_PENDING.clear()


def test_redis_registration_keeps_room_and_site_scopes(monkeypatch):
    client = _FakeRedis()
    monkeypatch.setattr(redis_metrics, "_client", client)
    event_date = datetime.date(2026, 8, 30)

    first = redis_metrics.register_payer(301, 44, 0, 1001, event_date, "202608", steel_coin=True)
    second = redis_metrics.register_payer(302, 45, 0, 1001, event_date, "202608", steel_coin=True)

    assert first.session is not None and first.session.added is True and first.session.size == 1
    assert first.daily.added is True and first.daily.size == 1
    assert first.monthly.added is True and first.monthly.size == 1
    assert first.steel_coin is not None and first.steel_coin.added is True
    assert second.daily.added is True and second.monthly.added is True
    assert second.steel_coin is not None and second.steel_coin.added is False
    assert client.expirations["vr:payer:day:20260830:301"] == redis_metrics.REDIS_KEY_TTL_SECONDS


def test_runtime_flushes_relative_buckets_and_keeps_launch_month(monkeypatch):
    writes: list[dict[str, int | float | str | datetime.datetime | None]] = []
    monkeypatch.setattr(
        metrics_runtime.LiveSession15mStats,
        "upsert",
        lambda **values: writes.append(values) or True,
    )
    start = datetime.datetime(2026, 8, 31, 23, 55, 0)

    metrics_runtime.start_session(44, 301, start)
    metrics_runtime.record_concurrency(44, start + datetime.timedelta(minutes=1), 10)
    metrics_runtime.record_concurrency(44, start + datetime.timedelta(minutes=5), 20)
    metrics_runtime.record_payment(
        44,
        start + datetime.timedelta(minutes=15),
        gift=10.0,
        payer_added=True,
    )
    metrics_runtime.record_concurrency(44, start + datetime.timedelta(minutes=16), 4)
    metrics_runtime.flush_session(44, start + datetime.timedelta(minutes=16))

    assert [row["bucket_index"] for row in writes] == [0, 1]
    assert all(row["month"] == "202608" for row in writes)
    assert writes[0]["end_time"] == start + datetime.timedelta(minutes=15)
    assert writes[0]["avg_concurrency"] == 15.0
    assert writes[0]["max_concurrency"] == 20
    assert writes[0]["payer_count"] == 0
    assert writes[0]["danmaku_count"] == 0
    assert writes[1]["gift"] == 10.0
    assert writes[1]["avg_concurrency"] == 4.0
    assert writes[1]["max_concurrency"] == 4
    assert writes[1]["payer_count"] == 1


def test_shutdown_flush_drains_pending_danmaku_before_metrics(monkeypatch):
    calls: list[str] = []
    runtime_state.CURRENT_SESSIONS[301] = 44
    monkeypatch.setattr(
        bootstrap.monitoring_jobs,
        "flush_pending_danmaku_for_room",
        lambda *_args: calls.append("danmaku"),
    )
    monkeypatch.setattr(bootstrap, "flush_session", lambda *_args: calls.append("metrics"))

    bootstrap._flush_active_metrics(datetime.datetime(2026, 8, 30, 12, 0, 0))

    assert calls == ["danmaku", "metrics"]
    runtime_state.CURRENT_SESSIONS.pop(301, None)


def test_pending_danmaku_flush_updates_parent_and_15m_bucket(monkeypatch):
    writes: list[tuple[int, _DanmakuCountView]] = []
    parent_writes: list[tuple[int, _DanmakuCountView]] = []
    monthly_writes: list[tuple[int, str, _DanmakuCountView]] = []
    monkeypatch.setattr(
        metrics_runtime.LiveSession15mStats,
        "add_danmaku_counts",
        classmethod(
            lambda _cls, target, counts: writes.append((target.bucket_index, counts)) or True
        ),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(
            add_danmaku_by_id=lambda session_id, counts: (
                parent_writes.append((session_id, counts)) or True
            )
        ),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(
            add_danmaku_counts=lambda room_id, month, counts: monthly_writes.append(
                (room_id, month, counts)
            )
            or True
        ),
    )
    start = datetime.datetime(2026, 8, 30, 12, 0, 0)
    handler = event_ingestion.MyHandler()
    runtime_state.LAST_STATUS[301] = 1
    metrics_runtime.start_session(44, 301, start)
    runtime_state.CURRENT_SESSIONS[301] = 44

    for privilege_type in (3, 3, 2, 1, 0, 0, 0):
        handler.__getattribute__("_on_danmaku")(
            SimpleNamespace(room_id=301),
            SimpleNamespace(
                is_mirror=False,
                privilege_type=privilege_type,
                timestamp=int(start.timestamp()),
            ),
        )

    monitoring_jobs.flush_pending_danmaku_for_room(301, 44, start + datetime.timedelta(minutes=1))
    metrics_runtime.flush_session(44, start + datetime.timedelta(minutes=1))

    assert len(parent_writes) == 1
    parent_counts = parent_writes[0][1]
    assert parent_writes[0][0] == 44
    assert parent_counts.total == 7
    assert parent_counts.captain == 2
    assert parent_counts.admiral == 1
    assert parent_counts.governor == 1
    assert parent_counts.normal == 3
    assert len(monthly_writes) == 1
    assert monthly_writes[0][0:2] == (301, "202608")
    assert monthly_writes[0][2] == parent_counts
    assert writes[0][0] == 0
    assert writes[0][1] == parent_counts
    assert 301 not in runtime_state.DANMAKU_PENDING
    runtime_state.LAST_STATUS.pop(301, None)


def test_danmaku_monthly_counts_follow_event_calendar_month(monkeypatch):
    monthly_writes: list[tuple[int, str, _DanmakuCountView]] = []
    parent_writes: list[_DanmakuCountView] = []
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(
            add_danmaku_counts=lambda room_id, month, counts: monthly_writes.append(
                (room_id, month, counts)
            )
            or True
        ),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(
            add_danmaku_by_id=lambda _session_id, counts: parent_writes.append(counts) or True
        ),
    )
    handler = event_ingestion.MyHandler()
    runtime_state.LAST_STATUS[301] = 1
    first_event_time = datetime.datetime.fromtimestamp(1_798_732_799)
    metrics_runtime.start_session(44, 301, first_event_time)
    runtime_state.CURRENT_SESSIONS[301] = 44

    handler.__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(is_mirror=False, privilege_type=3, timestamp=1_798_732_799),
    )
    handler.__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(is_mirror=False, privilege_type=0, timestamp=1_798_732_801),
    )
    runtime_state.LAST_STATUS[302] = 0
    handler.__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=302),
        SimpleNamespace(is_mirror=False, privilege_type=1, timestamp=1_798_732_801),
    )

    monitoring_jobs.flush_pending_danmaku_for_room(301, 44)

    assert [(room_id, month) for room_id, month, _counts in monthly_writes] == [
        (301, "202612"),
        (301, "202701"),
    ]
    assert monthly_writes[0][2].captain == 1
    assert monthly_writes[1][2].normal == 1
    assert parent_writes[0].total == 2
    assert 302 not in runtime_state.DANMAKU_PENDING
    runtime_state.LAST_STATUS.pop(301, None)
    runtime_state.LAST_STATUS.pop(302, None)
