from __future__ import annotations

import datetime
from types import SimpleNamespace

import pytest

from app import event_ingestion, metrics_runtime, monitoring_jobs, runtime_state


@pytest.fixture(autouse=True)
def clear_danmaku_runtime_state():
    with metrics_runtime._lock:
        metrics_runtime._buckets.clear()
    runtime_state.DANMAKU_PENDING.clear()
    runtime_state.CURRENT_SESSIONS.clear()
    yield
    with metrics_runtime._lock:
        metrics_runtime._buckets.clear()
    runtime_state.DANMAKU_PENDING.clear()
    runtime_state.CURRENT_SESSIONS.clear()


def test_pending_danmaku_keeps_original_15m_bucket_across_flush_boundary(monkeypatch):
    writes: list[tuple[int, int]] = []
    monkeypatch.setattr(
        metrics_runtime.LiveSession15mStats,
        "add_danmaku_counts",
        classmethod(
            lambda _cls, target, counts: writes.append(
                (target.bucket_index, counts.total)
            )
            or True
        ),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(add_danmaku_counts=lambda *_args: True),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(add_danmaku_by_id=lambda *_args: True),
    )
    start = datetime.datetime(2026, 8, 30, 12, 0, 0)
    metrics_runtime.start_session(44, 301, start)
    runtime_state.CURRENT_SESSIONS[301] = 44
    runtime_state.LAST_STATUS[301] = 1

    event_ingestion.MyHandler().__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(
            is_mirror=False,
            privilege_type=0,
            timestamp=int((start + datetime.timedelta(minutes=14, seconds=59)).timestamp()),
        ),
    )
    monitoring_jobs.flush_pending_danmaku_for_room(
        301,
        44,
        start + datetime.timedelta(minutes=15, seconds=1),
    )
    metrics_runtime.flush_session(44, start + datetime.timedelta(minutes=15, seconds=1))

    assert writes == [(0, 1)]
    runtime_state.LAST_STATUS.pop(301, None)


def test_failed_monthly_danmaku_write_remains_pending(monkeypatch):
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(add_danmaku_counts=lambda *_args: False),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(add_danmaku_by_id=lambda *_args: True),
    )
    runtime_state.LAST_STATUS[301] = 1
    event_time = datetime.datetime(2026, 8, 30, 12, 1, 0)
    metrics_runtime.start_session(44, 301, event_time)
    runtime_state.CURRENT_SESSIONS[301] = 44

    event_ingestion.MyHandler().__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(is_mirror=False, privilege_type=3, timestamp=int(event_time.timestamp())),
    )
    monitoring_jobs.flush_pending_danmaku_for_room(301, 44, event_time)

    assert 301 in runtime_state.DANMAKU_PENDING
    runtime_state.LAST_STATUS.pop(301, None)


def test_late_pending_danmaku_backfills_its_original_bucket(monkeypatch):
    bucket_writes: list[tuple[int, int]] = []
    monkeypatch.setattr(metrics_runtime.LiveSession15mStats, "upsert", lambda **_values: True)
    monkeypatch.setattr(
        metrics_runtime.LiveSession15mStats,
        "add_danmaku_counts",
        classmethod(
            lambda _cls, target, counts: bucket_writes.append(
                (target.bucket_index, counts.total)
            )
            or True
        ),
        raising=False,
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(add_danmaku_counts=lambda *_args: True),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(add_danmaku_by_id=lambda *_args: True),
    )
    start = datetime.datetime(2026, 8, 30, 12, 0, 0)
    metrics_runtime.start_session(44, 301, start)
    runtime_state.CURRENT_SESSIONS[301] = 44
    runtime_state.LAST_STATUS[301] = 1

    event_ingestion.MyHandler().__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(
            is_mirror=False,
            privilege_type=0,
            timestamp=int((start + datetime.timedelta(minutes=14, seconds=59)).timestamp()),
        ),
    )
    metrics_runtime.record_concurrency(44, start + datetime.timedelta(minutes=15, seconds=10), 20)
    monitoring_jobs.flush_pending_danmaku_for_room(301, 44)

    assert bucket_writes == [(0, 1)]
    runtime_state.LAST_STATUS.pop(301, None)


def test_failed_15m_danmaku_write_remains_pending(monkeypatch):
    monkeypatch.setattr(
        metrics_runtime.LiveSession15mStats,
        "add_danmaku_counts",
        classmethod(lambda _cls, _target, _counts: False),
        raising=False,
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(add_danmaku_counts=lambda *_args: True),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(add_danmaku_by_id=lambda *_args: True),
    )
    start = datetime.datetime(2026, 8, 30, 12, 0, 0)
    metrics_runtime.start_session(44, 301, start)
    runtime_state.CURRENT_SESSIONS[301] = 44
    runtime_state.LAST_STATUS[301] = 1

    event_ingestion.MyHandler().__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(is_mirror=False, privilege_type=3, timestamp=int(start.timestamp())),
    )
    monitoring_jobs.flush_pending_danmaku_for_room(301, 44)

    assert 301 in runtime_state.DANMAKU_PENDING
    assert next(iter(runtime_state.DANMAKU_PENDING[301].buckets.values())).captain == 1
    runtime_state.LAST_STATUS.pop(301, None)


def test_failed_final_bucket_retry_keeps_original_session(monkeypatch):
    attempted_sessions: list[int] = []

    def persist(_cls, target, _counts):
        attempted_sessions.append(target.session_id)
        return len(attempted_sessions) > 1

    monkeypatch.setattr(
        metrics_runtime.LiveSession15mStats,
        "add_danmaku_counts",
        classmethod(persist),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "RoomStatsMonthly",
        SimpleNamespace(add_danmaku_counts=lambda *_args: True),
    )
    monkeypatch.setattr(
        monitoring_jobs,
        "LiveSession",
        SimpleNamespace(add_danmaku_by_id=lambda *_args: True),
    )
    start = datetime.datetime(2026, 8, 30, 12, 0, 0)
    metrics_runtime.start_session(44, 301, start)
    runtime_state.CURRENT_SESSIONS[301] = 44
    runtime_state.LAST_STATUS[301] = 1

    event_ingestion.MyHandler().__getattribute__("_on_danmaku")(
        SimpleNamespace(room_id=301),
        SimpleNamespace(is_mirror=False, privilege_type=0, timestamp=int(start.timestamp())),
    )
    monitoring_jobs.flush_pending_danmaku_for_room(301, 44)
    metrics_runtime.flush_session(44, start + datetime.timedelta(minutes=1))
    runtime_state.CURRENT_SESSIONS[301] = 45
    metrics_runtime.start_session(45, 301, start + datetime.timedelta(hours=3))
    monitoring_jobs.flush_pending_danmaku_for_room(301, 45)

    assert attempted_sessions == [44, 44]
    runtime_state.LAST_STATUS.pop(301, None)
