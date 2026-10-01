import datetime
from asyncio import CancelledError, Queue

import anyio
import pytest

from app import DanmakuCounts, PendingDanmaku, bootstrap, runtime_state


@pytest.fixture(autouse=True)
def eligible_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bootstrap.archive_service, "closed_session_ids", lambda _cutoff: (123,))


def test_crossmonth_session_is_archived_after_late_close_without_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: October starts while a September session is still open.
    now = datetime.datetime(2026, 10, 1)
    closed_at = now + datetime.timedelta(minutes=4)
    deadline = now + datetime.timedelta(minutes=20)
    archived: list[datetime.datetime] = []

    async def startup_archive(_month: str | None = None) -> None:
        return None

    async def advance(target: datetime.datetime) -> None:
        nonlocal now
        if target > deadline:
            raise CancelledError
        now = target

    def archive_closed(_month: str | None = None, *, end_time_before: datetime.datetime | None = None, session_ids: tuple[int, ...] | None = None) -> int:
        if end_time_before is not None and closed_at < end_time_before:
            archived.append(now)
        return int(bool(archived))

    monkeypatch.setattr(bootstrap, "_now", lambda: now)
    monkeypatch.setattr(bootstrap, "_month_str_now", lambda: now.strftime("%Y%m"))
    monkeypatch.setattr(bootstrap, "_sleep_until", advance)
    monkeypatch.setattr(bootstrap, "_archive_month", startup_archive)
    monkeypatch.setattr(bootstrap.archive_service, "archive_live_session", archive_closed)

    # When: the real archive scheduler runs past the end-snapshot safety window.
    with pytest.raises(CancelledError):
        anyio.run(bootstrap.monthly_reset_scheduler)

    # Then: the September session is caught up before another month boundary or restart.
    assert archived
    assert closed_at <= archived[0] <= deadline


@pytest.mark.parametrize("session_pending", [True, False])
def test_catchup_waits_for_session_writes_but_not_monthly_only_writes(monkeypatch: pytest.MonkeyPatch, session_pending: bool) -> None:
    # Given: a failed write that remains queued after the normal retry pass.
    pending = PendingDanmaku()
    if session_pending:
        pending.sessions[123] = DanmakuCounts(total=1, normal=1)
    else:
        pending.monthly["202609"] = DanmakuCounts(total=1, normal=1)
    archived: list[str | None] = []

    def archive(month: str | None = None, *, end_time_before: datetime.datetime | None = None, session_ids: tuple[int, ...] | None = None) -> int:
        archived.append(month)
        return 0

    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", Queue())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", Queue())
    monkeypatch.setattr(runtime_state, "DANMAKU_PENDING", {10: pending})
    monkeypatch.setattr(bootstrap.monitoring_jobs, "flush_pending_danmaku_for_room", lambda _room: None)
    monkeypatch.setattr(bootstrap.archive_service, "archive_live_session", archive)

    # When: the real catchup entry attempts archival.
    anyio.run(bootstrap._archive_closed_sessions)

    # Then: hot-session retries block migration, unrelated monthly retries do not.
    assert archived == ([] if session_pending else [None])


def test_catchup_skips_when_end_snapshot_queue_cannot_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: an end-snapshot task with no consumer and an expired drain budget.
    queue: Queue[tuple[int, int, str]] = Queue()
    queue.put_nowait((10, 123, "end"))
    archived: list[str | None] = []

    def archive(month: str | None = None, *, end_time_before: datetime.datetime | None = None, session_ids: tuple[int, ...] | None = None) -> int:
        archived.append(month)
        return 0

    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", queue)
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", Queue())
    monkeypatch.setattr(bootstrap, "SESSION_ARCHIVE_DRAIN_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(bootstrap.archive_service, "archive_live_session", archive)

    # When: the real catchup entry cannot complete its snapshot barrier.
    anyio.run(bootstrap._archive_closed_sessions)

    # Then: neither parent nor child archival is allowed to run.
    assert archived == []


def test_session_finalized_after_snapshot_waits_for_the_next_archive_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: one closed session, and a delayed close whose effective end time is old.
    closed = {1}
    migrated: list[int] = []

    def archive(month: str | None = None, *, end_time_before: datetime.datetime | None = None, session_ids: tuple[int, ...] | None = None) -> int:
        migrated.extend(sorted(closed if session_ids is None else session_ids))
        return len(migrated)

    async def dispatch(job, month, **kwargs):
        closed.add(2)
        return job(month, **kwargs)

    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", Queue())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", Queue())
    monkeypatch.setattr(runtime_state, "DANMAKU_PENDING", {})
    monkeypatch.setattr(bootstrap.archive_service, "closed_session_ids", lambda _cutoff: tuple(sorted(closed)), raising=False)
    monkeypatch.setattr(bootstrap.archive_service, "archive_live_session", archive)
    monkeypatch.setattr(bootstrap.asyncio, "to_thread", dispatch)

    # When: a session finishes between the snapshot barrier and the SQL worker.
    anyio.run(bootstrap._archive_closed_sessions)

    # Then: its queued end snapshots cannot be lost to this archive pass.
    assert migrated == [1]
