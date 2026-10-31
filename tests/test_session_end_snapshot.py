"""End values describe the interruption, even when a session closes later."""

import asyncio
import datetime
from dataclasses import replace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import bilibili_gateway, bootstrap, monitoring_jobs, room_lifecycle, runtime_state
from app.models import LiveSession
from app.repositories import live_session_snapshots, live_sessions, session_duration

ROOM = 123
END = datetime.datetime(2026, 10, 31, 23, 59)


@pytest.fixture
def end_state(monkeypatch):
    engine = create_engine("sqlite://")
    LiveSession.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    for module in (live_sessions, live_session_snapshots, session_duration):
        monkeypatch.setattr(module, "Session", factory)
    for name in ("CURRENT_SESSIONS", "PENDING_SESSION_ENDS", "STREAM_STARTS",
                 "INVALID_DURATION_SESSIONS", "CONCURRENCY_CACHE", "ROOM_UIDS", "LAST_STATUS"):
        monkeypatch.setattr(runtime_state, name, {})
    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", asyncio.Queue())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", asyncio.Queue())
    monkeypatch.setattr(room_lifecycle, "flush_session", lambda *args: None)
    session_id = LiveSession.start_session(ROOM, END - datetime.timedelta(hours=1), "test")
    runtime_state.CURRENT_SESSIONS[ROOM] = session_id
    runtime_state.ROOM_UIDS[ROOM] = 456
    dependencies = room_lifecycle.LifecycleDependencies(
        lambda *args: None, lambda *args: None, lambda *args: (None, None),
        lambda: END, lambda room: asyncio.sleep(0), lambda room: asyncio.sleep(0),
        monitoring_jobs.capture_end_snapshot,
    )
    values = {"guard": (3, 2, 1), "fans": 20, "attention": 100}
    calls = []

    async def fetch(field, *args):
        calls.append(field)
        return values[field]

    monkeypatch.setattr(bilibili_gateway, "fetch_guard_counts", lambda *args: fetch("guard", *args))
    monkeypatch.setattr(bilibili_gateway, "fetch_fans_count", lambda *args: fetch("fans", *args))
    monkeypatch.setattr(bilibili_gateway, "fetch_room_attention", lambda *args: fetch("attention", *args))
    yield factory, session_id, dependencies, values, calls
    engine.dispose()


def end_values(factory, session_id):
    with factory() as db:
        row = db.get(LiveSession, session_id)
        return row.end_time, row.end_guard_1, row.end_guard_2, row.end_guard_3, row.end_fans_count, row.end_attention


def test_end_snapshot_is_cached_immediately_and_written_only_after_grace(end_state):
    factory, session_id, deps, values, calls = end_state

    async def scenario():
        room_lifecycle.defer_live_session_finish(ROOM, END, deps)
        snapshot = runtime_state.SESSION_END_SNAPSHOTS[session_id]
        await snapshot.task
        assert end_values(factory, session_id) == (None,) * 6
        values.update(guard=(30, 20, 10), fans=200, attention=1000)
        # A repeated stop must neither recapture nor enqueue a delayed HTTP read.
        room_lifecycle.defer_live_session_finish(ROOM, END, deps)
        room_lifecycle.finish_expired_live_sessions(END + datetime.timedelta(seconds=180), deps)
        assert end_values(factory, session_id) == (None,) * 6
        room_lifecycle.finish_expired_live_sessions(END + datetime.timedelta(seconds=181), deps)
        assert end_values(factory, session_id) == (END, 3, 2, 1, 20, 100)
        assert sorted(calls) == ["attention", "fans", "guard"]
        assert not runtime_state.SESSION_END_SNAPSHOTS
        assert runtime_state.GUARD_FANS_QUEUE.empty()
        assert runtime_state.ATTENTION_QUEUE.empty()

    asyncio.run(scenario())


def test_resume_discards_old_snapshot_and_final_stop_recaptures(end_state):
    factory, session_id, deps, values, calls = end_state

    async def scenario():
        room_lifecycle.defer_live_session_finish(ROOM, END, deps)
        await runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        reopened = END + datetime.timedelta(seconds=180)
        assert room_lifecycle.resume_interrupted_session(ROOM, reopened, reopened) == session_id
        assert not runtime_state.SESSION_END_SNAPSHOTS
        values.update(guard=(4, 3, 2), fans=21, attention=101)
        final_end = reopened + datetime.timedelta(hours=1)
        room_lifecycle.defer_live_session_finish(ROOM, final_end, deps)
        await runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        room_lifecycle.finish_expired_live_sessions(final_end + datetime.timedelta(seconds=181), deps)
        assert end_values(factory, session_id) == (final_end, 4, 3, 2, 21, 101)
        assert len(calls) == 6

    asyncio.run(scenario())


def test_resume_cancels_inflight_capture_without_polluting_next_stop(end_state):
    factory, session_id, deps, values, calls = end_state

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        async def slow(snapshot):
            started.set()
            await release.wait()
            snapshot.attention = 999

        room_lifecycle.defer_live_session_finish(ROOM, END, replace(deps, capture_end_snapshot=slow))
        old_task = runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        await started.wait()
        reopened = END + datetime.timedelta(seconds=60)
        room_lifecycle.resume_interrupted_session(ROOM, reopened, reopened)
        await asyncio.gather(old_task, return_exceptions=True)
        assert old_task.cancelled()
        room_lifecycle.defer_live_session_finish(ROOM, reopened, deps)
        await runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        room_lifecycle.finish_expired_live_sessions(reopened + datetime.timedelta(seconds=181), deps)
        assert end_values(factory, session_id)[-1] == 100

    asyncio.run(scenario())


def test_failed_fields_stay_missing_and_zero_is_preserved(end_state):
    factory, session_id, deps, values, calls = end_state
    values.update(guard=None, fans=0, attention=None)

    async def scenario():
        room_lifecycle.defer_live_session_finish(ROOM, END, deps)
        await runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        values.update(guard=(9, 9, 9), fans=99, attention=999)
        room_lifecycle.finish_expired_live_sessions(END + datetime.timedelta(seconds=181), deps)
        assert end_values(factory, session_id) == (END, None, None, None, 0, None)
        assert len(calls) == 3

    asyncio.run(scenario())


def test_field_exception_does_not_block_other_snapshot_reads(end_state, monkeypatch):
    factory, session_id, deps, values, calls = end_state

    async def failed_attention(room):
        raise TimeoutError

    monkeypatch.setattr(bilibili_gateway, "fetch_room_attention", failed_attention)

    async def scenario():
        room_lifecycle.defer_live_session_finish(ROOM, END, deps)
        await runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        room_lifecycle.finish_expired_live_sessions(END + datetime.timedelta(seconds=181), deps)
        assert end_values(factory, session_id) == (END, 3, 2, 1, 20, None)

    asyncio.run(scenario())


def test_restored_pending_end_does_not_collect_late_replacement(end_state):
    factory, session_id, deps, values, calls = end_state
    # A pending end can survive in the duration ledger, but its in-memory values cannot.
    runtime_state.PENDING_SESSION_ENDS[ROOM] = END
    room_lifecycle.finish_expired_live_sessions(END + datetime.timedelta(seconds=181), deps)
    assert end_values(factory, session_id) == (END, None, None, None, None, None)
    assert calls == []


def test_normal_status_poll_starts_capture_before_grace_expiry(end_state, monkeypatch):
    factory, session_id, deps, values, calls = end_state
    runtime_state.LAST_STATUS[ROOM] = 1

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def json(self, **kwargs):
            return {"data": {"456": {"live_status": 0}}}

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    async def stop_after_capture(seconds):
        await runtime_state.SESSION_END_SNAPSHOTS[session_id].task
        raise asyncio.CancelledError

    monkeypatch.setattr(runtime_state, "LAST_STATUS", {ROOM: 1})
    monkeypatch.setattr(runtime_state, "LOCKED_ROOM_UNTIL", {})
    monkeypatch.setattr(runtime_state, "LIVE_INFO", {})
    monkeypatch.setattr(runtime_state, "aiohttp_session", Session())
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [ROOM])
    monkeypatch.setattr(monitoring_jobs, "lifecycle_dependencies", lambda: deps)
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: END)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stop_after_capture)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.monitor_all_rooms_status())
    assert sorted(calls) == ["attention", "fans", "guard"]
    assert runtime_state.PENDING_SESSION_ENDS[ROOM] == END
    assert end_values(factory, session_id) == (None,) * 6


@pytest.mark.parametrize("data, expected", [({"attention": 0}, 0), ({"attention": 123}, 123),
                                          ({}, None), (None, None), ({"attention": "bad"}, None)])
def test_attention_read_preserves_failure_without_persisting(monkeypatch, data, expected):
    async def room_info(room):
        return data

    monkeypatch.setattr(bilibili_gateway, "_fetch_room_info", room_info)
    monkeypatch.setattr(bilibili_gateway.RoomInfo, "upsert", lambda *args, **kwargs: pytest.fail("capture must not persist room info"))
    assert asyncio.run(bilibili_gateway.fetch_room_attention(ROOM)) == expected


def test_archive_waits_for_inflight_end_snapshot_without_refetch(end_state, monkeypatch):
    factory, session_id, deps, values, calls = end_state
    monkeypatch.setattr(bootstrap.archive_service, "closed_session_ids", lambda cutoff: (session_id,))
    monkeypatch.setattr(runtime_state, "DANMAKU_PENDING", {})
    monkeypatch.setattr(bootstrap, "_now", lambda: END + datetime.timedelta(minutes=20))
    monkeypatch.setattr(bootstrap, "log_pool_status", lambda *args: None)
    monkeypatch.setattr("app.api_cache.invalidate_history", lambda *args: None)
    archived = []
    # The real snapshot write happens on the main loop before archive runs in a thread.
    monkeypatch.setattr(bootstrap.archive_service, "archive_live_session", lambda *args, **kwargs: archived.append(True))

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        async def slow(snapshot):
            started.set()
            await release.wait()
            snapshot.attention = 100

        room_lifecycle.defer_live_session_finish(ROOM, END, replace(deps, capture_end_snapshot=slow))
        await started.wait()
        room_lifecycle.finish_expired_live_sessions(END + datetime.timedelta(seconds=181), deps)
        archive_task = asyncio.create_task(bootstrap._archive_closed_sessions())
        await asyncio.sleep(0)
        assert not archived
        release.set()
        await archive_task
        assert archived == [True]
        assert end_values(factory, session_id)[-1] == 100

    asyncio.run(scenario())
