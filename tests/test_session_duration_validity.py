import asyncio
import datetime
import json
from contextlib import contextmanager

import pytest
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from app import (
    api_cache,
    database,
    metrics_runtime,
    monitoring_jobs,
    room_lifecycle,
    room_lock_events,
    runtime_state,
)
from app.blivedm.clients.ws_base import WebSocketClientBase
from app.event_ingestion import MyHandler
from app.models import LiveSession, RoomLiveStats
from app.repositories import live_sessions, live_stats, session_duration

ROOM = 21756924
START = datetime.datetime(2026, 10, 2, 12).astimezone().replace(tzinfo=None)


@pytest.fixture
def duration_state(monkeypatch):
    engine = create_engine("sqlite://")
    LiveSession.__table__.create(engine)
    RoomLiveStats.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    for module in (live_sessions, live_stats, session_duration):
        monkeypatch.setattr(module, "Session", factory)
    for module in (live_stats, session_duration):
        monkeypatch.setattr(module, "is_current_month", lambda month: month == "202610")
    for name in ("CURRENT_SESSIONS", "LAST_STATUS", "STREAM_STARTS", "LIVE_INFO",
                 "PENDING_SESSION_ENDS", "CONCURRENCY_CACHE", "LOCKED_ROOM_UNTIL",
                 "INVALID_DURATION_SESSIONS", "FORCED_OFFLINE_AT", "ROOM_UIDS"):
        monkeypatch.setattr(runtime_state, name, {})
    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", asyncio.Queue())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", asyncio.Queue())
    monkeypatch.setattr(metrics_runtime, "_buckets", {})
    monkeypatch.setattr(room_lifecycle, "flush_session", lambda *args: None)
    monkeypatch.setattr(monitoring_jobs, "flush_pending_danmaku_for_room", lambda *args: None)
    dependencies = room_lifecycle.LifecycleDependencies(
        monitoring_jobs._record_stream_segment, lambda *args: None,
        lambda *args: (None, None), lambda: START,
        lambda room: asyncio.sleep(0), lambda room: asyncio.sleep(0),
    )
    monkeypatch.setattr(room_lock_events, "_lifecycle_dependencies", lambda: dependencies)
    monkeypatch.setattr(monitoring_jobs, "lifecycle_dependencies", lambda: dependencies)
    yield factory, dependencies, engine
    engine.dispose()


def start_live(start=START):
    session_id = LiveSession.start_session(ROOM, start, "test")
    runtime_state.CURRENT_SESSIONS[ROOM] = session_id
    runtime_state.STREAM_STARTS[ROOM] = start
    runtime_state.LAST_STATUS[ROOM] = 1
    return session_id


def command(kind, when):
    payload = {"cmd": kind, "send_time": int(when.timestamp() * 1000),
               "roomid" if kind == "WARNING" else "room_id": ROOM}
    if kind == "ROOM_LOCK":
        payload["expire"] = (when + datetime.timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
    return payload


def dispatch(payload):
    client = WebSocketClientBase.__new__(WebSocketClientBase)
    client._room_id = ROOM
    MyHandler().handle(client, payload)


def totals(factory):
    with factory() as session:
        return RoomLiveStats.month_aggregate_for_month(ROOM, "202610", session=session)


def poll(monkeypatch, now, status, start=None):
    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def json(self, **kwargs):
            return {"data": {"1": {"live_status": status, "live_time": int((start or now).timestamp()), "title": "new"}}}

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    async def stop(seconds):
        raise asyncio.CancelledError

    runtime_state.ROOM_UIDS[ROOM] = 1
    monkeypatch.setattr(runtime_state, "aiohttp_session", Session())
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [ROOM])
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: now)
    monkeypatch.setattr(monitoring_jobs, "init_concurrency_cache", lambda *args: None)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stop)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.monitor_all_rooms_status())


@pytest.mark.parametrize("kind", ["WARNING", "CUT_OFF", "ROOM_LOCK"])
def test_invalid_session_and_quick_new_broadcast(duration_state, monkeypatch, kind):
    factory, _, _ = duration_state
    session_id = start_live()
    punished = START + datetime.timedelta(hours=3)
    payload = command(kind, punished)
    dispatch(payload)
    dispatch(payload)
    assert totals(factory) == (0, 0)
    if kind == "WARNING":
        # WARNING leaves the same broadcast active until a real offline transition.
        assert runtime_state.LAST_STATUS[ROOM] == 1
        assert runtime_state.CURRENT_SESSIONS[ROOM] == session_id
        with factory() as session:
            assert session.get(LiveSession, session_id).end_time is None
        ended = punished + datetime.timedelta(hours=1)
        poll(monkeypatch, ended, 0)
    else:
        ended = punished
    assert runtime_state.CURRENT_SESSIONS[ROOM] == session_id
    assert runtime_state.PENDING_SESSION_ENDS[ROOM] == ended
    with factory() as session:
        row = session.get(LiveSession, session_id)
        assert row.duration_valid == 0
        assert row.end_time is None

    # A real status poll resumes the same ID, with a new valid broadcast segment.
    restarted = ended + datetime.timedelta(seconds=61)
    poll(monkeypatch, restarted, 1)
    new_session = runtime_state.CURRENT_SESSIONS[ROOM]
    assert new_session == session_id
    end = restarted + datetime.timedelta(hours=2)
    poll(monkeypatch, end, 0)
    poll(monkeypatch, end + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)
    with factory() as session:
        assert session.get(LiveSession, new_session).duration_valid == 1


@pytest.mark.parametrize("kind", ["WARNING", "CUT_OFF", "ROOM_LOCK"])
def test_warning_in_resumed_broadcast_preserves_previous_healthy_segments(duration_state, kind):
    factory, dependencies, _ = duration_state
    healthy = start_live()
    room_lifecycle.finish_live_session(ROOM, START + datetime.timedelta(hours=1), dependencies)
    unhealthy_start = START + datetime.timedelta(hours=2)
    unhealthy = start_live(unhealthy_start)
    interrupted = unhealthy_start + datetime.timedelta(hours=2)
    room_lifecycle.defer_live_session_finish(ROOM, interrupted, dependencies)
    with factory.begin() as session:
        row = session.get(RoomLiveStats, (ROOM, START.date()))
        row.gift, row.guard, row.super_chat = 11, 12, 13
        row.payer_count, row.steel_coin_count = 14, 15
    assert totals(factory) == (10800, 1)
    resumed = interrupted + datetime.timedelta(seconds=30)
    assert room_lifecycle.resume_interrupted_session(ROOM, resumed, resumed) == unhealthy
    dispatch(command(kind, resumed + datetime.timedelta(hours=1)))
    assert totals(factory) == (10800, 1)
    if kind == "WARNING":
        room_lifecycle.defer_live_session_finish(ROOM, resumed + datetime.timedelta(hours=2), dependencies)
    assert totals(factory) == (10800, 1)
    with factory() as session:
        assert session.get(LiveSession, healthy).duration_valid == 1
        ledger = json.loads(session.get(LiveSession, unhealthy).duration_ledger)
        assert ledger["days"] == {"2026-10-02": {"room_live_stats": 7200}}
        assert ledger["segment_days"] == {}
        row = session.get(RoomLiveStats, (ROOM, START.date()))
        assert (row.gift, row.guard, row.super_chat, row.payer_count, row.steel_coin_count) == (11, 12, 13, 14, 15)


def test_warning_validity_survives_process_state_loss(duration_state):
    factory, dependencies, _ = duration_state
    session_id = start_live()
    dispatch(command("WARNING", START + datetime.timedelta(hours=1)))
    runtime_state.CURRENT_SESSIONS.clear()
    runtime_state.INVALID_DURATION_SESSIONS.clear()
    runtime_state.STREAM_STARTS.clear()
    assert start_live() == session_id
    assert runtime_state.INVALID_DURATION_SESSIONS[ROOM] == session_id
    room_lifecycle.defer_live_session_finish(ROOM, START + datetime.timedelta(hours=4), dependencies)
    assert totals(factory) == (0, 0)
    assert ROOM in runtime_state.PENDING_SESSION_ENDS


def test_segment_ledger_survives_recovery_and_prevents_recounting(duration_state):
    factory, _, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=2)
    LiveSession.record_duration(session_id, START, ended)
    LiveSession.record_duration(session_id, START, ended)
    assert totals(factory) == (7200, 1)
    runtime_state.CURRENT_SESSIONS.clear()
    assert start_live() == session_id
    # A delayed warning sent before offline revokes the recovered segment once.
    dispatch(command("WARNING", ended - datetime.timedelta(seconds=30)))
    dispatch(command("WARNING", ended - datetime.timedelta(seconds=30)))
    assert totals(factory) == (0, 0)


@pytest.mark.parametrize("kind", ["CUT_OFF", "ROOM_LOCK"])
def test_stale_live_poll_after_forced_offline_cannot_start_valid_session(duration_state, monkeypatch, kind):
    factory, _, _ = duration_state
    start_live()
    ended = START + datetime.timedelta(hours=2)
    dispatch(command(kind, ended))
    poll(monkeypatch, ended + datetime.timedelta(seconds=61), 1, START)
    assert ROOM in runtime_state.CURRENT_SESSIONS
    assert runtime_state.LAST_STATUS[ROOM] == 0
    assert totals(factory) == (0, 0)
    with factory() as session:
        assert session.query(LiveSession).count() == 1


@pytest.mark.parametrize("archived", [False, True])
def test_cross_month_revocation_from_hot_or_archived_days(duration_state, monkeypatch, archived):
    factory, _, engine = duration_state
    dirty_months = []
    monkeypatch.setattr(api_cache, "invalidate_history", dirty_months.append)
    table = RoomLiveStats.__table__.to_metadata(MetaData(), name="room_live_stats_202609")
    table.indexes.clear()
    table.create(engine)
    monkeypatch.setattr(session_duration, "ensure_room_live_stats_archive_table", lambda month: f"room_live_stats_{month}")
    # First record September's part while it is still the current month.
    monkeypatch.setattr(session_duration, "is_current_month", lambda month: True)
    start = datetime.datetime(2026, 9, 30, 21).astimezone().replace(tzinfo=None)
    session_id = start_live(start)
    midnight = datetime.datetime(2026, 10, 1).astimezone().replace(tzinfo=None)
    LiveSession.record_duration(session_id, start, midnight)
    monkeypatch.setattr(session_duration, "is_current_month", lambda month: month == "202610")
    LiveSession.record_duration(session_id, midnight, midnight + datetime.timedelta(hours=3))
    if archived:
        with factory.begin() as session:
            values = dict(session.execute(RoomLiveStats.__table__.select()).mappings().all()[0])
            session.execute(table.insert().values(**values))
            session.execute(RoomLiveStats.__table__.delete().where(RoomLiveStats.date == start.date()))
    dispatch(command("WARNING", midnight + datetime.timedelta(hours=4)))
    with factory() as session:
        assert all(row.duration == 0 for row in session.query(RoomLiveStats).all())
        assert all(row[0] == 0 for row in session.execute(table.select().with_only_columns(table.c.duration)))
    assert totals(factory) == (0, 0)
    assert "202609" in dirty_months


@pytest.mark.parametrize("kind", ["WARNING", "CUT_OFF", "ROOM_LOCK"])
@pytest.mark.parametrize("changes", [{"roomid": 99, "room_id": 99}, {"send_time": "bad"}, {"send_time": 1}])
def test_unrelated_malformed_or_stale_event_keeps_duration_valid(duration_state, kind, changes):
    factory, dependencies, _ = duration_state
    session_id = start_live()
    dispatch(command(kind, START + datetime.timedelta(hours=1)) | changes)
    room_lifecycle.finish_live_session(ROOM, START + datetime.timedelta(hours=2), dependencies)
    assert totals(factory) == (7200, 1)
    with factory() as session:
        assert session.get(LiveSession, session_id).duration_valid == 1


def test_warning_while_offline_does_not_penalize_next_broadcast(duration_state):
    factory, dependencies, _ = duration_state
    dispatch(command("WARNING", START))
    start_live(START + datetime.timedelta(seconds=30))
    room_lifecycle.finish_live_session(ROOM, START + datetime.timedelta(hours=2, seconds=30), dependencies)
    assert totals(factory) == (7200, 1)


@pytest.mark.parametrize("sent_before_end", [True, False])
def test_warning_in_grace_uses_server_time_to_identify_active_broadcast(duration_state, sent_before_end):
    factory, dependencies, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=2)
    room_lifecycle.defer_live_session_finish(ROOM, ended, dependencies)
    sent = ended + datetime.timedelta(seconds=-1 if sent_before_end else 1)
    dispatch(command("WARNING", sent))
    assert totals(factory) == ((0, 0) if sent_before_end else (7200, 1))
    with factory() as session:
        assert session.get(LiveSession, session_id).duration_valid == (0 if sent_before_end else 1)
    assert ROOM in runtime_state.PENDING_SESSION_ENDS


def test_schema_upgrade_adds_duration_state_to_hot_and_existing_archives(monkeypatch):
    names = ["live_session", "live_session_202609"]
    statements = []

    class Inspector:
        def get_table_names(self):
            return names

        def get_columns(self, name):
            return [{"name": column.name} for column in LiveSession.__table__.columns
                    if column.name not in {"duration_valid", "duration_ledger"}]

    class Connection:
        def execute(self, statement):
            statements.append(str(statement))

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(database, "engine", Engine())
    monkeypatch.setattr(database, "inspect", lambda engine: Inspector())
    database.ensure_runtime_schema()
    assert statements == [
        f"ALTER TABLE `{table}` ADD COLUMN `{column}` {ddl}"
        for table in names
        for column, ddl in (("duration_valid", "INT NOT NULL DEFAULT 1"), ("duration_ledger", "TEXT NULL"))
    ]


def test_duration_revocation_is_atomic_on_write_failure(duration_state):
    factory, _, engine = duration_state
    session_id = start_live()
    LiveSession.record_duration(session_id, START, START + datetime.timedelta(hours=2))

    def fail_update(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE room_live_stats"):
            raise SQLAlchemyError("isolated failure")

    event.listen(engine, "before_cursor_execute", fail_update)
    try:
        with pytest.raises(SQLAlchemyError):
            LiveSession.invalidate_duration(session_id)
    finally:
        event.remove(engine, "before_cursor_execute", fail_update)
    assert totals(factory) == (7200, 1)
    with factory() as session:
        assert session.get(LiveSession, session_id).duration_valid == 1
        assert json.loads(session.get(LiveSession, session_id).duration_ledger)["days"] == {
            "2026-10-02": {"room_live_stats": 7200}
        }
    LiveSession.invalidate_duration(session_id)
    assert totals(factory) == (0, 0)


def test_failed_warning_in_grace_is_retried_when_start_cache_is_empty(duration_state):
    factory, dependencies, engine = duration_state
    start_live()
    ended = START + datetime.timedelta(hours=2)
    room_lifecycle.defer_live_session_finish(ROOM, ended, dependencies)

    def fail_update(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE room_live_stats"):
            raise SQLAlchemyError("isolated failure")

    event.listen(engine, "before_cursor_execute", fail_update)
    try:
        with pytest.raises(SQLAlchemyError):
            dispatch(command("WARNING", ended - datetime.timedelta(seconds=1)))
    finally:
        event.remove(engine, "before_cursor_execute", fail_update)
    assert ROOM not in runtime_state.STREAM_STARTS
    room_lifecycle.finish_expired_live_sessions(ended + datetime.timedelta(seconds=181), dependencies)
    assert totals(factory) == (0, 0)
    assert ROOM not in runtime_state.CURRENT_SESSIONS


def test_warning_at_2030_offline_at_2100_resume_at_2101_keeps_session_id(duration_state, monkeypatch):
    factory, _, _ = duration_state
    started = START.replace(hour=20)
    session_id = start_live(started)
    dispatch(command("WARNING", started + datetime.timedelta(minutes=30)))
    stopped = started + datetime.timedelta(hours=1)
    poll(monkeypatch, stopped, 0)
    resumed = stopped + datetime.timedelta(minutes=1)
    poll(monkeypatch, resumed, 1)
    assert runtime_state.CURRENT_SESSIONS[ROOM] == session_id
    assert runtime_state.STREAM_STARTS[ROOM] == resumed
    end = resumed + datetime.timedelta(hours=2)
    poll(monkeypatch, end, 0)
    poll(monkeypatch, end + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)
    with factory() as session:
        row = session.get(LiveSession, session_id)
        assert session.query(LiveSession).count() == 1
        assert row.start_time == started
        assert row.end_time == end
        assert row.duration_valid == 1
        ledger = json.loads(row.duration_ledger)
        assert ledger["days"] == {"2026-10-02": {"room_live_stats": 7200}}
        assert ledger["segment_start"] == resumed.isoformat()


@pytest.mark.parametrize("kind", ["WARNING", "CUT_OFF", "ROOM_LOCK"])
@pytest.mark.parametrize("gap", [180, 181])
def test_punished_broadcast_honors_inclusive_grace_boundary(duration_state, monkeypatch, kind, gap):
    factory, _, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=1)
    dispatch(command(kind, ended))
    if kind == "WARNING":
        poll(monkeypatch, ended, 0)
    restarted = ended + datetime.timedelta(seconds=gap)
    poll(monkeypatch, restarted, 1)
    new_id = runtime_state.CURRENT_SESSIONS[ROOM]
    assert (new_id == session_id) is (gap == 180)
    with factory() as session:
        assert session.get(LiveSession, session_id).end_time == (None if gap == 180 else ended)
    end = restarted + datetime.timedelta(hours=2)
    poll(monkeypatch, end, 0)
    poll(monkeypatch, end + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)


@pytest.mark.parametrize("kind", ["WARNING", "CUT_OFF", "ROOM_LOCK"])
def test_grace_resume_after_restart_restores_segment_validity(duration_state, monkeypatch, kind):
    factory, _, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=2)
    dispatch(command(kind, ended))
    if kind == "WARNING":
        poll(monkeypatch, ended, 0)
    for name in ("CURRENT_SESSIONS", "INVALID_DURATION_SESSIONS", "STREAM_STARTS",
                 "PENDING_SESSION_ENDS", "FORCED_OFFLINE_AT", "LAST_STATUS", "LIVE_INFO"):
        getattr(runtime_state, name).clear()
    restarted = ended + datetime.timedelta(seconds=61)
    poll(monkeypatch, restarted, 1)
    assert runtime_state.CURRENT_SESSIONS[ROOM] == session_id
    assert ROOM not in runtime_state.INVALID_DURATION_SESSIONS
    end = restarted + datetime.timedelta(hours=2)
    poll(monkeypatch, end, 0)
    poll(monkeypatch, end + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)
    with factory() as session:
        assert session.query(LiveSession).count() == 1


def test_second_warning_revokes_only_its_own_segment_in_same_id(duration_state, monkeypatch):
    factory, _, _ = duration_state
    session_id = start_live()
    stopped = START + datetime.timedelta(hours=2)
    poll(monkeypatch, stopped, 0)
    healthy_start = stopped + datetime.timedelta(seconds=30)
    poll(monkeypatch, healthy_start, 1)
    dispatch(command("WARNING", healthy_start + datetime.timedelta(minutes=30)))
    dispatch(command("WARNING", healthy_start + datetime.timedelta(minutes=40)))
    stopped = healthy_start + datetime.timedelta(hours=1)
    poll(monkeypatch, stopped, 0)
    resumed = stopped + datetime.timedelta(seconds=30)
    poll(monkeypatch, resumed, 1)
    stopped = resumed + datetime.timedelta(hours=1)
    poll(monkeypatch, stopped, 0)
    assert totals(factory) == (10800, 1)
    # This is a delayed warning from the just-ended third segment.
    dispatch(command("WARNING", stopped - datetime.timedelta(seconds=1)))
    dispatch(command("WARNING", stopped - datetime.timedelta(seconds=1)))
    assert totals(factory) == (7200, 1)
    poll(monkeypatch, stopped + datetime.timedelta(seconds=181), 0)
    with factory() as session:
        assert session.query(LiveSession).count() == 1
        row = session.get(LiveSession, session_id)
        assert row.duration_valid == 0
        assert json.loads(row.duration_ledger)["days"] == {"2026-10-02": {"room_live_stats": 7200}}


def test_resume_failure_keeps_invalid_segment_and_grace_for_retry(duration_state):
    factory, dependencies, engine = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=1)
    dispatch(command("WARNING", ended - datetime.timedelta(seconds=1)))
    room_lifecycle.defer_live_session_finish(ROOM, ended, dependencies)
    resumed = ended + datetime.timedelta(seconds=30)

    def fail_update(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE live_session"):
            raise SQLAlchemyError("isolated failure")

    event.listen(engine, "before_cursor_execute", fail_update)
    try:
        with pytest.raises(SQLAlchemyError):
            room_lifecycle.resume_interrupted_session(ROOM, resumed, resumed)
    finally:
        event.remove(engine, "before_cursor_execute", fail_update)
    assert runtime_state.PENDING_SESSION_ENDS[ROOM] == ended
    assert runtime_state.INVALID_DURATION_SESSIONS[ROOM] == session_id
    with factory() as session:
        assert session.get(LiveSession, session_id).duration_valid == 0
    assert room_lifecycle.resume_interrupted_session(ROOM, resumed, resumed) == session_id
    assert ROOM not in runtime_state.INVALID_DURATION_SESSIONS


def test_normal_grace_merge_retains_all_healthy_segments_and_excludes_gaps(duration_state, monkeypatch):
    factory, _, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=1)
    poll(monkeypatch, ended, 0)
    resumed = ended + datetime.timedelta(seconds=180)
    poll(monkeypatch, resumed, 1)
    assert runtime_state.CURRENT_SESSIONS[ROOM] == session_id
    ended = resumed + datetime.timedelta(hours=1)
    poll(monkeypatch, ended, 0)
    poll(monkeypatch, ended + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)


@pytest.mark.parametrize("kind", ["WARNING", "CUT_OFF", "ROOM_LOCK"])
def test_late_old_event_cannot_revoke_resumed_segment_after_state_loss(duration_state, monkeypatch, kind):
    factory, _, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=1)
    dispatch(command("WARNING", ended - datetime.timedelta(seconds=1)))
    poll(monkeypatch, ended, 0)
    resumed = ended + datetime.timedelta(seconds=30)
    poll(monkeypatch, resumed, 1)
    # Lose transient caches before the next status poll restores the current start.
    runtime_state.CURRENT_SESSIONS.clear()
    runtime_state.STREAM_STARTS.clear()
    dispatch(command(kind, ended - datetime.timedelta(seconds=1)))
    assert ROOM not in runtime_state.INVALID_DURATION_SESSIONS
    assert ROOM not in runtime_state.LOCKED_ROOM_UNTIL
    end = resumed + datetime.timedelta(hours=2)
    poll(monkeypatch, end, 0)
    poll(monkeypatch, end + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)
    with factory() as session:
        assert session.query(LiveSession).count() == 1
        assert session.get(LiveSession, session_id).duration_valid == 1


def test_legacy_ledger_without_segment_keys_can_resume_without_recounting(duration_state, monkeypatch):
    factory, _, _ = duration_state
    session_id = start_live()
    ended = START + datetime.timedelta(hours=2)
    poll(monkeypatch, ended, 0)
    with factory.begin() as session:
        row = session.get(LiveSession, session_id)
        ledger = json.loads(row.duration_ledger)
        row.duration_ledger = json.dumps({"days": ledger["days"], "end": ledger["end"]})
    resumed = ended + datetime.timedelta(seconds=30)
    poll(monkeypatch, resumed, 1)
    dispatch(command("WARNING", resumed + datetime.timedelta(minutes=30)))
    stopped = resumed + datetime.timedelta(hours=2)
    poll(monkeypatch, stopped, 0)
    poll(monkeypatch, stopped + datetime.timedelta(seconds=181), 0)
    assert totals(factory) == (7200, 1)
