import asyncio
import datetime
import json
from contextlib import contextmanager
from types import SimpleNamespace

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
from app.live_client import AuthenticatedLiveClient
from app.ws_live_status import RoomStatusFeed
from app.models import LiveSession, LiveSession15mStats, RoomLiveStats
from app.repositories import live_sessions, live_stats, session_15m, session_duration

ROOM = 21756924
START = datetime.datetime(2026, 10, 2, 12).astimezone().replace(tzinfo=None)


@pytest.fixture
def duration_state(monkeypatch):
    engine = create_engine("sqlite://")
    LiveSession.__table__.create(engine)
    LiveSession15mStats.__table__.create(engine)
    RoomLiveStats.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    for module in (live_sessions, live_stats, session_15m, session_duration):
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


@pytest.fixture
def room_socket(duration_state, monkeypatch):
    client = AuthenticatedLiveClient.__new__(AuthenticatedLiveClient)
    client._room_id = client._tmp_room_id = ROOM
    client._websocket = SimpleNamespace(closed=False)
    client._health_authenticated = True
    client.status_feed = RoomStatusFeed()
    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {ROOM: client})
    monkeypatch.setattr(monitoring_jobs, "init_concurrency_cache", lambda *args: None)
    return client


def test_ws_transitions_titles_and_grace(duration_state, room_socket, monkeypatch):
    factory, _, _ = duration_state
    handler = MyHandler()
    def send(cmd, when, **fields):
        monkeypatch.setattr(monitoring_jobs, "_now", lambda: when)
        handler.handle(room_socket, {"cmd": cmd, "roomid": str(ROOM), **fields})

    send("LIVE", START, live_time=int(START.timestamp()), live_key="A")
    sid = runtime_state.event_session(ROOM)
    assert sid and runtime_state.LAST_STATUS[ROOM] == 1
    later = START + datetime.timedelta(minutes=30)
    send("LIVE", later, live_key="A")
    poll(monkeypatch, later, 0)
    assert runtime_state.STREAM_STARTS[ROOM] == START
    assert runtime_state.event_session(ROOM) == sid
    assert runtime_state.LIVE_INFO[ROOM]["title"] == "new"
    with factory() as db:
        assert db.get(LiveSession, sid).title == "new"
    send("PREPARING", later + datetime.timedelta(seconds=5), send_time=int(later.timestamp()*1000))
    send("PREPARING", later + datetime.timedelta(seconds=6), round=1)
    send("LIVE", later + datetime.timedelta(seconds=7), live_key="A")
    assert runtime_state.LAST_STATUS[ROOM] == 0 and runtime_state.PENDING_SESSION_ENDS[ROOM] == later
    reopened = later + datetime.timedelta(seconds=180)
    send("LIVE", reopened, live_time=int(reopened.timestamp()), live_key="B")
    send("PREPARING", reopened, send_time=int(later.timestamp()*1000))
    assert runtime_state.event_session(ROOM) == sid
    assert runtime_state.STREAM_STARTS[ROOM] == reopened
    stop = reopened + datetime.timedelta(minutes=1)
    send("PREPARING", stop)
    poll(monkeypatch, stop + datetime.timedelta(seconds=181), 1)
    with factory() as db:
        assert db.get(LiveSession, sid).end_time == stop


def test_ws_undated_live_and_http_reconnect(duration_state, room_socket, monkeypatch):
    handler = MyHandler()
    now = START + datetime.timedelta(seconds=30)
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: now)
    def send(**fields):
        handler.handle(room_socket, {"cmd": "LIVE", "roomid": ROOM, "live_key": "A", **fields})

    send()
    assert runtime_state.LAST_STATUS[ROOM] == 1 and runtime_state.event_session(ROOM) is None
    poll(monkeypatch, now, 0)
    assert runtime_state.LAST_STATUS[ROOM] == 1
    send(live_time=int(START.timestamp()))
    sid = runtime_state.event_session(ROOM)
    assert sid and runtime_state.STREAM_STARTS[ROOM] == START
    assert runtime_state.LIVE_INFO[ROOM]["title"] == "new"
    room_socket._health_authenticated = False
    room_socket.status_feed.reset_connection()
    poll(monkeypatch, now, 0)
    assert runtime_state.PENDING_SESSION_ENDS[ROOM] == now
    room_socket._health_authenticated = True
    reopened = now + datetime.timedelta(seconds=60)
    poll(monkeypatch, reopened, 1, reopened)
    assert room_socket.status_feed.synchronized
    assert runtime_state.event_session(ROOM) == sid and runtime_state.STREAM_STARTS[ROOM] == reopened
    poll(monkeypatch, reopened, 0)
    assert runtime_state.LAST_STATUS[ROOM] == 1


@pytest.mark.parametrize("kind", ["CUT_OFF", "ROOM_LOCK", "WARNING"])
def test_ws_ordinary_events_respect_moderation(duration_state, room_socket, monkeypatch, kind):
    handler = MyHandler()
    now = START + datetime.timedelta(hours=1)
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: now)
    def send(cmd, **fields):
        handler.handle(room_socket, {"cmd": cmd, "roomid": ROOM, **fields})
    send("LIVE", live_time=int(START.timestamp()))
    sid = runtime_state.event_session(ROOM)
    handler.handle(room_socket, command(kind, now))
    assert runtime_state.INVALID_DURATION_SESSIONS[ROOM] == sid
    if kind == "WARNING":
        assert runtime_state.LAST_STATUS[ROOM] == 1
        send("PREPARING", send_time=int(now.timestamp()*1000))
    else:
        send("LIVE", live_time=int(START.timestamp()))
        send("LIVE")
        send("PREPARING")
    assert runtime_state.LAST_STATUS[ROOM] == 0
    assert runtime_state.PENDING_SESSION_ENDS[ROOM] == now


def test_http_in_flight_cannot_undo_ws_event(duration_state, room_socket, monkeypatch):
    room = ROOM
    runtime_state.ROOM_UIDS[room] = 77
    monkeypatch.setattr(runtime_state, "aiohttp_session", object())
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [room])
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: START)
    async def batches():
        MyHandler().handle(room_socket, {"cmd": "LIVE", "roomid": room, "live_time": int(START.timestamp())})
        room_socket._health_authenticated = False
        room_socket.status_feed.reset_connection()
        yield {room: (77, {"live_status": 0, "title": "old"})}
    async def stop(_seconds):
        raise asyncio.CancelledError
    monkeypatch.setattr(monitoring_jobs, "status_batches", batches)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stop)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.monitor_all_rooms_status())
    assert runtime_state.LAST_STATUS[room] == 1 and runtime_state.event_session(room)
    assert runtime_state.LIVE_INFO[room]["title"] == ""


def test_ws_retry_keeps_original_start_and_discards_foreign_frames(duration_state, room_socket, monkeypatch):
    factory, _, _ = duration_state
    packet = {"cmd": "LIVE", "roomid": ROOM, "live_time": int(START.timestamp())}
    handle = MyHandler().handle
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: START)
    handle(room_socket, dict(packet, roomid=ROOM + 1))
    runtime_state.ROOM_CLIENTS.pop(ROOM)
    handle(room_socket, packet)
    assert not room_socket.status_feed.pending and ROOM not in runtime_state.LAST_STATUS
    runtime_state.ROOM_CLIENTS[ROOM] = room_socket
    with monkeypatch.context() as fail:
        fail.setattr(LiveSession, "start_session", lambda *args: None)
        handle(room_socket, packet)
        assert len(room_socket.status_feed.pending) == 1 and runtime_state.event_session(ROOM) is None
    now = START + datetime.timedelta(seconds=45)
    poll(monkeypatch, now, 0)
    assert not room_socket.status_feed.pending
    sid = runtime_state.event_session(ROOM)
    with factory() as db:
        assert db.get(LiveSession, sid).start_time == START


def test_new_undated_broadcast_does_not_accrue_time_to_previous_one(duration_state, room_socket, monkeypatch):
    factory, _, _ = duration_state
    handle = MyHandler().handle
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: START)
    handle(room_socket, {"cmd": "LIVE", "live_key": "A", "roomid": ROOM, "live_time": int(START.timestamp())})
    old = runtime_state.event_session(ROOM)
    seen = START + datetime.timedelta(minutes=10)
    poll(monkeypatch, seen, 1, START)
    now = seen + datetime.timedelta(minutes=5)
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: now)
    handle(room_socket, {"cmd": "LIVE", "live_key": "B", "roomid": ROOM})
    assert runtime_state.event_session(ROOM) is None and runtime_state.LAST_STATUS[ROOM] == 1
    handle(room_socket, {"cmd": "PREPARING", "roomid": str(ROOM)})
    poll(monkeypatch, now, 0)
    with factory() as db:
        assert db.get(LiveSession, old).end_time == seen


def test_live_and_preparing_through_collector_websocket(duration_state, monkeypatch):
    import aiohttp
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from app.blivedm.clients.ws_base import HEADER_STRUCT, Operation

    def command_frame(data):
        body = json.dumps(data).encode()
        return HEADER_STRUCT.pack(16 + len(body), 16, 0, Operation.SEND_MSG_REPLY, 1) + body

    async def run():
        ended = START + datetime.timedelta(minutes=1)
        finish = asyncio.Event()
        async def serve(request):
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            await socket.receive()  # Room authentication packet.
            await socket.send_bytes(AuthenticatedLiveClient._make_packet({"code": 0}, Operation.AUTH_REPLY)
                                    + command_frame({"cmd": "LIVE", "roomid": ROOM, "live_time": int(START.timestamp())}))
            await finish.wait()
            await socket.send_bytes(command_frame({"cmd": "PREPARING", "roomid": str(ROOM),
                                                   "send_time": int(ended.timestamp() * 1000)}))
            async for _ in socket:
                pass
            return socket

        async def initialize(client, _retry):
            client._room_id = ROOM
            client._host_server_token = "offline-test"

        app = web.Application()
        app.router.add_get("/ws", serve)
        async with TestServer(app) as server, aiohttp.ClientSession() as session:
            monkeypatch.setattr(runtime_state, "aiohttp_session", session)
            monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {})
            monkeypatch.setattr(AuthenticatedLiveClient, "_on_before_ws_connect", initialize)
            monkeypatch.setattr(AuthenticatedLiveClient, "_get_ws_url", lambda *_: str(server.make_url("/ws")))
            monkeypatch.setattr(monitoring_jobs, "_now", lambda: ended)
            monkeypatch.setattr(monitoring_jobs, "init_concurrency_cache", lambda *args: None)
            await monitoring_jobs.start_client(ROOM)
            client = runtime_state.ROOM_CLIENTS[ROOM]
            try:
                async with asyncio.timeout(3):
                    await client.wait_connected()
                    while runtime_state.event_session(ROOM) is None:
                        await asyncio.sleep(0.001)
                    assert runtime_state.STREAM_STARTS[ROOM] == START
                    finish.set()
                    while runtime_state.LAST_STATUS[ROOM] == 1:
                        await asyncio.sleep(0.001)
                    assert runtime_state.PENDING_SESSION_ENDS[ROOM] == ended
            finally:
                finish.set()
                await client.stop_and_close()

    asyncio.run(run())


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


@pytest.mark.parametrize("gap", [180, 181, 7200])
def test_cold_restart_checks_broadcast_identity_before_attaching_metrics(duration_state, gap):
    from app import bootstrap

    factory, deps, _ = duration_state
    sid = start_live()
    observed = START + datetime.timedelta(minutes=38)
    info = {"live_status": 1, "live_time": int(START.timestamp()), "title": "A"}
    assert monitoring_jobs._apply_room_status(ROOM, info, observed)
    # Even a later shutdown must persist the poll timestamp, not shutdown time.
    bootstrap._flush_active_metrics(observed + datetime.timedelta(minutes=1))
    with factory() as sql:
        ledger = json.loads(sql.get(LiveSession, sid).duration_ledger)
        assert ledger["last_live_at"] == observed.isoformat()
    for state in (runtime_state.CURRENT_SESSIONS, runtime_state.STREAM_STARTS,
                  runtime_state.LAST_STATUS, runtime_state.LAST_LIVE_OBSERVATIONS):
        state.clear()
    start_b = observed + datetime.timedelta(seconds=gap)
    recovered_at = start_b + datetime.timedelta(minutes=10)
    for invalid in (0, None, int((recovered_at + datetime.timedelta(days=1)).timestamp())):
        assert not monitoring_jobs._apply_room_status(ROOM, dict(info, live_time=invalid), recovered_at)
        assert ROOM not in runtime_state.CURRENT_SESSIONS

    assert monitoring_jobs._apply_room_status(ROOM, dict(info, live_time=int(start_b.timestamp()), title="B"), recovered_at)
    new_id = runtime_state.CURRENT_SESSIONS[ROOM]
    target = metrics_runtime.danmaku_bucket_target(new_id, start_b + datetime.timedelta(minutes=1))
    assert target is not None
    with factory() as sql:
        old = sql.get(LiveSession, sid)
        if gap <= 180:
            assert new_id == sid and old.end_time is None
            assert target.bucket_index == 2
            assert json.loads(old.duration_ledger)["segment_start"] == start_b.isoformat()
        else:
            assert new_id != sid and old.end_time == observed
            assert sql.get(LiveSession, new_id).start_time == start_b
            assert target.bucket_index == 0 and target.start_time == start_b
    room_lifecycle.finish_live_session(ROOM, recovered_at, deps)
    assert totals(factory)[0] == 38 * 60 + 10 * 60


def test_cold_restart_keeps_resumed_segment_and_its_original_buckets(duration_state):
    factory, deps, _ = duration_state
    sid = start_live()
    end = START + datetime.timedelta(minutes=40)
    room_lifecycle.defer_live_session_finish(ROOM, end, deps)
    second = end + datetime.timedelta(seconds=150)
    # The process returns late but the actual restart occurred inside the grace window.
    for state in (runtime_state.CURRENT_SESSIONS, runtime_state.STREAM_STARTS,
                  runtime_state.LAST_STATUS, runtime_state.PENDING_SESSION_ENDS):
        state.clear()
    info = {"live_status": 1, "live_time": int(second.timestamp())}
    now = second + datetime.timedelta(hours=1)
    assert monitoring_jobs._apply_room_status(ROOM, info, now)
    assert runtime_state.CURRENT_SESSIONS[ROOM] == sid
    runtime_state.LAST_STATUS.clear()
    runtime_state.CURRENT_SESSIONS.clear()
    runtime_state.LAST_LIVE_OBSERVATIONS.clear()
    runtime_state.STREAM_STARTS.clear()
    assert monitoring_jobs._apply_room_status(ROOM, info, now)
    assert runtime_state.STREAM_STARTS[ROOM] == second
    room_lifecycle.finish_live_session(ROOM, now, deps)
    assert totals(factory)[0] == 100 * 60


def test_cold_restart_retries_failed_close_before_accepting_new_session(duration_state, monkeypatch):
    factory, _, _ = duration_state
    sid = start_live()
    runtime_state.CURRENT_SESSIONS.clear()
    runtime_state.STREAM_STARTS.clear()
    runtime_state.LAST_STATUS.clear()
    start_b = START + datetime.timedelta(hours=2)
    info = {"live_status": 1, "live_time": int(start_b.timestamp())}

    def fail_close(*args):
        raise SQLAlchemyError("temporary close failure")

    with monkeypatch.context() as failure:
        failure.setattr(LiveSession, "close_session_by_id", fail_close)
        with pytest.raises(SQLAlchemyError):
            monitoring_jobs._apply_room_status(ROOM, info, start_b)
    assert runtime_state.LAST_STATUS.get(ROOM) != 1
    with factory() as sql:
        assert sql.query(LiveSession).count() == 1
        assert sql.get(LiveSession, sid).end_time is None
    assert monitoring_jobs._apply_room_status(ROOM, info, start_b)
    assert runtime_state.CURRENT_SESSIONS[ROOM] != sid
    with factory() as sql:
        assert sql.get(LiveSession, sid).end_time == START


@pytest.mark.parametrize("gap", [180, 181])
def test_stale_first_live_time_corrected_while_room_stays_live(duration_state, gap):
    factory, deps, _ = duration_state
    sid = start_live()
    observed = START + datetime.timedelta(minutes=18)
    runtime_state.LAST_LIVE_OBSERVATIONS[sid] = observed
    LiveSession.checkpoint_live_observations()
    runtime_state.LAST_LIVE_OBSERVATIONS.clear()
    runtime_state.CURRENT_SESSIONS.clear()
    runtime_state.LAST_STATUS.clear()
    runtime_state.STREAM_STARTS.clear()
    resumed = observed + datetime.timedelta(seconds=gap)
    now = resumed + datetime.timedelta(minutes=20)
    stale = {"live_status": 1, "live_time": int(START.timestamp())}
    corrected = dict(stale, live_time=int(resumed.timestamp()))
    assert monitoring_jobs._apply_room_status(ROOM, stale, now)
    assert runtime_state.LAST_LIVE_OBSERVATIONS[sid] == observed
    # A second cached batch now appears to put A beyond B's real start.
    assert monitoring_jobs._apply_room_status(ROOM, stale, now)
    assert monitoring_jobs._apply_room_status(ROOM, corrected, now)
    current = runtime_state.event_session(ROOM)
    assert current and (current == sid) == (gap == 180)
    bucket = metrics_runtime.danmaku_bucket_target(current, resumed + datetime.timedelta(seconds=1))
    assert bucket and bucket.bucket_index == (1 if gap == 180 else 0)
    assert not monitoring_jobs._apply_room_status(ROOM, stale, now)
    with factory() as sql:
        assert sql.get(LiveSession, sid).end_time == (None if gap == 180 else observed)
    room_lifecycle.finish_live_session(ROOM, now, deps)
    assert totals(factory)[0] == 38 * 60


@pytest.mark.parametrize("elapsed", [180, 181])
def test_startup_inventory_reconciles_explicit_offline_at_last_observation(duration_state, elapsed, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from app import event_ingestion

    factory, _, _ = duration_state
    sid = start_live()
    last_seen = START + datetime.timedelta(minutes=17)
    runtime_state.LAST_LIVE_OBSERVATIONS[sid] = last_seen
    LiveSession.checkpoint_live_observations()
    for state in (runtime_state.CURRENT_SESSIONS, runtime_state.LAST_LIVE_OBSERVATIONS,
                  runtime_state.LAST_STATUS, runtime_state.STREAM_STARTS):
        state.clear()
    assert LiveSession.open_room_ids([ROOM, ROOM + 1]) == {ROOM}
    runtime_state.RECOVERING_ROOMS.add(ROOM)
    assert monitoring_jobs._apply_room_status(ROOM, {"live_status": 0}, last_seen + datetime.timedelta(seconds=elapsed))
    assert ROOM not in runtime_state.RECOVERING_ROOMS
    assert runtime_state.event_session(ROOM) is None
    client = SimpleNamespace(room_id=ROOM)
    handler = MyHandler()
    assert handler._resolve_session(client, None, last_seen) is None
    monthly, daily = Mock(), Mock()
    monkeypatch.setattr(event_ingestion, "record_whale_revenue", Mock())
    monkeypatch.setattr(event_ingestion.RoomStatsMonthly, "add_amounts", monthly)
    monkeypatch.setattr(RoomLiveStats, "add_metrics", daily)
    for method in ("add_values_by_id", "add_values_by_room_open", "find_open_session"):
        monkeypatch.setattr(LiveSession, method, Mock(side_effect=AssertionError("offline session write")))
    handler._record_gift(client, "test", 1, 1000, event_time=last_seen)
    monthly.assert_called_once_with(ROOM, "202610", gift=1)
    assert daily.call_args.kwargs["gift"] == 1
    with factory() as sql:
        assert sql.get(LiveSession, sid).end_time == (last_seen if elapsed > 180 else None)
    assert runtime_state.PENDING_SESSION_ENDS.get(ROOM) == (None if elapsed > 180 else last_seen)
    assert totals(factory)[0] == 17 * 60



def test_missing_start_during_grace_waits_for_confirmed_reopen(duration_state, monkeypatch):
    factory, deps, _ = duration_state
    sid = start_live()
    ended = START + datetime.timedelta(minutes=10)
    room_lifecycle.defer_live_session_finish(ROOM, ended, deps)
    runtime_state.LAST_STATUS[ROOM] = 0
    runtime_state.ROOM_UIDS[ROOM] = 99
    reopened = ended + datetime.timedelta(seconds=180)
    now = reopened + datetime.timedelta(seconds=5)
    observations = iter([{"live_status": 1}, {"live_status": 1, "live_time": int(reopened.timestamp())}])

    async def batches():
        yield {ROOM: (99, next(observations))}

    async def no_detail(_):
        return None

    async def tick(_):
        with factory() as sql:
            assert sql.get(LiveSession, sid).end_time is None
        if runtime_state.LAST_STATUS[ROOM] == 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(runtime_state, "aiohttp_session", object())
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: now)
    monkeypatch.setattr(monitoring_jobs, "status_batches", batches)
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [ROOM])
    monkeypatch.setattr(monitoring_jobs.bilibili_gateway, "_fetch_room_info", no_detail)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", tick)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.monitor_all_rooms_status())
    assert runtime_state.event_session(ROOM) == sid
    assert runtime_state.STREAM_STARTS[ROOM] == reopened
