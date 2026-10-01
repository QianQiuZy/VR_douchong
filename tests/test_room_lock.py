import asyncio
import datetime
import time

import pytest

from app import (
    event_ingestion,
    metrics_runtime,
    monitoring_jobs,
    room_lifecycle,
    room_lock_events,
    runtime_state,
)
from app.blivedm.clients.ws_base import WebSocketClientBase


def replay_client(room_id: int) -> WebSocketClientBase:
    client = WebSocketClientBase.__new__(WebSocketClientBase)
    client._room_id = room_id
    return client


@pytest.fixture
def lock_state(monkeypatch):
    for name in (
        "CURRENT_SESSIONS", "LAST_STATUS", "STREAM_STARTS", "LIVE_INFO",
        "PENDING_SESSION_ENDS", "CONCURRENCY_CACHE", "LOCKED_ROOM_UNTIL", "ROOM_UIDS",
    ):
        monkeypatch.setattr(runtime_state, name, {})
    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", asyncio.Queue())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", asyncio.Queue())
    monkeypatch.setattr(metrics_runtime, "_buckets", {})
    monkeypatch.setattr(room_lifecycle.LiveSession, "find_open_session", lambda room: None)


def lock_command():
    return {
        "cmd": "ROOM_LOCK", "room_id": 27628030,
        "send_time": 1790762420089, "expire": "2026-10-01 18:00:20",
    }


@pytest.mark.parametrize("host_timezone", ["UTC", "Asia/Shanghai"])
def test_room_lock_expiry_uses_beijing_time_on_every_host(lock_state, monkeypatch, host_timezone):
    # Given: the same Bilibili lock payload on hosts with different local timezones.
    try:
        with monkeypatch.context() as configuration:
            configuration.setenv("TZ", host_timezone)
            time.tzset()

            # When: the actual dispatcher records the lock expiration.
            event_ingestion.MyHandler().handle(replay_client(27628030), lock_command())

            # Then: both hosts unlock at the same epoch, exactly 24 hours after the event.
            assert runtime_state.LOCKED_ROOM_UNTIL[27628030] == 1790848820
    finally:
        time.tzset()


def test_room_lock_closes_session_at_event_time_without_grace(lock_state, monkeypatch):
    # Given: a live session and the exact top-level ROOM_LOCK payload from the incident.
    room = 27628030
    ended = datetime.datetime.fromtimestamp(
        1790762420089 / 1000, tz=datetime.timezone.utc
    ).astimezone().replace(tzinfo=None, microsecond=0)
    calls = []
    runtime_state.CURRENT_SESSIONS[room] = 17436
    runtime_state.LAST_STATUS[room] = 1
    runtime_state.STREAM_STARTS[room] = ended - datetime.timedelta(minutes=56)
    runtime_state.PENDING_SESSION_ENDS[room] = ended - datetime.timedelta(seconds=1)
    dependencies = room_lifecycle.LifecycleDependencies(
        lambda room, end: "00:56:00" if runtime_state.STREAM_STARTS.pop(room, None) else None,
        lambda room, session: calls.append(("danmaku", session)),
        lambda room, session: (68.0, 101),
        lambda: ended,
        lambda room: asyncio.sleep(0),
        lambda room: asyncio.sleep(0),
    )
    monkeypatch.setattr(room_lock_events, "_lifecycle_dependencies", lambda: dependencies)
    monkeypatch.setattr(room_lifecycle, "flush_session", lambda session, end: calls.append(("bucket", session, end)))
    monkeypatch.setattr(room_lifecycle.LiveSession, "close_session_by_id", lambda session, end: calls.append(("close", session, end)))
    monkeypatch.setattr(room_lifecycle.LiveSession, "update_concurrency_by_id", lambda *args, **kwargs: None)

    # When: the websocket dispatcher receives the room lock, twice.
    handler = event_ingestion.MyHandler()
    handler.handle(replay_client(room), lock_command())
    handler.handle(replay_client(room), lock_command())

    # Then: it closes once at send_time and clears all active/session-grace state immediately.
    assert calls == [("danmaku", 17436), ("bucket", 17436, ended), ("close", 17436, ended)]
    assert runtime_state.LAST_STATUS[room] == 0
    assert room not in runtime_state.CURRENT_SESSIONS
    assert room not in runtime_state.PENDING_SESSION_ENDS
    assert room not in runtime_state.STREAM_STARTS
    assert runtime_state.GUARD_FANS_QUEUE.qsize() == 1


@pytest.mark.parametrize("changes", [{"room_id": 99}, {"send_time": "invalid"}, {"expire": "invalid"}])
def test_invalid_or_foreign_room_lock_does_not_end_session(lock_state, changes):
    # Given: a live room and a malformed or differently addressed event.
    runtime_state.CURRENT_SESSIONS[27628030] = 17436
    runtime_state.LAST_STATUS[27628030] = 1
    command = lock_command() | changes

    # When: the event enters the real websocket dispatcher.
    event_ingestion.MyHandler().handle(replay_client(27628030), command)

    # Then: the unrelated live session remains untouched.
    assert runtime_state.CURRENT_SESSIONS[27628030] == 17436
    assert runtime_state.LAST_STATUS[27628030] == 1


@pytest.mark.parametrize("locked", [True, False])
def test_status_poll_only_starts_session_after_room_lock_expires(lock_state, monkeypatch, locked):
    # Given: the polling API still claims live while the lock is active or has just expired.
    room = 27628030
    now = datetime.datetime.fromisoformat("2026-10-01 18:00:20")
    runtime_state.ROOM_UIDS[room] = 1
    runtime_state.LAST_STATUS[room] = 0
    runtime_state.LOCKED_ROOM_UNTIL[room] = int(now.timestamp()) + (1 if locked else 0)
    started = []

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def json(self, **kwargs):
            return {"data": {"1": {"live_status": 1, "title": "new live"}}}

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    async def stop_after_poll(seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(runtime_state, "aiohttp_session", Session())
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [room])
    monkeypatch.setattr(monitoring_jobs, "_now", lambda: now)
    monkeypatch.setattr(monitoring_jobs.LiveSession, "start_session", lambda *args: started.append(args) or None)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stop_after_poll)

    # When: one real status-monitor iteration runs.
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.monitor_all_rooms_status())

    # Then: stale live status cannot resurrect a locked session; expiry permits a new broadcast.
    assert len(started) == (0 if locked else 1)
    assert runtime_state.LAST_STATUS[room] == (0 if locked else 1)


def test_inflight_concurrency_response_does_not_revive_ended_session(lock_state, monkeypatch):
    # Given: a concurrency request whose session closes while the HTTP request is in flight.
    room = 27628030
    runtime_state.ROOM_UIDS[room] = 1
    runtime_state.CURRENT_SESSIONS[room] = 17436
    runtime_state.LAST_STATUS[room] = 1
    updates = []

    async def late_count(uid, room_id):
        runtime_state.CURRENT_SESSIONS.pop(room_id)
        runtime_state.LAST_STATUS[room_id] = 0
        return 100

    async def stop_after_response(seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [room])
    monkeypatch.setattr(monitoring_jobs.bilibili_gateway, "fetch_contribution_count", late_count)
    monkeypatch.setattr(monitoring_jobs, "update_concurrency_cache", lambda *args: updates.append(args))
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stop_after_response)

    # When: the pending response returns to the actual concurrency scheduler.
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitoring_jobs.concurrency_poll_scheduler())

    # Then: no new accumulator is created for the closed session.
    assert updates == []
