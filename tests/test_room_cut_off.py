import datetime
from queue import Queue

import pytest

from app import event_ingestion, room_lifecycle, room_lock_events, runtime_state
from app.blivedm.clients.ws_base import WebSocketClientBase

EXAMPLES = [(27628019, 1789624441982), (31368705, 1790429146927)]


def replay_client(room_id: int) -> WebSocketClientBase:
    client = WebSocketClientBase.__new__(WebSocketClientBase)
    client._room_id = room_id
    return client


@pytest.fixture
def cutoff_state(monkeypatch):
    for name in (
        "CURRENT_SESSIONS", "LAST_STATUS", "STREAM_STARTS", "LIVE_INFO",
        "PENDING_SESSION_ENDS", "CONCURRENCY_CACHE", "LOCKED_ROOM_UNTIL",
    ):
        monkeypatch.setattr(runtime_state, name, {})
    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", Queue())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", Queue())
    calls = []

    def record_segment(room_id: int, end: datetime.datetime) -> str | None:
        start = runtime_state.STREAM_STARTS.pop(room_id, None)
        if start is None:
            return None
        calls.append(("segment", room_id, end))
        return "01:00:00"

    async def no_client(room_id: int) -> None:
        return None

    dependencies = room_lifecycle.LifecycleDependencies(
        record_segment, lambda *args: None, lambda *args: (None, None),
        datetime.datetime.now, no_client, no_client,
    )
    monkeypatch.setattr(room_lock_events, "_lifecycle_dependencies", lambda: dependencies)
    monkeypatch.setattr(room_lifecycle, "flush_session", lambda *args: None)
    monkeypatch.setattr(room_lifecycle.LiveSession, "find_open_session", lambda room: None)
    monkeypatch.setattr(room_lifecycle.LiveSession, "close_session_by_id", lambda session, end: calls.append(("close", session, end)))
    monkeypatch.setattr(room_lifecycle.LiveSession, "update_concurrency_by_id", lambda *args, **kwargs: None)
    return calls, dependencies


def open_room(room_id: int, timestamp: int) -> datetime.datetime:
    end = datetime.datetime.fromtimestamp(timestamp // 1000, tz=datetime.timezone.utc).astimezone().replace(tzinfo=None)
    runtime_state.CURRENT_SESSIONS[room_id] = 99
    runtime_state.LAST_STATUS[room_id] = 1
    runtime_state.STREAM_STARTS[room_id] = end - datetime.timedelta(hours=1)
    return end


@pytest.mark.parametrize("room_id,timestamp", EXAMPLES)
def test_cut_off_starts_grace_at_server_time_without_closing(cutoff_state, room_id, timestamp):
    # Given: an active session and either actual CUT_OFF example from the log.
    calls, _ = cutoff_state
    end = open_room(room_id, timestamp)
    command = {"cmd": "CUT_OFF", "room_id": room_id, "send_time": timestamp}

    # When: the same websocket command is dispatched twice.
    handler = event_ingestion.MyHandler()
    handler.handle(replay_client(room_id), command)
    handler.handle(replay_client(room_id), command)

    # Then: one segment ends, but the original session stays open for the grace period.
    assert calls == [("segment", room_id, end)]
    assert runtime_state.PENDING_SESSION_ENDS[room_id] == end
    assert runtime_state.CURRENT_SESSIONS[room_id] == 99
    assert runtime_state.LAST_STATUS[room_id] == 0
    assert room_id not in runtime_state.LOCKED_ROOM_UNTIL


@pytest.mark.parametrize("resumes", [True, False])
def test_cut_off_resumes_within_grace_or_closes_at_original_end(cutoff_state, resumes):
    # Given: a CUT_OFF-interrupted session whose grace period is being resolved.
    calls, dependencies = cutoff_state
    room, timestamp = EXAMPLES[0]
    end = open_room(room, timestamp)
    event_ingestion.MyHandler().handle(replay_client(room), {"cmd": "CUT_OFF", "room_id": room, "send_time": timestamp})

    # When: it resumes at 180 seconds, or the expiry sweep runs at 181 seconds.
    if resumes:
        session = room_lifecycle.resume_interrupted_session(room, end + datetime.timedelta(seconds=180), end + datetime.timedelta(seconds=180))
    else:
        room_lifecycle.finish_expired_live_sessions(end + datetime.timedelta(seconds=181), dependencies)
        session = None

    # Then: resume preserves the session ID; expiry closes at CUT_OFF time, not three minutes later.
    assert session == (99 if resumes else None)
    assert room not in runtime_state.PENDING_SESSION_ENDS
    assert [(name, session_id, when) for name, session_id, when in calls if name == "close"] == ([] if resumes else [("close", 99, end)])


def test_cut_off_keeps_grace_when_segment_cache_is_missing(cutoff_state):
    # Given: the session is known, but its transient segment-start cache is missing.
    room, timestamp = EXAMPLES[0]
    end = open_room(room, timestamp)
    runtime_state.STREAM_STARTS.pop(room)

    # When: CUT_OFF is received through websocket dispatch.
    event_ingestion.MyHandler().handle(replay_client(room), {"cmd": "CUT_OFF", "room_id": room, "send_time": timestamp})

    # Then: loss of the duration cache cannot strand this session forever.
    assert runtime_state.PENDING_SESSION_ENDS[room] == end


@pytest.mark.parametrize("changes", [{"room_id": 99}, {"send_time": "bad"}, {"send_time": 1}])
def test_invalid_foreign_or_stale_cut_off_leaves_session_active(cutoff_state, changes):
    # Given: a valid active session and an unrelated, malformed or stale event.
    room, timestamp = EXAMPLES[0]
    open_room(room, timestamp)
    command = {"cmd": "CUT_OFF", "room_id": room, "send_time": timestamp} | changes

    # When: the event enters the actual dispatcher.
    event_ingestion.MyHandler().handle(replay_client(room), command)

    # Then: it cannot terminate the active broadcast.
    assert runtime_state.LAST_STATUS[room] == 1
    assert room not in runtime_state.PENDING_SESSION_ENDS
