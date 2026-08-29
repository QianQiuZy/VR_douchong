from __future__ import annotations

import asyncio
import datetime
from types import SimpleNamespace

import pytest

from app import room_config, room_lifecycle, runtime_state


@pytest.fixture()
def isolated_runtime_state():
    room_ids = list(runtime_state.ROOM_IDS)
    room_anchors = dict(runtime_state.ROOM_ANCHORS)
    room_uids = dict(runtime_state.ROOM_UIDS)
    current_sessions = dict(runtime_state.CURRENT_SESSIONS)
    room_clients = dict(runtime_state.ROOM_CLIENTS)
    last_reconnect = dict(runtime_state.LAST_RECONNECT)
    last_status = dict(runtime_state.LAST_STATUS)
    stream_starts = dict(runtime_state.STREAM_STARTS)
    live_info = dict(runtime_state.LIVE_INFO)
    pending_session_ends = dict(runtime_state.PENDING_SESSION_ENDS)
    danmaku_pending = dict(runtime_state.DANMAKU_PENDING)
    fans_count = dict(runtime_state.FANS_COUNT)
    guard_counts = dict(runtime_state.GUARD_COUNTS)
    concurrency_cache = dict(runtime_state.CONCURRENCY_CACHE)
    locked_room_until = dict(runtime_state.LOCKED_ROOM_UNTIL)
    runtime_state.ROOM_UIDS.clear()
    runtime_state.CURRENT_SESSIONS.clear()
    runtime_state.ROOM_CLIENTS.clear()
    runtime_state.LAST_RECONNECT.clear()
    runtime_state.LAST_STATUS.clear()
    runtime_state.STREAM_STARTS.clear()
    runtime_state.LIVE_INFO.clear()
    runtime_state.PENDING_SESSION_ENDS.clear()
    runtime_state.DANMAKU_PENDING.clear()
    runtime_state.FANS_COUNT.clear()
    runtime_state.GUARD_COUNTS.clear()
    runtime_state.CONCURRENCY_CACHE.clear()
    runtime_state.LOCKED_ROOM_UNTIL.clear()
    runtime_state.ROOM_IDS.clear()
    runtime_state.ROOM_ANCHORS.clear()
    try:
        yield
    finally:
        runtime_state.ROOM_IDS[:] = room_ids
        runtime_state.ROOM_ANCHORS.clear()
        runtime_state.ROOM_ANCHORS.update(room_anchors)
        runtime_state.ROOM_UIDS.clear()
        runtime_state.ROOM_UIDS.update(room_uids)
        runtime_state.CURRENT_SESSIONS.clear()
        runtime_state.CURRENT_SESSIONS.update(current_sessions)
        runtime_state.ROOM_CLIENTS.clear()
        runtime_state.ROOM_CLIENTS.update(room_clients)
        runtime_state.LAST_RECONNECT.clear()
        runtime_state.LAST_RECONNECT.update(last_reconnect)
        runtime_state.LAST_STATUS.clear()
        runtime_state.LAST_STATUS.update(last_status)
        runtime_state.STREAM_STARTS.clear()
        runtime_state.STREAM_STARTS.update(stream_starts)
        runtime_state.LIVE_INFO.clear()
        runtime_state.LIVE_INFO.update(live_info)
        runtime_state.PENDING_SESSION_ENDS.clear()
        runtime_state.PENDING_SESSION_ENDS.update(pending_session_ends)
        runtime_state.DANMAKU_PENDING.clear()
        runtime_state.DANMAKU_PENDING.update(danmaku_pending)
        runtime_state.FANS_COUNT.clear()
        runtime_state.FANS_COUNT.update(fans_count)
        runtime_state.GUARD_COUNTS.clear()
        runtime_state.GUARD_COUNTS.update(guard_counts)
        runtime_state.CONCURRENCY_CACHE.clear()
        runtime_state.CONCURRENCY_CACHE.update(concurrency_cache)
        runtime_state.LOCKED_ROOM_UNTIL.clear()
        runtime_state.LOCKED_ROOM_UNTIL.update(locked_room_until)


class TestRoomLifecycleWithoutBilibiliClient:
    def test_add_duplicate_missing_delete_and_no_client_cleanup(
        self, isolated_rooms_json, isolated_runtime_state, monkeypatch
    ):
        # Given: fixture-backed config, no HTTP session, and no Bilibili client.
        monkeypatch.setattr(runtime_state, "ROOMS_JSON_PATH", str(isolated_rooms_json))
        monkeypatch.setattr(room_lifecycle, "RoomInfo", SimpleNamespace(upsert=lambda *_args, **_kwargs: None))
        dependencies = room_lifecycle.LifecycleDependencies(
            record_stream_segment=lambda _room_id, _end_dt: None,
            flush_pending_danmaku=lambda _room_id, _session_id: None,
            finalize_concurrency=lambda _room_id, _session_id: (None, None),
            now=datetime.datetime.now,
            initialize_room=lambda _room_id: asyncio.sleep(0),
            start_client=lambda _room_id: asyncio.sleep(0),
        )
        room_config.load_rooms_config()

        # When: room configuration is loaded, saved, added, rejected as duplicate, and deleted.
        room_config.save_rooms_config()
        added = asyncio.run(room_lifecycle.add_room_async(333333, "FixtureAnchor", dependencies))
        duplicate = asyncio.run(room_lifecycle.add_room_async(333333, "FixtureAnchor", dependencies))
        missing = asyncio.run(room_lifecycle.delete_room_async(444444, dependencies))
        deleted = asyncio.run(room_lifecycle.delete_room_async(333333, dependencies))

        # Then: the legacy no-session result and cleanup behavior stay intact without network I/O.
        assert added == (False, "aiohttp_session 未初始化")
        assert duplicate == (False, "房间已存在")
        assert missing == (False, "房间不存在")
        assert deleted == (True, "房间已删除并停止任务")
        assert 333333 not in runtime_state.ROOM_IDS
        assert 333333 not in runtime_state.ROOM_UIDS
        assert 333333 not in runtime_state.CURRENT_SESSIONS
        assert 333333 not in runtime_state.ROOM_CLIENTS
        assert 333333 not in runtime_state.LAST_RECONNECT
        assert 333333 not in runtime_state.LAST_STATUS
        assert 333333 not in runtime_state.STREAM_STARTS
        assert 333333 not in runtime_state.LIVE_INFO
        assert 333333 not in runtime_state.PENDING_SESSION_ENDS
        assert 333333 not in runtime_state.DANMAKU_PENDING
        assert 333333 not in runtime_state.FANS_COUNT
        assert 333333 not in runtime_state.GUARD_COUNTS
        assert 333333 not in runtime_state.CONCURRENCY_CACHE
        assert 333333 not in runtime_state.LOCKED_ROOM_UNTIL

    def test_resume_honors_the_three_minute_grace_boundary(self, isolated_runtime_state):
        # Given: an interrupted live session at the inclusive three-minute boundary.
        now = datetime.datetime(2026, 8, 29, 12, 0, 0)
        runtime_state.PENDING_SESSION_ENDS[333333] = now - datetime.timedelta(seconds=180)
        runtime_state.CURRENT_SESSIONS[333333] = 99

        # When: the room returns exactly at the grace threshold.
        resumed = room_lifecycle.resume_interrupted_session(333333, now, now)

        # Then: the original session resumes and the pending end is cleared.
        assert resumed == 99
        assert 333333 not in runtime_state.PENDING_SESSION_ENDS

    def test_expired_finish_preserves_cleanup_order(self, isolated_runtime_state, monkeypatch):
        # Given: an expired session with a fake persistence boundary and no Bilibili client.
        room_id = 333333
        now = datetime.datetime(2026, 8, 29, 12, 5, 0)
        calls: list[str] = []
        runtime_state.PENDING_SESSION_ENDS[room_id] = now - datetime.timedelta(seconds=181)
        runtime_state.CURRENT_SESSIONS[room_id] = 99
        runtime_state.CONCURRENCY_CACHE[room_id] = {"session_id": 99, "total": 0, "samples": 0, "max": 0, "last": 0}
        monkeypatch.setattr(
            room_lifecycle,
            "LiveSession",
            SimpleNamespace(
                close_session_by_id=lambda *_args: calls.append("close"),
                update_concurrency_by_id=lambda *_args, **_kwargs: calls.append("update"),
            ),
        )
        dependencies = room_lifecycle.LifecycleDependencies(
            record_stream_segment=lambda _room_id, _end_dt: calls.append("record") or "00:00:01",
            flush_pending_danmaku=lambda _room_id, _session_id: calls.append("flush"),
            finalize_concurrency=lambda _room_id, _session_id: calls.append("finalize") or (1.0, 2),
            now=lambda: now,
            initialize_room=lambda _room_id: asyncio.sleep(0),
            start_client=lambda _room_id: asyncio.sleep(0),
        )

        # When: the expiry sweep finalizes the interrupted session.
        room_lifecycle.finish_expired_live_sessions(now, dependencies)

        # Then: lifecycle persistence and queue cleanup retain their legacy order.
        assert calls == ["record", "flush", "finalize", "close", "update"]
        assert room_id not in runtime_state.CURRENT_SESSIONS
        assert room_id not in runtime_state.PENDING_SESSION_ENDS
        assert room_id not in runtime_state.CONCURRENCY_CACHE
        runtime_state.GUARD_FANS_QUEUE.get_nowait()
        runtime_state.GUARD_FANS_QUEUE.task_done()
        runtime_state.ATTENTION_QUEUE.get_nowait()
        runtime_state.ATTENTION_QUEUE.task_done()
