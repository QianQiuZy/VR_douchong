import asyncio
import datetime
import threading
import time

import pytest

from app import DanmakuCounts, PendingDanmaku, danmaku_recovery as recovery, runtime_state


def pending_room(room):
    pending = PendingDanmaku()
    pending.add(datetime.datetime(2026, 2, 1), DanmakuCounts.from_privilege_type(0), None, uid=100)
    runtime_state.DANMAKU_PENDING[room] = pending


def test_failed_handoff_preserves_ram(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "DANMAKU_PENDING", {})
    monkeypatch.setattr(runtime_state, "ROOMS_JSON_PATH", str(tmp_path / "missing" / "rooms.json"))
    pending_room(7)
    with pytest.raises(OSError):
        recovery.capture([7])
    assert runtime_state.DANMAKU_PENDING[7].monthly["202602"].total == 1
    assert not runtime_state.DANMAKU_RECOVERY_ROOMS


def test_shutdown_deadline_keeps_work_for_next_process(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_state, "DANMAKU_PENDING", {})
    monkeypatch.setattr(runtime_state, "ROOMS_JSON_PATH", str(tmp_path / "rooms.json"))
    monkeypatch.setattr(recovery, "_worker", None)
    pending_room(7)
    recovery.capture([7])
    started = threading.Event()
    release = threading.Event()

    def unavailable(path, deadline):
        started.set()
        release.wait(3)

    monkeypatch.setattr(recovery, "replay", unavailable)
    try:
        begin = time.monotonic()
        asyncio.run(recovery.drain(0.1))
        assert time.monotonic() - begin < 1
        assert started.is_set() and recovery._worker.is_alive()
        runtime_state.DANMAKU_RECOVERY_ROOMS.clear()
        recovery.refresh_rooms()
        assert runtime_state.DANMAKU_RECOVERY_ROOMS == {7}
    finally:
        release.set()
        if recovery._worker:
            recovery._worker.join(1)
