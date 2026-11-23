"""Durable, per-room FIFO for danmaku left at a collector handoff."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import sqlite3
import threading
import time
from collections import Counter
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from uuid import uuid4

import redis

from . import Audience, DanmakuBucketTarget, DanmakuCounts, SenderBatch, danmaku_metrics, runtime_state
from .models import LiveSession, LiveSession15mStats, RoomStatsMonthly

logger = logging.getLogger(__name__)
_worker: threading.Thread | None = None


def journal_path() -> Path:
    return Path(runtime_state.ROOMS_JSON_PATH).with_suffix(".danmaku.sqlite3")


def refresh_rooms() -> None:
    path = journal_path()
    if path.exists():
        with closing(sqlite3.connect(path)) as db:
            initialized = db.execute("SELECT 1 FROM sqlite_master WHERE name='batch'").fetchone()
            rooms = {row[0] for row in db.execute("SELECT DISTINCT room FROM batch")} if initialized else set()
    else:
        rooms = set()
    runtime_state.DANMAKU_RECOVERY_ROOMS.difference_update(runtime_state.DANMAKU_RECOVERY_ROOMS - rooms)
    runtime_state.DANMAKU_RECOVERY_ROOMS.update(rooms)


def _rows(room, pending):
    for items in (pending.monthly, pending.sessions, pending.buckets):
        for key, counts in items.items():
            if isinstance(key, DanmakuBucketTarget):
                kind, target, start = "bucket", asdict(key), key.start_time
                target["start_time"] = start.isoformat()
                target["end_time"] = key.end_time.isoformat()
            elif isinstance(key, str):
                kind, target, start = "month", key, danmaku_metrics.month_start(key)
            else:
                kind, target = "session", key
                start = pending.session_starts.get(key, datetime.datetime.min)
            batches = [dict(token=batch.token, counts=dict(batch.counts))
                       for store in (pending.retry, pending.senders) if (batch := store.get(key))]
            audience = pending.results.get(key, counts.audience)
            data = dict(kind=kind, target=target, start=start.isoformat(), counts=asdict(counts),
                        audience=asdict(audience) if audience else None, batches=batches)
            yield room, uuid4().hex, json.dumps(data, separators=(",", ":"))


def capture(rooms) -> None:
    """Commit before releasing RAM. A later shutdown appends behind earlier rows."""
    rooms = [r for r in rooms if r in runtime_state.DANMAKU_PENDING]
    if not rooms:
        return
    path = journal_path()
    if not path.exists():
        path.touch(mode=0o600, exist_ok=True)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("CREATE TABLE IF NOT EXISTS journal (id TEXT NOT NULL)")
        db.execute("INSERT INTO journal SELECT ? WHERE NOT EXISTS (SELECT 1 FROM journal)", (uuid4().hex,))
        db.execute("CREATE TABLE IF NOT EXISTS batch (seq INTEGER PRIMARY KEY, room INTEGER NOT NULL, "
                   "token TEXT NOT NULL, data TEXT NOT NULL)")
        db.execute("CREATE INDEX IF NOT EXISTS batch_room_seq ON batch(room, seq)")
        for room in rooms:
            db.executemany("INSERT INTO batch(room,token,data) VALUES(?,?,?)",
                           _rows(room, runtime_state.DANMAKU_PENDING[room]))
    runtime_state.DANMAKU_RECOVERY_ROOMS.update(rooms)
    for room in rooms:
        del runtime_state.DANMAKU_PENDING[room]


def _deliver(db, stream, seq, room, token, data, deadline):
    kind, target = data["kind"], data["target"]
    if kind == "bucket":
        target = DanmakuBucketTarget(**{**target,
            "start_time": datetime.datetime.fromisoformat(target["start_time"]),
            "end_time": datetime.datetime.fromisoformat(target["end_time"])})
        scope_key = f"bucket:{target.session_id}:{target.bucket_index}"
    elif kind == "session":
        scope_key = f"session:{target}"
    else:
        scope_key = f"month:{target}:{room}"
    scope = danmaku_metrics.Scope(scope_key, datetime.datetime.fromisoformat(data["start"]), kind != "bucket")
    def save(counts, phase):
        counts = replace(counts, replay_cursor=(f"{stream}:{room}:{phase}", token))
        if kind == "month":
            return RoomStatsMonthly.add_danmaku_counts(room, target, counts)
        if kind == "session":
            return LiveSession.add_danmaku_by_id(target, counts)
        return LiveSession15mStats.add_danmaku_counts(target, counts)

    def checkpoint():
        with db:
            db.execute("UPDATE batch SET data=? WHERE seq=?", (json.dumps(data), seq))

    if data["counts"]["total"]:
        # A SQL commit may succeed even when its reply or the local checkpoint
        # is lost. Each phase has a cursor in the same transaction as its write.
        raw_counts = DanmakuCounts(**data["counts"])
        unknown = Audience(None, None) if data["batches"] or data["audience"] else None
        if not save(replace(raw_counts, audience=unknown), "t"):
            return False
        data["counts"] = asdict(DanmakuCounts())
        checkpoint()
    while data["batches"]:
        if time.monotonic() >= deadline:
            return False
        raw = data["batches"][0]
        batch = SenderBatch(Counter({int(uid): n for uid, n in raw["counts"].items()}), raw["token"])
        audience = danmaku_metrics.record(scope, batch)
        # Commit this Redis token before sending the next one to the same scope.
        data["audience"] = asdict(audience)
        data["batches"].pop(0)
        checkpoint()
    if time.monotonic() >= deadline:
        return False
    audience = Audience(**data["audience"]) if data["audience"] is not None else None
    done = audience is None or save(DanmakuCounts(audience=audience), "a")
    if done:
        with db:
            db.execute("DELETE FROM batch WHERE seq=?", (seq,))
    return done


def replay(path: Path, deadline: float) -> None:
    """A failed room retains its order; independent rooms can continue."""
    try:
        with closing(sqlite3.connect(path)) as db:
            stream, = db.execute("SELECT id FROM journal").fetchone()
            rooms = [r for r, in db.execute("SELECT DISTINCT room FROM batch ORDER BY room")]
            for room in rooms:
                while time.monotonic() < deadline:
                    row = db.execute("SELECT seq,token,data FROM batch WHERE room=? ORDER BY seq LIMIT 1",
                                     (room,)).fetchone()
                    if row is None:
                        break
                    seq, token, data = row
                    try:
                        if not _deliver(db, stream, seq, room, token, json.loads(data), deadline):
                            break
                    except redis.RedisError as exc:
                        logger.warning("[DanmakuRecovery] room=%s 等待 Redis error_type=%s", room, type(exc).__name__)
                        break
    except Exception:
        logger.exception("[DanmakuRecovery] 恢复中断，保留本地批次")


async def drain(seconds: float) -> None:
    """The journal is already durable. A slow dependency cannot hold up shutdown."""
    global _worker
    if not runtime_state.DANMAKU_RECOVERY_ROOMS:
        return
    deadline = time.monotonic() + seconds
    next_attempt = 0.0
    while runtime_state.DANMAKU_RECOVERY_ROOMS and time.monotonic() < deadline:
        if _worker is None or not _worker.is_alive():
            refresh_rooms()
            if not runtime_state.DANMAKU_RECOVERY_ROOMS:
                return
            if time.monotonic() >= next_attempt:
                _worker = threading.Thread(target=replay, args=(journal_path(), deadline),
                                           name="danmaku-recovery", daemon=True)
                _worker.start()
                next_attempt = time.monotonic() + 1
        await asyncio.sleep(min(0.25, max(0, deadline - time.monotonic())))
    if _worker is None or not _worker.is_alive():
        refresh_rooms()


async def retry_scheduler() -> None:
    while True:
        if runtime_state.DANMAKU_RECOVERY_ROOMS:
            await drain(5)
        await asyncio.sleep(15)
