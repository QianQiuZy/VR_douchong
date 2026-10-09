import asyncio
import base64
import datetime
import json
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.schema import CreateTable

from app import bootstrap, event_ingestion, runtime_state
from app.blivedm.models.pb import InteractWordV2
from app.event_ingestion import EntryMonitor, event_datetime
from app.models import RoomEntryLog
from app.repositories import entries


@pytest.fixture
def monitor(tmp_path, monkeypatch):
    path = tmp_path / "entry_users.json"
    path.write_text(
        json.dumps([{"uid": 7, "name": "visitor"}, {"uid": 9, "name": "owner"}]),
        encoding="utf-8",
    )
    result = EntryMonitor(path)
    assert result.reload()
    monkeypatch.setattr(event_ingestion, "entry_monitor", result)
    monkeypatch.setattr(runtime_state, "ROOM_UIDS", {111111: 9, 222222: 10})
    return result


@pytest.fixture
def entry_db(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    RoomEntryLog.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(entries, "Session", Session)
    monkeypatch.setattr("app.api_cache.invalidate_history", lambda *_: None)
    yield Session
    engine.dispose()


def test_entry_schema_has_only_three_columns_and_mysql_millisecond_precision():
    table = RoomEntryLog.__table__
    assert list(table.columns.keys()) == ["room_id", "uid", "event_time"]
    assert [column.name for column in table.primary_key] == [
        "room_id",
        "event_time",
        "uid",
    ]
    ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
    assert "BIGINT UNSIGNED" in ddl and "DATETIME(3)" in ddl


def test_hourly_list_reload_add_remove_and_invalid_edit_preserves_previous(monitor):
    monitor.path.write_text('[{"uid": 8, "name": "new"}]', encoding="utf-8")
    assert monitor.reload() and monitor.names == {8: "new"}
    for invalid in (
        "{",
        "{}",
        '[{"uid": true, "name": "bad"}]',
        '[{"uid": 8, "name": "a"}, {"uid": 8, "name": "b"}]',
    ):
        monitor.path.write_text(invalid, encoding="utf-8")
        assert not monitor.reload() and monitor.names == {8: "new"}
    monitor.path.unlink()
    assert not monitor.reload() and monitor.names == {8: "new"}
    monitor.path.write_text("[]", encoding="utf-8")
    assert monitor.reload() and monitor.names == {}


def test_reload_scheduler_uses_hourly_interval(monitor, monkeypatch):
    intervals = []

    async def sleep(seconds):
        intervals.append(seconds)
        if len(intervals) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(event_ingestion.asyncio, "sleep", sleep)
    monitor.path.write_text('[{"uid": 8, "name": "new"}]', encoding="utf-8")
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitor.reload_scheduler())
    assert intervals == [3600, 3600] and monitor.names == {8: "new"}


def test_whitelist_self_entry_nonentry_and_unknown_owner_are_skipped(monitor):
    client = SimpleNamespace(room_id=111111)
    for uid, kind in ((9, 1), (8, 1), (7, 2), (7, 3), (7, 0)):
        assert not monitor.receive(
            client, SimpleNamespace(uid=uid, msg_type=kind, timestamp=1700000000)
        )
    assert not monitor.receive(
        SimpleNamespace(room_id=333333), SimpleNamespace(uid=7, msg_type=1)
    )
    assert monitor.queue.empty()
    # The owner is still a visitor when entering someone else's room, including offline rooms.
    assert monitor.receive(
        SimpleNamespace(room_id=222222),
        SimpleNamespace(uid=9, msg_type=1, timestamp=1700000000),
    )


def test_both_real_protocol_callbacks_deduplicate_without_suppressing_reentry(
    monitor, entry_db
):
    handler = event_ingestion.MyHandler()
    client = SimpleNamespace(room_id=111111)
    handler.handle(
        client,
        {
            "cmd": "INTERACT_WORD",
            "data": {"uid": 7, "msg_type": 1, "timestamp": 1700000000},
        },
    )
    for stamp in (1700000000, 1700000001):
        proto = InteractWordV2(uid=7, msg_type=1, timestamp=stamp)
        handler.handle(
            client,
            {
                "cmd": "INTERACT_WORD_V2",
                "data": {"pb": base64.b64encode(proto.dumps()).decode()},
            },
        )
    asyncio.run(monitor.flush())
    with entry_db() as session:
        rows = session.scalars(
            select(RoomEntryLog).order_by(RoomEntryLog.event_time)
        ).all()
    assert len(rows) == 2 and all(row.uid == 7 for row in rows)
    assert rows[1].event_time - rows[0].event_time == datetime.timedelta(seconds=1)
    assert monitor.queue.empty() and not monitor.batch


def test_failed_write_retains_original_fallback_time_for_retry(
    monitor, entry_db, monkeypatch
):
    client = SimpleNamespace(room_id=111111)
    assert monitor.receive(client, SimpleNamespace(uid=7, msg_type=1, timestamp=0))
    queued_time = monitor.queue._queue[0]["event_time"]
    original = event_ingestion.save_entries

    def fail(_rows):
        raise SQLAlchemyError("synthetic outage")

    monkeypatch.setattr(event_ingestion, "save_entries", fail)
    asyncio.run(monitor.flush())
    assert monitor.batch[0]["event_time"] == queued_time
    monkeypatch.setattr(event_ingestion, "save_entries", original)
    asyncio.run(monitor.flush())
    with entry_db() as session:
        row = session.scalar(select(RoomEntryLog))
        assert row.event_time == queued_time
    assert not monitor.batch


def test_background_worker_retries_and_drains_queue(monitor, entry_db, monkeypatch):
    original = event_ingestion.save_entries
    calls = []

    def save(rows):
        calls.append(rows)
        if len(calls) == 1:
            raise SQLAlchemyError("synthetic outage")
        original(rows)

    monkeypatch.setattr(event_ingestion, "save_entries", save)
    assert monitor.receive(
        SimpleNamespace(room_id=111111),
        SimpleNamespace(uid=7, msg_type=1, timestamp=1700000000),
    )

    async def run():
        worker = asyncio.create_task(monitor.worker())
        try:
            await asyncio.wait_for(monitor.queue.join(), timeout=5)
        finally:
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker

    asyncio.run(run())
    assert len(calls) == 2 and calls[0] == calls[1]
    with entry_db() as session:
        assert session.query(RoomEntryLog).count() == 1


def test_runtime_stops_entry_worker_before_shutdown_drain(monitor, monkeypatch):
    state = {"started": False, "stopped": False, "flushed": False}
    original_gather = asyncio.gather

    async def worker():
        state["started"] = True
        try:
            await asyncio.Event().wait()
        finally:
            state["stopped"] = True

    async def flush():
        assert state["started"] and state["stopped"]
        state["flushed"] = True

    async def ready():
        pass

    async def fail_runtime(*jobs, **kwargs):
        if kwargs.get("return_exceptions"):
            return await original_gather(*jobs, **kwargs)
        # No network/other collectors are run in this lifecycle test.
        for job in jobs:
            if not isinstance(job, asyncio.Task):
                job.close()
        await asyncio.sleep(0)
        raise RuntimeError("synthetic collector failure")

    monkeypatch.setattr(monitor, "worker", worker)
    monkeypatch.setattr(monitor, "flush", flush)
    monkeypatch.setattr(bootstrap, "init_room_info", lambda: None)
    monkeypatch.setattr(bootstrap, "init_session", lambda: None)
    monkeypatch.setattr(bootstrap, "_flush_active_metrics", lambda *_: None)
    monkeypatch.setattr(
        bootstrap.monitoring_jobs, "init_uids_and_attention_once", ready
    )
    monkeypatch.setattr(runtime_state, "MAIN_LOOP", None)
    monkeypatch.setattr(runtime_state, "aiohttp_session", None)
    monkeypatch.setattr(bootstrap.asyncio, "gather", fail_runtime)
    with pytest.raises(RuntimeError, match="synthetic collector failure"):
        asyncio.run(bootstrap.main())
    assert state["flushed"]


def test_deployed_list_path_follows_env_file_directory(tmp_path):
    deployed_env = tmp_path / "deployment" / "service.env"
    deployed_env.parent.mkdir()
    deployed_env.write_text("", encoding="utf-8")
    env = {**os.environ, "ENV_FILE": str(deployed_env)}
    env.pop("ENTRY_USERS_JSON_PATH", None)
    script = (
        "from app.config import ENTRY_USERS_JSON_PATH; print(ENTRY_USERS_JSON_PATH)"
    )
    repo = str(event_ingestion.Path(__file__).resolve().parents[1])
    for setting, expected in (
        (None, deployed_env.parent / "entry_users.json"),
        ("visitors.json", deployed_env.parent / "visitors.json"),
    ):
        if setting:
            env["ENTRY_USERS_JSON_PATH"] = setting
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == str(expected)


def test_seconds_milliseconds_and_invalid_timestamp_normalization():
    received = datetime.datetime.fromisoformat("2026-10-08T12:00:00.123456")
    assert event_datetime(1700000000, received) == event_datetime(
        1700000000000, received
    )
    assert event_datetime(1700000000123, received).microsecond == 123000
    for invalid in (None, 0, "bad", 10**30, 9999999999999):
        assert event_datetime(invalid, received) == received.replace(microsecond=123000)


def test_queue_overflow_is_bounded_and_does_not_raise(monitor, monkeypatch):
    monkeypatch.setattr(monitor, "queue", asyncio.Queue(maxsize=1))
    message = SimpleNamespace(uid=7, msg_type=1, timestamp=1700000000)
    assert monitor.receive(SimpleNamespace(room_id=111111), message)
    assert not monitor.receive(SimpleNamespace(room_id=111111), message)
    assert monitor.queue.qsize() == 1
