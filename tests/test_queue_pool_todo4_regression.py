from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from app import bootstrap, database

REPO_ROOT = Path(__file__).resolve().parents[1]
EMPTY_ENV = REPO_ROOT / "tests" / "_fixtures" / "empty.env"


def _pool_process(overrides: dict[str, str]) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(overrides)
    environment["ENV_FILE"] = str(EMPTY_ENV)
    program = """import json
from app import database
pool = database.engine.pool
print(json.dumps({
    'size': pool.size(),
    'max_overflow': pool._max_overflow,
    'timeout': pool._timeout,
    'recycle': pool._recycle,
}))
"""
    return subprocess.run(
        [sys.executable, "-c", program],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
        timeout=15,
    )


def test_pool_settings_accept_non_secret_environment_overrides() -> None:
    # Given: non-default, non-secret QueuePool environment settings.
    result = _pool_process(
        {
            "DB_POOL_SIZE": "2",
            "DB_POOL_MAX_OVERFLOW": "3",
            "DB_POOL_TIMEOUT": "4",
            "DB_POOL_RECYCLE": "5",
        }
    )

    # When: a fresh isolated interpreter creates the application engine.
    # Then: every explicit value reaches QueuePool.
    assert result.returncode == 0
    assert json.loads(result.stdout) == {
        "size": 2,
        "max_overflow": 3,
        "timeout": 4,
        "recycle": 5,
    }


def test_malformed_pool_settings_fall_back_without_echoing_values() -> None:
    # Given: malformed pool settings that must never appear in logs.
    malformed = "not-an-integer"
    result = _pool_process(
        {
            "DB_POOL_SIZE": malformed,
            "DB_POOL_MAX_OVERFLOW": malformed,
            "DB_POOL_TIMEOUT": malformed,
            "DB_POOL_RECYCLE": malformed,
        }
    )

    # When: a fresh isolated interpreter creates the application engine.
    # Then: defaults are retained and diagnostics identify settings, not values.
    assert result.returncode == 0
    assert json.loads(result.stdout) == {
        "size": 5,
        "max_overflow": 10,
        "timeout": 30,
        "recycle": 1800,
    }
    assert malformed not in result.stderr
    for setting in ("DB_POOL_SIZE", "DB_POOL_MAX_OVERFLOW", "DB_POOL_TIMEOUT", "DB_POOL_RECYCLE"):
        assert setting in result.stderr


@pytest.mark.parametrize(
    ("setting", "invalid"),
    [
        ("DB_POOL_SIZE", "0"),
        ("DB_POOL_MAX_OVERFLOW", "-1"),
        ("DB_POOL_TIMEOUT", "0"),
        ("DB_POOL_RECYCLE", "-10"),
    ],
)
def test_out_of_range_pool_settings_fall_back(setting: str, invalid: str) -> None:
    # Given: a pool setting outside its supported lower bound.
    result = _pool_process({setting: invalid})

    # When: an isolated engine is constructed with that setting.
    # Then: the setting falls back to its established default.
    assert result.returncode == 0
    defaults = {"DB_POOL_SIZE": 5, "DB_POOL_MAX_OVERFLOW": 10, "DB_POOL_TIMEOUT": 30, "DB_POOL_RECYCLE": 1800}
    setting_key = setting.removeprefix("DB_POOL_").lower()
    assert json.loads(result.stdout)[setting_key] == defaults[setting]


def test_pool_status_log_is_limited_to_label_and_numeric_counters(caplog) -> None:
    # Given: the application QueuePool under test isolation.
    # When: a report error emits its safe pool observation.
    with caplog.at_level("WARNING"):
        database.log_pool_status("get_stats_by_month")

    # Then: the line contains an operation label and numeric pool counters only.
    message = caplog.messages[-1]
    assert "label=get_stats_by_month" in message
    assert "checked_out=" in message
    assert "overflow=" in message
    assert "mysql" not in message.lower()
    assert "vr_test" not in message


def test_monthly_archive_jobs_do_not_overlap(monkeypatch) -> None:
    active = 0
    maximum_active = 0

    async def fake_to_thread(_job, _month):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1

    monkeypatch.setattr(bootstrap.asyncio, "to_thread", fake_to_thread)

    # Given: thread dispatch yields so overlapping archive jobs are observable.
    # When: one monthly archive operation is scheduled.
    asyncio.run(bootstrap._archive_month("202601"))

    # Then: at most one archive job has entered the runner at a time.
    assert maximum_active == 1


def test_monthly_archive_failure_does_not_skip_following_jobs(monkeypatch) -> None:
    calls: list[str] = []

    def fail(_month: str) -> None:
        calls.append("archive_super_chat_log")
        raise RuntimeError("synthetic archive failure")

    async def fake_to_thread(job, _month):
        calls.append(job.__name__)
        if job is fail:
            raise RuntimeError("synthetic archive failure")

    monkeypatch.setattr(bootstrap.archive_service, "archive_super_chat_log", fail)
    monkeypatch.setattr(bootstrap.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(bootstrap, "log_pool_status", lambda _label: None)

    # Given: the first serialized archive job fails in its worker thread.
    # When: the monthly archive sequence runs.
    asyncio.run(bootstrap._archive_month("202601"))

    # Then: all later archive jobs are still dispatched in order.
    assert calls == [
        "fail",
        "archive_room_live_stats",
        "archive_attention",
        "archive_whale_month",
        "archive_live_session",
    ]


def test_report_metadata_scope_reflects_each_table_once_per_operation() -> None:
    from app.repositories.tables import ReportTableMetadata

    reflection_calls = 0

    def reflect_columns(_table_name: str) -> frozenset[str]:
        nonlocal reflection_calls
        reflection_calls += 1
        return frozenset({"steel_coin_count"})

    metadata = ReportTableMetadata(lambda _table_name: True, reflect_columns)

    # Given: one report operation with repeated archive-column needs.
    # When: it asks for the same table metadata twice.
    first = metadata.column_names("room_live_stats_202601")
    second = metadata.column_names("room_live_stats_202601")

    # Then: response-shaping metadata is stable and reflected once only.
    assert first == second == frozenset({"steel_coin_count"})
    assert reflection_calls == 1


def test_fresh_report_metadata_scope_does_not_reuse_prior_reflection() -> None:
    from app.repositories.tables import ReportTableMetadata

    first_scope = ReportTableMetadata(
        lambda _table_name: True,
        lambda _table_name: frozenset({"first_request_column"}),
    )
    second_scope = ReportTableMetadata(
        lambda _table_name: True,
        lambda _table_name: frozenset({"second_request_column"}),
    )

    # Given: two separate report operations with different reflected schemas.
    # When: each operation reads the same archive table name.
    first = first_scope.column_names("room_live_stats_202601")
    second = second_scope.column_names("room_live_stats_202601")

    # Then: no schema result crosses the request-operation boundary.
    assert first == frozenset({"first_request_column"})
    assert second == frozenset({"second_request_column"})


def test_report_metadata_reflection_uses_the_checked_out_session_connection() -> None:
    # Given: a real SQLite ORM Session holding the only QueuePool connection.
    engine = create_engine(
        "sqlite://",
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE room_live_stats_202601 (room_id INTEGER)"))
    session = sessionmaker(bind=engine)()
    session.connection()

    from app import api_app

    try:
        # When: report metadata reflects table and column data during that Session.
        metadata = api_app._report_table_metadata(session)

        # Then: reflection completes on the checked-out connection, without pool checkout.
        assert metadata.has_table("room_live_stats_202601")
        assert metadata.column_names("room_live_stats_202601") == frozenset({"room_id"})
        assert isinstance(engine.pool, QueuePool)
        assert engine.pool.checkedout() == 1
    finally:
        session.close()
        engine.dispose()
