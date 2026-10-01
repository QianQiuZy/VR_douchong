import datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.inspection import inspect as real_inspect

from app.gift_repair_targets import prepare
from app.gift_repair_types import GiftCorrection, GiftEvent, RepairError


@pytest.fixture(autouse=True)
def real_sqlite_reflection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.gift_repair_targets.inspect", real_inspect)


def correction(timestamp: str) -> GiftCorrection:
    event = GiftEvent("a" * 64, datetime.datetime.fromisoformat(timestamp), 1, 2, "gift", 10, Decimal("0.10"), "fixture.log", 1)
    return GiftCorrection(event, Decimal("0.10"), Decimal("0.90"), "fixture.log", 2)


def test_cross_month_gift_targets_parent_start_month_and_event_calendar_month():
    # Given: October revenue belongs to a session that started in September.
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE room_stats_monthly (room_id INT,month TEXT,gift FLOAT)"))
        connection.execute(text("INSERT INTO room_stats_monthly VALUES (1,'202610',0.1)"))
        connection.execute(text("CREATE TABLE room_live_stats (room_id INT,date TEXT,gift FLOAT)"))
        connection.execute(text("INSERT INTO room_live_stats VALUES (1,'2026-10-01',0.1)"))
        connection.execute(text("CREATE TABLE live_session_202609 (id INT,room_id INT,start_time TEXT,end_time TEXT,gift FLOAT)"))
        connection.execute(text("INSERT INTO live_session_202609 VALUES (99,1,'2026-09-30 23:50:00','2026-10-01 01:00:00',0.1)"))
        connection.execute(text("CREATE TABLE live_session_15m_stats_202609 (session_id INT,bucket_index INT,gift FLOAT)"))
        connection.execute(text("INSERT INTO live_session_15m_stats_202609 VALUES (99,1,0.1)"))

    # When: the planner resolves the gift's existing destinations.
    prepared, report = prepare(engine, [correction("2026-10-01 00:06:00")])

    # Then: month/day are October, while parent and child use September archive tables.
    assert [target.table for target in prepared[0].targets] == [
        "room_stats_monthly", "room_live_stats", "live_session_202609", "live_session_15m_stats_202609",
    ]
    assert report["no_bucket"] == 0
    engine.dispose()


def test_old_gift_does_not_invent_late_feature_rows():
    # Given: a July gift predates daily and 15-minute revenue collection.
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE room_stats_monthly (room_id INT,month TEXT,gift FLOAT)"))
        connection.execute(text("INSERT INTO room_stats_monthly VALUES (1,'202607',0.1)"))

    # When: the planner resolves that historical correction.
    prepared, report = prepare(engine, [correction("2026-07-31 12:00:00")])

    # Then: only the available monthly counter is eligible.
    assert [target.table for target in prepared[0].targets] == ["room_stats_monthly"]
    assert report["before_daily_feature"] == 1
    engine.dispose()


def test_duplicate_hot_and_archive_day_is_rejected_before_writing():
    # Given: the same room-day incorrectly exists in both hot and archive tables.
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE room_stats_monthly (room_id INT,month TEXT,gift FLOAT)"))
        connection.execute(text("INSERT INTO room_stats_monthly VALUES (1,'202609',0.1)"))
        for table in ("room_live_stats", "room_live_stats_202609"):
            connection.execute(text(f"CREATE TABLE {table} (room_id INT,date TEXT,gift FLOAT)"))
            connection.execute(text(f"INSERT INTO {table} VALUES (1,'2026-09-30',0.1)"))

    # When / Then: resolution refuses to choose or update both copies.
    with pytest.raises(RepairError, match="Duplicate hot/archive daily"):
        prepare(engine, [correction("2026-09-30 12:00:00")])
    engine.dispose()
