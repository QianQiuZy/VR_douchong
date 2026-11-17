from __future__ import annotations

import datetime

from sqlalchemy.dialects import mysql

from app.models import LiveSession15mStats
from app.repositories import session_15m


def test_metric_upsert_preserves_durable_danmaku_columns(monkeypatch):
    statements = []

    class Session:
        def execute(self, statement):
            statements.append(statement)

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(session_15m, "Session", Session)
    start = datetime.datetime.now().astimezone().replace(tzinfo=None)

    persisted = session_15m.upsert_stats(
        LiveSession15mStats,
        session_id=44,
        room_id=301,
        month=start.strftime("%Y%m"),
        bucket_index=0,
        start_time=start,
        end_time=start + datetime.timedelta(minutes=15),
        gift=1.0,
        guard=2.0,
        super_chat=3.0,
        blind_box_count=4,
        blind_box_profit=5,
        danmaku_count=0,
        captain_danmaku_count=0,
        admiral_danmaku_count=0,
        governor_danmaku_count=0,
        normal_danmaku_count=0,
        avg_concurrency=6.0,
        max_concurrency=7,
        sample_count=8,
        payer_count=9,
    )

    assert persisted is True
    sql = str(statements[0].compile(dialect=mysql.dialect()))
    update_clause = sql.split("ON DUPLICATE KEY UPDATE", maxsplit=1)[1]
    assert "danmaku_count" not in update_clause


def test_late_danmaku_increments_archive_without_creating_a_hot_stub(monkeypatch):
    from types import SimpleNamespace

    from app import DanmakuBucketTarget, DanmakuCounts

    timeline = []
    connection = object()

    class Session:
        def connection(self):
            return connection

        def execute(self, statement, params):
            sql = str(statement)
            timeline.append((sql, params))
            result = 44 if sql.startswith("SELECT session_id FROM `live_session_15m_stats_200101`") else None
            return SimpleNamespace(scalar=lambda: result)

        def commit(self):
            timeline.append("commit")

        def close(self):
            timeline.append("close")

    def inspect(conn):
        assert conn is connection
        return SimpleNamespace(
            has_table=lambda name: name == "live_session_15m_stats_200101",
            get_columns=lambda _name: [
                {"name": "danmaku_count"},
                {"name": "normal_danmaku_count"},
                {"name": "gift"},
            ],
        )

    monkeypatch.setattr(session_15m, "Session", Session)
    monkeypatch.setattr(session_15m, "inspect", inspect)
    monkeypatch.setattr(session_15m, "notify_history_change", lambda month: timeline.append(month))
    start = datetime.datetime(2001, 1, 31, 23, 30)
    target = DanmakuBucketTarget(44, 301, "200101", 0, start, start + datetime.timedelta(minutes=15))
    assert session_15m.add_danmaku_counts(LiveSession15mStats, target, DanmakuCounts(total=3, normal=2, captain=1))
    parent_lock, hot_lock, archive_lock, update, *tail = timeline
    assert "FROM live_session " in parent_lock[0] and "FOR UPDATE" in parent_lock[0]
    assert "FROM live_session_15m_stats " in hot_lock[0] and "FOR UPDATE" in hot_lock[0]
    assert archive_lock[0].endswith("FOR UPDATE")
    sql, params = update
    assert sql.startswith("UPDATE `live_session_15m_stats_200101` SET ")
    assert "`danmaku_count` = COALESCE(`danmaku_count`, 0) + :danmaku_count" in sql
    assert "`normal_danmaku_count` = COALESCE(`normal_danmaku_count`, 0) + :normal_danmaku_count" in sql
    assert "gift" not in sql and "captain_danmaku_count" not in sql
    assert (params["danmaku_count"], params["normal_danmaku_count"]) == (3, 2)
    assert tail == ["commit", "200101", "close"]
