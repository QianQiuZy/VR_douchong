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
    start = datetime.datetime(2026, 8, 30, 12, 0, 0)

    persisted = session_15m.upsert_stats(
        LiveSession15mStats,
        session_id=44,
        room_id=301,
        month="202608",
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
