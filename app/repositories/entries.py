"""Idempotent batched writes and bounded, transactional monthly entry archive."""

import datetime
import logging

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session, engine
from .tables import month_range, month_str, normalize_month_code

logger = logging.getLogger(__name__)
ARCHIVE_BATCH_SIZE = 1000


def save_entries(rows: list[dict]) -> None:
    from ..models import RoomEntryLog

    if not rows:
        return
    with Session.begin() as session:
        if session.get_bind().dialect.name == "sqlite":
            statement = sqlite_insert(RoomEntryLog).on_conflict_do_nothing()
        else:
            statement = mysql_insert(RoomEntryLog)
            statement = statement.on_duplicate_key_update(uid=statement.inserted.uid)
        session.execute(statement, rows)
    # Late events change historical query results even before the next archive pass.
    from ..api_cache import invalidate_history

    for month in {month_str(row["event_time"]) for row in rows}:
        if month < month_str():
            invalidate_history(month)


def archive_entries(target_month: str | None = None) -> int:
    current = month_str()
    cutoff = datetime.datetime.combine(month_range(current)[0], datetime.time.min)
    normalized = normalize_month_code(target_month) if target_month else None
    if target_month and (normalized is None or normalized >= current):
        return 0
    moved = 0
    try:
        if normalized:
            months = [normalized]
        else:
            with Session() as session:
                months = (
                    session.execute(
                        text(
                            "SELECT DISTINCT DATE_FORMAT(event_time, '%Y%m') FROM room_entry_log WHERE event_time < :cutoff"
                        ),
                        {"cutoff": cutoff},
                    )
                    .scalars()
                    .all()
                )
        for month in sorted(months):
            # Table identifiers are constructed exclusively from validated month codes.
            if normalize_month_code(month) != month or month >= current:
                continue
            table = f"room_entry_log_{month}"
            start, end = month_range(month)
            # DDL must finish before the copy/delete transaction begins.
            with engine.begin() as connection:
                connection.execute(
                    text(f"CREATE TABLE IF NOT EXISTS `{table}` LIKE `room_entry_log`")
                )
            while True:
                with engine.begin() as connection:
                    rows = (
                        connection.execute(
                            text(
                                "SELECT room_id, uid, event_time FROM room_entry_log "
                                "WHERE event_time >= :start AND event_time < :end "
                                "ORDER BY event_time, room_id, uid LIMIT :limit FOR UPDATE"
                            ),
                            {"start": start, "end": end, "limit": ARCHIVE_BATCH_SIZE},
                        )
                        .mappings()
                        .all()
                    )
                    if not rows:
                        break
                    connection.execute(
                        text(
                            f"INSERT INTO `{table}` (room_id, uid, event_time) "
                            "VALUES (:room_id, :uid, :event_time) "
                            "ON DUPLICATE KEY UPDATE uid=VALUES(uid)"
                        ),
                        [dict(row) for row in rows],
                    )
                    connection.execute(
                        text(
                            "DELETE FROM room_entry_log WHERE (room_id, event_time, uid) IN :keys"
                        ).bindparams(bindparam("keys", expanding=True)),
                        {
                            "keys": [
                                (row["room_id"], row["event_time"], row["uid"])
                                for row in rows
                            ]
                        },
                    )
                moved += len(rows)
        return moved
    except SQLAlchemyError as exc:
        logger.error("[entry] archive failed error_type=%s", type(exc).__name__)
        return moved
