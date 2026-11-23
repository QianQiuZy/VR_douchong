"""Settle monthly danmaku snapshots and retry interrupted settlements."""

from sqlalchemy import or_, select
from sqlalchemy.dialects.mysql import insert

from . import danmaku_metrics as metrics, room_config
from .database import Session
from .models import LiveSession, RoomStatsMonthly
from .repositories.history import notify_history_change
from .repositories.tables import month_str


_registered: tuple[str, frozenset[int]] | None = None


def register_month(month: str) -> None:
    """Track known zero months even when a monitored room receives no events."""
    global _registered
    rooms = frozenset(room_config.get_room_ids())
    if _registered == (month, rooms):
        return
    if rooms:
        with Session.begin() as session:
            existing = set(
                session.scalars(
                    select(RoomStatsMonthly.room_id).where(
                        RoomStatsMonthly.month == month, RoomStatsMonthly.room_id.in_(rooms)
                    )
                )
            )
            missing = sorted(rooms - existing)
            if missing:
                count = 0 if metrics.covered(metrics.month_start(month)) else None
                stmt = insert(RoomStatsMonthly).values(
                    [
                        dict(
                            room_id=rid,
                            month=month,
                            danmaku_user_count=count,
                            emoticon_danmaku_count=0,
                            danmaku_status="collecting",
                        )
                        for rid in missing
                    ]
                )
                session.execute(stmt.on_duplicate_key_update(room_id=stmt.inserted.room_id))
    _registered = (month, rooms)


def archive_danmaku_metrics(month: str | None = None) -> None:
    current = month_str()
    since = metrics.tracking_start().strftime("%Y%m")
    if month is None:
        register_month(current)
    with Session() as session:
        rooms = session.query(RoomStatsMonthly.room_id, RoomStatsMonthly.month).filter(
            RoomStatsMonthly.month >= since,
            RoomStatsMonthly.month < current,
            or_(
                RoomStatsMonthly.danmaku_status == "collecting",
                RoomStatsMonthly.danmaku_status.is_(None),
            ),
        )
        if month:
            rooms = rooms.filter_by(month=month)
        room_keys = rooms.all()
        session_ids = session.query(LiveSession.id).filter_by(danmaku_status="pending").all()

    for rid, m in room_keys:
        _settle(RoomStatsMonthly, (rid, m), f"month:{m}:{rid}", metrics.month_start(m))
    for (sid,) in session_ids:
        _settle(LiveSession, sid, f"session:{sid}")


def _settle(model, key, scope, start=None) -> None:
    with Session.begin() as session:
        row = session.get(model, key, with_for_update=True)
        if row is None or row.danmaku_status in ("archived", "partial", "unavailable"):
            return
        result = metrics.settle(row, scope, start or row.start_time)
        if result is None:
            return
        month = row.month
    result.committed()
    notify_history_change(month)
