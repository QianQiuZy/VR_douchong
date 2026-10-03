"""Account for broadcast segments within a grace-merged live session."""

import datetime
import json
import logging
from copy import deepcopy

from sqlalchemy import MetaData, select, update
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session
from .tables import ensure_room_live_stats_archive_table, is_current_month, month_str

logger = logging.getLogger(__name__)


def _duration_table(day: datetime.date) -> str:
    month = month_str(datetime.datetime.combine(day, datetime.time.min))
    return "room_live_stats" if is_current_month(month) else ensure_room_live_stats_archive_table(month)


def _stats_table(name: str):
    from ..models import RoomLiveStats

    return RoomLiveStats.__table__.to_metadata(MetaData(), name=name)


def _deduct_day(session, table, room_id: int, day: datetime.date, seconds: int) -> int:
    condition = (table.c.room_id == room_id) & (table.c.date == day)
    duration = session.execute(select(table.c.duration).where(condition).with_for_update()).scalar()
    deducted = min(seconds, int(duration or 0))
    if deducted:
        session.execute(update(table).where(condition).values(duration=table.c.duration - deducted))
    return seconds - deducted


def _adjust_day(session, room_id: int, day: datetime.date, seconds: int, source: str | None = None) -> str:
    if seconds > 0:
        name = _duration_table(day)
        table = _stats_table(name)
        condition = (table.c.room_id == room_id) & (table.c.date == day)
        result = session.execute(update(table).where(condition).values(duration=table.c.duration + seconds))
        if not result.rowcount:
            session.execute(table.insert().values(room_id=room_id, date=day, duration=seconds))
        return name
    else:
        # A month rollover may have moved an earlier contribution to its archive.
        if source is None:
            raise ValueError("duration revocation requires its source table")
        remaining = _deduct_day(session, _stats_table(source), room_id, day, -seconds)
        if remaining and source == "room_live_stats" and not is_current_month(day.strftime("%Y%m")):
            _deduct_day(session, _stats_table(_duration_table(day)), room_id, day, remaining)
        return source


def account_duration(model, session_id: int, start: datetime.datetime | None = None,
                     end: datetime.datetime | None = None, *, invalidate: bool = False,
                     begin: bool = False, event_time: datetime.datetime | None = None) -> bool:
    """Keep the validity flag, contribution ledger and daily totals in one transaction.

    The end cursor avoids recounting a recorded segment after process recovery.
    ``days`` holds all retained contributions; ``segment_days`` holds only the
    current broadcast's contributions. A grace resume starts a fresh segment,
    so a later warning cannot revoke earlier healthy broadcasts in the same ID.
    """
    for attempt in range(3):
        session = Session()
        try:
            row = session.query(model).filter_by(id=session_id).with_for_update().one()
            ledger = json.loads(row.duration_ledger or '{"days": {}}')
            days = ledger["days"]
            segment_days = ledger.setdefault("segment_days", deepcopy(days))
            ledger.setdefault("segment_start", row.start_time.isoformat())
            if invalidate and event_time is not None and event_time < datetime.datetime.fromisoformat(ledger["segment_start"]):
                return False
            changed_months = set()
            if begin:
                if start is None:
                    raise ValueError("a broadcast segment requires a start time")
                ledger["segment_start"] = start.isoformat()
                ledger.pop("segment_end", None)
                segment_days.clear()
                row.duration_valid = 1
            elif invalidate:
                for day, contributions in segment_days.items():
                    for source, seconds in contributions.items():
                        _adjust_day(session, row.room_id, datetime.date.fromisoformat(day), -seconds, source)
                        days[day][source] -= seconds
                        if days[day][source] == 0:
                            del days[day][source]
                    if not days[day]:
                        del days[day]
                    changed_months.add(day[:7].replace("-", ""))
                segment_days.clear()
                row.duration_valid = 0
            elif end is not None:
                if row.duration_valid and start is not None:
                    current = max(start, datetime.datetime.fromisoformat(ledger["segment_start"]),
                                  datetime.datetime.fromisoformat(ledger.get("end", start.isoformat())))
                    while current < end:
                        boundary = min(end, datetime.datetime.combine(current.date() + datetime.timedelta(days=1), datetime.time.min))
                        seconds = int((boundary - current).total_seconds())
                        if seconds > 0:
                            source = _adjust_day(session, row.room_id, current.date(), seconds)
                            day = current.date().isoformat()
                            for totals in (days, segment_days):
                                contributions = totals.setdefault(day, {})
                                contributions[source] = contributions.get(source, 0) + seconds
                            changed_months.add(current.strftime("%Y%m"))
                        current = boundary
                ledger["end"] = max(end, datetime.datetime.fromisoformat(ledger.get("end", end.isoformat()))).isoformat()
                ledger["segment_end"] = end.isoformat()
            row.duration_ledger = json.dumps(ledger)
            session.commit()
            from ..api_cache import invalidate_history

            for month in changed_months:
                if not is_current_month(month):
                    invalidate_history(month)
            return True
        except SQLAlchemyError as exc:
            session.rollback()
            logger.warning("[SessionDuration] write failed session_id=%s attempt=%s error_type=%s",
                           session_id, attempt + 1, type(exc).__name__)
            if attempt == 2:
                raise
        finally:
            session.close()
