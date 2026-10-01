"""Resolve existing gift counters without creating historical business rows."""

import datetime
import re
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import Connection, Engine, inspect, text

from .gift_repair_types import DAILY_FEATURE_SINCE, GiftCorrection, RepairError


@dataclass(frozen=True, slots=True)
class Target:
    table: str
    keys: tuple[tuple[str, int | str], ...]


@dataclass(frozen=True, slots=True)
class PreparedCorrection:
    correction: GiftCorrection
    targets: tuple[Target, ...]


@dataclass(frozen=True, slots=True)
class SessionWindow:
    table: str
    session_id: int
    room_id: int
    start: datetime.datetime
    end: datetime.datetime | None


def predicate(target: Target) -> str:
    return " AND ".join(f"`{name}`=:{name}" for name, _ in target.keys)


def read_amount(connection: Connection, target: Target, *, lock: bool = False) -> Decimal:
    suffix = " FOR UPDATE" if lock else ""
    rows = connection.execute(
        text(f"SELECT gift FROM `{target.table}` WHERE {predicate(target)}{suffix}"), dict(target.keys)
    ).fetchall()
    if len(rows) != 1:
        raise RepairError(f"Expected exactly one existing gift row in {target.table}")
    return Decimal(str(rows[0][0]))


def prepare(engine: Engine, corrections: list[GiftCorrection]) -> tuple[list[PreparedCorrection], dict[str, int]]:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "room_stats_monthly" not in tables:
        raise RepairError("room_stats_monthly is missing")
    gift_tables = {
        name for name in tables
        if re.fullmatch(r"(?:room_stats_monthly|room_live_stats|live_session|live_session_15m_stats)(?:_\d{6})?", name)
        and "gift" in {column["name"] for column in inspector.get_columns(name)}
    }
    sessions: dict[int, list[SessionWindow]] = {}
    daily: dict[tuple[int, str], list[str]] = {}
    buckets: dict[tuple[int, int], list[str]] = {}
    monthly: set[tuple[int, str]] = set()
    with engine.connect() as connection:
        for table in sorted(gift_tables):
            if table == "room_stats_monthly":
                for room, month in connection.execute(text("SELECT room_id,month FROM room_stats_monthly")):
                    monthly.add((int(room), str(month)))
            elif re.fullmatch(r"room_live_stats(?:_\d{6})?", table):
                for room, date in connection.execute(text(f"SELECT room_id,date FROM `{table}`")):
                    daily.setdefault((int(room), str(date)), []).append(table)
            elif re.fullmatch(r"live_session_15m_stats(?:_\d{6})?", table):
                for session, bucket in connection.execute(text(f"SELECT session_id,bucket_index FROM `{table}`")):
                    buckets.setdefault((int(session), int(bucket)), []).append(table)
            elif re.fullmatch(r"live_session(?:_\d{6})?", table):
                for session, room, start, end in connection.execute(text(f"SELECT id,room_id,start_time,end_time FROM `{table}`")):
                    sessions.setdefault(int(room), []).append(SessionWindow(
                        table, int(session), int(room), datetime.datetime.fromisoformat(str(start)),
                        datetime.datetime.fromisoformat(str(end)) if end is not None else None,
                    ))
    report = {"no_daily_row": 0, "no_session": 0, "no_bucket": 0, "before_daily_feature": 0}
    prepared: list[PreparedCorrection] = []
    for correction in corrections:
        event = correction.event
        month = event.when.strftime("%Y%m")
        if (event.room_id, month) not in monthly:
            raise RepairError(f"Missing monthly row room={event.room_id} month={month}")
        targets = [Target("room_stats_monthly", (("room_id", event.room_id), ("month", month)))]
        modern = event.when.date() >= DAILY_FEATURE_SINCE
        if modern:
            names = daily.get((event.room_id, event.when.date().isoformat()), [])
            if len(names) > 1:
                raise RepairError("Duplicate hot/archive daily row; resolve before repairing")
            if names:
                targets.append(Target(names[0], (("room_id", event.room_id), ("date", event.when.date().isoformat()))))
            else:
                report["no_daily_row"] += 1
        else:
            report["before_daily_feature"] += 1
        when = event.when.replace(microsecond=0)
        matches = [window for window in sessions.get(event.room_id, []) if window.start <= when and (window.end is None or when <= window.end)]
        if len(matches) > 1:
            raise RepairError("Ambiguous session window; resolve duplicate/overlapping sessions first")
        if matches:
            window = matches[0]
            targets.append(Target(window.table, (("id", window.session_id),)))
            if modern:
                index = int((when - window.start).total_seconds()) // 900
                names = buckets.get((window.session_id, index), [])
                if len(names) > 1:
                    raise RepairError("Duplicate hot/archive 15-minute bucket")
                if names:
                    targets.append(Target(names[0], (("session_id", window.session_id), ("bucket_index", index))))
                else:
                    report["no_bucket"] += 1
        else:
            report["no_session"] += 1
        prepared.append(PreparedCorrection(correction, tuple(targets)))
    return prepared, report
