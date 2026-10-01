"""Private, immutable, offline audit of unit-priced gift batches."""

import csv
import datetime
import json
import os
import sqlite3
from collections import Counter
from collections.abc import Generator, Iterator, Sequence
from contextlib import closing, contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Final, TextIO

from app.gift_repair_log import load_events
from app.gift_repair_types import AuditSummary, GiftCorrection, GiftEvent, RepairError

_SCHEMA: Final = """
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32768;
PRAGMA temp_store = FILE;
CREATE TABLE events (
    event_id TEXT PRIMARY KEY, ts TEXT NOT NULL, room_id INTEGER NOT NULL,
    uid INTEGER NOT NULL, gift TEXT NOT NULL, quantity INTEGER NOT NULL,
    recorded TEXT NOT NULL, source TEXT NOT NULL, line INTEGER NOT NULL
);
CREATE TABLE corrections (
    event_id TEXT PRIMARY KEY REFERENCES events(event_id), unit TEXT NOT NULL,
    delta TEXT NOT NULL, baseline_source TEXT NOT NULL, baseline_line INTEGER NOT NULL
);
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
_INDEXES: Final = """
CREATE INDEX paid_room_baselines ON events(room_id, gift, ts)
    WHERE quantity = 1 AND recorded <> '0';
CREATE INDEX paid_global_baselines ON events(gift, ts)
    WHERE quantity = 1 AND recorded <> '0';
CREATE INDEX gift_batches ON events(quantity) WHERE quantity > 1;
"""


@contextmanager
def _private_text(path: Path) -> Generator[TextIO]:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
        os.fchmod(stream.fileno(), 0o600)
        yield stream


def _event(row: sqlite3.Row) -> GiftEvent:
    return GiftEvent(
        event_id=str(row["event_id"]), when=datetime.datetime.fromisoformat(row["ts"]),
        room_id=int(row["room_id"]), uid=int(row["uid"]), gift=str(row["gift"]),
        quantity=int(row["quantity"]), recorded=Decimal(row["recorded"]),
        source=str(row["source"]), line=int(row["line"]),
    )


def _nearest(
    connection: sqlite3.Connection, event: GiftEvent, same_room: bool,
) -> list[GiftEvent]:
    criteria = "quantity = 1 AND recorded <> '0' AND gift = ?"
    parameters: list[str | int] = [event.gift]
    if same_room:
        criteria += " AND room_id = ?"
        parameters.append(event.room_id)
    timestamp = event.when.isoformat(timespec="milliseconds")
    candidates: list[GiftEvent] = []
    for operator, direction in [("<=", "DESC"), (">=", "ASC")]:
        row = connection.execute(
            f"SELECT * FROM events WHERE {criteria} AND ts {operator} ? "
            f"ORDER BY ts {direction} LIMIT 1", (*parameters, timestamp),
        ).fetchone()
        if row is not None:
            candidates.append(_event(row))
    if not candidates:
        return []
    distance = min(abs(candidate.when - event.when) for candidate in candidates)
    prices: dict[Decimal, GiftEvent] = {}
    for candidate in candidates:
        if abs(candidate.when - event.when) != distance:
            continue
        for row in connection.execute(
            f"SELECT * FROM events WHERE {criteria} AND ts = ? ORDER BY source, line",
            (*parameters, candidate.when.isoformat(timespec="milliseconds")),
        ):
            baseline = _event(row)
            prices.setdefault(baseline.recorded, baseline)
    return list(prices.values())


def _classify(connection: sqlite3.Connection) -> tuple[AuditSummary, Counter[str]]:
    bounds = connection.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM events").fetchone()
    summary: AuditSummary = {
        "files": 0, "duplicate_files": 0, "gifts": int(bounds[0]), "paid_batches": 0,
        "free_batches": 0, "corrections": 0, "delta_rmb": "0.00", "already_total": 0,
        "missing_baselines": 0, "missing_gifts": [], "other_prices": 0,
        "first_timestamp": bounds[1] or "", "last_timestamp": bounds[2] or "",
    }
    missing: Counter[str] = Counter()
    total = Decimal(0)
    for row in connection.execute("SELECT * FROM events WHERE quantity > 1"):
        event = _event(row)
        if event.recorded == 0:
            summary["free_batches"] += 1
            continue
        summary["paid_batches"] += 1
        baselines = _nearest(connection, event, True) or _nearest(connection, event, False)
        if not baselines:
            missing[event.gift] += 1
            summary["missing_baselines"] += 1
            continue
        if len(baselines) > 1:
            summary["other_prices"] += 1
            continue
        baseline = baselines[0]
        unit = baseline.recorded
        if event.recorded == unit:
            delta = (event.quantity - 1) * unit
            connection.execute(
                "INSERT INTO corrections VALUES (?, ?, ?, ?, ?)",
                (event.event_id, format(unit, "f"), format(delta, "f"),
                 baseline.source, baseline.line),
            )
            summary["corrections"] += 1
            total += delta
        elif event.recorded == event.quantity * unit:
            summary["already_total"] += 1
        else:
            summary["other_prices"] += 1
    summary["missing_gifts"] = sorted(missing)
    summary["delta_rmb"] = format(total, ".2f")
    return summary, missing


def audit(log_paths: Sequence[Path], audit_path: Path) -> AuditSummary:
    """Audit logs without production imports; refuse to overwrite any existing artifact."""
    try:
        with _private_text(audit_path), closing(sqlite3.connect(audit_path)) as connection:
            connection.row_factory = sqlite3.Row
            connection.executescript(_SCHEMA)
            duplicates = load_events(connection, log_paths)
            connection.executescript(_INDEXES)
            with connection:
                summary, missing = _classify(connection)
                summary["files"], summary["duplicate_files"] = len(log_paths), duplicates
                payload = json.dumps(summary, ensure_ascii=False, indent=2)
                with _private_text(audit_path.with_suffix(".summary.json")) as report:
                    report.write(payload + "\n")
                with _private_text(audit_path.with_suffix(".missing.csv")) as report:
                    writer = csv.writer(report)
                    writer.writerow(["gift", "count"])
                    writer.writerows(sorted(missing.items()))
                connection.execute("INSERT INTO metadata VALUES ('summary', ?)", (payload,))
        return summary
    except (OSError, sqlite3.Error, UnicodeError) as exc:
        raise RepairError(f"Offline audit failed; no existing files were replaced: {exc}") from exc


def read_summary(audit_path: Path) -> AuditSummary:
    """Read the completed audit's JSON metadata through a read-only connection."""
    try:
        uri = audit_path.resolve().as_uri() + "?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            row = connection.execute("SELECT value FROM metadata WHERE key = 'summary'").fetchone()
            if row is None:
                raise RepairError("Audit has no completed summary")
            summary: AuditSummary = json.loads(row[0])
            return summary
    except (OSError, sqlite3.Error, json.JSONDecodeError) as exc:
        raise RepairError(f"Cannot read completed audit {audit_path.name}") from exc


def iter_corrections(audit_path: Path) -> Iterator[GiftCorrection]:
    """Stream typed corrections chronologically without modifying the private audit."""
    read_summary(audit_path)
    try:
        uri = audit_path.resolve().as_uri() + "?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            for row in connection.execute(
                "SELECT e.*, c.unit, c.delta, c.baseline_source, c.baseline_line "
                "FROM corrections c JOIN events e USING (event_id) ORDER BY e.ts, e.event_id",
            ):
                yield GiftCorrection(
                    event=_event(row), unit=Decimal(row["unit"]), delta=Decimal(row["delta"]),
                    baseline_source=str(row["baseline_source"]), baseline_line=int(row["baseline_line"]),
                )
    except (OSError, sqlite3.Error) as exc:
        raise RepairError(f"Cannot read corrections from {audit_path.name}") from exc
