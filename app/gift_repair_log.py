"""Offline gift-log ingestion with rotation-aware event multiplicity."""

import datetime
import hashlib
import json
import re
import sqlite3
from collections import Counter
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Final

from app.gift_repair_types import GiftEvent, RepairError

_GIFT: Final = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - INFO - "
    r"\[(?P<room>\d+)\] .*?uid(?P<uid>\d+) 赠送 (?P<gift>.+)×(?P<quantity>\d+) "
    r"\((?P<price>-?\d+(?:\.\d+)?)\)\s*$"
)


def _parse(text: str, source: Path, line: int) -> GiftEvent | None:
    fields = _GIFT.fullmatch(text)
    if fields is None:
        return None
    try:
        when = datetime.datetime.fromisoformat(fields["ts"].replace(",", "."))
        quantity = int(fields["quantity"])
        recorded = Decimal(fields["price"])
        room_id, uid = int(fields["room"]), int(fields["uid"])
    except ValueError as exc:
        raise RepairError(f"Invalid gift fields in {source.name}:{line}") from exc
    if quantity <= 0 or recorded < 0:
        raise RepairError(f"Invalid gift quantity or price in {source.name}:{line}")
    gift = fields["gift"]
    canonical = json.dumps(
        [when.isoformat(timespec="milliseconds"), room_id, uid, gift, quantity,
         format(recorded.normalize(), "f")],
        ensure_ascii=False, separators=(",", ":"),
    )
    event_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return GiftEvent(event_id, when, room_id, uid, gift, quantity, recorded, str(source), line)


def load_events(connection: sqlite3.Connection, log_paths: Sequence[Path]) -> int:
    """Stream gifts into SQLite; return the number of byte-identical files skipped."""
    seen_files: set[bytes] = set()
    duplicates = 0
    for path in log_paths:
        source = path.resolve()
        with source.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").digest()
        if digest in seen_files:
            duplicates += 1
            continue
        seen_files.add(digest)
        occurrences: Counter[str] = Counter()
        with source.open(encoding="utf-8-sig") as stream:
            for line, text in enumerate(stream, 1):
                event = _parse(text, source, line)
                if event is None:
                    continue
                occurrences[event.event_id] += 1
                connection.execute(
                    "INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (f"{event.event_id}:{occurrences[event.event_id]}",
                     event.when.isoformat(timespec="milliseconds"), event.room_id, event.uid,
                     event.gift, event.quantity, format(event.recorded.normalize(), "f"),
                     event.source, event.line),
                )
    return duplicates
