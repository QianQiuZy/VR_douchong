"""Transactional gift deltas and a durable, cross-run repair ledger."""

import json
from collections import defaultdict
from decimal import Decimal

from sqlalchemy import Engine, text

from .gift_repair_targets import PreparedCorrection, Target, predicate, read_amount
from .gift_repair_types import WHALE_FIRST_MONTH, RepairError

LEDGER = "gift_v2_repair_ledger"


def apply_sql(engine: Engine, prepared: list[PreparedCorrection], repair_id: str) -> int:
    with engine.begin() as connection:
        connection.execute(text(f"""CREATE TABLE IF NOT EXISTS `{LEDGER}` (
            event_id VARCHAR(128) PRIMARY KEY, repair_id VARCHAR(128) NOT NULL,
            room_id BIGINT NOT NULL, month CHAR(6) NOT NULL, uid BIGINT NOT NULL,
            event_time DATETIME(3) NOT NULL, recorded DECIMAL(20,3) NOT NULL,
            unit_price DECIMAL(20,3) NOT NULL, delta DECIMAL(20,3) NOT NULL,
            source TEXT NOT NULL, line_number INT NOT NULL, targets_json LONGTEXT NOT NULL,
            redis_done TINYINT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB"""))
    grouped: dict[tuple[int, str], list[PreparedCorrection]] = defaultdict(list)
    for item in prepared:
        event = item.correction.event
        grouped[(event.room_id, event.when.strftime("%Y%m"))].append(item)
    applied = 0
    for items in grouped.values():
        with engine.begin() as connection:
            existing: dict[str, Decimal] = {}
            for offset in range(0, len(items), 500):
                ids = {f"id{i}": item.correction.event.event_id for i, item in enumerate(items[offset:offset + 500])}
                placeholders = ",".join(f":{key}" for key in ids)
                rows = connection.execute(text(f"SELECT event_id,delta FROM `{LEDGER}` WHERE event_id IN ({placeholders})"), ids)
                existing.update((str(event_id), Decimal(str(delta))) for event_id, delta in rows)
            updates: dict[Target, Decimal] = defaultdict(Decimal)
            before_values: dict[Target, Decimal] = {}
            new_rows = []
            for item in items:
                correction = item.correction
                event = correction.event
                if event.event_id in existing:
                    if existing[event.event_id] != correction.delta:
                        raise RepairError("Previously repaired event has a different correction amount")
                    continue
                snapshots = []
                for target in item.targets:
                    if target not in before_values:
                        before_values[target] = read_amount(connection, target, lock=True)
                    before = before_values[target]
                    snapshots.append({"table": target.table, "keys": dict(target.keys), "gift_before": str(before)})
                    updates[target] += correction.delta
                new_rows.append({
                    "event_id": event.event_id, "repair_id": repair_id,
                    "room_id": event.room_id, "month": event.when.strftime("%Y%m"), "uid": event.uid,
                    "event_time": event.when, "recorded": str(event.recorded), "unit": str(correction.unit),
                    "delta": str(correction.delta), "source": event.source, "line": event.line,
                    "targets": json.dumps(snapshots, ensure_ascii=False),
                    "redis_done": int(event.when.strftime("%Y%m") < WHALE_FIRST_MONTH),
                })
            for offset in range(0, len(new_rows), 500):
                connection.execute(text(f"""INSERT INTO `{LEDGER}`
                    (event_id,repair_id,room_id,month,uid,event_time,recorded,unit_price,delta,source,line_number,targets_json,redis_done)
                    VALUES (:event_id,:repair_id,:room_id,:month,:uid,:event_time,:recorded,:unit,:delta,:source,:line,:targets,:redis_done)"""), new_rows[offset:offset + 500])
            for target, delta in updates.items():
                result = connection.execute(
                    text(f"UPDATE `{target.table}` SET gift=gift+:delta WHERE {predicate(target)}"),
                    {**dict(target.keys), "delta": str(delta)},
                )
                if result.rowcount != 1:
                    raise RepairError("A planned gift target disappeared during the repair")
            applied += len(new_rows)
    return applied


def pending_redis(engine: Engine) -> dict[tuple[int, str, int], list[tuple[str, int]]]:
    pending: dict[tuple[int, str, int], list[tuple[str, int]]] = defaultdict(list)
    with engine.connect() as connection:
        for event_id, room, month, uid, delta in connection.execute(text(
            f"SELECT event_id,room_id,month,uid,delta FROM `{LEDGER}` WHERE redis_done=0 ORDER BY event_id"
        )):
            pending[(int(room), str(month), int(uid))].append((str(event_id), int(Decimal(str(delta)) * 1000)))
    return pending


def mark_redis_done(engine: Engine, event_ids: list[str]) -> None:
    with engine.begin() as connection:
        for offset in range(0, len(event_ids), 500):
            ids = {f"id{i}": value for i, value in enumerate(event_ids[offset:offset + 500])}
            placeholders = ",".join(f":{key}" for key in ids)
            connection.execute(text(f"UPDATE `{LEDGER}` SET redis_done=1 WHERE event_id IN ({placeholders})"), ids)
