"""Offline audit first; explicit, resumable MySQL/Redis repair second."""

import argparse
import json
import os
import re
from collections import defaultdict
from pathlib import Path

from redis import Redis, RedisError
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

from .gift_repair_database import LEDGER, apply_sql, mark_redis_done, pending_redis
from .gift_repair_redis import apply_whale, refresh_metrics, verify_whale
from .gift_repair_targets import prepare
from .gift_repair_types import WHALE_FIRST_MONTH, RepairError


def main() -> int:
    parser = argparse.ArgumentParser(description="V2 gift repair: default is an offline audit, zero prices are FREE")
    parser.add_argument("--log-dir", type=Path, help="Directory containing historical *.log files")
    parser.add_argument("--logs", nargs="+", type=Path, help="Explicit log files")
    parser.add_argument("--audit-db", type=Path, required=True, help="Private, immutable SQLite audit ledger")
    parser.add_argument("--verify", action="store_true", help="Read-only checks against configured MySQL/Redis")
    parser.add_argument("--apply", action="store_true", help="Apply eligible corrections once")
    parser.add_argument("--maintenance-confirmed", action="store_true", help="Collector stopped and MySQL/Redis backups taken")
    parser.add_argument("--allow-missing-baselines", action="store_true", help="Explicitly skip unresolved gifts listed in audit report")
    parser.add_argument("--env-file", type=Path, help="Runtime configuration file, loaded ONLY with --verify/--apply")
    parser.add_argument("--repair-id", default="v2-gift-20261001", help="Human-readable label; event dedupe is independent of this label")
    args = parser.parse_args()
    if args.apply and not args.maintenance_confirmed:
        parser.error("--apply requires --maintenance-confirmed")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", args.repair_id):
        parser.error("invalid --repair-id")
    from .gift_repair_audit import audit, iter_corrections, read_summary

    paths = args.logs or (sorted(args.log_dir.glob("*.log")) if args.log_dir else [])
    if args.audit_db.exists():
        summary = read_summary(args.audit_db)
    else:
        if not paths:
            parser.error("provide --log-dir or --logs for the first offline audit")
        args.audit_db.parent.mkdir(parents=True, exist_ok=True)
        summary = audit(paths, args.audit_db)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not args.verify and not args.apply:
        return 0
    if summary["missing_baselines"] and not args.allow_missing_baselines:
        raise RepairError("Missing paid ×1 baselines: review the missing report; no live changes made")
    if args.env_file:
        os.environ["ENV_FILE"] = str(args.env_file.resolve())
    from .config import DB_CONFIG, REDIS_URL
    from .whale_metrics import _redis_db1_url

    url = URL.create("mysql+pymysql", username=str(DB_CONFIG["user"]), password=str(DB_CONFIG["password"]),
                     host=str(DB_CONFIG["host"]), port=int(DB_CONFIG["port"]), database=str(DB_CONFIG["db"]))
    engine = create_engine(url, pool_pre_ping=True)
    client = Redis.from_url(_redis_db1_url(REDIS_URL), decode_responses=True, socket_connect_timeout=10, socket_timeout=30)
    try:
        corrections = list(iter_corrections(args.audit_db))
        prepared, target_report = prepare(engine, corrections)
        groups: dict[tuple[int, str, int], list[tuple[str, int]]] = defaultdict(list)
        rooms: set[tuple[int, str]] = set()
        for correction in corrections:
            event = correction.event
            month = event.when.strftime("%Y%m")
            if month >= WHALE_FIRST_MONTH:
                groups[(event.room_id, month, event.uid)].append((event.event_id, int(correction.delta * 1000)))
                rooms.add((event.room_id, month))
        if inspect(engine).has_table(LEDGER):
            groups.update(pending_redis(engine))
        whale_delta = sum(verify_whale(client, group, events) for group, events in groups.items())
        print(json.dumps({"targets": target_report, "whale_delta_gold": whale_delta, "whale_groups": len(groups)}, ensure_ascii=False))
        if not args.apply:
            print("Read-only verification finished; no SQL or Redis writes performed")
            return 0
        sql_events = apply_sql(engine, prepared, args.repair_id)
        redis_gold = 0
        for group, events in pending_redis(engine).items():
            redis_gold += apply_whale(client, group, events)
            mark_redis_done(engine, [event for event, _ in events])
            rooms.add((group[0], group[1]))
        refreshed = refresh_metrics(engine, client, rooms)
        print(json.dumps({"sql_events_applied": sql_events, "redis_gold_applied": redis_gold,
                          "whale_archives_refreshed": refreshed}, ensure_ascii=False))
        return 0
    finally:
        client.close()
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RepairError as error:
        raise SystemExit(f"Repair stopped safely: {error.reason}") from None
    except (SQLAlchemyError, RedisError, OSError) as error:
        raise SystemExit(f"Repair stopped on {type(error).__name__}; fix connectivity/configuration and rerun the same audit") from None
