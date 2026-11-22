"""Bulk snapshot presentation, using the same scalar helpers as legacy reports."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from types import SimpleNamespace

from . import danmaku_metrics, room_config, rounded_concurrency, runtime_state
from .api_cache_http import LIVE_SESSIONS_PATH, LIVE_SESSIONS_WITH_STATS_CACHE_PATH
from .api_cache_source import Snapshot
from .api_cache_store import encode
from .repositories.tables import month_str


def daily_aggregates(rows: list[dict]) -> dict[int, tuple[int, int, int]]:
    result = {}
    for row in rows:
        rid = int(row["room_id"])
        duration, days, steel = result.get(rid, (0, 0, 0))
        seconds = int(row.get("duration") or 0)
        result[rid] = (
            duration + seconds,
            days + int(seconds >= 7200),
            steel + int(row.get("steel_coin_count") or 0),
        )
    return result


def summaries(
    month: str,
    base: dict[str, list[dict]],
    aggregates: dict,
    session_rooms: set[int],
    *,
    empty: bool = False,
) -> list[dict]:
    from . import api_app

    current = month == month_str() and not empty
    info = {int(row["room_id"]): row for row in base["room_info"]}
    monthly = {
        int(row["room_id"]): row
        for row in base["room_stats_monthly"]
        if row["month"] == month and not empty
    }
    blind = {
        int(row["room_id"]): row
        for row in base["room_blind_box_monthly"]
        if row["month"] == month and not empty
    }
    rooms = (
        set(room_config.get_room_ids()) | set(monthly) | set(aggregates) | session_rooms
    )
    out = []
    for rid in sorted(rooms):
        row = monthly.get(rid, {})
        bb = blind.get(rid, {})
        room = info.get(rid, {})
        duration, days, steel = aggregates.get(rid, (0, 0, 0))
        live = runtime_state.LIVE_INFO.get(rid, {}).copy() if current else {}
        guards = (
            (runtime_state.GUARD_COUNTS.get(rid, {}) or {}).copy() if current else {}
        )
        status = runtime_state.LAST_STATUS.get(rid, 0) if current else 0
        concurrency = None
        cache = (runtime_state.CONCURRENCY_CACHE.get(rid) or {}).copy()
        if (
            current
            and status == 1
            and cache.get("session_id") == runtime_state.CURRENT_SESSIONS.get(rid)
            and int(cache.get("samples", 0)) > 0
        ):
            concurrency = int(cache.get("last", 0))
        value = {
            "room_id": rid,
            "anchor_name": room.get("anchor_name")
            or room_config.get_room_anchor_name(rid),
            "attention": room.get("attention") or 0,
            "status": status,
            "gift": row.get("gift") or 0.0,
            "guard": row.get("guard") or 0.0,
            "super_chat": row.get("super_chat") or 0.0,
            "payer_count": row.get("payer_count") or 0,
            "steel_coin_count": steel,
            "blind_box_count": bb.get("blind_box_count") or 0,
            "blind_box_profit": api_app._profit_display(bb.get("blind_box_profit")),
            "live_duration": api_app._seconds_to_hms(duration),
            "effective_days": days,
            "live_time": live.get("live_time", "0000-00-00 00:00:00"),
            "title": live.get("title", ""),
            "month": month,
            **{
                name: guards.get(name, 0) if current else None
                for name in ("guard_1", "guard_2", "guard_3")
            },
            "fans_count": runtime_state.FANS_COUNT.get(rid, 0) if current else None,
            "danmaku": api_app._monthly_danmaku_payload(
                SimpleNamespace(**row) if row else None
            ),
            **danmaku_metrics.monthly_report(SimpleNamespace(**row), rid, month, current=current, empty=empty),
            "whale_dependency": (
                {
                    "status": "unavailable",
                    "source": "redis",
                    "top1": None,
                    "top5": None,
                    "top10": None,
                    "top1_percent": None,
                }
                if empty
                else api_app._whale_dependency_for_room(
                    SimpleNamespace(**row) if row else None, rid, month
                )
            ),
        }
        if current:
            value["current_concurrency"] = concurrency
        out.append(value)
    return out


def year_summaries(year: str, base: dict, inputs: dict, *, empty: bool = False) -> list[dict]:
    """Roll up the existing monthly totals and cached daily aggregates, without SQL."""
    from . import api_app

    current_month = month_str()
    current = year == current_month[:4] and not empty
    cutoff = current_month if current else year + "12"

    def selected(month):
        return not empty and month[:4] == year and month <= cutoff

    info = {int(row["room_id"]): row for row in base["room_info"]}
    monthly = defaultdict(dict)
    blind = defaultdict(list)
    aggregates = defaultdict(lambda: [0, 0])
    active = defaultdict(set)
    rooms = set(room_config.get_room_ids())
    for row in base["room_stats_monthly"]:
        if selected(row["month"]):
            rid = int(row["room_id"])
            monthly[rid][row["month"]] = row
            active[rid].add(row["month"])
            rooms.add(rid)
    for row in base["room_blind_box_monthly"]:
        if selected(row["month"]):
            rid = int(row["room_id"])
            blind[rid].append(row)
            active[rid].add(row["month"])
            rooms.add(rid)
    for month, (daily, session_rooms) in inputs.items():
        if not selected(month):
            continue
        for rid, (seconds, days, _steel) in daily.items():
            aggregates[rid][0] += seconds
            aggregates[rid][1] += days
        for rid in daily.keys() | session_rooms:
            active[rid].add(month)
            rooms.add(rid)
    out = []
    for rid in sorted(rooms):
        rows = list(monthly[rid].values())
        room = info.get(rid, {})
        live = runtime_state.LIVE_INFO.get(rid, {}).copy() if current else {}
        guards = (runtime_state.GUARD_COUNTS.get(rid, {}) or {}).copy() if current else {}
        danmaku = {}
        for name, column in (
            ("total", "danmaku_count"), ("captain", "captain_danmaku_count"),
            ("admiral", "admiral_danmaku_count"), ("governor", "governor_danmaku_count"),
            ("normal", "normal_danmaku_count"),
        ):
            counts = [monthly[rid].get(month, {}).get(column) for month in active[rid]]
            danmaku[name] = (
                sum(int(value) for value in counts)
                if counts and all(value is not None for value in counts) else None
            )
        seconds, days = aggregates[rid]
        out.append({
            "room_id": rid,
            "anchor_name": room.get("anchor_name") or room_config.get_room_anchor_name(rid),
            "attention": room.get("attention") or 0,
            "status": runtime_state.LAST_STATUS.get(rid, 0) if current else 0,
            **{
                field: float(sum(Decimal(str(row.get(field) or 0)) for row in rows))
                for field in ("gift", "guard", "super_chat")
            },
            "payer_count": None,
            "steel_coin_count": None,
            "blind_box_count": sum(int(row.get("blind_box_count") or 0) for row in blind[rid]),
            "blind_box_profit": float(sum(
                Decimal(str(api_app._profit_display(row.get("blind_box_profit"))))
                for row in blind[rid]
            )),
            "live_duration": api_app._seconds_to_hms(seconds),
            "effective_days": days,
            "live_time": live.get("live_time", "0000-00-00 00:00:00"),
            "title": live.get("title", ""),
            "year": year,
            **{
                field: guards.get(field, 0) if current else None
                for field in ("guard_1", "guard_2", "guard_3")
            },
            "fans_count": runtime_state.FANS_COUNT.get(rid, 0) if current else None,
            "danmaku": danmaku,
            "whale_dependency": None,
            "danmaku_user_count": None,
            "danmaku_dependency": None,
        })
    return out


class PayloadBuilder:
    def __init__(self):
        self.previous_sc: dict[int, list[dict]] = {}
        self.sc_bodies: dict[int, bytes] = {}
        self.current_month: str | None = None
        self.previous_payloads: dict[str, dict] = {}
        self.payload_bodies: dict[str, bytes] = {}
        self.summary_inputs: dict[str, tuple[dict, set[int]]] = {}

    def year_values(self, year: str, base: dict) -> dict[str, bytes]:
        return {
            "/gift/by_month": encode(year_summaries(year, base, self.summary_inputs)),
            "_empty:/gift/by_month": encode(year_summaries(year, base, {}, empty=True)),
        }

    def summary_values(
        self, month: str, base: dict, aggregates: dict, rooms: set[int]
    ) -> dict[str, bytes]:
        rows = summaries(month, base, aggregates, rooms)
        values = {
            "/gift/by_month": encode(
                [
                    {k: v for k, v in row.items() if k != "current_concurrency"}
                    for row in rows
                ]
            )
        }
        if month == month_str():
            values["/gift"] = encode(rows)
        return values

    def body(self, field: str, payload: dict, current: bool) -> bytes:
        if current and self.previous_payloads.get(field) == payload:
            return self.payload_bodies[field]
        value = encode(payload)
        if current:
            self.previous_payloads[field] = payload
            self.payload_bodies[field] = value
        return value

    def build(self, snapshot: Snapshot) -> dict[str, bytes]:
        from . import api_app

        month = snapshot.month
        current = month == month_str()
        aggregates = daily_aggregates(snapshot.tables["room_live_stats"])
        session_rooms = {int(row["room_id"]) for row in snapshot.tables["live_session"]}
        values = self.summary_values(month, snapshot.base, aggregates, session_rooms)
        values[api_app.ENTRY_PATH] = encode(
            {"items": api_app._serialize_entry_rows(snapshot.tables.get("room_entry_log", []))}
        )
        values[f"_empty:{api_app.ENTRY_PATH}"] = encode({"items": []})
        if current:
            values[api_app.RECENT_PATH] = encode(
                {"items": api_app._serialize_entry_rows(snapshot.recent_entries)}
            )
        self.summary_inputs[month] = (aggregates, session_rooms)
        if current and self.current_month != month:
            self.previous_sc.clear()
            self.sc_bodies.clear()
            self.previous_payloads.clear()
            self.payload_bodies.clear()
            self.current_month = month
        groups: dict[str, dict[int, list[dict]]] = {}
        rooms = set(room_config.get_room_ids())
        for table, rows in snapshot.tables.items():
            if table == "room_entry_log":
                continue  # Entry-only rooms must not change existing report domains.
            grouped = defaultdict(list)
            for row in rows:
                grouped[int(row["room_id"])].append(row)
            groups[table] = grouped
            rooms.update(grouped)
        for rows in snapshot.base.values():
            rooms.update(int(row["room_id"]) for row in rows)
        buckets = defaultdict(list)
        for row in snapshot.tables["live_session_15m_stats"]:
            buckets[int(row["session_id"])].append(row)
        for rid in sorted(rooms):
            values[f"_room:{rid}"] = b"1"
            daily_metrics = {
                str(row["date"]): row for row in groups["room_live_stats"].get(rid, [])
            }
            attention = {
                str(row["date"]): row for row in groups["attention"].get(rid, [])
            }
            daily = []
            for date in sorted(daily_metrics.keys() | attention.keys()):
                row = attention.get(date, {})
                metrics = daily_metrics.get(date, {})
                daily.append(
                    {
                        "date": date.replace("-", ""),
                        "attention": str(int(row.get("attention") or 0)),
                        **{
                            key: None if row.get(key) in (None, "") else int(row[key])
                            for key in ("guard_1", "guard_2", "guard_3", "fans_count")
                        },
                        **{
                            key: float(metrics.get(key) or 0)
                            for key in ("gift", "guard", "super_chat")
                        },
                        **{
                            key: int(metrics.get(key) or 0)
                            for key in ("payer_count", "steel_coin_count")
                        },
                    }
                )
            values[f"/gift/attention:{rid}"] = self.body(
                f"/gift/attention:{rid}",
                {"room_id": rid, "month": month, "attention": daily},
                current,
            )
            sc = sorted(
                groups["super_chat_log"].get(rid, []),
                key=lambda row: (row["send_time"], row["id"]),
            )
            if current and rid in self.sc_bodies and self.previous_sc.get(rid) == sc:
                body = self.sc_bodies[rid]
            else:
                logs = [
                    {
                        key: api_app._format_optional_timestamp(row[key])
                        if key == "send_time"
                        else row[key]
                        for key in ("send_time", "uname", "uid", "price", "message")
                    }
                    for row in sc
                ]
                body = encode({"room_id": rid, "month": month, "list": logs})
                if current:
                    self.previous_sc[rid] = sc
                    self.sc_bodies[rid] = body
            values[f"/gift/sc:{rid}"] = body
            sessions = []
            sessions_with_stats = []
            for row in sorted(
                groups["live_session"].get(rid, []),
                key=lambda row: (row["start_time"], row["id"]),
            ):
                value = {
                    key: row.get(key)
                    for key in (
                        "title",
                        "gift",
                        "guard",
                        "super_chat",
                        "blind_box_count",
                        "captain_danmaku_count",
                        "admiral_danmaku_count",
                        "governor_danmaku_count",
                        "normal_danmaku_count",
                        "start_guard_1",
                        "start_guard_2",
                        "start_guard_3",
                        "start_fans_count",
                        "start_attention",
                        "end_guard_1",
                        "end_guard_2",
                        "end_guard_3",
                        "end_fans_count",
                        "end_attention",
                        "avg_concurrency",
                        "max_concurrency",
                    )
                }
                concurrency = None
                if current and row["end_time"] is None:
                    cache = (runtime_state.CONCURRENCY_CACHE.get(rid) or {}).copy()
                    if cache.get("session_id") == row["id"]:
                        samples = int(cache.get("samples", 0))
                        value["avg_concurrency"] = (
                            int(cache.get("total", 0)) / samples if samples else None
                        )
                        value["max_concurrency"] = (
                            int(cache.get("max", 0)) if samples else None
                        )
                        if samples and runtime_state.LAST_STATUS.get(rid, 0) == 1:
                            concurrency = int(cache.get("last", 0))
                value.update(
                    start_time=api_app._format_optional_timestamp(row["start_time"]),
                    end_time=api_app._format_optional_timestamp(row["end_time"]),
                    payer_count=row.get("payer_count") or 0,
                    danmaku_count=row.get("danmaku_count") or 0,
                    blind_box_profit=api_app._profit_display(
                        row.get("blind_box_profit")
                    ),
                    avg_concurrency=rounded_concurrency(value["avg_concurrency"]),
                    current_concurrency=concurrency,
                    **danmaku_metrics.report(SimpleNamespace(**row), f"session:{row['id']}",
                                             live=row["end_time"] is None or row.get("danmaku_status") == "pending"),
                )
                sessions.append(value)
                sessions_with_stats.append({
                    **value,
                    "stats_15m": [
                        api_app._format_15m_stats(bucket)
                        for bucket in sorted(
                            buckets.get(int(row["id"]), []),
                            key=lambda b: b["bucket_index"],
                        )
                    ],
                })
            for path, rows in (
                (LIVE_SESSIONS_PATH, sessions),
                (LIVE_SESSIONS_WITH_STATS_CACHE_PATH, sessions_with_stats),
            ):
                field = f"{path}:{rid}"
                values[field] = self.body(
                    field, {"room_id": rid, "month": month, "sessions": rows}, current
                )
        values["_empty:/gift/by_month"] = encode(
            summaries(month, snapshot.base, {}, set(), empty=True)
        )
        for path, name in (
            (LIVE_SESSIONS_PATH, "sessions"),
            (LIVE_SESSIONS_WITH_STATS_CACHE_PATH, "sessions"),
            ("/gift/attention", "attention"),
            ("/gift/sc", "list"),
        ):
            values[f"_empty:{path}"] = encode({"room_id": 0, "month": month, name: []})
        return values
