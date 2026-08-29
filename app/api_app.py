"""FastAPI transport and reporting layer.

Owns the seven public routes Todo 5 extracts out of ``gift.py``:

* ``POST /add/room``
* ``POST /delete/room``
* ``GET  /gift``
* ``GET  /gift/by_month``
* ``GET  /gift/live_sessions``
* ``GET  /gift/attention``
* ``GET  /gift/sc``

Route paths, response keys, status codes, and current-versus-history
branching are preserved verbatim.  ``/gift`` still emits
``current_concurrency``; ``/gift/by_month`` still omits it.

The routes resolve the mutable, monkey-patchable dependencies
(``Session``, ``_room_ids_for_month``, ``sc_log_table_exists``,
``_run_in_main_loop``, ``add_room_async``, ``delete_room_async``) through
the compatibility facade ``gift`` at request time.  This is what preserves
the pre-extraction test contract, which patches these names on ``gift``
without knowing about ``api_app``.  The canonical implementations live
here; the compatibility layer just re-exports them.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import sys
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional, Tuple, TypedDict, assert_never

from fastapi import Body, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import JsonValue
from sqlalchemy import Column, and_, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from . import room_config, runtime_state
from .config import API_SECRET
from .database import Session as _default_Session
from .database import engine
from .models import (
    Attention,
    LiveSession,
    LiveSession15mStats,
    RoomBlindBoxMonthly,
    RoomInfo,
    RoomLiveStats,
    RoomStatsMonthly,
    SuperChatLog,
)
from .repositories.tables import (
    attention_table_name,
    is_current_month,
    live_session_table_name,
    live_session_15m_stats_table_name,
    month_range,
    month_str,
    normalize_month_code,
    room_live_stats_table_name,
    sc_log_table_exists as _default_sc_log_table_exists,
    sc_log_table_name,
)


# ------------------ FastAPI app (Todo 5 canonical owner) ------------------ #
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


class RoomPayload(TypedDict, total=False):
    room_id: JsonValue
    room_anchors: JsonValue
    api_key: JsonValue


# ------------------ Runtime dependency resolution ------------------ #
def _gift() -> Any:
    """Return the ``gift`` compat module if it has been imported.

    The route implementations honor test-time monkey patches applied on
    ``gift`` (e.g. ``gift.Session``, ``gift._run_in_main_loop``,
    ``gift._room_ids_for_month``, ``gift.add_room_async``,
    ``gift.delete_room_async``, ``gift.sc_log_table_exists``).  When
    ``gift`` has not been loaded yet - e.g. this module is imported
    stand-alone in a test that only cares about the app object - the
    ``_default_*`` fallbacks defined in this module are used.
    """
    return sys.modules.get("app.gift")


def _resolved_session_factory():
    gift = _gift()
    if gift is not None and hasattr(gift, "Session"):
        return gift.Session
    return _default_Session


def _resolved_sc_log_table_exists():
    gift = _gift()
    if gift is not None and hasattr(gift, "sc_log_table_exists"):
        return gift.sc_log_table_exists
    return _default_sc_log_table_exists


def _resolved_room_ids_for_month():
    gift = _gift()
    if gift is not None and hasattr(gift, "_room_ids_for_month"):
        return gift._room_ids_for_month
    return _room_ids_for_month


def _resolved_add_room_async():
    gift = _gift()
    if gift is not None and hasattr(gift, "add_room_async"):
        return gift.add_room_async
    return add_room_async


def _resolved_delete_room_async():
    gift = _gift()
    if gift is not None and hasattr(gift, "delete_room_async"):
        return gift.delete_room_async
    return delete_room_async


def _resolved_run_in_main_loop():
    gift = _gift()
    if gift is not None and hasattr(gift, "_run_in_main_loop"):
        return gift._run_in_main_loop
    return _run_in_main_loop


# ------------------ Pure formatting helpers ------------------ #
def _seconds_to_hms(sec: int) -> str:
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _tenths_to_decimal(value: Optional[float]) -> Decimal:
    if value is None:
        return Decimal("0.0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0.0")


def _profit_display(value: float | int | Column[int] | None) -> float:
    match value:
        case None:
            return 0.0
        case int():
            decimal_value = Decimal(value) / Decimal(10)
        case float():
            decimal_value = _tenths_to_decimal(value)
        case Column():
            return 0.0
        case unreachable:
            assert_never(unreachable)
    return float(decimal_value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def _format_optional_timestamp(value: datetime.datetime | Column[datetime.datetime] | None) -> str | None:
    match value:
        case None:
            return None
        case datetime.datetime():
            return value.strftime("%Y-%m-%d %H:%M:%S")
        case Column():
            return None
        case unreachable:
            assert_never(unreachable)


# ------------------ Parse / auth helpers ------------------ #
def _parse_room_payload(payload: RoomPayload) -> Tuple[Optional[int], Optional[str], str]:
    room_id_raw = payload.get("room_id")
    anchor_raw = payload.get("room_anchors")
    if room_id_raw is None:
        return None, None, "room_id 必填"
    match room_id_raw:
        case str() | int() | float():
            try:
                room_id = int(room_id_raw)
            except ValueError:
                return None, None, "room_id 必须为整数"
        case _:
            return None, None, "room_id 必须为整数"
    if room_id <= 0:
        return None, None, "room_id 必须为正整数"
    if anchor_raw is None:
        return room_id, None, "room_anchors 必填"
    if isinstance(anchor_raw, dict):
        name = anchor_raw.get(str(room_id))
    else:
        name = anchor_raw
    if not isinstance(name, str) or not name.strip():
        return room_id, None, "room_anchors 必须为非空字符串"
    return room_id, name.strip(), ""


def _check_api_secret(request: Request, payload: RoomPayload) -> Tuple[bool, str]:
    if not API_SECRET:
        return False, "API_SECRET 未配置"
    provided = (
        request.headers.get("X-API-Key")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        or payload.get("api_key")
    )
    if not provided:
        return False, "缺少 API 密钥"
    if provided != API_SECRET:
        return False, "API 密钥无效"
    return True, ""


# ------------------ Main-loop bridge ------------------ #
def _run_in_main_loop(coro, timeout: int = 30):
    if runtime_state.MAIN_LOOP is None:
        raise RuntimeError("MAIN_LOOP 未初始化")
    future = asyncio.run_coroutine_threadsafe(coro, runtime_state.MAIN_LOOP)
    return future.result(timeout=timeout)


# ------------------ Room lifecycle wrappers ------------------ #
async def add_room_async(room_id: int, anchor_name: str) -> Tuple[bool, str]:
    """Call the canonical room-lifecycle add-room implementation."""
    from . import monitoring_jobs, room_lifecycle

    return await room_lifecycle.add_room_async(
        room_id, anchor_name, monitoring_jobs.lifecycle_dependencies()
    )


async def delete_room_async(room_id: int) -> Tuple[bool, str]:
    """Call the canonical room-lifecycle delete-room implementation."""
    from . import monitoring_jobs, room_lifecycle

    return await room_lifecycle.delete_room_async(
        room_id, monitoring_jobs.lifecycle_dependencies()
    )


# ------------------ Room-set enumeration ------------------ #
def _room_ids_for_month(m: str, include_config: bool = True) -> list[int]:
    """返回指定月份应展示的房间集合：DB出现过的房间 ∪ (可选) ROOM_IDS"""
    session_factory = _resolved_session_factory()
    session = session_factory()
    sc_log_table_exists = _resolved_sc_log_table_exists()
    try:
        start, end = month_range(m)
        ids = set(room_config.get_room_ids()) if include_config else set()

        # room_stats_monthly 有记录的房间
        q1 = session.query(RoomStatsMonthly.room_id).filter_by(month=m).all()
        ids.update(rid for (rid,) in q1)

        # live_session 以开播月归档的房间
        if is_current_month(m):
            q2 = session.query(LiveSession.room_id).filter_by(month=m).all()
            ids.update(rid for (rid,) in q2)
        else:
            table_name = live_session_table_name(m)
            if sc_log_table_exists(table_name):
                rows = session.execute(
                    text(f"SELECT DISTINCT room_id FROM `{table_name}` WHERE month = :month"),
                    {"month": m},
                ).fetchall()
                ids.update(rid for (rid,) in rows)
            else:
                q2 = session.query(LiveSession.room_id).filter_by(month=m).all()
                ids.update(rid for (rid,) in q2)

        # room_live_stats 在该月有天级时长记录的房间
        if is_current_month(m):
            q3 = (
                session.query(RoomLiveStats.room_id)
                .filter(RoomLiveStats.date >= start, RoomLiveStats.date < end)
                .distinct()
                .all()
            )
            ids.update(rid for (rid,) in q3)
        else:
            table_name = room_live_stats_table_name(m)
            if sc_log_table_exists(table_name):
                rows = session.execute(
                    text(
                        f"SELECT DISTINCT room_id FROM `{table_name}` "
                        "WHERE date >= :start AND date < :end"
                    ),
                    {"start": start, "end": end},
                ).fetchall()
                ids.update(rid for (rid,) in rows)
            else:
                q3 = (
                    session.query(RoomLiveStats.room_id)
                    .filter(RoomLiveStats.date >= start, RoomLiveStats.date < end)
                    .distinct()
                    .all()
                )
                ids.update(rid for (rid,) in q3)

        return sorted(ids)
    finally:
        session.close()


def _daily_metrics_for_room(session, room_id: int, month: str) -> dict[str, dict[str, float | int]]:
    start_date, end_date = month_range(month)
    if is_current_month(month):
        rows = (
            session.query(
                RoomLiveStats.date,
                RoomLiveStats.gift,
                RoomLiveStats.guard,
                RoomLiveStats.super_chat,
                RoomLiveStats.payer_count,
                RoomLiveStats.steel_coin_count,
            )
            .filter(
                and_(
                    RoomLiveStats.room_id == room_id,
                    RoomLiveStats.date >= start_date,
                    RoomLiveStats.date < end_date,
                )
            )
            .all()
        )
    else:
        table_name = room_live_stats_table_name(month)
        if not _resolved_sc_log_table_exists()(table_name):
            rows = (
                session.query(
                    RoomLiveStats.date,
                    RoomLiveStats.gift,
                    RoomLiveStats.guard,
                    RoomLiveStats.super_chat,
                    RoomLiveStats.payer_count,
                    RoomLiveStats.steel_coin_count,
                )
                .filter(
                    and_(
                        RoomLiveStats.room_id == room_id,
                        RoomLiveStats.date >= start_date,
                        RoomLiveStats.date < end_date,
                    )
                )
                .all()
            )
        else:
            archive_columns = {col.get("name") for col in inspect(engine).get_columns(table_name)}
            metric_names = ("gift", "guard", "super_chat", "payer_count", "steel_coin_count")
            metric_select = ", ".join(
                f"`{name}`" if name in archive_columns else f"0 AS `{name}`"
                for name in metric_names
            )
            rows = session.execute(
                text(
                    f"SELECT `date`, {metric_select} FROM `{table_name}` "
                    "WHERE room_id = :room_id AND `date` >= :start_date AND `date` < :end_date"
                ),
                {"room_id": room_id, "start_date": start_date, "end_date": end_date},
            ).fetchall()
    return {
        str(row[0]): {
            "gift": float(row[1] or 0),
            "guard": float(row[2] or 0),
            "super_chat": float(row[3] or 0),
            "payer_count": int(row[4] or 0),
            "steel_coin_count": int(row[5] or 0),
        }
        for row in rows
    }


def _format_15m_stats(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        values = row
    else:
        values = {
            "bucket_index": row[0],
            "start_time": row[1],
            "end_time": row[2],
            "gift": row[3],
            "guard": row[4],
            "super_chat": row[5],
            "blind_box_count": row[6],
            "blind_box_profit": row[7],
            "danmaku_count": row[8],
            "avg_concurrency": row[9],
            "max_concurrency": row[10],
            "sample_count": row[11],
            "payer_count": row[12],
        }
    return {
        "bucket_index": int(values.get("bucket_index") or 0),
        "start_time": _format_optional_timestamp(values.get("start_time")),
        "end_time": _format_optional_timestamp(values.get("end_time")),
        "gift": float(values.get("gift") or 0),
        "guard": float(values.get("guard") or 0),
        "super_chat": float(values.get("super_chat") or 0),
        "blind_box_count": int(values.get("blind_box_count") or 0),
        "blind_box_profit": _profit_display(values.get("blind_box_profit")),
        "danmaku_count": int(values.get("danmaku_count") or 0),
        "avg_concurrency": values.get("avg_concurrency"),
        "max_concurrency": values.get("max_concurrency"),
        "sample_count": int(values.get("sample_count") or 0),
        "payer_count": int(values.get("payer_count") or 0),
    }


def _session_15m_stats(session, session_id: int | Column[int], month: str) -> list[dict[str, Any]]:
    if is_current_month(month):
        rows = (
            session.query(LiveSession15mStats)
            .filter_by(session_id=session_id)
            .order_by(LiveSession15mStats.bucket_index.asc())
            .all()
        )
        return [_format_15m_stats({
            "bucket_index": row.bucket_index,
            "start_time": row.start_time,
            "end_time": row.end_time,
            "gift": row.gift,
            "guard": row.guard,
            "super_chat": row.super_chat,
            "blind_box_count": row.blind_box_count,
            "blind_box_profit": row.blind_box_profit,
            "danmaku_count": row.danmaku_count,
            "avg_concurrency": row.avg_concurrency,
            "max_concurrency": row.max_concurrency,
            "sample_count": row.sample_count,
            "payer_count": row.payer_count,
        }) for row in rows]
    table_name = live_session_15m_stats_table_name(month)
    if not _resolved_sc_log_table_exists()(table_name):
        rows = (
            session.query(LiveSession15mStats)
            .filter_by(session_id=session_id)
            .order_by(LiveSession15mStats.bucket_index.asc())
            .all()
        )
        return [_format_15m_stats({
            "bucket_index": row.bucket_index,
            "start_time": row.start_time,
            "end_time": row.end_time,
            "gift": row.gift,
            "guard": row.guard,
            "super_chat": row.super_chat,
            "blind_box_count": row.blind_box_count,
            "blind_box_profit": row.blind_box_profit,
            "danmaku_count": row.danmaku_count,
            "avg_concurrency": row.avg_concurrency,
            "max_concurrency": row.max_concurrency,
            "sample_count": row.sample_count,
            "payer_count": row.payer_count,
        }) for row in rows]
    rows = session.execute(
        text(
            "SELECT bucket_index, start_time, end_time, gift, guard, super_chat, "
            "blind_box_count, blind_box_profit, danmaku_count, avg_concurrency, "
            "max_concurrency, sample_count, payer_count FROM `{table_name}` "
            "WHERE session_id = :session_id ORDER BY bucket_index ASC"
        ),
        {"session_id": session_id},
    ).fetchall()
    return [_format_15m_stats(row) for row in rows]


# ------------------ Routes ------------------ #
@app.post("/add/room")
def add_room_api(request: Request, payload: RoomPayload = Body(default={})):
    ok, error = _check_api_secret(request, payload)
    if not ok:
        return JSONResponse({"error": error}, status_code=401)
    room_id, anchor_name, error = _parse_room_payload(payload)
    if error or room_id is None or anchor_name is None:
        return JSONResponse({"error": error}, status_code=400)
    try:
        add_room_impl = _resolved_add_room_async()
        run_in_main_loop = _resolved_run_in_main_loop()
        ok, message = run_in_main_loop(add_room_impl(room_id, anchor_name))
    except Exception as exc:
        logging.error(f"[API] /add/room 执行失败: {exc}")
        return JSONResponse({"error": "添加房间失败"}, status_code=500)
    status = 200 if ok else 409
    return JSONResponse({"ok": ok, "room_id": room_id, "message": message}, status_code=status)


@app.post("/delete/room")
def delete_room_api(request: Request, payload: RoomPayload = Body(default={})):
    ok, error = _check_api_secret(request, payload)
    if not ok:
        return JSONResponse({"error": error}, status_code=401)
    room_id, anchor_name, error = _parse_room_payload(payload)
    if error or room_id is None:
        return JSONResponse({"error": error}, status_code=400)
    try:
        delete_room_impl = _resolved_delete_room_async()
        run_in_main_loop = _resolved_run_in_main_loop()
        ok, message = run_in_main_loop(delete_room_impl(room_id))
    except Exception as exc:
        logging.error(f"[API] /delete/room 执行失败: {exc}")
        return JSONResponse({"error": "删除房间失败"}, status_code=500)
    status = 200 if ok else 404
    return JSONResponse({"ok": ok, "room_id": room_id, "message": message}, status_code=status)


@app.get("/gift")
def get_stats_current_month():
    """
    当月汇总：
      - room_stats_monthly 当月 gift/guard/super_chat
      - room_live_stats 聚合当月直播时长与有效天数
      - 当前月返回实时 live_time/title/status；历史月置空
      - 当前月开播时返回即时同接（current_concurrency），未开播返回 null
    """
    results = []
    session_factory = _resolved_session_factory()
    room_ids_for_month = _resolved_room_ids_for_month()
    session = session_factory()
    m = month_str()  # 当前月
    try:
        for room_id in room_ids_for_month(m, include_config=True):
            # 读当月累计
            rsm = session.query(RoomStatsMonthly).filter_by(room_id=room_id, month=m).first()
            g = rsm.gift if rsm else 0.0
            gd = rsm.guard if rsm else 0.0
            sc = rsm.super_chat if rsm else 0.0
            payer_count = getattr(rsm, "payer_count", 0) if rsm else 0
            rbm = session.query(RoomBlindBoxMonthly).filter_by(room_id=room_id, month=m).first()
            bb_count = rbm.blind_box_count if rbm else 0
            bb_profit = _profit_display(rbm.blind_box_profit) if rbm else 0.0

            anchor_name = (
                session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar()
            ) or room_config.get_room_anchor_name(room_id)
            attention = (
                session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar()
            ) or 0

            total_sec, eff_days = RoomLiveStats.month_aggregate_for_month(room_id, m)
            steel_coin_count = RoomLiveStats.month_steel_coin_for_month(room_id, m)
            live_dur_str = _seconds_to_hms(total_sec)

            info = runtime_state.LIVE_INFO.get(room_id, {})
            live_time_val = info.get("live_time", "0000-00-00 00:00:00")
            title_val = info.get("title", "")
            status_val = runtime_state.LAST_STATUS.get(room_id, 0)
            current_concurrency = None
            if status_val == 1:
                session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
                cache = runtime_state.CONCURRENCY_CACHE.get(room_id)
                if cache and cache.get("session_id") == session_id:
                    samples = int(cache.get("samples", 0))
                    if samples > 0:
                        current_concurrency = int(cache.get("last", 0))

            # 当前守护数量 / 粉丝团数量（最新状态）
            guard_info = runtime_state.GUARD_COUNTS.get(room_id, {}) or {}
            guard_1 = guard_info.get("guard_1", 0)  # 舰长
            guard_2 = guard_info.get("guard_2", 0)  # 提督
            guard_3 = guard_info.get("guard_3", 0)  # 总督
            fans_count = runtime_state.FANS_COUNT.get(room_id, 0)

            results.append(
                {
                    "room_id": room_id,
                    "anchor_name": anchor_name,
                    "attention": attention,
                    "status": status_val,
                    "gift": g,
                    "guard": gd,
                    "super_chat": sc,
                    "payer_count": payer_count,
                    "steel_coin_count": steel_coin_count,
                    "blind_box_count": bb_count,
                    "blind_box_profit": bb_profit,
                    "live_duration": live_dur_str,
                    "effective_days": eff_days,
                    "live_time": live_time_val,
                    "title": title_val,
                    "month": m,
                    "guard_1": guard_1,  # 舰长
                    "guard_2": guard_2,  # 提督
                    "guard_3": guard_3,  # 总督
                    "fans_count": fans_count,  # 粉丝团数量
                    "current_concurrency": current_concurrency,
                }
            )
        return JSONResponse(results)
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_stats_current_month] 数据库查询出错: {e}")
        return JSONResponse({"error": "数据库查询失败"}, status_code=500)
    finally:
        session.close()


@app.get("/gift/by_month")
def get_stats_by_month(request: Request):
    """
    指定月份汇总：
    GET ?month=YYYYMM
    历史月不返回实时 live_time/title/status（置空/0）
    """
    m = request.query_params.get("month") or month_str()
    results = []
    session_factory = _resolved_session_factory()
    room_ids_for_month = _resolved_room_ids_for_month()
    session = session_factory()
    try:
        is_current = m == month_str()
        for room_id in room_ids_for_month(m, include_config=True):
            rsm = session.query(RoomStatsMonthly).filter_by(room_id=room_id, month=m).first()
            g = rsm.gift if rsm else 0.0
            gd = rsm.guard if rsm else 0.0
            sc = rsm.super_chat if rsm else 0.0
            payer_count = getattr(rsm, "payer_count", 0) if rsm else 0
            rbm = session.query(RoomBlindBoxMonthly).filter_by(room_id=room_id, month=m).first()
            bb_count = rbm.blind_box_count if rbm else 0
            bb_profit = _profit_display(rbm.blind_box_profit) if rbm else 0.0

            anchor_name = (
                session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar()
            ) or room_config.get_room_anchor_name(room_id)
            attention = (
                session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar()
            ) or 0

            total_sec, eff_days = RoomLiveStats.month_aggregate_for_month(room_id, m)
            steel_coin_count = RoomLiveStats.month_steel_coin_for_month(room_id, m)
            live_dur_str = _seconds_to_hms(total_sec)

            if is_current:
                info = runtime_state.LIVE_INFO.get(room_id, {})
                live_time_val = info.get("live_time", "0000-00-00 00:00:00")
                title_val = info.get("title", "")
                status_val = runtime_state.LAST_STATUS.get(room_id, 0)

                guard_info = runtime_state.GUARD_COUNTS.get(room_id, {}) or {}
                guard_1 = guard_info.get("guard_1", 0)
                guard_2 = guard_info.get("guard_2", 0)
                guard_3 = guard_info.get("guard_3", 0)
                fans_count = runtime_state.FANS_COUNT.get(room_id, 0)
            else:
                live_time_val = "0000-00-00 00:00:00"
                title_val = ""
                status_val = 0
                # 历史月份不保留守护 / 粉丝团历史，直接返回 null
                guard_1 = None
                guard_2 = None
                guard_3 = None
                fans_count = None

            results.append(
                {
                    "room_id": room_id,
                    "anchor_name": anchor_name,
                    "attention": attention,
                    "status": status_val,
                    "gift": g,
                    "guard": gd,
                    "super_chat": sc,
                    "payer_count": payer_count,
                    "steel_coin_count": steel_coin_count,
                    "blind_box_count": bb_count,
                    "blind_box_profit": bb_profit,
                    "live_duration": live_dur_str,
                    "effective_days": eff_days,
                    "live_time": live_time_val,
                    "title": title_val,
                    "month": m,
                    "guard_1": guard_1,
                    "guard_2": guard_2,
                    "guard_3": guard_3,
                    "fans_count": fans_count,
                }
            )
        return JSONResponse(results)
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_stats_by_month] 数据库查询出错: {e}")
        return JSONResponse({"error": "数据库查询失败"}, status_code=500)
    finally:
        session.close()


@app.get("/gift/live_sessions")
def get_live_sessions_by_room_month(request: Request):
    """
    指定房间 + 月份的单场直播清单：
    GET ?room_id=xxx&month=YYYYMM
    """
    try:
        room_id = int(request.query_params.get("room_id", "0"))
    except ValueError:
        return JSONResponse({"error": "room_id 参数无效"}, status_code=400)
    if room_id <= 0:
        return JSONResponse({"error": "room_id 必填且需为正整数"}, status_code=400)

    m = request.query_params.get("month") or month_str()
    session_factory = _resolved_session_factory()
    sc_log_table_exists = _resolved_sc_log_table_exists()
    session = session_factory()
    try:
        out = []
        if is_current_month(m):
            rows = (
                session.query(LiveSession)
                .filter(and_(LiveSession.room_id == room_id, LiveSession.month == m))
                .order_by(LiveSession.start_time.asc())
                .all()
            )
            for r in rows:
                avg_concurrency = r.avg_concurrency
                max_concurrency = r.max_concurrency
                current_concurrency = None
                if r.end_time is None:
                    cache = runtime_state.CONCURRENCY_CACHE.get(room_id)
                    if cache and cache.get("session_id") == r.id:
                        samples = int(cache.get("samples", 0))
                        total = int(cache.get("total", 0))
                        avg_concurrency = (total / samples) if samples > 0 else None
                        max_concurrency = int(cache.get("max", 0)) if samples > 0 else None
                        if runtime_state.LAST_STATUS.get(room_id, 0) == 1 and samples > 0:
                            current_concurrency = int(cache.get("last", 0))
                out.append(
                    {
                        "start_time": r.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": _format_optional_timestamp(r.end_time),
                        "title": r.title,
                        "gift": r.gift,
                        "guard": r.guard,
                        "super_chat": r.super_chat,
                        "payer_count": getattr(r, "payer_count", 0) or 0,
                        "blind_box_count": r.blind_box_count,
                        "blind_box_profit": _profit_display(r.blind_box_profit),
                        "danmaku_count": r.danmaku_count or 0,
                        # 开播时快照（旧数据为 None -> JSON null）
                        "start_guard_1": r.start_guard_1,  # 舰长
                        "start_guard_2": r.start_guard_2,  # 提督
                        "start_guard_3": r.start_guard_3,  # 总督
                        "start_fans_count": r.start_fans_count,
                        "start_attention": r.start_attention,
                        # 下播时快照（旧数据为 None -> JSON null）
                        "end_guard_1": r.end_guard_1,
                        "end_guard_2": r.end_guard_2,
                        "end_guard_3": r.end_guard_3,
                        "end_fans_count": r.end_fans_count,
                        "end_attention": r.end_attention,
                        "avg_concurrency": avg_concurrency,
                        "max_concurrency": max_concurrency,
                        "current_concurrency": current_concurrency,
                        "stats_15m": _session_15m_stats(session, r.id, m),
                    }
                )
        else:
            table_name = live_session_table_name(m)
            if sc_log_table_exists(table_name):
                archive_columns = {col.get("name") for col in inspect(engine).get_columns(table_name)}
                payer_select = "`payer_count`" if "payer_count" in archive_columns else "0 AS payer_count"
                rows = session.execute(
                    text(
                        f"SELECT id, start_time, end_time, title, gift, guard, super_chat, "
                        "blind_box_count, blind_box_profit, danmaku_count, "
                        "start_guard_1, start_guard_2, start_guard_3, start_fans_count, "
                        "start_attention, end_guard_1, end_guard_2, end_guard_3, end_fans_count, "
                        "end_attention, "
                        f"avg_concurrency, max_concurrency, {payer_select} "
                        f"FROM `{table_name}` "
                        "WHERE room_id = :room_id AND month = :month "
                        "ORDER BY start_time ASC"
                    ),
                    {"room_id": room_id, "month": m},
                ).fetchall()
                for row in rows:
                    out.append(
                        {
                            "start_time": row[1].strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": (row[2].strftime("%Y-%m-%d %H:%M:%S") if row[2] else None),
                            "title": row[3],
                            "gift": row[4],
                            "guard": row[5],
                            "super_chat": row[6],
                            "payer_count": row[22] or 0,
                            "blind_box_count": row[7],
                            "blind_box_profit": _profit_display(row[8]),
                            "danmaku_count": row[9] or 0,
                            "start_guard_1": row[10],
                            "start_guard_2": row[11],
                            "start_guard_3": row[12],
                            "start_fans_count": row[13],
                            "start_attention": row[14],
                            "end_guard_1": row[15],
                            "end_guard_2": row[16],
                            "end_guard_3": row[17],
                            "end_fans_count": row[18],
                            "end_attention": row[19],
                            "avg_concurrency": row[20],
                            "max_concurrency": row[21],
                            "current_concurrency": None,
                            "stats_15m": _session_15m_stats(session, row[0], m),
                        }
                    )
            else:
                rows = (
                    session.query(LiveSession)
                    .filter(and_(LiveSession.room_id == room_id, LiveSession.month == m))
                    .order_by(LiveSession.start_time.asc())
                    .all()
                )
                for r in rows:
                    out.append(
                        {
                            "start_time": r.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": _format_optional_timestamp(r.end_time),
                            "title": r.title,
                            "gift": r.gift,
                            "guard": r.guard,
                            "super_chat": r.super_chat,
                            "payer_count": getattr(r, "payer_count", 0) or 0,
                            "blind_box_count": r.blind_box_count,
                            "blind_box_profit": _profit_display(r.blind_box_profit),
                            "danmaku_count": r.danmaku_count or 0,
                            "start_guard_1": r.start_guard_1,
                            "start_guard_2": r.start_guard_2,
                            "start_guard_3": r.start_guard_3,
                            "start_fans_count": r.start_fans_count,
                            "start_attention": r.start_attention,
                            "end_guard_1": r.end_guard_1,
                            "end_guard_2": r.end_guard_2,
                            "end_guard_3": r.end_guard_3,
                            "end_fans_count": r.end_fans_count,
                            "end_attention": r.end_attention,
                            "avg_concurrency": r.avg_concurrency,
                            "max_concurrency": r.max_concurrency,
                            "current_concurrency": None,
                            "stats_15m": _session_15m_stats(session, r.id, m),
                        }
                    )
        payload = {"room_id": room_id, "month": m, "sessions": out}
        return JSONResponse(content=jsonable_encoder(payload))
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_live_sessions_by_room_month] 查询失败: {e}")
        return JSONResponse({"error": "数据库查询失败"}, status_code=500)
    finally:
        session.close()


@app.get("/gift/attention")
def get_attention_logs(request: Request):
    """
    粉丝数、守护与粉丝团日快照查询：
      GET /gift/attention?room_id=1111&month=202603
    month 为空时返回当前月数据；无数据返回 attention: []
    """
    try:
        room_id = int(request.query_params.get("room_id", "0"))
    except ValueError:
        return JSONResponse({"error": "room_id 参数无效"}, status_code=400)
    if room_id <= 0:
        return JSONResponse({"error": "room_id 必填且需为正整数"}, status_code=400)

    month_param = request.query_params.get("month")
    if month_param:
        m = normalize_month_code(month_param)
        if not m:
            return JSONResponse({"error": "month 参数无效，支持 YYYYMM 或 YYYY-MM"}, status_code=400)
    else:
        m = month_str()

    start_date, end_date = month_range(m)
    session_factory = _resolved_session_factory()
    sc_log_table_exists = _resolved_sc_log_table_exists()
    session = session_factory()
    try:
        daily_metrics = _daily_metrics_for_room(session, room_id, m)
        if is_current_month(m):
            rows = (
                session.query(
                    Attention.date,
                    Attention.attention,
                    Attention.guard_1,
                    Attention.guard_2,
                    Attention.guard_3,
                    Attention.fans_count,
                )
                .filter(
                    and_(
                        Attention.room_id == room_id,
                        Attention.date >= start_date,
                        Attention.date < end_date,
                    )
                )
                .order_by(Attention.date.asc())
                .all()
            )
        else:
            table_name = attention_table_name(m)
            if sc_log_table_exists(table_name):
                archive_columns = {
                    col.get("name") for col in inspect(engine).get_columns(table_name)
                }
                metric_select = ", ".join(
                    f"`{name}`" if name in archive_columns else f"NULL AS `{name}`"
                    for name in ("guard_1", "guard_2", "guard_3", "fans_count")
                )
                rows = session.execute(
                    text(
                        f"SELECT `date`, attention, {metric_select} FROM `{table_name}` "
                        "WHERE room_id = :room_id AND `date` >= :start_date AND `date` < :end_date "
                        "ORDER BY `date` ASC"
                    ),
                    {"room_id": room_id, "start_date": start_date, "end_date": end_date},
                ).fetchall()
            else:
                rows = (
                    session.query(
                        Attention.date,
                        Attention.attention,
                        Attention.guard_1,
                        Attention.guard_2,
                        Attention.guard_3,
                        Attention.fans_count,
                    )
                    .filter(
                        and_(
                            Attention.room_id == room_id,
                            Attention.date >= start_date,
                            Attention.date < end_date,
                        )
                    )
                    .order_by(Attention.date.asc())
                    .all()
                )
        attention_by_date = {str(row[0]): row for row in rows}
        dates = sorted(set(attention_by_date) | set(daily_metrics))
        out = []
        for date_key in dates:
            row = attention_by_date.get(date_key)
            metrics = daily_metrics.get(date_key, {})
            out.append(
                {
                    "date": date_key.replace("-", ""),
                    "attention": str(int(row[1] or 0)) if row else "0",
                    "guard_1": None if not row or row[2] in (None, "") else int(row[2]),
                    "guard_2": None if not row or row[3] in (None, "") else int(row[3]),
                    "guard_3": None if not row or row[4] in (None, "") else int(row[4]),
                    "fans_count": None if not row or row[5] in (None, "") else int(row[5]),
                    "gift": metrics.get("gift", 0.0),
                    "guard": metrics.get("guard", 0.0),
                    "super_chat": metrics.get("super_chat", 0.0),
                    "payer_count": metrics.get("payer_count", 0),
                    "steel_coin_count": metrics.get("steel_coin_count", 0),
                }
            )
        payload = {"room_id": room_id, "month": m, "attention": out}
        return JSONResponse(content=jsonable_encoder(payload))
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_attention_logs] 查询失败: {e}")
        return JSONResponse({"error": "数据库查询失败"}, status_code=500)
    finally:
        session.close()


@app.get("/gift/sc")
def get_sc_logs(request: Request):
    """
    SC 日志查询：
      GET /gift/sc?room_id=1111&month=202511
      - room_id 必填
      - month 可选，默认当前月；支持 YYYYMM 或 YYYY-MM
      返回：发送时间、发送人名称、UID、价格、内容
    """
    room_id_str = request.query_params.get("room_id")
    if not room_id_str:
        return JSONResponse({"error": "room_id 参数必填"}, status_code=400)
    try:
        room_id = int(room_id_str)
    except ValueError:
        return JSONResponse({"error": "room_id 参数无效"}, status_code=400)
    if room_id <= 0:
        return JSONResponse({"error": "room_id 必须为正整数"}, status_code=400)

    month_raw = request.query_params.get("month")
    if month_raw:
        month_code = normalize_month_code(month_raw)
        if not month_code:
            return JSONResponse({"error": "month 格式不正确，应为 YYYYMM 或 YYYY-MM"}, status_code=400)
    else:
        month_code = month_str()

    # 利用已有 month_range，算出该月起止 date，再转为 datetime
    start_date, end_date = month_range(month_code)
    start_dt = datetime.datetime.combine(start_date, datetime.time.min)
    end_dt = datetime.datetime.combine(end_date, datetime.time.min)

    session_factory = _resolved_session_factory()
    sc_log_table_exists = _resolved_sc_log_table_exists()
    session = session_factory()
    try:
        out = []
        if is_current_month(month_code):
            rows = (
                session.query(SuperChatLog)
                .filter(
                    SuperChatLog.room_id == room_id,
                    SuperChatLog.send_time >= start_dt,
                    SuperChatLog.send_time < end_dt,
                )
                .order_by(SuperChatLog.send_time.asc())
                .all()
            )
            for r in rows:
                out.append(
                    {
                        "send_time": r.send_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "uname": r.uname,
                        "uid": r.uid,
                        "price": r.price,
                        "message": r.message,
                    }
                )
        else:
            table_name = sc_log_table_name(month_code)
            if sc_log_table_exists(table_name):
                rows = session.execute(
                    text(
                        f"SELECT send_time, uname, uid, price, message "
                        f"FROM `{table_name}` "
                        "WHERE room_id = :room_id "
                        "AND send_time >= :start_dt AND send_time < :end_dt "
                        "ORDER BY send_time ASC"
                    ),
                    {"room_id": room_id, "start_dt": start_dt, "end_dt": end_dt},
                ).fetchall()
                for row in rows:
                    send_time = row[0]
                    out.append(
                        {
                            "send_time": send_time.strftime("%Y-%m-%d %H:%M:%S"),
                            "uname": row[1],
                            "uid": row[2],
                            "price": row[3],
                            "message": row[4],
                        }
                    )
            else:
                rows = (
                    session.query(SuperChatLog)
                    .filter(
                        SuperChatLog.room_id == room_id,
                        SuperChatLog.send_time >= start_dt,
                        SuperChatLog.send_time < end_dt,
                    )
                    .order_by(SuperChatLog.send_time.asc())
                    .all()
                )
                for r in rows:
                    out.append(
                        {
                            "send_time": r.send_time.strftime("%Y-%m-%d %H:%M:%S"),
                            "uname": r.uname,
                            "uid": r.uid,
                            "price": r.price,
                            "message": r.message,
                        }
                    )

        return JSONResponse(
            {
                "room_id": room_id,
                "month": month_code,
                "list": out,
            }
        )
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_sc_logs] 查询失败: {e}")
        return JSONResponse({"error": "数据库查询失败"}, status_code=500)
    finally:
        session.close()
