"""Shared monthly table naming and archive-table persistence helpers."""

import datetime
import logging
import re

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from ..database import engine


def month_str(dt: datetime.datetime | None = None) -> str:
    """Return the runtime month code for a datetime or the current instant."""
    dt = dt or datetime.datetime.now()
    return dt.strftime("%Y%m")


def month_range(month: str) -> tuple[datetime.date, datetime.date]:
    """Return the inclusive month start and exclusive next-month date."""
    year = int(month[:4])
    mon = int(month[4:])
    start = datetime.date(year, mon, 1)
    end = datetime.date(year + 1, 1, 1) if mon == 12 else datetime.date(year, mon + 1, 1)
    return start, end


def normalize_month_code(raw: str | None) -> str | None:
    """Normalize YYYYMM or YYYY-MM month input, returning None when invalid."""
    if not raw:
        return None
    value = raw.strip()
    compact_match = re.fullmatch(r"(\d{4})(\d{2})", value)
    dashed_match = re.fullmatch(r"(\d{4})-(\d{2})", value)
    if compact_match:
        year, month = compact_match.group(1), compact_match.group(2)
    elif dashed_match:
        year, month = dashed_match.group(1), dashed_match.group(2)
    else:
        return None
    try:
        month_number = int(month)
        if 1 <= month_number <= 12:
            return f"{year}{month}"
    except ValueError:
        return None
    return None


def sc_log_table_name(month_code: str) -> str:
    return f"super_chat_log_{month_code}"


def live_session_table_name(month_code: str) -> str:
    return f"live_session_{month_code}"


def room_live_stats_table_name(month_code: str) -> str:
    return f"room_live_stats_{month_code}"


def live_session_15m_stats_table_name(month_code: str) -> str:
    return f"live_session_15m_stats_{month_code}"


def attention_table_name(month_code: str) -> str:
    return f"attention_{month_code}"


def is_current_month(month_code: str) -> bool:
    return month_code == month_str()


def sc_log_table_exists(table_name: str) -> bool:
    try:
        return inspect(engine).has_table(table_name)
    except SQLAlchemyError as exc:
        logging.error(f"[SuperChatLog] 检查表存在失败: {exc}")
        return False


def _ensure_archive_table(month_code: str, table_prefix: str, source_table: str, label: str) -> str:
    table_name = f"{table_prefix}_{month_code}"
    if sc_log_table_exists(table_name):
        return table_name
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS `{table_name}` LIKE `{source_table}`"))
        logging.info(f"[{label}] 已确保归档表存在: {table_name}")
    except SQLAlchemyError as exc:
        logging.error(f"[{label}] 创建归档表失败 {table_name}: {exc}")
    return table_name


def ensure_sc_archive_table(month_code: str) -> str:
    return _ensure_archive_table(month_code, "super_chat_log", "super_chat_log", "SuperChatLog")


def ensure_live_session_archive_table(month_code: str) -> str:
    return _ensure_archive_table(month_code, "live_session", "live_session", "LiveSession")


def ensure_room_live_stats_archive_table(month_code: str) -> str:
    return _ensure_archive_table(month_code, "room_live_stats", "room_live_stats", "RoomLiveStats")


def ensure_live_session_15m_stats_archive_table(month_code: str) -> str:
    return _ensure_archive_table(
        month_code,
        "live_session_15m_stats",
        "live_session_15m_stats",
        "LiveSession15mStats",
    )


def ensure_attention_archive_table(month_code: str) -> str:
    return _ensure_archive_table(month_code, "attention", "attention", "Attention")
