"""Monthly archive maintenance service.

Owns the four archive migrations that Todo 5 extracts out of ``gift.py``:

* :func:`archive_super_chat_log`
* :func:`archive_live_session`
* :func:`archive_room_live_stats`
* :func:`archive_attention` (scheduler-only; never called by the CLI)

Behavior is preserved verbatim from the pre-extraction implementation - same
month enumeration, same suffix rules, same ``INSERT ... SELECT`` +
``DELETE`` transaction boundary per month, same log strings, same return
value (approximate row count moved).  ``migrate_sc_archive.py`` continues to
call exactly the first three functions via ``gift.py`` compat re-exports.
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from .database import Session, engine
from .repositories.tables import (
    ensure_attention_archive_table,
    ensure_live_session_archive_table,
    ensure_room_live_stats_archive_table,
    ensure_sc_archive_table,
    month_range,
    month_str,
    normalize_month_code,
)


def archive_super_chat_log(target_month: Optional[str] = None) -> int:
    """
    将 super_chat_log 中历史月份数据迁移到归档表。
    - target_month: 指定归档月份（YYYYMM）；None 表示归档所有早于当前月的数据
    返回迁移的记录数（预估）。
    """
    current_month = month_str()
    start_current, _ = month_range(current_month)
    cutoff_dt = datetime.datetime.combine(start_current, datetime.time.min)

    months: list[str] = []
    if target_month:
        normalized = normalize_month_code(target_month)
        if not normalized:
            logging.error(f"[SuperChatLog] 归档月份格式非法: {target_month}")
            return 0
        if normalized == current_month:
            logging.info("[SuperChatLog] 当前月不归档，跳过")
            return 0
        months = [normalized]
    else:
        session = Session()
        try:
            rows = session.execute(
                text(
                    "SELECT DATE_FORMAT(send_time, '%Y%m') AS m "
                    "FROM super_chat_log "
                    "WHERE send_time < :cutoff "
                    "GROUP BY m"
                ),
                {"cutoff": cutoff_dt},
            ).fetchall()
            for (m,) in rows:
                normalized = normalize_month_code(m)
                if normalized and normalized != current_month:
                    months.append(normalized)
        except SQLAlchemyError as e:
            logging.error(f"[SuperChatLog] 读取待归档月份失败: {e}")
            return 0
        finally:
            session.close()

    if not months:
        return 0

    moved_total = 0
    for month_code in sorted(set(months)):
        start_date, end_date = month_range(month_code)
        start_dt = datetime.datetime.combine(start_date, datetime.time.min)
        end_dt = datetime.datetime.combine(end_date, datetime.time.min)
        table_name = ensure_sc_archive_table(month_code)
        try:
            with engine.begin() as conn:
                count = conn.execute(
                    text(
                        "SELECT COUNT(1) FROM `super_chat_log` "
                        "WHERE send_time >= :start AND send_time < :end"
                    ),
                    {"start": start_dt, "end": end_dt},
                ).scalar()
                if not count:
                    continue
                conn.execute(
                    text(
                        f"INSERT IGNORE INTO `{table_name}` "
                        "(id, room_id, uname, uid, send_time, price, message) "
                        "SELECT id, room_id, uname, uid, send_time, price, message "
                        "FROM `super_chat_log` "
                        "WHERE send_time >= :start AND send_time < :end"
                    ),
                    {"start": start_dt, "end": end_dt},
                )
                conn.execute(
                    text(
                        "DELETE FROM `super_chat_log` "
                        "WHERE send_time >= :start AND send_time < :end"
                    ),
                    {"start": start_dt, "end": end_dt},
                )
                moved_total += int(count or 0)
            logging.info(f"[SuperChatLog] 已归档 {month_code}，记录数 ~{count}")
        except SQLAlchemyError as e:
            logging.error(f"[SuperChatLog] 归档失败 {month_code}: {e}")
    return moved_total


def archive_live_session(target_month: Optional[str] = None) -> int:
    """
    将 live_session 中历史月份数据迁移到归档表。
    - target_month: 指定归档月份（YYYYMM）；None 表示归档所有早于当前月的数据
    返回迁移的记录数（预估）。
    """
    current_month = month_str()
    months: list[str] = []
    if target_month:
        normalized = normalize_month_code(target_month)
        if not normalized:
            logging.error(f"[LiveSession] 归档月份格式非法: {target_month}")
            return 0
        if normalized == current_month:
            logging.info("[LiveSession] 当前月不归档，跳过")
            return 0
        months = [normalized]
    else:
        session = Session()
        try:
            rows = session.execute(
                text(
                    "SELECT DISTINCT month "
                    "FROM live_session "
                    "WHERE month < :current_month"
                ),
                {"current_month": current_month},
            ).fetchall()
            for (m,) in rows:
                normalized = normalize_month_code(m)
                if normalized and normalized != current_month:
                    months.append(normalized)
        except SQLAlchemyError as e:
            logging.error(f"[LiveSession] 读取待归档月份失败: {e}")
            return 0
        finally:
            session.close()

    if not months:
        return 0

    moved_total = 0
    for month_code in sorted(set(months)):
        table_name = ensure_live_session_archive_table(month_code)
        try:
            with engine.begin() as conn:
                count = conn.execute(
                    text(
                        "SELECT COUNT(1) FROM `live_session` "
                        "WHERE month = :month AND end_time IS NOT NULL"
                    ),
                    {"month": month_code},
                ).scalar()
                if not count:
                    continue
                conn.execute(
                    text(
                        f"INSERT IGNORE INTO `{table_name}` "
                        "(id, room_id, start_time, end_time, title, gift, guard, super_chat, month, "
                        "danmaku_count, start_guard_1, start_guard_2, start_guard_3, start_fans_count, "
                        "start_attention, end_guard_1, end_guard_2, end_guard_3, end_fans_count, "
                        "end_attention, "
                        "avg_concurrency, max_concurrency) "
                        "SELECT id, room_id, start_time, end_time, title, gift, guard, super_chat, month, "
                        "danmaku_count, start_guard_1, start_guard_2, start_guard_3, start_fans_count, "
                        "start_attention, end_guard_1, end_guard_2, end_guard_3, end_fans_count, "
                        "end_attention, "
                        "avg_concurrency, max_concurrency "
                        "FROM `live_session` "
                        "WHERE month = :month AND end_time IS NOT NULL"
                    ),
                    {"month": month_code},
                )
                conn.execute(
                    text(
                        "DELETE FROM `live_session` "
                        "WHERE month = :month AND end_time IS NOT NULL"
                    ),
                    {"month": month_code},
                )
                moved_total += int(count or 0)
            logging.info(f"[LiveSession] 已归档 {month_code}，记录数 ~{count}")
        except SQLAlchemyError as e:
            logging.error(f"[LiveSession] 归档失败 {month_code}: {e}")
    return moved_total


def archive_room_live_stats(target_month: Optional[str] = None) -> int:
    """
    将 room_live_stats 中历史月份数据迁移到归档表。
    - target_month: 指定归档月份（YYYYMM）；None 表示归档所有早于当前月的数据
    返回迁移的记录数（预估）。
    """
    current_month = month_str()
    start_current, _ = month_range(current_month)
    cutoff_dt = datetime.datetime.combine(start_current, datetime.time.min)

    months: list[str] = []
    if target_month:
        normalized = normalize_month_code(target_month)
        if not normalized:
            logging.error(f"[RoomLiveStats] 归档月份格式非法: {target_month}")
            return 0
        if normalized == current_month:
            logging.info("[RoomLiveStats] 当前月不归档，跳过")
            return 0
        months = [normalized]
    else:
        session = Session()
        try:
            rows = session.execute(
                text(
                    "SELECT DATE_FORMAT(date, '%Y%m') AS m "
                    "FROM room_live_stats "
                    "WHERE date < :cutoff "
                    "GROUP BY m"
                ),
                {"cutoff": cutoff_dt},
            ).fetchall()
            for (m,) in rows:
                normalized = normalize_month_code(m)
                if normalized and normalized != current_month:
                    months.append(normalized)
        except SQLAlchemyError as e:
            logging.error(f"[RoomLiveStats] 读取待归档月份失败: {e}")
            return 0
        finally:
            session.close()

    if not months:
        return 0

    moved_total = 0
    for month_code in sorted(set(months)):
        start_date, end_date = month_range(month_code)
        start_dt = datetime.datetime.combine(start_date, datetime.time.min)
        end_dt = datetime.datetime.combine(end_date, datetime.time.min)
        table_name = ensure_room_live_stats_archive_table(month_code)
        try:
            with engine.begin() as conn:
                count = conn.execute(
                    text(
                        "SELECT COUNT(1) FROM `room_live_stats` "
                        "WHERE date >= :start AND date < :end"
                    ),
                    {"start": start_dt, "end": end_dt},
                ).scalar()
                if not count:
                    continue
                conn.execute(
                    text(
                        f"INSERT IGNORE INTO `{table_name}` "
                        "(room_id, date, duration) "
                        "SELECT room_id, date, duration "
                        "FROM `room_live_stats` "
                        "WHERE date >= :start AND date < :end"
                    ),
                    {"start": start_dt, "end": end_dt},
                )
                conn.execute(
                    text(
                        "DELETE FROM `room_live_stats` "
                        "WHERE date >= :start AND date < :end"
                    ),
                    {"start": start_dt, "end": end_dt},
                )
                moved_total += int(count or 0)
            logging.info(f"[RoomLiveStats] 已归档 {month_code}，记录数 ~{count}")
        except SQLAlchemyError as e:
            logging.error(f"[RoomLiveStats] 归档失败 {month_code}: {e}")
    return moved_total


def archive_attention(target_month: Optional[str] = None) -> int:
    """
    将 attention 中历史月份数据迁移到归档表。
    - target_month: 指定归档月份（YYYYMM）；None 表示归档所有早于当前月的数据
    返回迁移的记录数（预估）。
    """
    current_month = month_str()
    months: list[str] = []
    if target_month:
        normalized = normalize_month_code(target_month)
        if not normalized:
            logging.error(f"[Attention] 归档月份格式非法: {target_month}")
            return 0
        if normalized == current_month:
            logging.info("[Attention] 当前月不归档，跳过")
            return 0
        months = [normalized]
    else:
        session = Session()
        try:
            rows = session.execute(
                text(
                    "SELECT DATE_FORMAT(`date`, '%Y%m') AS m "
                    "FROM attention "
                    "WHERE `date` < DATE_FORMAT(CURDATE(), '%Y-%m-01') "
                    "GROUP BY m"
                )
            ).fetchall()
            for (m,) in rows:
                normalized = normalize_month_code(m)
                if normalized and normalized != current_month:
                    months.append(normalized)
        except SQLAlchemyError as e:
            logging.error(f"[Attention] 读取待归档月份失败: {e}")
            return 0
        finally:
            session.close()

    if not months:
        return 0

    moved_total = 0
    for month_code in sorted(set(months)):
        start_date, end_date = month_range(month_code)
        table_name = ensure_attention_archive_table(month_code)
        try:
            source_columns = {
                col.get("name") for col in inspect(engine).get_columns("attention")
            }
            archive_columns = {
                col.get("name") for col in inspect(engine).get_columns(table_name)
            }
            copy_columns = [
                name
                for name in (
                    "room_id",
                    "date",
                    "attention",
                    "guard_1",
                    "guard_2",
                    "guard_3",
                    "fans_count",
                )
                if name in source_columns and name in archive_columns
            ]
            quoted_columns = ", ".join(f"`{name}`" for name in copy_columns)
            update_clause = ", ".join(
                f"`{name}` = VALUES(`{name}`)"
                for name in copy_columns
                if name not in ("room_id", "date")
            )
            with engine.begin() as conn:
                count = conn.execute(
                    text(
                        "SELECT COUNT(1) FROM `attention` "
                        "WHERE `date` >= :start_date AND `date` < :end_date"
                    ),
                    {"start_date": start_date, "end_date": end_date},
                ).scalar()
                if not count:
                    continue
                conn.execute(
                    text(
                        f"INSERT INTO `{table_name}` ({quoted_columns}) "
                        f"SELECT {quoted_columns} "
                        "FROM `attention` "
                        "WHERE `date` >= :start_date AND `date` < :end_date "
                        f"ON DUPLICATE KEY UPDATE {update_clause}"
                    ),
                    {"start_date": start_date, "end_date": end_date},
                )
                conn.execute(
                    text(
                        "DELETE FROM `attention` "
                        "WHERE `date` >= :start_date AND `date` < :end_date"
                    ),
                    {"start_date": start_date, "end_date": end_date},
                )
                moved_total += int(count or 0)
            logging.info(f"[Attention] 已归档 {month_code}，记录数 ~{count}")
        except SQLAlchemyError as e:
            logging.error(f"[Attention] 归档失败 {month_code}: {e}")
    return moved_total
