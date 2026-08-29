import datetime
import logging

from sqlalchemy import and_, func, inspect, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session, engine
from .tables import (
    ensure_room_live_stats_archive_table,
    is_current_month,
    month_range,
    month_str,
    room_live_stats_table_name,
    sc_log_table_exists,
)


def add_duration(model, room_id: int, date_value: datetime.date, seconds: int) -> None:
    month_code = month_str(datetime.datetime.combine(date_value, datetime.time.min))
    if is_current_month(month_code):
        for attempt in range(3):
            session = Session()
            try:
                row = session.query(model).filter_by(room_id=room_id, date=date_value).first()
                if row:
                    row.duration += seconds
                else:
                    session.add(model(room_id=room_id, date=date_value, duration=seconds))
                session.commit()
                return
            except SQLAlchemyError as exc:
                session.rollback()
                logging.warning(f"[RoomLiveStats] 第 {attempt + 1} 次尝试 add_duration 失败: {exc}")
            finally:
                try:
                    session.close()
                except Exception:
                    pass
    else:
        table_name = ensure_room_live_stats_archive_table(month_code)
        for attempt in range(3):
            session = Session()
            try:
                session.execute(
                    text(
                        f"INSERT INTO `{table_name}` (room_id, date, duration) "
                        "VALUES (:room_id, :date, :duration) "
                        "ON DUPLICATE KEY UPDATE duration = duration + :duration"
                    ),
                    {"room_id": room_id, "date": date_value, "duration": seconds},
                )
                session.commit()
                return
            except SQLAlchemyError as exc:
                session.rollback()
                logging.warning(f"[RoomLiveStats] 第 {attempt + 1} 次尝试 add_duration 失败: {exc}")
            finally:
                try:
                    session.close()
                except Exception:
                    pass
    logging.error("[RoomLiveStats] add_duration 最终失败，数据可能不完整。")


def add_daily_metrics(
    model,
    room_id: int,
    date_value: datetime.date,
    gift: float = 0.0,
    guard: float = 0.0,
    super_chat: float = 0.0,
    payer_count: int | None = None,
    steel_coin_delta: int = 0,
) -> None:
    """Immediately upsert room-day money and absolute/delta counters."""
    month_code = month_str(datetime.datetime.combine(date_value, datetime.time.min))
    session = Session()
    try:
        values = {
            "room_id": room_id,
            "date": date_value,
            "duration": 0,
            "gift": gift,
            "guard": guard,
            "super_chat": super_chat,
            "payer_count": int(payer_count or 0),
            "steel_coin_count": int(steel_coin_delta),
        }
        if is_current_month(month_code):
            stmt = insert(model).values(**values).on_duplicate_key_update(
                gift=model.gift + gift,
                guard=model.guard + guard,
                super_chat=model.super_chat + super_chat,
                payer_count=(func.greatest(model.payer_count, int(payer_count)) if payer_count is not None else model.payer_count),
                steel_coin_count=model.steel_coin_count + int(steel_coin_delta),
            )
            _ = session.execute(stmt)
        else:
            table_name = ensure_room_live_stats_archive_table(month_code)
            _ = session.execute(
                text(
                    f"INSERT INTO `{table_name}` "
                    "(room_id, date, duration, gift, guard, super_chat, payer_count, steel_coin_count) "
                    "VALUES (:room_id, :date, 0, :gift, :guard, :super_chat, :payer_count, :steel_coin_count) "
                    "ON DUPLICATE KEY UPDATE "
                    "gift = gift + :gift, guard = guard + :guard, super_chat = super_chat + :super_chat, "
                 "payer_count = CASE WHEN :payer_count_is_set = 1 THEN GREATEST(payer_count, :payer_count) ELSE payer_count END, "
                    "steel_coin_count = steel_coin_count + :steel_coin_delta"
                ),
                {
                    **values,
                    "payer_count_is_set": int(payer_count is not None),
                    "steel_coin_delta": int(steel_coin_delta),
                },
            )
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error("[RoomLiveStats] 日统计写入失败 room_id=%s date=%s: %s", room_id, date_value, exc)
    finally:
        session.close()


def month_aggregate_for_month(model, room_id: int, month: str) -> tuple[int, int]:
    session = Session()
    try:
        start, end = month_range(month)
        if is_current_month(month):
            from sqlalchemy import case, func

            total_sec, eff_days = session.query(
                func.coalesce(func.sum(model.duration), 0),
                func.coalesce(func.sum(case((model.duration >= 7200, 1), else_=0)), 0),
            ).filter(and_(model.room_id == room_id, model.date >= start, model.date < end)).one()
            return int(total_sec), int(eff_days)
        table_name = room_live_stats_table_name(month)
        if sc_log_table_exists(table_name):
            total_sec, eff_days = session.execute(
                text(
                    f"SELECT COALESCE(SUM(duration), 0) AS total_sec, "
                    "COALESCE(SUM(CASE WHEN duration >= 7200 THEN 1 ELSE 0 END), 0) AS eff_days "
                    f"FROM `{table_name}` WHERE room_id = :room_id AND date >= :start AND date < :end"
                ),
                {"room_id": room_id, "start": start, "end": end},
            ).one()
            return int(total_sec or 0), int(eff_days or 0)
        from sqlalchemy import case, func

        total_sec, eff_days = session.query(
            func.coalesce(func.sum(model.duration), 0),
            func.coalesce(func.sum(case((model.duration >= 7200, 1), else_=0)), 0),
        ).filter(and_(model.room_id == room_id, model.date >= start, model.date < end)).one()
        return int(total_sec), int(eff_days)
    except SQLAlchemyError as exc:
        logging.error(f"[RoomLiveStats] month_aggregate_for_month 读取失败: {exc}")
        return 0, 0
    finally:
        session.close()


def month_steel_coin_for_month(model, room_id: int, month: str) -> int:
    session = Session()
    try:
        start, end = month_range(month)
        if is_current_month(month):
            value = (
                session.query(func.coalesce(func.sum(model.steel_coin_count), 0))
                .filter(and_(model.room_id == room_id, model.date >= start, model.date < end))
                .scalar()
            )
            return int(value or 0)
        table_name = room_live_stats_table_name(month)
        if sc_log_table_exists(table_name):
            columns = {column.get("name") for column in inspect(engine).get_columns(table_name)}
            if "steel_coin_count" not in columns:
                return 0
            value = session.execute(
                text(
                    f"SELECT COALESCE(SUM(`steel_coin_count`), 0) FROM `{table_name}` "
                    "WHERE room_id = :room_id AND date >= :start AND date < :end"
                ),
                {"room_id": room_id, "start": start, "end": end},
            ).scalar()
            return int(value or 0)
        value = (
            session.query(func.coalesce(func.sum(model.steel_coin_count), 0))
            .filter(and_(model.room_id == room_id, model.date >= start, model.date < end))
            .scalar()
        )
        return int(value or 0)
    except SQLAlchemyError as exc:
        logging.error(f"[RoomLiveStats] month_steel_coin_for_month 读取失败: {exc}")
        return 0
    finally:
        session.close()
