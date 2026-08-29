import datetime
import logging

from sqlalchemy import and_, text
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session
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
