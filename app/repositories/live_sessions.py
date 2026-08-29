import datetime
import logging

from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session
from .tables import month_str


def start_session(model, room_id: int, start_dt: datetime.datetime, title: str) -> int | None:
    session = Session()
    try:
        open_row = (
            session.query(model)
            .filter(and_(model.room_id == room_id, model.end_time.is_(None)))
            .order_by(model.start_time.desc())
            .first()
        )
        if open_row:
            logging.info(
                "[LiveSession] 恢复未结束场次 room_id=%s session_id=%s",
                room_id,
                open_row.id,
            )
            return open_row.id
        row = model(
            room_id=room_id,
            start_time=start_dt,
            end_time=None,
            title=title or "",
            month=month_str(start_dt),
        )
        session.add(row)
        session.commit()
        return row.id
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] start_session 失败: {exc}")
        return None
    finally:
        session.close()


def add_values_by_id(
    model,
    session_id: int,
    gift: float = 0.0,
    guard: float = 0.0,
    super_chat: float = 0.0,
    blind_box_count: int = 0,
    blind_box_profit: int = 0,
) -> None:
    if not session_id:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return
        row.gift += gift
        row.guard += guard
        row.super_chat += super_chat
        if blind_box_count:
            row.blind_box_count += int(blind_box_count)
        if blind_box_profit:
            row.blind_box_profit += int(blind_box_profit)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] add_values_by_id 失败: {exc}")
    finally:
        session.close()


def add_values_by_room_open(
    model,
    room_id: int,
    gift: float = 0.0,
    guard: float = 0.0,
    super_chat: float = 0.0,
    blind_box_count: int = 0,
    blind_box_profit: int = 0,
) -> None:
    session = Session()
    try:
        row = (
            session.query(model)
            .filter(and_(model.room_id == room_id, model.end_time.is_(None)))
            .order_by(model.start_time.desc())
            .first()
        )
        if not row:
            return
        row.gift += gift
        row.guard += guard
        row.super_chat += super_chat
        if blind_box_count:
            row.blind_box_count += int(blind_box_count)
        if blind_box_profit:
            row.blind_box_profit += int(blind_box_profit)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] add_values_by_room_open 失败: {exc}")
    finally:
        session.close()


def close_session_by_id(model, session_id: int | None, end_dt: datetime.datetime) -> None:
    if not session_id:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if row and row.end_time is None:
            row.end_time = end_dt
            session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] close_session_by_id 失败: {exc}")
    finally:
        session.close()


def update_concurrency_by_id(
    model,
    session_id: int | None,
    avg_concurrency: float | None = None,
    max_concurrency: int | None = None,
) -> None:
    if not session_id:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return
        if avg_concurrency is not None:
            row.avg_concurrency = float(avg_concurrency)
        if max_concurrency is not None:
            row.max_concurrency = int(max_concurrency)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] update_concurrency_by_id 失败: {exc}")
    finally:
        session.close()
