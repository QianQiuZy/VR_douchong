import logging

from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError

from .. import DanmakuCounts
from ..database import Session


def update_start_counts(
    model,
    session_id: int,
    guard_1: int | None = None,
    guard_2: int | None = None,
    guard_3: int | None = None,
    fans_count: int | None = None,
) -> None:
    if not session_id:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return
        if guard_1 is not None:
            row.start_guard_1 = guard_1
        if guard_2 is not None:
            row.start_guard_2 = guard_2
        if guard_3 is not None:
            row.start_guard_3 = guard_3
        if fans_count is not None:
            row.start_fans_count = fans_count
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] update_start_counts 失败: {exc}")
    finally:
        session.close()


def update_end_counts(
    model,
    session_id: int,
    guard_1: int | None = None,
    guard_2: int | None = None,
    guard_3: int | None = None,
    fans_count: int | None = None,
) -> None:
    if not session_id:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return
        if guard_1 is not None:
            row.end_guard_1 = guard_1
        if guard_2 is not None:
            row.end_guard_2 = guard_2
        if guard_3 is not None:
            row.end_guard_3 = guard_3
        if fans_count is not None:
            row.end_fans_count = fans_count
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] update_end_counts 失败: {exc}")
    finally:
        session.close()


def update_start_attention(model, session_id: int, attention: int | None = None) -> None:
    if not session_id or attention is None:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return
        row.start_attention = int(attention)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] update_start_attention 失败: {exc}")
    finally:
        session.close()


def update_end_attention(model, session_id: int, attention: int | None = None) -> None:
    if not session_id or attention is None:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return
        row.end_attention = int(attention)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] update_end_attention 失败: {exc}")
    finally:
        session.close()


def _add_danmaku_counts(row, counts: DanmakuCounts) -> None:
    row.danmaku_count = (row.danmaku_count or 0) + counts.total
    row.captain_danmaku_count = (row.captain_danmaku_count or 0) + counts.captain
    row.admiral_danmaku_count = (row.admiral_danmaku_count or 0) + counts.admiral
    row.governor_danmaku_count = (row.governor_danmaku_count or 0) + counts.governor
    row.normal_danmaku_count = (row.normal_danmaku_count or 0) + counts.normal


def add_danmaku_by_id(model, session_id: int, counts: DanmakuCounts) -> bool:
    if not session_id or counts.total <= 0:
        return counts.total <= 0
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).first()
        if not row:
            return False
        _add_danmaku_counts(row, counts)
        session.commit()
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] add_danmaku_by_id 失败: {exc}")
        return False
    finally:
        session.close()


def add_danmaku_by_room_open(model, room_id: int, counts: DanmakuCounts) -> bool:
    if counts.total <= 0:
        return True
    session = Session()
    try:
        row = (
            session.query(model)
            .filter(and_(model.room_id == room_id, model.end_time.is_(None)))
            .order_by(model.start_time.desc())
            .first()
        )
        if not row:
            return False
        _add_danmaku_counts(row, counts)
        session.commit()
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[LiveSession] add_danmaku_by_room_open 失败: {exc}")
        return False
    finally:
        session.close()
