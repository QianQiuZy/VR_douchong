import logging

from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError

from .. import DanmakuCounts
from .. import danmaku_metrics
from ..database import Session
from .history import notify_history_change

logger = logging.getLogger(__name__)


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
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] update_start_counts 失败: {exc}")
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
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] update_end_counts 失败: {exc}")
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
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] update_start_attention 失败: {exc}")
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
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] update_end_attention 失败: {exc}")
    finally:
        session.close()


def _add_danmaku_counts(row, counts: DanmakuCounts) -> None:
    if row.danmaku_count == 0:
        row.emoticon_danmaku_count = counts.emoticon
    elif row.emoticon_danmaku_count is not None:
        row.emoticon_danmaku_count += counts.emoticon
    row.danmaku_count = (row.danmaku_count or 0) + counts.total
    row.captain_danmaku_count = (row.captain_danmaku_count or 0) + counts.captain
    row.admiral_danmaku_count = (row.admiral_danmaku_count or 0) + counts.admiral
    row.governor_danmaku_count = (row.governor_danmaku_count or 0) + counts.governor
    row.normal_danmaku_count = (row.normal_danmaku_count or 0) + counts.normal


def add_danmaku_by_id(model, session_id: int, counts: DanmakuCounts) -> bool:
    if not session_id or (counts.total <= 0 and counts.audience is None):
        return counts.total <= 0
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).with_for_update().first()
        if not row:
            return False
        _add_danmaku_counts(row, counts)
        settlement = None
        if counts.audience is not None:
            row.danmaku_user_count = counts.audience.count if counts.audience.total == row.danmaku_count else None
            row.danmaku_status = "collecting"
            if row.end_time is not None:
                row.danmaku_status = "pending"
                settlement = danmaku_metrics.settle(row, f"session:{session_id}", row.start_time)
        changed_month = getattr(row, "month", None)
        session.commit()
        if settlement:
            settlement.committed()
        notify_history_change(changed_month)
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] add_danmaku_by_id 失败: {exc}")
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
            .with_for_update()
            .first()
        )
        if not row:
            return False
        _add_danmaku_counts(row, counts)
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] add_danmaku_by_room_open 失败: {exc}")
        return False
    finally:
        session.close()
