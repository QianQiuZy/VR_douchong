"""Persistence helpers for session-relative 15-minute aggregates."""

from __future__ import annotations

import datetime
import logging

from sqlalchemy import func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from .. import DanmakuBucketTarget, DanmakuCounts
from ..database import Session

DANMAKU_COLUMNS = {
    "danmaku_count",
    "captain_danmaku_count",
    "admiral_danmaku_count",
    "governor_danmaku_count",
    "normal_danmaku_count",
}


def upsert_stats(
    model,
    session_id: int,
    room_id: int,
    month: str,
    bucket_index: int,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    gift: float,
    guard: float,
    super_chat: float,
    blind_box_count: int,
    blind_box_profit: int,
    danmaku_count: int,
    captain_danmaku_count: int,
    admiral_danmaku_count: int,
    governor_danmaku_count: int,
    normal_danmaku_count: int,
    avg_concurrency: float | None,
    max_concurrency: int | None,
    sample_count: int,
    payer_count: int,
) -> bool:
    """Replace one completed bucket idempotently."""
    session = Session()
    try:
        values = {
            "session_id": session_id,
            "room_id": room_id,
            "month": month,
            "bucket_index": bucket_index,
            "start_time": start_time,
            "end_time": end_time,
            "gift": gift,
            "guard": guard,
            "super_chat": super_chat,
            "blind_box_count": blind_box_count,
            "blind_box_profit": blind_box_profit,
            "danmaku_count": danmaku_count,
            "captain_danmaku_count": captain_danmaku_count,
            "admiral_danmaku_count": admiral_danmaku_count,
            "governor_danmaku_count": governor_danmaku_count,
            "normal_danmaku_count": normal_danmaku_count,
            "avg_concurrency": avg_concurrency,
            "max_concurrency": max_concurrency,
            "sample_count": sample_count,
            "payer_count": payer_count,
        }
        stmt = insert(model).values(**values).on_duplicate_key_update(**{
            key: values[key]
            for key in values
            if key not in {"session_id", "bucket_index", *DANMAKU_COLUMNS}
        })
        _ = session.execute(stmt)
        session.commit()
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error("[LiveSession15m] 写入失败 session_id=%s bucket=%s: %s", session_id, bucket_index, exc)
        return False
    finally:
        session.close()


def add_danmaku_counts(
    model,
    target: DanmakuBucketTarget,
    counts: DanmakuCounts,
) -> bool:
    session = Session()
    try:
        stmt = insert(model).values(
            session_id=target.session_id,
            room_id=target.room_id,
            month=target.month,
            bucket_index=target.bucket_index,
            start_time=target.start_time,
            end_time=target.end_time,
            gift=0.0,
            guard=0.0,
            super_chat=0.0,
            blind_box_count=0,
            blind_box_profit=0,
            danmaku_count=counts.total,
            captain_danmaku_count=counts.captain,
            admiral_danmaku_count=counts.admiral,
            governor_danmaku_count=counts.governor,
            normal_danmaku_count=counts.normal,
            avg_concurrency=None,
            max_concurrency=None,
            sample_count=0,
            payer_count=0,
        ).on_duplicate_key_update(
            danmaku_count=func.coalesce(model.danmaku_count, 0) + counts.total,
            captain_danmaku_count=func.coalesce(model.captain_danmaku_count, 0)
            + counts.captain,
            admiral_danmaku_count=func.coalesce(model.admiral_danmaku_count, 0)
            + counts.admiral,
            governor_danmaku_count=func.coalesce(model.governor_danmaku_count, 0)
            + counts.governor,
            normal_danmaku_count=func.coalesce(model.normal_danmaku_count, 0)
            + counts.normal,
        )
        session.execute(stmt)
        session.commit()
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(
            "[LiveSession15m] 弹幕写入失败 session_id=%s bucket=%s: %s",
            target.session_id,
            target.bucket_index,
            exc,
        )
        return False
    finally:
        session.close()
