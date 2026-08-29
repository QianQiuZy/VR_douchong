"""Persistence helpers for session-relative 15-minute aggregates."""

from __future__ import annotations

import datetime
import logging

from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session


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
            "avg_concurrency": avg_concurrency,
            "max_concurrency": max_concurrency,
            "sample_count": sample_count,
            "payer_count": payer_count,
        }
        stmt = insert(model).values(**values).on_duplicate_key_update(**{
            key: values[key]
            for key in values
            if key not in {"session_id", "bucket_index"}
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
