"""Persistence helpers for session-relative 15-minute aggregates."""

from __future__ import annotations

import datetime
import logging

from sqlalchemy import func, inspect, or_, select, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from .. import DanmakuBucketTarget, DanmakuCounts
from .. import danmaku_metrics, rounded_concurrency
from ..database import Session
from .danmaku import claim_replay, increments, upsert_fields
from .history import notify_history_change
from .tables import month_str, normalize_month_code

logger = logging.getLogger(__name__)

DANMAKU_COLUMNS = {
    "danmaku_count",
    "emoticon_danmaku_count",
    "captain_danmaku_count",
    "admiral_danmaku_count",
    "governor_danmaku_count",
    "normal_danmaku_count",
}


def read_open_metrics(model, parent, session_id: int):
    """Summarize all samples, then load just the last metric bucket in the same transaction."""
    # Open sessions stay in the hot tables across month boundaries. Older rows
    # only contain a rounded average and sample count.
    sample_sum = func.coalesce(model.concurrency_total, func.round(model.avg_concurrency * model.sample_count))
    with Session() as session:
        parent_payers = select(parent.payer_count).where(parent.id == session_id).scalar_subquery()
        total, samples, maximum, payers = session.execute(
            select(func.sum(sample_sum), func.sum(model.sample_count), func.max(model.max_concurrency), parent_payers)
            .where(model.session_id == session_id)
        ).one()
        # Danmaku has an independent write path and can create later placeholder
        # rows while the last payment/concurrency interval is still partial.
        latest = session.execute(
            select(model, sample_sum).where(model.session_id == session_id, or_(
                model.sample_count > 0, model.gift != 0, model.guard != 0, model.super_chat != 0,
                model.blind_box_count != 0, model.blind_box_profit != 0, model.payer_count > 0,
            )).order_by(model.bucket_index.desc()).limit(1)
        ).first()
    return latest, int(total or 0), int(samples or 0), int(maximum or 0), int(payers or 0)


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
    concurrency_total: int | None = None,
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
            "emoticon_danmaku_count": 0 if danmaku_count == 0 else None,
            "captain_danmaku_count": captain_danmaku_count,
            "admiral_danmaku_count": admiral_danmaku_count,
            "governor_danmaku_count": governor_danmaku_count,
            "normal_danmaku_count": normal_danmaku_count,
            "danmaku_user_count": 0 if danmaku_metrics.covered(start_time) else None,
            "avg_concurrency": rounded_concurrency(avg_concurrency),
            "max_concurrency": max_concurrency,
            "sample_count": sample_count,
            "concurrency_total": concurrency_total,
            "payer_count": payer_count,
        }
        replace = {
            key: value
            for key, value in values.items()
            if key not in {"session_id", "bucket_index", "danmaku_user_count", *DANMAKU_COLUMNS}
        }
        archived = _find_archived_bucket(session, month, session_id, bucket_index)
        if archived is None:
            session.execute(insert(model).values(**values).on_duplicate_key_update(**replace))
        else:
            archive, available = archived
            replace = {key: value for key, value in replace.items() if key in available}
            assignments = ", ".join(f"`{name}` = :{name}" for name in replace)
            session.execute(text(
                f"UPDATE `{archive}` SET {assignments} WHERE session_id = :session_id "
                "AND bucket_index = :bucket_index"
            ), {**replace, "session_id": session_id, "bucket_index": bucket_index})
        session.commit()
        notify_history_change(month)
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("[LiveSession15m] 写入失败 session_id=%s bucket=%s: %s", session_id, bucket_index, exc)
        return False
    finally:
        session.close()


def _find_archived_bucket(
    session, month: str, session_id: int, bucket_index: int,
) -> tuple[str, set[str]] | None:
    if normalize_month_code(month) != month or month >= month_str():
        return None
    key = {"session_id": session_id, "bucket_index": bucket_index, "month": month}
    # Serialize historical writes with the archive's copy/delete transaction.
    # A locking read sees the current row even if the archive committed while waiting.
    session.execute(text(
        "SELECT id FROM live_session WHERE id = :session_id AND month = :month FOR UPDATE"
    ), key).scalar()
    hot = session.execute(text(
        "SELECT session_id FROM live_session_15m_stats WHERE session_id = :session_id "
        "AND bucket_index = :bucket_index FOR UPDATE"
    ), key).scalar()
    if hot is not None:
        return None  # A full hot correction takes precedence over the archive.
    conn = session.connection()
    archive = f"live_session_15m_stats_{month}"
    inspector = inspect(conn)
    if not inspector.has_table(archive):
        return None
    found = session.execute(text(
        f"SELECT session_id FROM `{archive}` WHERE session_id = :session_id "
        "AND bucket_index = :bucket_index FOR UPDATE"
    ), key).scalar()
    if found is None:
        return None
    return archive, {column["name"] for column in inspector.get_columns(archive)}


def _add_to_archived_bucket(session, target: DanmakuBucketTarget, counts: DanmakuCounts) -> bool:
    archived = _find_archived_bucket(session, target.month, target.session_id, target.bucket_index)
    if archived is None:
        return False
    archive, available = archived
    # Legacy archives deliberately lack some subscriber breakdowns. Preserve
    # their missing-column semantics while still updating the known totals.
    deltas = {name: amount for name, amount in increments(counts).items() if name in available}
    if "danmaku_count" not in deltas:
        raise SQLAlchemyError("Archived bucket missing danmaku_count")
    assignments = ", ".join(f"`{name}` = COALESCE(`{name}`, 0) + :{name}" for name in deltas)
    if "emoticon_danmaku_count" in available:
        assignments = (
            "emoticon_danmaku_count = CASE WHEN danmaku_count = 0 THEN :emoticon "
            "ELSE emoticon_danmaku_count + :emoticon END, " + assignments
        )
        deltas["emoticon"] = counts.emoticon
    if counts.audience is not None and "danmaku_user_count" in available:
        # The total assignment above has already applied this delta.
        assignments += ", danmaku_user_count = CASE WHEN danmaku_count = :covered_total THEN :senders END"
        deltas.update(covered_total=counts.audience.total, senders=counts.audience.count)
    session.execute(text(
        f"UPDATE `{archive}` SET {assignments} WHERE session_id = :session_id "
        "AND bucket_index = :bucket_index"
    ), {**deltas, "session_id": target.session_id, "bucket_index": target.bucket_index})
    return True


def add_danmaku_counts(
    model,
    target: DanmakuBucketTarget,
    counts: DanmakuCounts,
) -> bool:
    session = Session()
    try:
        if not claim_replay(session, counts):
            return True
        if _add_to_archived_bucket(session, target, counts):
            session.commit()
            notify_history_change(target.month)
            return True
        values, updates = upsert_fields(model, counts)
        stmt = insert(model).values(
            **values,
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
            avg_concurrency=None,
            max_concurrency=None,
            sample_count=0,
            payer_count=0,
        ).on_duplicate_key_update(updates)
        session.execute(stmt)
        session.commit()
        notify_history_change(target.month)
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(
            "[LiveSession15m] 弹幕写入失败 session_id=%s bucket=%s: %s",
            target.session_id,
            target.bucket_index,
            exc,
        )
        return False
    finally:
        session.close()
