import datetime
import json
import logging

from sqlalchemy import and_, case, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only

from .. import config, danmaku_metrics, rounded_concurrency, runtime_state
from ..database import Session
from ..redis_metrics import delete_session_keys
from .history import notify_history_change
from .tables import month_str

logger = logging.getLogger(__name__)


def _restore_duration_state(row) -> None:
    ledger = json.loads(row.duration_ledger or "{}")
    room_id = int(row.room_id)
    if stamp := ledger.get("last_live_at"):
        runtime_state.LAST_LIVE_OBSERVATIONS[int(row.id)] = datetime.datetime.fromisoformat(stamp)
    if not row.duration_valid:
        runtime_state.INVALID_DURATION_SESSIONS[room_id] = int(row.id)
    else:
        runtime_state.INVALID_DURATION_SESSIONS.pop(room_id, None)
    if ledger.get("segment_end"):
        runtime_state.PENDING_SESSION_ENDS[room_id] = datetime.datetime.fromisoformat(ledger["segment_end"])
        runtime_state.STREAM_STARTS.pop(room_id, None)
        if not row.duration_valid:
            runtime_state.FORCED_OFFLINE_AT[room_id] = runtime_state.PENDING_SESSION_ENDS[room_id]
    elif ledger.get("segment_start"):
        runtime_state.STREAM_STARTS[room_id] = datetime.datetime.fromisoformat(ledger["segment_start"])


def checkpoint_live_observations(model) -> None:
    observations = {
        sid: runtime_state.LAST_LIVE_OBSERVATIONS[sid]
        for sid in runtime_state.CURRENT_SESSIONS.values()
        if sid in runtime_state.LAST_LIVE_OBSERVATIONS
    }
    if not observations:
        return
    try:
        with Session.begin() as session:
            rows = (
                session.query(model).options(load_only(model.id, model.duration_ledger))
                .filter(model.id.in_(observations), model.end_time.is_(None))
                .order_by(model.id).with_for_update()
            )
            for row in rows:
                ledger = json.loads(row.duration_ledger or '{"days": {}}')
                stamp = observations[row.id].isoformat()
                if stamp > ledger.get("last_live_at", ""):
                    ledger["last_live_at"] = stamp
                    row.duration_ledger = json.dumps(ledger)
    except SQLAlchemyError as exc:
        logger.warning("[LiveSession] 在播检查点保存失败，下轮重试 error_type=%s", type(exc).__name__)


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
            _restore_duration_state(open_row)
            logger.info(
                "[LiveSession] 恢复未结束场次 room_id=%s session_id=%s",
                room_id,
                open_row.id,
            )
            return open_row.id
        row = model(
            room_id=room_id,
            start_time=start_dt,
            end_time=None,
            emoticon_danmaku_count=0,
            danmaku_user_count=0 if danmaku_metrics.covered(start_dt) else None,
            danmaku_status="collecting",
            title=title or "",
            month=month_str(start_dt),
            duration_ledger=json.dumps({"days": {}, "segment_days": {}, "segment_start": start_dt.isoformat()}),
        )
        session.add(row)
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
        runtime_state.INVALID_DURATION_SESSIONS.pop(room_id, None)
        return row.id
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] start_session 失败: {exc}")
        return None
    finally:
        session.close()


def open_room_ids(model, rooms: list[int]) -> set[int]:
    with Session() as session:
        return {room for room, in session.query(model.room_id).filter(
            model.room_id.in_(rooms), model.end_time.is_(None)
        ).distinct()}


def fill_missing_title(model, session_id: int, title: str) -> None:
    with Session.begin() as session:
        row = session.query(model).filter_by(id=session_id, end_time=None, title="").first()
        if row is None:
            return
        row.title = title
        month = row.month
    notify_history_change(month)


def find_open_session(model, room_id: int, *, strict: bool = False) -> tuple[int, datetime.datetime] | None:
    session = Session()
    try:
        row = (
            session.query(model)
            .filter(and_(model.room_id == room_id, model.end_time.is_(None)))
            .order_by(model.start_time.desc())
            .first()
        )
        if row is None:
            return None
        _restore_duration_state(row)
        return int(row.id), row.start_time
    except SQLAlchemyError as exc:
        if strict:
            raise
        logger.error(f"[LiveSession] find_open_session 失败: {exc}")
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
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] add_values_by_id 失败: {exc}")
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
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] add_values_by_room_open 失败: {exc}")
    finally:
        session.close()


def close_session_by_id(model, session_id: int | None, end_dt: datetime.datetime) -> None:
    if not session_id:
        return
    session = Session()
    try:
        row = session.query(model).filter_by(id=session_id).with_for_update().first()
        if row and row.end_time is None:
            row.end_time = end_dt
            row.danmaku_status = "pending"
            pending = runtime_state.DANMAKU_PENDING.get(row.room_id)
            settlement = None
            if row.room_id not in runtime_state.DANMAKU_RECOVERY_ROOMS and (not pending or session_id not in pending.sessions):
                settlement = danmaku_metrics.settle(row, f"session:{session_id}", row.start_time)
            changed_month = getattr(row, "month", None)
            session.commit()
            delete_session_keys(session_id)
            if settlement:
                settlement.committed()
            notify_history_change(changed_month)
    except SQLAlchemyError:
        session.rollback()
        raise
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
            row.avg_concurrency = rounded_concurrency(avg_concurrency)
        if max_concurrency is not None:
            row.max_concurrency = int(max_concurrency)
        changed_month = getattr(row, "month", None)
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error(f"[LiveSession] update_concurrency_by_id 失败: {exc}")
    finally:
        session.close()


def set_payer_count(model, session_id: int | None, count: int) -> None:
    """Persist an absolute session payer cardinality."""
    if not session_id:
        return
    session = Session()
    try:
        stmt = (
            update(model)
            .where(model.id == session_id)
            .values(payer_count=case((model.payer_count < int(count), int(count)), else_=model.payer_count))
        )
        session.execute(stmt)
        changed_month = None
        if config.API_CACHE_ENABLED:
            from ..metrics_runtime import session_month

            changed_month = session_month(session_id)
            if changed_month is None:
                changed_month = session.query(model.month).filter_by(id=session_id).scalar()
        session.commit()
        notify_history_change(changed_month)
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("[LiveSession] payer_count写入失败 session_id=%s: %s", session_id, exc)
    finally:
        session.close()
