import datetime
import logging
from collections.abc import Sequence
from typing import ClassVar, Protocol, SupportsInt

from sqlalchemy import (
    Column,
    Integer,
    and_,
    bindparam,
    case,
    column,
    func,
    inspect,
    text,
    type_coerce,
)
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session as OrmSession

from ..database import Session, engine
from .tables import (
    ReportTableMetadata,
    ensure_room_live_stats_archive_table,
    is_current_month,
    month_range,
    month_str,
    room_live_stats_table_name,
    sc_log_table_exists,
)

logger = logging.getLogger(__name__)


class _LiveStatsAggregateModel(Protocol):
    room_id: ClassVar[Column[int]]
    date: ClassVar[Column[datetime.date]]
    duration: ClassVar[Column[int]]
    steel_coin_count: ClassVar[Column[int]]


class _TupleRows[T](Protocol):
    def all(self) -> Sequence[T]: ...
    def one(self) -> T: ...


class _TupleResult[T](Protocol):
    def tuples(self) -> _TupleRows[T]: ...


class _ScalarResult[T](Protocol):
    def scalar(self) -> T | None: ...


type _IntegerLike = SupportsInt | str | bytes | bytearray


class _IntegerPairRows(Protocol):
    def one(self) -> tuple[_IntegerLike, _IntegerLike]: ...


class _IntegerPairResult(Protocol):
    def tuples(self) -> _IntegerPairRows: ...


def _tuple_rows[T](result: _TupleResult[T]) -> Sequence[T]:
    return result.tuples().all()


def _scalar_value[T](result: _ScalarResult[T]) -> T | None:
    return result.scalar()


def _archive_aggregate_values(result: _IntegerPairResult) -> tuple[int, int]:
    total_sec, eff_days = result.tuples().one()
    return int(total_sec), int(eff_days)


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
                logger.warning(f"[RoomLiveStats] 第 {attempt + 1} 次尝试 add_duration 失败: {exc}")
            finally:
                try:
                    session.close()
                except Exception as close_error:
                    logger.exception("[db] session close failed error_type=%s", type(close_error).__name__, exc_info=False)
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
                logger.warning(f"[RoomLiveStats] 第 {attempt + 1} 次尝试 add_duration 失败: {exc}")
            finally:
                try:
                    session.close()
                except Exception as close_error:
                    logger.exception("[db] session close failed error_type=%s", type(close_error).__name__, exc_info=False)
    logger.error("[RoomLiveStats] add_duration 最终失败，数据可能不完整。")


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
        logger.error("[RoomLiveStats] 日统计写入失败 room_id=%s date=%s: %s", room_id, date_value, exc)
    finally:
        session.close()


def month_aggregate_for_month(
    model: type[_LiveStatsAggregateModel],
    room_id: int | list[int],
    month: str,
    session: OrmSession | None = None,
    metadata: ReportTableMetadata | None = None,
) -> tuple[int, int] | dict[int, tuple[int, int]]:
    owns_session = session is None
    session = Session() if owns_session else session
    try:
        start, end = month_range(month)
        table_name = room_live_stats_table_name(month)
        if not is_current_month(month) and (
            metadata.has_table(table_name) if metadata else sc_log_table_exists(table_name)
        ):
            if isinstance(room_id, list):
                archive_rows: Sequence[tuple[int, int, int]] = _tuple_rows(session.execute(
                    text("".join((
                        "SELECT room_id, COALESCE(SUM(duration), 0), ",
                        "COALESCE(SUM(CASE WHEN duration >= 7200 THEN 1 ELSE 0 END), 0) ",
                        f"FROM `{table_name}` WHERE room_id IN :room_ids ",
                        "AND date >= :start AND date < :end GROUP BY room_id",
                    ))).bindparams(bindparam("room_ids", expanding=True)).columns(column("room_id", Integer), column("total_sec", Integer), column("eff_days", Integer)),
                    {"room_ids": room_id, "start": start, "end": end},
                ))
                return {row_id: (int(total_sec or 0), int(eff_days or 0)) for row_id, total_sec, eff_days in archive_rows}
            total_sec, eff_days = _archive_aggregate_values(session.execute(
                text(
                    f"SELECT COALESCE(SUM(duration), 0) AS total_sec, "
                    "COALESCE(SUM(CASE WHEN duration >= 7200 THEN 1 ELSE 0 END), 0) AS eff_days "
                    f"FROM `{table_name}` WHERE room_id = :room_id AND date >= :start AND date < :end"
                ).columns(column("total_sec", Integer), column("eff_days", Integer)),
                {"room_id": room_id, "start": start, "end": end},
            ))
            return int(total_sec or 0), int(eff_days or 0)
        if isinstance(room_id, list):
            current_rows: Sequence[tuple[int, int, int]] = _tuple_rows(session.query(
                model.room_id,
                type_coerce(func.coalesce(func.sum(model.duration), 0), Integer),
                type_coerce(func.coalesce(func.sum(case((model.duration >= 7200, 1), else_=0)), 0), Integer),
            ).filter(
                and_(model.room_id.in_(room_id), model.date >= start, model.date < end)
            ).group_by(model.room_id))
            return {row_id: (int(total_sec or 0), int(eff_days or 0)) for row_id, total_sec, eff_days in current_rows}
        total_sec, eff_days = session.query(
            type_coerce(func.coalesce(func.sum(model.duration), 0), Integer),
            type_coerce(func.coalesce(func.sum(case((model.duration >= 7200, 1), else_=0)), 0), Integer),
        ).filter(and_(model.room_id == room_id, model.date >= start, model.date < end)).tuples().one()
        return int(total_sec), int(eff_days)
    except SQLAlchemyError as exc:
        if not owns_session: raise
        session.rollback()
        logger.error(f"[RoomLiveStats] month_aggregate_for_month 读取失败: {exc}")
        return {} if isinstance(room_id, list) else (0, 0)
    finally:
        if owns_session: session.close()


def month_steel_coin_for_month(
    model: type[_LiveStatsAggregateModel],
    room_id: int | list[int],
    month: str,
    session: OrmSession | None = None,
    metadata: ReportTableMetadata | None = None,
) -> int | dict[int, int]:
    owns_session = session is None
    session = Session() if owns_session else session
    try:
        start, end = month_range(month)
        table_name = room_live_stats_table_name(month)
        if not is_current_month(month) and (
            metadata.has_table(table_name) if metadata else sc_log_table_exists(table_name)
        ):
            columns = (
                metadata.column_names(table_name)
                if metadata
                else frozenset(column.get("name") for column in inspect(engine).get_columns(table_name))
            )
            if "steel_coin_count" not in columns:
                return {} if isinstance(room_id, list) else 0
            if isinstance(room_id, list):
                archive_rows: Sequence[tuple[int, int]] = _tuple_rows(session.execute(
                    text("".join((
                        "SELECT room_id, COALESCE(SUM(`steel_coin_count`), 0) ",
                        f"FROM `{table_name}` WHERE room_id IN :room_ids ",
                        "AND date >= :start AND date < :end GROUP BY room_id",
                    ))).bindparams(bindparam("room_ids", expanding=True)).columns(column("room_id", Integer), column("steel_coin_count", Integer)),
                    {"room_ids": room_id, "start": start, "end": end},
                ))
                return {row_id: int(value or 0) for row_id, value in archive_rows}
            archive_value: int | None = _scalar_value(session.execute(
                text(
                    f"SELECT COALESCE(SUM(`steel_coin_count`), 0) FROM `{table_name}` "
                    "WHERE room_id = :room_id AND date >= :start AND date < :end"
                ).columns(column("steel_coin_count", Integer)),
                {"room_id": room_id, "start": start, "end": end},
            ))
            return int(archive_value or 0)
        if isinstance(room_id, list):
            current_rows: Sequence[tuple[int, int]] = _tuple_rows(session.query(
                model.room_id,
                type_coerce(func.coalesce(func.sum(model.steel_coin_count), 0), Integer),
            ).filter(
                and_(model.room_id.in_(room_id), model.date >= start, model.date < end)
            ).group_by(model.room_id))
            return {row_id: int(value or 0) for row_id, value in current_rows}
        current_value: int | None = _scalar_value(session.query(
            type_coerce(func.coalesce(func.sum(model.steel_coin_count), 0), Integer)
        ).filter(and_(model.room_id == room_id, model.date >= start, model.date < end)))
        return int(current_value or 0)
    except SQLAlchemyError as exc:
        if not owns_session: raise
        session.rollback()
        logger.error(f"[RoomLiveStats] month_steel_coin_for_month 读取失败: {exc}")
        return {} if isinstance(room_id, list) else 0
    finally:
        if owns_session: session.close()
