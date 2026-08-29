import datetime
import logging

from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session


def room_info_upsert(model, room_id: int, anchor_name: str | None = None, attention: int | None = None) -> None:
    session = Session()
    try:
        info = session.query(model).filter_by(room_id=room_id).first()
        if info:
            if anchor_name is not None:
                info.anchor_name = anchor_name
            if attention is not None:
                info.attention = attention
        else:
            info = model(room_id=room_id, anchor_name=anchor_name or "", attention=attention or 0)
            session.add(info)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[RoomInfo] upsert 失败: {exc}")
    finally:
        session.close()


def attention_upsert_daily(model, room_id: int, date_value: datetime.date, attention_value: int) -> None:
    session = Session()
    try:
        stmt = insert(model).values(
            room_id=room_id,
            date=date_value,
            attention=int(attention_value),
        ).on_duplicate_key_update(attention=int(attention_value))
        session.execute(stmt)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[Attention] upsert_daily 失败: {exc}")
    finally:
        session.close()


def attention_upsert_daily_guards(
    model, room_id: int, date_value: datetime.date, guard_values: tuple[int, int, int]
) -> None:
    guard_1, guard_2, guard_3 = guard_values
    session = Session()
    try:
        stmt = insert(model).values(
            room_id=room_id,
            date=date_value,
            guard_1=guard_1,
            guard_2=guard_2,
            guard_3=guard_3,
        ).on_duplicate_key_update(
            guard_1=guard_1,
            guard_2=guard_2,
            guard_3=guard_3,
        )
        session.execute(stmt)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[Attention] upsert_daily_guards 失败: {exc}")
    finally:
        session.close()


def attention_upsert_daily_fans(model, room_id: int, date_value: datetime.date, fans_count: int) -> None:
    session = Session()
    try:
        stmt = insert(model).values(
            room_id=room_id,
            date=date_value,
            fans_count=int(fans_count),
        ).on_duplicate_key_update(fans_count=int(fans_count))
        session.execute(stmt)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[Attention] upsert_daily_fans 失败: {exc}")
    finally:
        session.close()
