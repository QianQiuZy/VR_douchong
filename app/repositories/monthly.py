import logging
import random
import time

from sqlalchemy import func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session


def add_room_stats_amounts(model, room_id: int, month: str, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0) -> None:
    if not gift and not guard and not super_chat:
        return
    for attempt in range(1, 7):
        session = Session()
        try:
            stmt = insert(model).values(
                room_id=room_id, month=month, gift=gift, guard=guard, super_chat=super_chat, payer_count=0
            ).on_duplicate_key_update(
                gift=model.gift + gift,
                guard=model.guard + guard,
                super_chat=model.super_chat + super_chat,
            )
            session.execute(stmt)
            session.commit()
            return
        except SQLAlchemyError as exc:
            session.rollback()
            err = getattr(exc, "orig", None)
            code = getattr(err, "args", [None])[0] if err else None
            if code in (1205, 1213):
                sleep = 0.05 * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                logging.warning(f"[RoomStatsMonthly] 死锁/锁超时，第 {attempt} 次退避 {sleep:.3f}s；code={code}")
                time.sleep(sleep)
                continue
            logging.error(f"[RoomStatsMonthly] 写入失败（非可重试）: {repr(exc)} orig={repr(err)}")
            return
        finally:
            try:
                session.close()
            except Exception:
                pass
    logging.error("[RoomStatsMonthly] 多次重试仍失败，数据可能不完整。")


def set_room_payer_count(model, room_id: int, month: str, count: int) -> None:
    """Persist the Redis cardinality for one room-month payer set."""
    session = Session()
    try:
        stmt = insert(model).values(
            room_id=room_id,
            month=month,
            gift=0.0,
            guard=0.0,
            super_chat=0.0,
            payer_count=int(count),
        ).on_duplicate_key_update(payer_count=func.greatest(model.payer_count, int(count)))
        session.execute(stmt)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error("[RoomStatsMonthly] payer_count写入失败 room_id=%s month=%s: %s", room_id, month, exc)
    finally:
        session.close()


def add_blind_box_amounts(model, room_id: int, month: str, count: int = 0, profit: int = 0) -> None:
    if not count and not profit:
        return
    for attempt in range(1, 7):
        session = Session()
        try:
            stmt = insert(model).values(
                room_id=room_id,
                month=month,
                blind_box_count=count,
                blind_box_profit=profit,
            ).on_duplicate_key_update(
                blind_box_count=model.blind_box_count + count,
                blind_box_profit=model.blind_box_profit + profit,
            )
            session.execute(stmt)
            session.commit()
            return
        except SQLAlchemyError as exc:
            session.rollback()
            err = getattr(exc, "orig", None)
            code = getattr(err, "args", [None])[0] if err else None
            if code in (1205, 1213):
                sleep = 0.05 * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                logging.warning(f"[RoomBlindBoxMonthly] 死锁/锁超时，第 {attempt} 次退避 {sleep:.3f}s；code={code}")
                time.sleep(sleep)
                continue
            logging.error(f"[RoomBlindBoxMonthly] 写入失败（非可重试）: {repr(exc)} orig={repr(err)}")
            return
        finally:
            try:
                session.close()
            except Exception:
                pass
    logging.error("[RoomBlindBoxMonthly] 多次重试仍失败，数据可能不完整。")
