import datetime
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ..database import Session
from .tables import ensure_sc_archive_table, is_current_month, month_str


def log_super_chat(
    model,
    room_id: int,
    uname: str,
    uid: int,
    price: float,
    content: str,
    send_time: datetime.datetime | None = None,
) -> None:
    session = Session()
    try:
        final_time = send_time or datetime.datetime.now()
        if final_time.year < 2000:
            final_time = datetime.datetime.now()
        month_code = month_str(final_time)
        if is_current_month(month_code):
            session.add(model(
                room_id=room_id,
                uname=uname or "",
                uid=int(uid or 0),
                price=float(price or 0.0),
                message=content or "",
                send_time=final_time,
            ))
        else:
            table_name = ensure_sc_archive_table(month_code)
            session.execute(
                text(
                    f"INSERT INTO `{table_name}` "
                    "(room_id, uname, uid, send_time, price, message) "
                    "VALUES (:room_id, :uname, :uid, :send_time, :price, :message)"
                ),
                {
                    "room_id": room_id,
                    "uname": uname or "",
                    "uid": int(uid or 0),
                    "send_time": final_time,
                    "price": float(price or 0.0),
                    "message": content or "",
                },
            )
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logging.error(f"[SuperChatLog] 写入失败: {exc}")
    finally:
        try:
            session.close()
        except Exception:
            pass
