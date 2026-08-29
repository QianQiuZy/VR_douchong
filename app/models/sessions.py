import datetime

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, String

from ..database import Base
from ..repositories.live_session_snapshots import (
    add_danmaku_by_id,
    add_danmaku_by_room_open,
    update_end_attention,
    update_end_counts,
    update_start_attention,
    update_start_counts,
)
from ..repositories.live_sessions import (
    add_values_by_id,
    add_values_by_room_open,
    close_session_by_id,
    find_open_session,
    set_payer_count,
    start_session,
    update_concurrency_by_id,
)
from ..repositories.session_15m import upsert_stats
from ..repositories.super_chat import log_super_chat


class LiveSession(Base):
    """单场直播（以开播为准）"""

    __tablename__ = "live_session"
    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(Integer, nullable=False, index=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    title = Column(String(255), default="", nullable=False)
    gift = Column(Float, default=0.0, nullable=False)
    guard = Column(Float, default=0.0, nullable=False)
    super_chat = Column(Float, default=0.0, nullable=False)
    month = Column(String(6), nullable=False, index=True)
    blind_box_count = Column(Integer, default=0, nullable=False)
    blind_box_profit = Column(Integer, default=0, nullable=False)
    danmaku_count = Column(Integer, default=0, nullable=False)
    payer_count = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        Index("idx_ls_room_month", "room_id", "month"),
        Index("idx_ls_room_month_start", "room_id", "month", "start_time"),
    )
    start_guard_1 = Column(Integer, nullable=True)
    start_guard_2 = Column(Integer, nullable=True)
    start_guard_3 = Column(Integer, nullable=True)
    start_fans_count = Column(Integer, nullable=True)
    start_attention = Column(Integer, nullable=True)
    end_guard_1 = Column(Integer, nullable=True)
    end_guard_2 = Column(Integer, nullable=True)
    end_guard_3 = Column(Integer, nullable=True)
    end_fans_count = Column(Integer, nullable=True)
    end_attention = Column(Integer, nullable=True)
    avg_concurrency = Column(Float, nullable=True)
    max_concurrency = Column(Integer, nullable=True)

    @classmethod
    def start_session(cls, room_id: int, start_dt: datetime.datetime, title: str) -> int | None:
        return start_session(cls, room_id, start_dt, title)

    @classmethod
    def find_open_session(cls, room_id: int) -> tuple[int, datetime.datetime] | None:
        return find_open_session(cls, room_id)

    @classmethod
    def add_values_by_id(
        cls, session_id: int, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0,
        blind_box_count: int = 0, blind_box_profit: int = 0,
    ) -> None:
        add_values_by_id(cls, session_id, gift, guard, super_chat, blind_box_count, blind_box_profit)

    @classmethod
    def add_values_by_room_open(
        cls, room_id: int, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0,
        blind_box_count: int = 0, blind_box_profit: int = 0,
    ) -> None:
        add_values_by_room_open(cls, room_id, gift, guard, super_chat, blind_box_count, blind_box_profit)

    @classmethod
    def close_session_by_id(cls, session_id: int | None, end_dt: datetime.datetime) -> None:
        close_session_by_id(cls, session_id, end_dt)

    @classmethod
    def update_concurrency_by_id(
        cls, session_id: int | None, avg_concurrency: float | None = None, max_concurrency: int | None = None
    ) -> None:
        update_concurrency_by_id(cls, session_id, avg_concurrency, max_concurrency)

    @classmethod
    def set_payer_count(cls, session_id: int | None, count: int) -> None:
        set_payer_count(cls, session_id, count)

    @classmethod
    def update_start_counts(
        cls, session_id: int, guard_1: int | None = None, guard_2: int | None = None,
        guard_3: int | None = None, fans_count: int | None = None,
    ) -> None:
        update_start_counts(cls, session_id, guard_1, guard_2, guard_3, fans_count)

    @classmethod
    def update_end_counts(
        cls, session_id: int, guard_1: int | None = None, guard_2: int | None = None,
        guard_3: int | None = None, fans_count: int | None = None,
    ) -> None:
        update_end_counts(cls, session_id, guard_1, guard_2, guard_3, fans_count)

    @classmethod
    def update_start_attention(cls, session_id: int, attention: int | None = None) -> None:
        update_start_attention(cls, session_id, attention)

    @classmethod
    def update_end_attention(cls, session_id: int, attention: int | None = None) -> None:
        update_end_attention(cls, session_id, attention)

    @classmethod
    def add_danmaku_by_id(cls, session_id: int, count: int) -> None:
        add_danmaku_by_id(cls, session_id, count)

    @classmethod
    def add_danmaku_by_room_open(cls, room_id: int, count: int) -> None:
        add_danmaku_by_room_open(cls, room_id, count)


class SuperChatLog(Base):
    """单条 SC 消息日志"""

    __tablename__ = "super_chat_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(Integer, nullable=False, index=True)
    uname = Column(String(100), nullable=False)
    uid = Column(BigInteger, nullable=False)
    send_time = Column(DateTime, nullable=False, index=True)
    price = Column(Float, nullable=False)
    message = Column(String(500), nullable=False)
    __table_args__ = (
        Index("idx_scl_room_time", "room_id", "send_time"),
        Index("idx_scl_uid_time", "uid", "send_time"),
    )

    @classmethod
    def log_sc(
        cls, room_id: int, uname: str, uid: int, price: float, content: str,
        send_time: datetime.datetime | None = None,
    ) -> None:
        log_super_chat(cls, room_id, uname, uid, price, content, send_time)


class LiveSession15mStats(Base):
    """Session-relative 15-minute metrics; mutable because ORM rows are updated."""  # noqa: MUTABLE_OK

    __tablename__ = "live_session_15m_stats"
    session_id = Column(Integer, primary_key=True)
    bucket_index = Column(Integer, primary_key=True)
    room_id = Column(Integer, nullable=False, index=True)
    month = Column(String(6), nullable=False, index=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    gift = Column(Float, default=0.0, nullable=False)
    guard = Column(Float, default=0.0, nullable=False)
    super_chat = Column(Float, default=0.0, nullable=False)
    blind_box_count = Column(Integer, default=0, nullable=False)
    blind_box_profit = Column(Integer, default=0, nullable=False)
    danmaku_count = Column(Integer, default=0, nullable=False)
    avg_concurrency = Column(Float, nullable=True)
    max_concurrency = Column(Integer, nullable=True)
    sample_count = Column(Integer, default=0, nullable=False)
    payer_count = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        Index("idx_ls15_room_month_start", "room_id", "month", "start_time"),
    )

    @classmethod
    def upsert(
        cls,
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
        avg_concurrency: float | None,
        max_concurrency: int | None,
        sample_count: int,
        payer_count: int,
    ) -> bool:
        return upsert_stats(
            cls,
            session_id=session_id,
            room_id=room_id,
            month=month,
            bucket_index=bucket_index,
            start_time=start_time,
            end_time=end_time,
            gift=gift,
            guard=guard,
            super_chat=super_chat,
            blind_box_count=blind_box_count,
            blind_box_profit=blind_box_profit,
            danmaku_count=danmaku_count,
            avg_concurrency=avg_concurrency,
            max_concurrency=max_concurrency,
            sample_count=sample_count,
            payer_count=payer_count,
        )
