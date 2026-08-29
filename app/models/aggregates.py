import datetime

from sqlalchemy import Column, Date, Float, Index, Integer, PrimaryKeyConstraint, String

from ..database import Base
from ..repositories.live_stats import (
    add_daily_metrics,
    add_duration,
    month_aggregate_for_month,
    month_steel_coin_for_month,
)
from ..repositories.monthly import (
    add_blind_box_amounts,
    add_room_stats_amounts,
    set_room_payer_count,
)


class RoomStatsMonthly(Base):
    """按月累计礼物/上舰/SC；主键：room_id + month"""

    __tablename__ = "room_stats_monthly"
    room_id = Column(Integer, nullable=False)
    month = Column(String(6), nullable=False)
    gift = Column(Float, default=0.0, nullable=False)
    guard = Column(Float, default=0.0, nullable=False)
    super_chat = Column(Float, default=0.0, nullable=False)
    payer_count = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "month", name="pk_room_month"),
        Index("idx_rsm_month", "month"),
    )

    @classmethod
    def add_amounts(
        cls, room_id: int, month: str, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0
    ) -> None:
        add_room_stats_amounts(cls, room_id, month, gift, guard, super_chat)

    @classmethod
    def set_payer_count(cls, room_id: int, month: str, count: int) -> None:
        set_room_payer_count(cls, room_id, month, count)


class RoomBlindBoxMonthly(Base):
    """按月累计盲盒数量/盈亏；主键：room_id + month"""

    __tablename__ = "room_blind_box_monthly"
    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(Integer, nullable=False)
    month = Column(String(6), nullable=False)
    blind_box_count = Column(Integer, default=0, nullable=False)
    blind_box_profit = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        Index("idx_rbbm_month", "month"),
        Index("idx_rbbm_room_month", "room_id", "month", unique=True),
    )

    @classmethod
    def add_amounts(cls, room_id: int, month: str, count: int = 0, profit: int = 0) -> None:
        add_blind_box_amounts(cls, room_id, month, count, profit)


class RoomLiveStats(Base):
    """按自然日累计直播、流水和去重人数。"""

    __tablename__ = "room_live_stats"
    room_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    duration = Column(Integer, default=0, nullable=False)
    gift = Column(Float, default=0.0, nullable=False)
    guard = Column(Float, default=0.0, nullable=False)
    super_chat = Column(Float, default=0.0, nullable=False)
    payer_count = Column(Integer, default=0, nullable=False)
    steel_coin_count = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "date", name="pk_room_date"),
        Index("idx_rls_date", "date"),
    )

    @classmethod
    def add_duration(cls, room_id: int, date_value: datetime.date, seconds: int) -> None:
        add_duration(cls, room_id, date_value, seconds)

    @classmethod
    def add_metrics(
        cls,
        room_id: int,
        date_value: datetime.date,
        gift: float = 0.0,
        guard: float = 0.0,
        super_chat: float = 0.0,
        payer_count: int | None = None,
        steel_coin_delta: int = 0,
    ) -> None:
        add_daily_metrics(cls, room_id, date_value, gift, guard, super_chat, payer_count, steel_coin_delta)

    @classmethod
    def month_aggregate_for_month(cls, room_id: int, month: str) -> tuple[int, int]:
        return month_aggregate_for_month(cls, room_id, month)

    @classmethod
    def month_steel_coin_for_month(cls, room_id: int, month: str) -> int:
        return month_steel_coin_for_month(cls, room_id, month)
