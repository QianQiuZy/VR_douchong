import datetime

from sqlalchemy import Column, Date, Index, Integer, PrimaryKeyConstraint, String

from ..database import Base
from ..repositories.info import (
    attention_upsert_daily,
    attention_upsert_daily_fans,
    attention_upsert_daily_guards,
    room_info_upsert,
)


class RoomInfo(Base):
    """房间基础信息：主播名称 & 粉丝数"""

    __tablename__ = "room_info"
    room_id = Column(Integer, primary_key=True)
    anchor_name = Column(String(100), nullable=False)
    attention = Column(Integer, default=0, nullable=False)

    @classmethod
    def upsert(cls, room_id: int, anchor_name: str | None = None, attention: int | None = None) -> None:
        room_info_upsert(cls, room_id, anchor_name, attention)


class Attention(Base):
    """每日粉丝数、守护与粉丝团快照；主键：room_id + date"""

    __tablename__ = "attention"
    room_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    attention = Column(Integer, default=0, nullable=False)
    guard_1 = Column(Integer, default=0, nullable=False)
    guard_2 = Column(Integer, default=0, nullable=False)
    guard_3 = Column(Integer, default=0, nullable=False)
    fans_count = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "date", name="pk_attention_room_date"),
        Index("idx_attention_date", "date"),
    )

    @classmethod
    def upsert_daily(cls, room_id: int, date_value: datetime.date, attention_value: int) -> None:
        attention_upsert_daily(cls, room_id, date_value, attention_value)

    @classmethod
    def upsert_daily_guards(
        cls, room_id: int, date_value: datetime.date, guard_values: tuple[int, int, int]
    ) -> None:
        attention_upsert_daily_guards(cls, room_id, date_value, guard_values)

    @classmethod
    def upsert_daily_fans(cls, room_id: int, date_value: datetime.date, fans_count: int) -> None:
        attention_upsert_daily_fans(cls, room_id, date_value, fans_count)
