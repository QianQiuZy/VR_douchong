"""Minimal visitor-entry records, independent of live-session metrics."""

from sqlalchemy import BigInteger, Column, DateTime, Index, PrimaryKeyConstraint
from sqlalchemy.dialects.mysql import BIGINT, DATETIME

from ..database import Base


class RoomEntryLog(Base):
    __tablename__ = "room_entry_log"

    room_id = Column(
        BigInteger().with_variant(BIGINT(unsigned=True), "mysql"), nullable=False
    )
    uid = Column(
        BigInteger().with_variant(BIGINT(unsigned=True), "mysql"), nullable=False
    )
    event_time = Column(
        DateTime().with_variant(DATETIME(fsp=3), "mysql"), nullable=False
    )
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "event_time", "uid", name="pk_entry"),
        Index("idx_entry_uid_time", "uid", "event_time", "room_id"),
        Index("idx_entry_time", "event_time"),
    )
