from sqlalchemy import Column, String

from ..database import Base


class DanmakuReplayCursor(Base):
    """Last committed batch for one room, journal and SQL write phase."""

    __tablename__ = "danmaku_replay_cursor"
    stream = Column(String(64), primary_key=True)
    token = Column(String(32), nullable=False)
