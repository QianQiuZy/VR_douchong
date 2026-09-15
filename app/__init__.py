from __future__ import annotations

import datetime
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DanmakuCounts:
    total: int = 0
    captain: int = 0
    admiral: int = 0
    governor: int = 0
    normal: int = 0

    @classmethod
    def from_privilege_type(cls, privilege_type: int) -> "DanmakuCounts":
        counts_by_privilege = {
            1: cls(total=1, governor=1),
            2: cls(total=1, admiral=1),
            3: cls(total=1, captain=1),
        }
        return counts_by_privilege.get(privilege_type, cls(total=1, normal=1))

    def __add__(self, other: "DanmakuCounts") -> "DanmakuCounts":
        return DanmakuCounts(
            total=self.total + other.total,
            captain=self.captain + other.captain,
            admiral=self.admiral + other.admiral,
            governor=self.governor + other.governor,
            normal=self.normal + other.normal,
        )


@dataclass(frozen=True, slots=True)
class DanmakuBucketTarget:
    session_id: int
    room_id: int
    month: str
    bucket_index: int
    start_time: datetime.datetime
    end_time: datetime.datetime


@dataclass(slots=True)  # noqa: MUTABLE_OK
class PendingDanmaku:
    """Mutable per-destination queues for one room's unflushed danmaku."""  # noqa: MUTABLE_OK

    monthly: dict[str, DanmakuCounts] = field(default_factory=dict)
    sessions: dict[int, DanmakuCounts] = field(default_factory=dict)
    buckets: dict[DanmakuBucketTarget, DanmakuCounts] = field(default_factory=dict)

    def add(
        self,
        event_time: datetime.datetime,
        counts: DanmakuCounts,
        target: DanmakuBucketTarget | None,
    ) -> None:
        month = event_time.strftime("%Y%m")
        self.monthly[month] = self.monthly.get(month, DanmakuCounts()) + counts
        if target is not None:
            self.sessions[target.session_id] = (
                self.sessions.get(target.session_id, DanmakuCounts()) + counts
            )
            self.buckets[target] = self.buckets.get(target, DanmakuCounts()) + counts

    def is_empty(self) -> bool:
        return not self.monthly and not self.sessions and not self.buckets
