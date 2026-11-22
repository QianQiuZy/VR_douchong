from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from collections import Counter
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class Audience:
    count: int | None
    total: int | None


@dataclass(slots=True)
class SenderBatch:
    counts: Counter[int] = field(default_factory=Counter)
    token: str = field(default_factory=lambda: uuid4().hex)


@dataclass(frozen=True, slots=True)
class DanmakuCounts:
    total: int = 0
    captain: int = 0
    admiral: int = 0
    governor: int = 0
    normal: int = 0
    audience: Audience | None = None

    @classmethod
    def from_privilege_type(cls, privilege_type: int) -> DanmakuCounts:
        counts_by_privilege = {
            1: cls(total=1, governor=1),
            2: cls(total=1, admiral=1),
            3: cls(total=1, captain=1),
        }
        return counts_by_privilege.get(privilege_type, cls(total=1, normal=1))

    def __add__(self, other: DanmakuCounts) -> DanmakuCounts:
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


@dataclass(slots=True)  # MUTABLE_OK
class PendingDanmaku:
    """Mutable per-destination queues for one room's unflushed danmaku."""  # MUTABLE_OK

    monthly: dict[str, DanmakuCounts] = field(default_factory=dict)
    sessions: dict[int, DanmakuCounts] = field(default_factory=dict)
    buckets: dict[DanmakuBucketTarget, DanmakuCounts] = field(default_factory=dict)

    # After a Redis timeout the frozen batch keeps its token. New arrivals
    # accumulate separately, including when the following MySQL write fails.
    session_starts: dict[int, datetime.datetime] = field(default_factory=dict)
    senders: dict[object, SenderBatch] = field(default_factory=dict)
    retry: dict[object, SenderBatch] = field(default_factory=dict)
    results: dict[object, Audience] = field(default_factory=dict)

    def add(
        self,
        event_time: datetime.datetime,
        counts: DanmakuCounts,
        target: DanmakuBucketTarget | None,
        uid: int = 0,
    ) -> None:
        month = event_time.strftime("%Y%m")
        self.monthly[month] = self.monthly.get(month, DanmakuCounts()) + counts
        if target is not None:
            self.session_starts[target.session_id] = target.start_time - target.bucket_index * (target.end_time - target.start_time)
            self.sessions[target.session_id] = (
                self.sessions.get(target.session_id, DanmakuCounts()) + counts
            )
            self.buckets[target] = self.buckets.get(target, DanmakuCounts()) + counts

        keys = (month, target.session_id, target) if target is not None else (month,)
        for key in keys:
            if key not in self.senders:
                self.senders[key] = SenderBatch()
            self.senders[key].counts[uid] += counts.total

    def is_empty(self) -> bool:
        return not self.monthly and not self.sessions and not self.buckets


def rounded_concurrency(value: float | None) -> float | None:
    return None if value is None else float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
