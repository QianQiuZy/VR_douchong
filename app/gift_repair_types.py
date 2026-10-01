"""Typed audit records shared by the one-time gift repair tools."""

import datetime
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, TypedDict

DAILY_FEATURE_SINCE: Final = datetime.date.fromisoformat("2026-08-30")
WHALE_FIRST_MONTH: Final = "202609"


class RepairError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class GiftEvent:
    event_id: str
    when: datetime.datetime
    room_id: int
    uid: int
    gift: str
    quantity: int
    recorded: Decimal
    source: str
    line: int


@dataclass(frozen=True, slots=True)
class GiftCorrection:
    event: GiftEvent
    unit: Decimal
    delta: Decimal
    baseline_source: str
    baseline_line: int


class AuditSummary(TypedDict):
    files: int
    duplicate_files: int
    gifts: int
    paid_batches: int
    free_batches: int
    corrections: int
    delta_rmb: str
    already_total: int
    missing_baselines: int
    missing_gifts: list[str]
    other_prices: int
    first_timestamp: str
    last_timestamp: str
