"""Session-relative 15-minute accumulation and persistence."""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass

from .models import LiveSession15mStats
from .redis_metrics import delete_session_keys


BUCKET_SECONDS = 15 * 60


@dataclass(slots=True)  # noqa: MUTABLE_OK
class _Bucket:
    """Mutable accumulator for one session-relative interval."""

    session_id: int
    room_id: int
    month: str
    bucket_index: int
    start_time: datetime.datetime
    gift: float = 0.0
    guard: float = 0.0
    super_chat: float = 0.0
    blind_box_count: int = 0
    blind_box_profit: int = 0
    concurrency_total: int = 0
    sample_count: int = 0
    max_concurrency: int = 0
    payer_count: int = 0

    @property
    def end_time(self) -> datetime.datetime:
        """Return the scheduled end of this interval."""
        return self.start_time + datetime.timedelta(seconds=BUCKET_SECONDS)

    def has_data(self) -> bool:
        """Return whether this interval has anything worth persisting."""
        return bool(
            self.gift
            or self.guard
            or self.super_chat
            or self.blind_box_count
            or self.blind_box_profit
            or self.sample_count
            or self.payer_count
        )


_buckets: dict[int, _Bucket] = {}
_lock = threading.RLock()


def start_session(session_id: int, room_id: int, start_time: datetime.datetime) -> None:
    """Create the first interval for a newly observed session."""
    with _lock:
        _ = _buckets.setdefault(
            session_id,
            _Bucket(session_id, room_id, start_time.strftime("%Y%m"), 0, start_time),
        )


def current_bucket_index(
    session_id: int | None,
    event_time: datetime.datetime | None = None,
) -> int | None:
    """Return the session-relative bucket index for an event timestamp."""
    if session_id is None:
        return None
    with _lock:
        bucket = _buckets.get(session_id)
        if bucket is None:
            return None
        if event_time is None or event_time < bucket.start_time:
            return bucket.bucket_index
        elapsed = int((event_time - bucket.start_time).total_seconds())
        return bucket.bucket_index + max(0, elapsed // BUCKET_SECONDS)


def _new_bucket(previous: _Bucket) -> _Bucket:
    return _Bucket(
        previous.session_id,
        previous.room_id,
        previous.month,
        previous.bucket_index + 1,
        previous.end_time,
    )


def _flush(bucket: _Bucket, end_time: datetime.datetime) -> None:
    if not bucket.has_data():
        return
    average = (
        bucket.concurrency_total / bucket.sample_count
        if bucket.sample_count
        else None
    )
    if not LiveSession15mStats.upsert(
        session_id=bucket.session_id,
        room_id=bucket.room_id,
        month=bucket.month,
        bucket_index=bucket.bucket_index,
        start_time=bucket.start_time,
        end_time=end_time,
        gift=bucket.gift,
        guard=bucket.guard,
        super_chat=bucket.super_chat,
        blind_box_count=bucket.blind_box_count,
        blind_box_profit=bucket.blind_box_profit,
        avg_concurrency=average,
        max_concurrency=bucket.max_concurrency if bucket.sample_count else None,
        sample_count=bucket.sample_count,
        payer_count=bucket.payer_count,
    ):
        logging.error(
            "[LiveSession15m] 区间写入失败 session_id=%s bucket=%s",
            bucket.session_id,
            bucket.bucket_index,
        )


def _advance(bucket: _Bucket, event_time: datetime.datetime) -> _Bucket:
    while event_time >= bucket.end_time:
        _flush(bucket, bucket.end_time)
        bucket = _new_bucket(bucket)
    return bucket


def record_payment(
    session_id: int,
    event_time: datetime.datetime,
    gift: float = 0.0,
    guard: float = 0.0,
    super_chat: float = 0.0,
    blind_box_count: int = 0,
    blind_box_profit: int = 0,
    payer_added: bool = False,
) -> None:
    """Add one paid event to its session-relative interval."""
    with _lock:
        bucket = _buckets.get(session_id)
        if bucket is None:
            return
        bucket = _advance(bucket, event_time)
        bucket.gift += gift
        bucket.guard += guard
        bucket.super_chat += super_chat
        bucket.blind_box_count += int(blind_box_count)
        bucket.blind_box_profit += int(blind_box_profit)
        bucket.payer_count += int(payer_added)
        _buckets[session_id] = bucket


def record_concurrency(
    session_id: int,
    event_time: datetime.datetime,
    concurrency: int,
) -> None:
    """Add one concurrency sample to its session-relative interval."""
    with _lock:
        bucket = _buckets.get(session_id)
        if bucket is None:
            return
        bucket = _advance(bucket, event_time)
        value = int(concurrency)
        bucket.concurrency_total += value
        bucket.sample_count += 1
        bucket.max_concurrency = max(bucket.max_concurrency, value)
        _buckets[session_id] = bucket


def flush_session(session_id: int | None, end_time: datetime.datetime) -> None:
    """Persist the completed and final partial intervals for a session."""
    if session_id is None:
        return
    with _lock:
        bucket = _buckets.pop(session_id, None)
        if bucket is None:
            return
        while end_time > bucket.end_time:
            _flush(bucket, bucket.end_time)
            bucket = _new_bucket(bucket)
        _flush(bucket, end_time)
    delete_session_keys(session_id)
