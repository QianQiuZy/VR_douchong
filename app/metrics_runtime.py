"""Session-relative 15-minute accumulation and persistence."""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass, replace

from . import DanmakuBucketTarget, DanmakuCounts, rounded_concurrency, runtime_state
from .models import LiveSession, LiveSession15mStats
from .redis_metrics import restore_session_payers
from .repositories.session_15m import read_open_metrics

logger = logging.getLogger(__name__)

BUCKET_SECONDS = 15 * 60


@dataclass(slots=True)  # MUTABLE_OK
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
    dirty: bool = False

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
_failed_writes: dict[tuple[int, int], tuple[_Bucket, datetime.datetime]] = {}


def start_session(session_id: int, room_id: int, start_time: datetime.datetime) -> None:
    """Create the first interval for a newly observed session."""
    with _lock:
        _ = _buckets.setdefault(
            session_id,
            _Bucket(session_id, room_id, start_time.strftime("%Y%m"), 0, start_time),
        )


def restore_session(session_id: int, room_id: int, start_time: datetime.datetime) -> None:
    """Restore absolute bucket totals once, before accepting events for an open session."""
    with _lock:
        if session_id in _buckets:
            return
        bucket = _Bucket(session_id, room_id, start_time.strftime("%Y%m"), 0, start_time)
        latest, total, samples, maximum, payers = read_open_metrics(LiveSession15mStats, LiveSession, session_id)
        if latest is not None:
            row, subtotal = latest
            # Pre-upgrade rows lack a poll checkpoint. Only metric-bearing
            # intervals are considered here (danmaku-only rows have a scheduled end).
            runtime_state.LAST_LIVE_OBSERVATIONS.setdefault(session_id, row.end_time)
            saved = _Bucket(
                session_id, room_id, row.month, row.bucket_index, row.start_time,
                gift=row.gift, guard=row.guard, super_chat=row.super_chat,
                blind_box_count=row.blind_box_count, blind_box_profit=row.blind_box_profit,
                concurrency_total=int(subtotal or 0), sample_count=row.sample_count,
                max_concurrency=row.max_concurrency or 0, payer_count=row.payer_count,
            )
            bucket = saved if row.end_time < saved.end_time else _new_bucket(saved)
        restore_session_payers(session_id, bucket.bucket_index, payers)
        runtime_state.CONCURRENCY_CACHE[room_id] = {
            "session_id": session_id, "total": total, "samples": samples, "max": maximum,
        }
        _buckets[session_id] = bucket


def session_month(session_id: int) -> str | None:
    """Reuse the known parent month when marking a payer-count write."""
    with _lock:
        bucket = _buckets.get(session_id)
        return bucket.month if bucket is not None else None


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


def _flush(bucket: _Bucket, end_time: datetime.datetime) -> bool:
    if not bucket.has_data():
        return True
    average = (
        rounded_concurrency(bucket.concurrency_total / bucket.sample_count)
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
        danmaku_count=0,
        captain_danmaku_count=0,
        admiral_danmaku_count=0,
        governor_danmaku_count=0,
        normal_danmaku_count=0,
        avg_concurrency=average,
        max_concurrency=bucket.max_concurrency if bucket.sample_count else None,
        sample_count=bucket.sample_count,
        concurrency_total=bucket.concurrency_total,
        payer_count=bucket.payer_count,
    ):
        logger.error(
            "[LiveSession15m] 区间写入失败 session_id=%s bucket=%s",
            bucket.session_id,
            bucket.bucket_index,
        )
        return False
    return True


def _save_bucket(bucket: _Bucket, end_time: datetime.datetime) -> None:
    key = (bucket.session_id, bucket.bucket_index)
    if _flush(bucket, end_time):
        bucket.dirty = False
        _failed_writes.pop(key, None)
    else:
        _failed_writes[key] = (replace(bucket), end_time)


def retry_failed_buckets() -> None:
    with _lock:
        for key, (bucket, end_time) in tuple(_failed_writes.items()):
            if _flush(bucket, end_time):
                del _failed_writes[key]


def _advance(bucket: _Bucket, event_time: datetime.datetime) -> _Bucket:
    while event_time >= bucket.end_time:
        _save_bucket(bucket, bucket.end_time)
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
        bucket.dirty = True
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
        bucket.dirty = True
        _buckets[session_id] = bucket


def danmaku_bucket_target(
    session_id: int,
    event_time: datetime.datetime,
) -> DanmakuBucketTarget | None:
    with _lock:
        bucket = _buckets.get(session_id)
        if bucket is None:
            return None
        session_start = bucket.start_time - datetime.timedelta(
            seconds=bucket.bucket_index * BUCKET_SECONDS
        )
        elapsed = max(0, int((event_time - session_start).total_seconds()))
        bucket_index = elapsed // BUCKET_SECONDS
        start_time = session_start + datetime.timedelta(seconds=bucket_index * BUCKET_SECONDS)
        target = DanmakuBucketTarget(
            session_id=session_id,
            room_id=bucket.room_id,
            month=bucket.month,
            bucket_index=bucket_index,
            start_time=start_time,
            end_time=start_time + datetime.timedelta(seconds=BUCKET_SECONDS),
        )
        return target


def record_danmaku(
    session_id: int,
    event_time: datetime.datetime,
    counts: DanmakuCounts,
) -> bool:
    target = danmaku_bucket_target(session_id, event_time)
    if target is None:
        return False
    return LiveSession15mStats.add_danmaku_counts(target, counts)


def checkpoint_session(session_id: int, when: datetime.datetime) -> None:
    """Save absolute values. Both repeated saves and subsequent recovery are idempotent."""
    with _lock:
        bucket = _buckets.get(session_id)
        if bucket is not None:
            bucket = _advance(bucket, when)
            _buckets[session_id] = bucket
            if bucket.dirty:
                _save_bucket(bucket, when)


def flush_session(session_id: int | None, end_time: datetime.datetime) -> None:
    """Save the last interval at the actual end of the live session."""
    if session_id is None:
        return
    with _lock:
        bucket = _buckets.pop(session_id, None)
        if bucket is not None:
            bucket = _advance(bucket, end_time)
            _save_bucket(bucket, end_time)
