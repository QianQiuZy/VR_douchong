"""Monthly whale-dependency metrics backed by Redis until archive time."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, TypedDict
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import redis

from .config import REDIS_KEY_TTL_SECONDS, REDIS_URL

RevenueType = Literal["gift", "guard", "super_chat"]
_CATEGORY_FIELDS: Final[dict[RevenueType, str]] = {
    "gift": "gift_revenue",
    "guard": "guard_revenue",
    "super_chat": "super_chat_revenue",
}
_KEY_PREFIX: Final = "vr:whale:v1"
WHALE_TOTALS_PREFIX: Final = f"{_KEY_PREFIX}:totals"
_UID_PREFIX: Final = f"{_KEY_PREFIX}:uid"
_DEDUPE_PREFIX: Final = f"{_KEY_PREFIX}:dedupe"
WHALE_STATE_PREFIX: Final = f"{_KEY_PREFIX}:state"
logger = logging.getLogger(__name__)
_FINALIZE_SCRIPT: Final = """
if redis.call('HGET', KEYS[1], 'status') ~= 'archiving' then
    return 0
end
redis.call('HSET', KEYS[1], 'status', 'archived')
return 1
"""
_RECORD_SCRIPT: Final = """
local dedupe_key = KEYS[1]
if redis.call('SISMEMBER', dedupe_key, ARGV[1]) == 1 then
    return 0
end
redis.call('SADD', dedupe_key, ARGV[1])
local uid = ARGV[2]
local amount = ARGV[3]
local category = ARGV[4]
if uid ~= '0' then
    redis.call('HINCRBY', KEYS[2], uid, amount)
    redis.call('HINCRBY', KEYS[3], 'attributed_revenue', amount)
else
    redis.call('HINCRBY', KEYS[3], 'unattributed_revenue', amount)
end
redis.call('HINCRBY', KEYS[3], 'total_revenue', amount)
redis.call('HINCRBY', KEYS[3], category, amount)
redis.call('HINCRBY', KEYS[3], 'event_count', 1)
local state = redis.call('HGET', KEYS[4], 'status')
if state == 'archiving' or state == 'archived' then
    redis.call('HSET', KEYS[4], 'status', 'dirty')
end
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('EXPIRE', KEYS[3], ARGV[5])
redis.call('EXPIRE', KEYS[4], ARGV[5])
return 1
"""

def _redis_db1_url(url: str) -> str:
    """Normalize a Redis URL so the whale cache always uses logical DB1."""
    parsed = urlsplit(url)
    query = urlencode(
        [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key != "db"]
    )
    return urlunsplit((parsed.scheme, parsed.netloc, "/1", query, parsed.fragment))


_client = redis.Redis.from_url(_redis_db1_url(REDIS_URL), decode_responses=True)


@dataclass(frozen=True, slots=True)
class WhaleMetrics:
    """Calculated monthly concentration values in integer minor units."""

    total_revenue: int
    attributed_revenue: int
    unattributed_revenue: int
    payer_count: int
    top1_count: int
    top1_amount: int
    top1_ratio: float | None
    top5_count: int
    top5_amount: int
    top5_ratio: float | None
    top10_count: int
    top10_amount: int
    top10_ratio: float | None
    top1pct_count: int
    top1pct_amount: int
    top1pct_ratio: float | None


class WhaleDependencyPayload(TypedDict):
    status: str
    source: str
    top1: float | None
    top5: float | None
    top10: float | None
    top1_percent: float | None


def calculate_whale_metrics(
    payer_spend: Mapping[int, int],
    total_revenue: int,
    unattributed_revenue: int,
) -> WhaleMetrics:
    """Calculate deterministic top-payer shares for one room-month."""
    ranked = sorted(
        ((int(uid), int(amount)) for uid, amount in payer_spend.items() if int(amount) > 0),
        key=lambda item: (-item[1], item[0]),
    )
    payer_count = len(ranked)
    safe_total = max(0, int(total_revenue))
    safe_unattributed = max(0, int(unattributed_revenue))
    safe_attributed = max(0, safe_total - safe_unattributed)

    def top_amount(limit: int) -> int:
        return sum(amount for _, amount in ranked[:limit])

    def ratio(amount: int) -> float | None:
        return None if safe_total == 0 else amount / safe_total

    top1_count = min(1, payer_count)
    top5_count = min(5, payer_count)
    top10_count = min(10, payer_count)
    top1pct_count = max(1, (payer_count + 99) // 100) if payer_count else 0
    top1_amount = top_amount(top1_count)
    top5_amount = top_amount(top5_count)
    top10_amount = top_amount(top10_count)
    top1pct_amount = top_amount(top1pct_count)
    return WhaleMetrics(
        total_revenue=safe_total,
        attributed_revenue=safe_attributed,
        unattributed_revenue=safe_unattributed,
        payer_count=payer_count,
        top1_count=top1_count,
        top1_amount=top1_amount,
        top1_ratio=ratio(top1_amount),
        top5_count=top5_count,
        top5_amount=top5_amount,
        top5_ratio=ratio(top5_amount),
        top10_count=top10_count,
        top10_amount=top10_amount,
        top10_ratio=ratio(top10_amount),
        top1pct_count=top1pct_count,
        top1pct_amount=top1pct_amount,
        top1pct_ratio=ratio(top1pct_amount),
    )


def _key(prefix: str, month: str, room_id: int) -> str:
    return f"{prefix}:{month}:{room_id}"


def record_whale_revenue(
    room_id: int,
    month: str,
    uid: int,
    amount: int,
    event_key: str,
    revenue_type: RevenueType,
) -> bool:
    """Atomically deduplicate and add one event to the Redis month cache."""
    if room_id <= 0 or amount <= 0 or not event_key:
        return False
    category = _CATEGORY_FIELDS.get(revenue_type)
    if category is None:
        return False
    keys = [
        _key(_DEDUPE_PREFIX, month, room_id),
        _key(_UID_PREFIX, month, room_id),
        _key(WHALE_TOTALS_PREFIX, month, room_id),
        _key(WHALE_STATE_PREFIX, month, room_id),
    ]
    try:
        result = _client.eval(
            _RECORD_SCRIPT,
            len(keys),
            *keys,
            event_key,
            str(max(0, uid)),
            str(int(amount)),
            category,
            str(max(1, REDIS_KEY_TTL_SECONDS)),
        )
    except redis.RedisError as exc:
        logger.error("[WhaleRedis] 记录收入失败 room_id=%s month=%s: %s", room_id, month, exc)
        return False
    return bool(int(result or 0))


def is_whale_event_recorded(room_id: int, month: str, event_key: str) -> bool | None:
    """Return whether Redis already contains an event key, or None on Redis failure."""
    if room_id <= 0 or not event_key:
        return False
    try:
        return bool(_client.sismember(_key(_DEDUPE_PREFIX, month, room_id), event_key))
    except redis.RedisError as exc:
        logger.error("[WhaleRedis] 查询去重事件失败 room_id=%s month=%s: %s", room_id, month, exc)
        return None


def _int_map(values: Mapping[str | bytes, str | bytes]) -> dict[int, int]:
    return {
        int(uid): int(amount)
        for uid, amount in values.items()
        if int(amount) > 0
    }


def get_live_whale_metrics(room_id: int, month: str) -> WhaleMetrics | None:
    """Read and calculate the Redis metrics for one room-month."""
    try:
        totals = _client.hgetall(_key(WHALE_TOTALS_PREFIX, month, room_id))
        if not totals:
            return None
        payer_spend = _int_map(_client.hgetall(_key(_UID_PREFIX, month, room_id)))
        total_revenue = int(totals.get("total_revenue", 0))
        unattributed_revenue = int(totals.get("unattributed_revenue", 0))
        return calculate_whale_metrics(payer_spend, total_revenue, unattributed_revenue)
    except (redis.RedisError, TypeError, ValueError) as exc:
        logger.error("[WhaleRedis] 读取收入失败 room_id=%s month=%s: %s", room_id, month, exc)
        return None


def get_whale_state(room_id: int, month: str) -> str | None:
    """Return the Redis archive state for one room-month."""
    try:
        value = _client.hget(_key(WHALE_STATE_PREFIX, month, room_id), "status")
    except redis.RedisError as exc:
        logger.error("[WhaleRedis] 读取归档状态失败 room_id=%s month=%s: %s", room_id, month, exc)
        return None
    return str(value) if value is not None else None


def begin_whale_archive(room_id: int, month: str) -> None:
    """Mark a room-month as being snapshotted before MySQL upsert."""
    try:
        state_key = _key(WHALE_STATE_PREFIX, month, room_id)
        _client.hset(state_key, mapping={"status": "archiving"})
        _client.expire(state_key, max(1, REDIS_KEY_TTL_SECONDS))
    except redis.RedisError as exc:
        logger.error("[WhaleRedis] 标记归档中失败 room_id=%s month=%s: %s", room_id, month, exc)


def finish_whale_archive(room_id: int, month: str) -> bool:
    """Mark a snapshot archived only when no event made it dirty meanwhile."""
    try:
        result = _client.eval(
            _FINALIZE_SCRIPT,
            1,
            _key(WHALE_STATE_PREFIX, month, room_id),
        )
    except redis.RedisError as exc:
        logger.error("[WhaleRedis] 标记归档完成失败 room_id=%s month=%s: %s", room_id, month, exc)
        return False
    return bool(int(result or 0))


def whale_dependency_payload(
    metrics: WhaleMetrics | None,
    status: str,
    source: str,
) -> WhaleDependencyPayload:
    """Convert metrics into the stable nested API response shape."""
    if metrics is None:
        return {
            "status": status,
            "source": source,
            "top1": None,
            "top5": None,
            "top10": None,
            "top1_percent": None,
        }
    return {
        "status": status,
        "source": source,
        "top1": metrics.top1_ratio,
        "top5": metrics.top5_ratio,
        "top10": metrics.top10_ratio,
        "top1_percent": metrics.top1pct_ratio,
    }
