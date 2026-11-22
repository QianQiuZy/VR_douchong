"""Batched danmaku sender cardinalities (DB3) and concentration snapshots (DB4)."""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, replace

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from . import Audience, DanmakuCounts, PendingDanmaku, SenderBatch
from .api_cache_store import redis_db_url
from .config import API_CACHE_PREFIX, REDIS_KEY_TTL_SECONDS, REDIS_URL

logger = logging.getLogger(__name__)


def _client(database: int) -> redis.Redis:
    return redis.Redis.from_url(
        redis_db_url(REDIS_URL, database),
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=2,
        retry=Retry(NoBackoff(), 0),
    )


users = _client(3)
ranks = _client(4)
PREFIX = f"{API_CACHE_PREFIX}:danmaku:v1"
RATIOS = {
    "top1": "danmaku_top1_ratio",
    "top5": "danmaku_top5_ratio",
    "top10": "danmaku_top10_ratio",
    "top1_percent": "danmaku_top1pct_ratio",
}
COLUMNS = ("danmaku_user_count", "danmaku_status", *RATIOS.values())

# A room's frozen batches are always replayed in order. The token makes a lost
# response safe to retry, including after DB3 succeeded and DB4 timed out.
_RECORD_USERS = """
local size = redis.call('SCARD', KEYS[1])
if size < tonumber(redis.call('HGET', KEYS[2], 'users') or '0') then
    redis.call('HSET', KEYS[2], 'partial', 1)
end
if redis.call('HGET', KEYS[2], 'batch') ~= ARGV[1] then
    for i = 4, #ARGV do redis.call('SADD', KEYS[1], ARGV[i]) end
    size = redis.call('SCARD', KEYS[1])
    redis.call('HINCRBY', KEYS[2], 'total', ARGV[3])
    redis.call('HSET', KEYS[2], 'batch', ARGV[1], 'users', size)
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    redis.call('EXPIRE', KEYS[2], ARGV[2])
end
return {size, redis.call('HGET', KEYS[2], 'total'), redis.call('HEXISTS', KEYS[2], 'partial')}
"""
_RECORD = """
if redis.call('HGET', KEYS[2], 'batch') == ARGV[1] then return end
local total = redis.call('HINCRBY', KEYS[2], 'total', ARGV[3])
redis.call('HINCRBY', KEYS[2], 'unattributed', ARGV[4])
for i = 5, #ARGV, 2 do
    redis.call('ZINCRBY', KEYS[1], ARGV[i+1], ARGV[i])
end
local n = redis.call('ZCARD', KEYS[1])
local pct = math.ceil(n / 100)
local scores = redis.call('ZREVRANGE', KEYS[1], 0, math.max(10, pct)-1, 'WITHSCORES')
local limits = {1, 5, 10, pct}
local fields = {'top1', 'top5', 'top10', 'top1_percent'}
for i = 1, 4 do
    local subtotal = 0
    for j = 2, math.min(limits[i]*2, #scores), 2 do
        subtotal = subtotal + tonumber(scores[j])
    end
    redis.call('HSET', KEYS[2], fields[i] .. '_count', subtotal, fields[i], tostring(subtotal / total))
end
local previous = redis.call('HGET', KEYS[2], 'state')
local state = (previous == 'archived' or previous == 'dirty') and 'dirty' or 'live'
redis.call('HSET', KEYS[2], 'batch', ARGV[1], 'users', n, 'state', state)
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
"""

_started: datetime.datetime | None = None


def tracking_start() -> datetime.datetime:
    """Persist the deployment boundary so a restart does not reset coverage."""
    global _started
    if _started is None:
        key = f"{PREFIX}:since"
        ranks.set(key, datetime.datetime.now().isoformat(), nx=True)
        _started = datetime.datetime.fromisoformat(ranks.get(key))
    return _started


def covered(start: datetime.datetime) -> bool:
    global _started
    try:
        if _started is None:
            stamp = ranks.get(f"{PREFIX}:since")
            if stamp is None:
                return False
            _started = datetime.datetime.fromisoformat(stamp)
        return start >= _started
    except redis.RedisError:
        return False


def start_tracking() -> None:
    try:
        tracking_start()
    except redis.RedisError:
        logger.warning("弹幕 UID 统计起始时间等待 Redis 恢复")


def initial_monthly_count(context) -> int | None:
    values = context.get_current_parameters()
    month = values.get("month")
    if month and not values.get("danmaku_count") and covered(month_start(month)):
        return 0
    return None


def month_start(month: str) -> datetime.datetime:
    return datetime.datetime.strptime(month, "%Y%m")


@dataclass(frozen=True, slots=True)
class Scope:
    key: str
    start: datetime.datetime
    ranked: bool = True


def record(scope: Scope, batch: SenderBatch) -> Audience:
    """Apply one batch; unknown UIDs contribute only to the denominator."""
    start = tracking_start()
    key = f"{PREFIX}:users:{scope.key}"
    valid = {uid: count for uid, count in batch.counts.items() if uid > 0}
    ttl = max(1, REDIS_KEY_TTL_SECONDS)
    size, total, partial = map(
        int,
        users.eval(
            _RECORD_USERS,
            2,
            key,
            f"{key}:state",
            batch.token,
            ttl,
            batch.counts.total(),
            *valid,
        ),
    )
    if scope.ranked:
        summary = f"{PREFIX}:summary:{scope.key}"
        args = [item for pair in valid.items() for item in pair]
        with ranks.pipeline() as pipe:
            pipe.eval(
                _RECORD,
                2,
                f"{PREFIX}:rank:{scope.key}",
                summary,
                batch.token,
                ttl,
                batch.counts.total(),
                batch.counts.total() - sum(valid.values()),
                *args,
            )
            pipe.hmget(summary, "users", "total")
            n, ranked_total = map(int, pipe.execute()[-1])
        if n != size or ranked_total != total:
            return Audience(None, total)
    return Audience(size if scope.start >= start and not partial else None, total)


def prepare(
    pending: PendingDanmaku, key: object, scope: Scope, counts: DanmakuCounts
) -> DanmakuCounts:
    if key in pending.retry:
        pending.results[key] = record(scope, pending.retry[key])
        del pending.retry[key]
    if key in pending.senders:
        pending.retry[key] = pending.senders.pop(key)
        pending.results[key] = record(scope, pending.retry[key])
        del pending.retry[key]
    return replace(counts, audience=pending.results.get(key))


def snapshot(scope: str) -> dict:
    return ranks.hgetall(f"{PREFIX}:summary:{scope}")


def ratio_values(summary: dict) -> dict:
    total = int(summary.get("total", 0))
    return {name: int(summary[f"{name}_count"]) / total if total else None for name in RATIOS}


def report(row, scope: str, *, live: bool, zero: bool = False, partial: bool = False) -> dict:
    """Render archived SQL rows and live Redis snapshots using one contract."""
    count = getattr(row, "danmaku_user_count", None)
    stored = getattr(row, "danmaku_status", None)
    status, source = "unavailable", "mysql"
    ratios = dict.fromkeys(RATIOS)
    if live:
        source = "redis"
        try:
            data = snapshot(scope)
            if data:
                ratios = ratio_values(data)
                complete = count == int(data["users"]) and int(data["total"]) == getattr(
                    row, "danmaku_count", 0
                )
                status = "live" if complete else "partial"
                if not complete:
                    count = None
            elif zero or (not partial and count == 0 and not getattr(row, "danmaku_count", 0)):
                status, count = "live", 0
            elif partial or stored in ("collecting", "partial"):
                status = "partial"
        except redis.RedisError:
            logger.warning("弹幕占比暂不可读 scope=%s", scope)
    elif stored in ("archived", "partial"):
        status = stored
        ratios = {name: getattr(row, column, None) for name, column in RATIOS.items()}
    if status == "partial":
        count = None
    dependency = {
        "status": status,
        "source": source,
        **{k: None if v is None else round(float(v), 4) for k, v in ratios.items()},
    }
    return {"danmaku_user_count": count, "danmaku_dependency": dependency}


def monthly_report(row, room_id: int, month: str, *, current: bool, empty: bool = False) -> dict:
    live = not empty and (current or getattr(row, "danmaku_status", None) == "collecting")
    full_period = live and covered(month_start(month))
    zero = full_period and not getattr(row, "danmaku_count", 0)
    return report(
        row, f"month:{month}:{room_id}", live=live, zero=zero, partial=live and not full_period
    )


@dataclass(frozen=True, slots=True)
class Settlement:
    key: str
    batch: str | None

    def committed(self) -> None:
        if self.batch is None:
            return
        try:
            ranks.eval(
                """
                if redis.call('HGET', KEYS[1], 'batch') == ARGV[1] then
                    redis.call('HSET', KEYS[1], 'state', 'archived')
                end
            """,
                1,
                self.key,
                self.batch,
            )
        except redis.RedisError:
            logger.warning("弹幕占比已写入 MySQL，Redis 归档标记未更新")


def settle(row, scope: str, start: datetime.datetime) -> Settlement | None:
    """Fill final columns under the caller's SQL row lock; retain live state on failure."""
    try:
        data = snapshot(scope)
        result = Settlement(f"{PREFIX}:summary:{scope}", data.get("batch"))
        if not data and row.danmaku_count:
            # Legacy totals or expired Redis detail cannot reconstruct a ranking.
            row.danmaku_status = "partial" if start < tracking_start() else "unavailable"
            if row.danmaku_status == "partial":
                row.danmaku_user_count = None
            for column in RATIOS.values():
                setattr(row, column, None)
            return result
        complete = start >= tracking_start() and int(data.get("total", 0)) == (
            row.danmaku_count or 0
        )
        if data:
            complete = complete and row.danmaku_user_count == int(data["users"])
        row.danmaku_status = "archived" if complete else "partial"
        if not complete or not data:
            row.danmaku_user_count = 0 if complete else None
        for name, value in ratio_values(data).items():
            setattr(row, RATIOS[name], value)
        return result
    except redis.RedisError:
        logger.warning("弹幕占比等待重试 scope=%s", scope)
        return None
