"""DB2-only storage, atomic generation publication and shared rolling budgets."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import threading
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import redis

from . import config

logger = logging.getLogger(__name__)

# Integer microseconds, half-open rolling second; denied requests do not extend it.
WINDOW_SCRIPT = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 1000000)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[1]) then
    local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    return {0, math.max(1, tonumber(first[2]) + 1000000 - now)}
end
redis.call('ZADD', KEYS[1], now, ARGV[2])
redis.call('PEXPIRE', KEYS[1], 2000)
return {1, now}
"""
LEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] == '0' then redis.call('DEL', KEYS[1])
else redis.call('EXPIRE', KEYS[1], ARGV[2]) end
return 1
"""
PUBLISH_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
local previous = redis.call('GET', KEYS[3])
redis.call('SADD', KEYS[4], ARGV[2])
redis.call('SET', KEYS[3], KEYS[2])
redis.call('PERSIST', KEYS[2])
if previous and previous ~= KEYS[2] then redis.call('EXPIRE', previous, 5) end
return 1
"""
BEGIN_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2], KEYS[3])
return 1
"""
READY_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[1], 'EX', 60)
return 1
"""
READ_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
local generation = redis.call('GET', KEYS[2])
local fallback = 0
if redis.call('SISMEMBER', KEYS[3], ARGV[3]) == 0 then
    generation = redis.call('GET', KEYS[4])
    fallback = 1
end
if not generation then return {} end
local stamp = redis.call('HGET', generation, '_snapshot')
local body
local field = ARGV[1]
if fallback == 1 then body = redis.call('HGET', generation, '_empty:' .. ARGV[4])
    field = '_empty:' .. ARGV[4]
else
    body = redis.call('HGET', generation, ARGV[1])
    if not body and ARGV[2] ~= '' and redis.call('HEXISTS', generation, '_room:' .. ARGV[2]) == 0 then
        body = redis.call('HGET', generation, '_empty:' .. ARGV[4])
        field = '_empty:' .. ARGV[4]
        fallback = 1
    end
end
local t = redis.call('TIME')
local digest = redis.call('HGET', generation, '_hash:' .. field)
return {stamp or '', body or '', t[1], t[2], fallback, digest or ''}
"""
SUMMARY_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local generation = redis.call('GET', KEYS[2])
if not generation or redis.call('EXISTS', generation) == 0 then return 0 end
redis.call('HSET', generation, '/gift/by_month', ARGV[2])
redis.call('HSET', generation, '_hash:/gift/by_month', ARGV[3])
return 1
"""


class CacheUnavailable(RuntimeError):
    """No complete, sufficiently fresh cached result can be returned."""


@dataclass
class ApiSqlScope:
    store: CacheStore
    active: bool = True
    stop: threading.Event = field(default_factory=threading.Event)


api_sql_scope: ContextVar[ApiSqlScope | None] = ContextVar(
    "api_sql_scope", default=None
)


def db2_url(url: str) -> str:
    parsed = urlsplit(url)
    query = urlencode([(k, v) for k, v in parse_qsl(parsed.query) if k != "db"])
    return urlunsplit((parsed.scheme, parsed.netloc, "/2", query, parsed.fragment))


def encode(payload) -> bytes:
    from fastapi.encoders import jsonable_encoder

    body = json.dumps(
        jsonable_encoder(payload),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode()
    return gzip.compress(body, compresslevel=1, mtime=0)


class FaultLog:
    """Only faults, at most one record per category per minute; no success logs."""

    def __init__(self):
        self.last: dict[str, float] = {}
        self.lock = threading.Lock()

    def record(self, category: str, exc: BaseException) -> None:
        with self.lock:
            now = time.monotonic()
            if now - self.last.get(category, float("-inf")) < 60:
                return
            self.last[category] = now
        # Exception text may contain SQL parameters, URLs or credentials.
        logger.error("[api-cache] fault=%s error_type=%s", category, type(exc).__name__)


class CacheStore:
    def __init__(self, client=None, prefix: str | None = None):
        self.client = (
            client
            if client is not None
            else redis.Redis.from_url(
                db2_url(config.REDIS_URL),
                decode_responses=False,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        )
        self.prefix = prefix or config.API_CACHE_PREFIX
        self.faults = FaultLog()
        self.max_bytes = config.API_CACHE_MAX_BYTES
        self.max_age = config.API_CACHE_MAX_AGE_SECONDS
        self.sql_limit = config.API_CACHE_SQL_PER_SECOND
        self.ip_limit = config.API_RATE_LIMIT_PER_IP
        self.owner = uuid.uuid4().hex

    def key(self, suffix: str) -> str:
        return f"{self.prefix}:{suffix}"

    @property
    def lease_key(self) -> str:
        return self.key("leader")

    def acquire(self) -> bool:
        return bool(self.client.set(self.lease_key, self.owner, nx=True, ex=60))

    def renew(self) -> None:
        if not self.client.eval(LEASE_SCRIPT, 1, self.lease_key, self.owner, 60):
            raise CacheUnavailable("builder lease lost")

    def release(self) -> None:
        self.client.eval(LEASE_SCRIPT, 1, self.lease_key, self.owner, 0)

    def clock(self) -> float:
        seconds, micros = self.client.time()
        return seconds + micros / 1_000_000

    def allow_ip(self, ip: str) -> bool:
        allowed, _ = self.client.eval(
            WINDOW_SCRIPT, 1, self.key(f"ip:{ip}"), self.ip_limit, uuid.uuid4().hex
        )
        return bool(allowed)

    def take_sql(self, stop: threading.Event, *, require_lease: bool = True) -> None:
        while not stop.is_set():
            if require_lease:
                self.renew()
            allowed, delay = self.client.eval(
                WINDOW_SCRIPT,
                1,
                self.key("sql-budget"),
                self.sql_limit,
                uuid.uuid4().hex,
            )
            if allowed:
                return
            stop.wait(min(float(delay) / 1_000_000 + 0.001, 1.01))
        raise CacheUnavailable("builder stopped")

    def publish(self, month: str, values: dict[str, bytes], stamp: float) -> None:
        self.renew()
        generation = self.key(f"generation:{month}:{uuid.uuid4().hex}")
        self.account_memory()
        total = int(self.client.get(self.key("bytes")) or 0)
        hashes = {
            f"_hash:{field}": hashlib.sha256(body).hexdigest()
            for field, body in values.items()
            if field.startswith(("/", "_empty:"))
        }
        new_size = sum(len(k) + len(v) for k, v in {**values, **hashes}.items())
        # Account staging + active + short-lived previous generations, not just the new one.
        if total + new_size > self.max_bytes:
            raise CacheUnavailable("cache application memory budget exceeded")
        mapping = {**values, **hashes, "_snapshot": repr(stamp)}
        # A failed/crashed stage always expires; never delete unrelated DB2 keys.
        with self.client.pipeline(transaction=True) as pipe:
            pipe.hset(generation, mapping=mapping)
            pipe.expire(generation, 60)
            pipe.execute()
        if not self.client.eval(
            PUBLISH_SCRIPT,
            4,
            self.lease_key,
            generation,
            self.key(f"month:{month}"),
            self.key("months"),
            self.owner,
            month,
        ):
            raise CacheUnavailable("generation publication failed")

    def account_memory(self) -> None:
        """Bounded scan of our generations only, including old generations pending expiry."""
        total = 0
        for key in self.client.scan_iter(match=self.key("generation:*"), count=100):
            total += int(self.client.memory_usage(key) or 0)
        self.client.set(self.key("bytes"), total)
        if total > self.max_bytes:
            raise CacheUnavailable("cache application memory budget exceeded")

    def ready(self) -> None:
        self.renew()
        if not self.client.eval(
            READY_SCRIPT, 2, self.lease_key, self.key("ready"), self.owner
        ):
            raise CacheUnavailable("ready publication lost its lease")

    def begin_prewarm(self) -> None:
        if not self.client.eval(
            BEGIN_SCRIPT,
            3,
            self.lease_key,
            self.key("ready"),
            self.key("months"),
            self.owner,
        ):
            raise CacheUnavailable("prewarm lost its lease")

    def update_summary(self, month: str, body: bytes) -> bool:
        self.renew()
        return bool(
            self.client.eval(
                SUMMARY_SCRIPT,
                2,
                self.lease_key,
                self.key(f"month:{month}"),
                self.owner,
                body,
                hashlib.sha256(body).hexdigest(),
            )
        )

    def read(self, month: str, path: str, room_id: int | None = None) -> bytes:
        from .repositories.tables import month_str

        current_month = month_str()
        field = path if room_id is None else f"{path}:{room_id}"
        result = self.client.eval(
            READ_SCRIPT,
            4,
            self.key("ready"),
            self.key(f"month:{month}"),
            self.key("months"),
            self.key(f"month:{current_month}"),
            field,
            "" if room_id is None else str(room_id),
            month,
            path,
        )
        if not result:
            raise CacheUnavailable("prewarm incomplete or month missing")
        stamp, body, seconds, micros, fallback, digest = result
        age = int(seconds) + int(micros) / 1_000_000 - float(stamp)
        if month == current_month and not 0 <= age <= self.max_age:
            raise CacheUnavailable("current snapshot stale")
        if not body:
            raise CacheUnavailable("known cache field missing")
        if hashlib.sha256(body).hexdigest().encode() != digest:
            raise CacheUnavailable("cache body checksum mismatch")
        if fallback:
            payload = json.loads(gzip.decompress(body))
            if isinstance(payload, list):
                for row in payload:
                    row["month"] = month
            else:
                payload.update(room_id=room_id, month=month)
            return encode(payload)
        return body
