"""Redis-backed UID sets for payer and steel-coin deduplication."""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass

import redis

from .config import REDIS_KEY_TTL_SECONDS, REDIS_URL


@dataclass(frozen=True, slots=True)
class SetAddResult:
    """Result of adding one identity to a Redis set."""

    added: bool
    size: int | None


@dataclass(frozen=True, slots=True)
class PayerRegistration:
    """UID cardinalities returned for all active reporting scopes."""

    session: SetAddResult | None
    bucket: SetAddResult | None
    daily: SetAddResult
    monthly: SetAddResult
    steel_coin: SetAddResult | None


_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _add(key: str, member: int) -> SetAddResult:
    try:
        added = bool(_client.sadd(key, str(member)))
        _ = _client.expire(key, REDIS_KEY_TTL_SECONDS)
        return SetAddResult(added, int(_client.scard(key)))
    except redis.RedisError as exc:
        logging.error("[Redis] UID去重失败 key=%s: %s", key, exc)
        return SetAddResult(False, None)


def _date_key(value: datetime.date) -> str:
    return value.strftime("%Y%m%d")


def register_payer(
    room_id: int,
    session_id: int | None,
    bucket_index: int | None,
    uid: int,
    event_date: datetime.date,
    month: str,
    steel_coin: bool = False,
) -> PayerRegistration:
    """Register one UID at session, bucket, room-day, and room-month scopes."""
    session_result = None
    bucket_result = None
    if session_id is not None:
        session_result = _add(f"vr:payer:session:{session_id}", uid)
        if bucket_index is not None:
            bucket_result = _add(f"vr:payer:bucket:{session_id}:{bucket_index}", uid)
    daily_result = _add(f"vr:payer:day:{_date_key(event_date)}:{room_id}", uid)
    monthly_result = _add(f"vr:payer:month:{month}:{room_id}", uid)
    steel_result = None
    if steel_coin:
        steel_result = _add(f"vr:steel:{_date_key(event_date)}", uid)
    return PayerRegistration(session_result, bucket_result, daily_result, monthly_result, steel_result)


def delete_session_keys(session_id: int) -> None:
    """Delete the long-lived session set after a session has been closed."""
    try:
        _ = _client.delete(f"vr:payer:session:{session_id}")
    except redis.RedisError as exc:
        logging.warning("[Redis] 清理场次UID集合失败 session_id=%s: %s", session_id, exc)
