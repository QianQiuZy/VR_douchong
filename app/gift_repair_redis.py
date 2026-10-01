"""Exactly-once whale income deltas without changing event or payer counts."""

import datetime
import re

from redis import Redis
from sqlalchemy import Engine, text

from .gift_repair_types import RepairError

SCRIPT = """
local marker_type = redis.call('TYPE', KEYS[1]).ok
if marker_type ~= 'none' and marker_type ~= 'set' then return redis.error_reply('repair marker has wrong type') end
local delta = 0
local pending = {}
for i=2,#ARGV,2 do
    local amount = tonumber(ARGV[i+1])
    if not amount or amount <= 0 or amount > 9007199254740991 then return redis.error_reply('invalid correction amount') end
    if redis.call('SISMEMBER', KEYS[1], ARGV[i]) == 0 then
        delta = delta + amount
        table.insert(pending, ARGV[i])
    end
end
if delta == 0 then return 0 end
if delta > 9007199254740991 then return redis.error_reply('correction overflow') end
local uid = ARGV[1]
if redis.call('TYPE', KEYS[3]).ok ~= 'hash' then return redis.error_reply('whale totals missing') end
if uid ~= '0' and (redis.call('TYPE', KEYS[2]).ok ~= 'hash' or not redis.call('HGET', KEYS[2], uid)) then
    return redis.error_reply('whale payer missing')
end
local state_type = redis.call('TYPE', KEYS[4]).ok
if state_type ~= 'none' and state_type ~= 'hash' then return redis.error_reply('whale state has wrong type') end
local attribution = uid == '0' and 'unattributed_revenue' or 'attributed_revenue'
local fields = {'total_revenue','gift_revenue',attribution}
local function validate(value)
    value = value or '0'
    local n = tonumber(value)
    return (value == '0' or string.match(value,'^[1-9]%d*$')) and n and n >= 0 and n + delta <= 9007199254740991
end
for _,field in ipairs(fields) do
    if not validate(redis.call('HGET',KEYS[3],field)) then return redis.error_reply('invalid whale counter') end
end
if uid ~= '0' and not validate(redis.call('HGET',KEYS[2],uid)) then return redis.error_reply('invalid whale payer counter') end
if uid ~= '0' then redis.call('HINCRBY',KEYS[2],uid,string.format('%.0f',delta)) end
for _,field in ipairs(fields) do redis.call('HINCRBY',KEYS[3],field,string.format('%.0f',delta)) end
redis.call('HSET',KEYS[4],'status','dirty')
for _,event in ipairs(pending) do redis.call('SADD',KEYS[1],event) end
return string.format('%.0f',delta)
"""


def keys(room: int, month: str) -> tuple[str, str, str, str]:
    return (
        f"vr:repair:gift-v2:v1:{month}:{room}",
        f"vr:whale:v1:uid:{month}:{room}",
        f"vr:whale:v1:totals:{month}:{room}",
        f"vr:whale:v1:state:{month}:{room}",
    )


def verify_whale(client: Redis, group: tuple[int, str, int], events: list[tuple[str, int]]) -> int:
    room, month, uid = group
    marker, payer, totals, state = keys(room, month)
    if client.type(marker) not in ("none", "set"):
        raise RepairError("Whale repair marker has an unexpected type")
    pending = [(event, amount) for event, amount in events if not client.sismember(marker, event)]
    if not pending:
        return 0
    if client.type(totals) != "hash" or (uid > 0 and not client.hexists(payer, str(uid))):
        raise RepairError(f"Whale cache absent for room={room} month={month}; do not fabricate past feature data")
    if client.type(state) not in ("none", "hash"):
        raise RepairError("Whale state has an unexpected type")
    delta = sum(amount for _, amount in pending)
    attribution = "attributed_revenue" if uid > 0 else "unattributed_revenue"
    values = [client.hget(totals, field) or "0" for field in ("total_revenue", "gift_revenue", attribution)]
    if uid > 0:
        values.append(client.hget(payer, str(uid)) or "0")
    if any(not re.fullmatch(r"0|[1-9]\d*", str(value)) for value in values):
        raise RepairError("Whale counter is not a canonical Redis integer")
    if any(int(value) < 0 or int(value) + delta > 9007199254740991 for value in values):
        raise RepairError("Whale counter is outside the safe integer range")
    return delta


def apply_whale(client: Redis, group: tuple[int, str, int], events: list[tuple[str, int]]) -> int:
    room, month, uid = group
    arguments: list[str] = [str(uid)]
    for event, amount in events:
        arguments.extend((event, str(amount)))
    return int(client.eval(SCRIPT, 4, *keys(room, month), *arguments))


def refresh_metrics(engine: Engine, client: Redis, rooms: set[tuple[int, str]]) -> int:
    from .whale_metrics import calculate_whale_metrics

    refreshed = 0
    current_month = datetime.datetime.now(datetime.timezone.utc).astimezone().strftime("%Y%m")
    for room, month in sorted(rooms):
        if month >= current_month:
            continue
        _, payer_key, totals_key, state_key = keys(room, month)
        totals = client.hgetall(totals_key)
        if not totals:
            raise RepairError("Whale totals expired before derived metrics were refreshed")
        payers = {int(uid): int(amount) for uid, amount in client.hgetall(payer_key).items()}
        metrics = calculate_whale_metrics(payers, int(totals.get("total_revenue", 0)), int(totals.get("unattributed_revenue", 0)))
        values = {
            "room": room, "month": month, "top1_amount": metrics.top1_amount, "top1_ratio": metrics.top1_ratio,
            "top5_amount": metrics.top5_amount, "top5_ratio": metrics.top5_ratio,
            "top10_amount": metrics.top10_amount, "top10_ratio": metrics.top10_ratio,
            "top1pct_amount": metrics.top1pct_amount, "top1pct_ratio": metrics.top1pct_ratio,
            "total": metrics.total_revenue, "attributed": metrics.attributed_revenue,
            "unattributed": metrics.unattributed_revenue, "payers": metrics.payer_count,
            "now": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
        }
        with engine.begin() as connection:
            result = connection.execute(text("""UPDATE room_stats_monthly SET
                whale_top1_amount=:top1_amount, whale_top1_ratio=:top1_ratio,
                whale_top5_amount=:top5_amount, whale_top5_ratio=:top5_ratio,
                whale_top10_amount=:top10_amount, whale_top10_ratio=:top10_ratio,
                whale_top1pct_amount=:top1pct_amount, whale_top1pct_ratio=:top1pct_ratio,
                whale_total_revenue=:total, whale_attributed_revenue=:attributed,
                whale_unattributed_revenue=:unattributed, whale_payer_count=:payers,
                whale_status='archived', whale_metric_version=1, whale_calculated_at=:now
                WHERE room_id=:room AND month=:month"""), values)
            if result.rowcount != 1:
                raise RepairError("Whale archive target is missing")
        client.hset(state_key, "status", "archived")
        refreshed += 1
    return refreshed
