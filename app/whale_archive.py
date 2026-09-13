"""MySQL archive adapter for Redis-backed monthly whale metrics."""

from __future__ import annotations

import datetime
import logging

import redis
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .database import engine
from .repositories.tables import month_str, normalize_month_code
from .whale_metrics import (
    WHALE_TOTALS_PREFIX,
    WhaleMetrics,
    _client,
    begin_whale_archive,
    finish_whale_archive,
    get_live_whale_metrics,
)

logger = logging.getLogger(__name__)


def _room_month_from_total_key(key: str) -> tuple[str, int] | None:
    parts = key.split(":")
    if len(parts) != 6 or parts[:4] != ["vr", "whale", "v1", "totals"]:
        return None
    try:
        return parts[4], int(parts[5])
    except ValueError:
        return None


def _months_and_rooms(target_month: str | None, current_month: str) -> dict[str, list[int]]:
    pattern = (
        f"{WHALE_TOTALS_PREFIX}:{target_month}:*"
        if target_month is not None
        else f"{WHALE_TOTALS_PREFIX}:*:*"
    )
    grouped: dict[str, list[int]] = {}
    for raw_key in _client.scan_iter(match=pattern):
        parsed = _room_month_from_total_key(str(raw_key))
        if parsed is None:
            continue
        month, room_id = parsed
        if target_month is None and month >= current_month:
            continue
        grouped.setdefault(month, []).append(room_id)
    return grouped


def _persist_metrics(room_id: int, month: str, metrics: WhaleMetrics) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO room_stats_monthly "
                "(room_id, month, gift, guard, super_chat, payer_count, "
                "whale_top1_amount, whale_top1_ratio, whale_top5_amount, whale_top5_ratio, "
                "whale_top10_amount, whale_top10_ratio, whale_top1pct_amount, whale_top1pct_ratio, "
                "whale_total_revenue, whale_attributed_revenue, whale_unattributed_revenue, "
                "whale_payer_count, whale_status, whale_metric_version, whale_calculated_at) "
                "VALUES (:room_id, :month, 0, 0, 0, 0, :top1_amount, :top1_ratio, "
                ":top5_amount, :top5_ratio, :top10_amount, :top10_ratio, :top1pct_amount, "
                ":top1pct_ratio, :total_revenue, :attributed_revenue, :unattributed_revenue, "
                ":payer_count, 'archived', 1, :calculated_at) "
                "ON DUPLICATE KEY UPDATE "
                "whale_top1_amount=VALUES(whale_top1_amount), whale_top1_ratio=VALUES(whale_top1_ratio), "
                "whale_top5_amount=VALUES(whale_top5_amount), whale_top5_ratio=VALUES(whale_top5_ratio), "
                "whale_top10_amount=VALUES(whale_top10_amount), whale_top10_ratio=VALUES(whale_top10_ratio), "
                "whale_top1pct_amount=VALUES(whale_top1pct_amount), whale_top1pct_ratio=VALUES(whale_top1pct_ratio), "
                "whale_total_revenue=VALUES(whale_total_revenue), whale_attributed_revenue=VALUES(whale_attributed_revenue), "
                "whale_unattributed_revenue=VALUES(whale_unattributed_revenue), whale_payer_count=VALUES(whale_payer_count), "
                "whale_status='archived', whale_metric_version=1, whale_calculated_at=VALUES(whale_calculated_at)"
            ),
            {
                "room_id": room_id,
                "month": month,
                "top1_amount": metrics.top1_amount,
                "top1_ratio": metrics.top1_ratio,
                "top5_amount": metrics.top5_amount,
                "top5_ratio": metrics.top5_ratio,
                "top10_amount": metrics.top10_amount,
                "top10_ratio": metrics.top10_ratio,
                "top1pct_amount": metrics.top1pct_amount,
                "top1pct_ratio": metrics.top1pct_ratio,
                "total_revenue": metrics.total_revenue,
                "attributed_revenue": metrics.attributed_revenue,
                "unattributed_revenue": metrics.unattributed_revenue,
                "payer_count": metrics.payer_count,
                "calculated_at": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
            },
        )


def archive_whale_month(target_month: str | None = None) -> int:
    """Persist every available historical Redis room-month idempotently."""
    current_month = month_str()
    normalized = normalize_month_code(target_month) if target_month else None
    if target_month and (normalized is None or normalized >= current_month):
        return 0
    try:
        grouped = _months_and_rooms(normalized, current_month)
    except redis.RedisError as exc:
        logger.error("[WhaleArchive] 枚举归档月份失败: %s", exc)
        return 0

    archived = 0
    for month, room_ids in sorted(grouped.items()):
        for room_id in sorted(set(room_ids)):
            begin_whale_archive(room_id, month)
            metrics = get_live_whale_metrics(room_id, month)
            if metrics is None:
                continue
            try:
                _persist_metrics(room_id, month, metrics)
                if finish_whale_archive(room_id, month):
                    archived += 1
            except SQLAlchemyError as exc:
                logger.error("[WhaleArchive] 归档失败 room_id=%s month=%s: %s", room_id, month, exc)
    return archived
