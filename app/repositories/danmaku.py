"""Atomic message deltas and absolute sender counts for MySQL aggregate upserts."""

from sqlalchemy import case, func


def increments(counts):
    return {
        "danmaku_count": counts.total,
        "captain_danmaku_count": counts.captain,
        "admiral_danmaku_count": counts.admiral,
        "governor_danmaku_count": counts.governor,
        "normal_danmaku_count": counts.normal,
    }


def upsert_fields(model, counts):
    values = increments(counts)
    updates = [
        (name, func.coalesce(getattr(model, name), 0) + delta) for name, delta in values.items()
    ]
    if counts.audience is not None:
        count, covered_total = counts.audience.count, counts.audience.total
        values["danmaku_user_count"] = count if covered_total == counts.total else None
        # MySQL evaluates assignments left to right. Compare with the old total first.
        updates.insert(
            0,
            (
                "danmaku_user_count",
                case(
                    (func.coalesce(model.danmaku_count, 0) + counts.total == covered_total, count),
                    else_=None,
                ),
            ),
        )
    return values, updates
