"""Atomic message deltas and absolute sender counts for MySQL aggregate upserts."""

from sqlalchemy import case, func


def claim_replay(session, counts) -> bool:
    if counts.replay_cursor is None:
        return True
    from ..models.recovery import DanmakuReplayCursor

    stream, token = counts.replay_cursor
    row = session.get(DanmakuReplayCursor, stream, with_for_update=True)
    if row is None:
        session.add(DanmakuReplayCursor(stream=stream, token=token))
    elif row.token == token:
        return False
    else:
        row.token = token
    return True


def increments(counts):
    """Counters that already treat a missing baseline as zero on write."""
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
    values["emoticon_danmaku_count"] = counts.emoticon
    # Compare the pre-update total. NULL with prior messages must stay unknown.
    emoticon_count = case(
        (model.danmaku_count == 0, counts.emoticon),
        else_=model.emoticon_danmaku_count + counts.emoticon,
    )
    updates.insert(0, ("emoticon_danmaku_count", emoticon_count))
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
