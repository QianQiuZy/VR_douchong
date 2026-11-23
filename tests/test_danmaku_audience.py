import datetime
from collections import Counter
from types import SimpleNamespace

import redis

from app import DanmakuBucketTarget, DanmakuCounts, PendingDanmaku, danmaku_metrics as dm
from app import monitoring_jobs, runtime_state


def test_ranking_cutoffs_replay_and_unknown_uids(monkeypatch):
    start = datetime.datetime(2026, 6, 1)
    monkeypatch.setattr(dm, "_started", start)
    counts = Counter({uid: 1 for uid in range(1, 102)})
    counts.update({1: 9, 2: 4, 0: 6})
    batch = dm.SenderBatch(counts)
    scope = dm.Scope("session:101", start)
    for _ in range(2):
        audience = dm.record(scope, batch)
        assert audience == dm.Audience(101, 120)
    summary = dm.snapshot(scope.key)
    assert int(summary["unattributed"]) == 6
    assert dm.ratio_values(summary) == {
        "top1": 10 / 120,
        "top5": 18 / 120,
        "top10": 23 / 120,
        "top1_percent": 15 / 120,
    }
    assert (
        dm.record(dm.Scope("month:202605:1", start - datetime.timedelta(days=1)), batch).count
        is None
    )


def test_failed_rank_flush_replays_only_senders_and_accepts_new_events(monkeypatch):
    start = datetime.datetime(2026, 11, 1)
    target = DanmakuBucketTarget(81, 8, "202611", 0, start, start + datetime.timedelta(minutes=15))
    pending = PendingDanmaku()
    writes = {scope: [] for scope in ("month", "session", "bucket")}

    def write(scope, counts):
        writes[scope].append(counts)
        return True

    monkeypatch.setattr(dm, "_started", start)
    monkeypatch.setattr(runtime_state, "DANMAKU_PENDING", {8: pending})
    monkeypatch.setattr(
        monitoring_jobs.RoomStatsMonthly,
        "add_danmaku_counts",
        lambda room, month, counts: write("month", counts),
    )
    monkeypatch.setattr(
        monitoring_jobs.LiveSession,
        "add_danmaku_by_id",
        lambda sid, counts: write("session", counts),
    )
    monkeypatch.setattr(
        monitoring_jobs.LiveSession15mStats,
        "add_danmaku_counts",
        lambda bucket, counts: write("bucket", counts),
    )
    original = dm.record

    def lost_reply(scope, batch):
        original(scope, batch)
        raise redis.TimeoutError("lost reply after successful write")

    pending.add(start, DanmakuCounts(total=1, normal=1, emoticon=1), target, 91)
    monkeypatch.setattr(dm, "record", lost_reply)
    monitoring_jobs.flush_pending_danmaku_for_room(8)
    pending.add(start, DanmakuCounts(total=1, normal=1), target, 92)
    monkeypatch.setattr(dm, "record", original)
    monitoring_jobs.flush_pending_danmaku_for_room(8)
    assert not runtime_state.DANMAKU_PENDING
    for rows in writes.values():
        assert [row.total for row in rows] == [1, 1]
        assert [row.emoticon for row in rows] == [1, 0]
        assert rows[-1].audience == dm.Audience(2, 2)
    assert dm.snapshot("session:81")["total"] == "2"


def test_missing_coverage_and_zero_keep_separate_contracts(monkeypatch):
    start = datetime.datetime(2026, 11, 23)
    monkeypatch.setattr(dm, "_started", start)
    row = SimpleNamespace(danmaku_count=0, danmaku_user_count=0)
    assert dm.settle(row, "session:1", start)
    result = dm.report(row, "session:1", live=False)
    assert result["danmaku_user_count"] == 0
    assert result["danmaku_dependency"]["status"] == "archived"
    assert result["danmaku_dependency"]["top1"] is None
    legacy = dm.report(SimpleNamespace(), "session:2", live=False)
    assert legacy["danmaku_user_count"] is None
    assert legacy["danmaku_dependency"]["status"] == "unavailable"
    batch = dm.SenderBatch(Counter({42: 2, 0: 1}))
    assert (
        dm.record(dm.Scope("session:3", start - datetime.timedelta(hours=1)), batch).count is None
    )
    row = SimpleNamespace(danmaku_count=5, danmaku_user_count=None)
    assert dm.settle(row, "session:3", start - datetime.timedelta(hours=1))
    assert dm.report(row, "session:3", live=False) == {
        "danmaku_user_count": None,
        "danmaku_dependency": {
            "status": "partial",
            "source": "mysql",
            "top1": 0.6667,
            "top5": 0.6667,
            "top10": 0.6667,
            "top1_percent": 0.6667,
        },
    }
