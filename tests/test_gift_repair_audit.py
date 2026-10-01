import csv
import datetime
import json
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import pytest

from app.gift_repair_audit import audit, iter_corrections, read_summary
from app.gift_repair_types import RepairError


def _log(path: Path, lines: Sequence[str]) -> Path:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("recorded", "expected"),
    [
        ("0.10", (1, 0, 0, "0.20")),
        ("0.30", (0, 1, 0, "0.00")),
        ("0.25", (0, 0, 1, "0.00")),
    ],
)
def test_paid_batch_when_price_is_unit_total_or_other(
    tmp_path: Path, recorded: str, expected: tuple[int, int, int, str],
) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-07-25 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.10)",
        f"2026-07-25 12:00:01,000 - INFO - [1] private-sender uid123456 赠送 star×3 ({recorded})",
    ])

    summary = audit([log], tmp_path / "audit.sqlite3")

    assert (summary["corrections"], summary["already_total"],
            summary["other_prices"], summary["delta_rmb"]) == expected


def test_free_batch_when_paid_baseline_exists(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-08-01 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (5.00)",
        "2026-08-01 12:00:01,000 - INFO - [1] private-sender uid123456 赠送 star×10 (0.00)",
    ])

    summary = audit([log], tmp_path / "audit.sqlite3")

    assert (summary["free_batches"], summary["paid_batches"], summary["corrections"],
            summary["missing_baselines"], summary["delta_rmb"]) == (1, 0, 0, 0, "0.00")


def test_paid_baseline_when_nearer_single_is_free(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-08-01 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.10)",
        "2026-08-01 12:00:01,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.00)",
        "2026-08-01 12:00:02,000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.10)",
    ])
    destination = tmp_path / "audit.sqlite3"

    audit([log], destination)

    correction, = iter_corrections(destination)
    assert (correction.unit, correction.delta, correction.baseline_line) == (
        Decimal("0.10"), Decimal("0.20"), 1,
    )
    assert correction.event.when == datetime.datetime.fromisoformat("2026-08-01T12:00:02")
    assert correction.event.quantity == 3
    assert correction.event.source == str(log)


def test_nearest_room_baseline_when_global_single_is_closer(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-08-01 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.10)",
        "2026-08-01 12:00:10,000 - INFO - [2] private-sender uid123456 赠送 star×1 (9.00)",
        "2026-08-01 12:00:10,000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.20)",
        "2026-08-01 12:00:11,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.20)",
    ])
    destination = tmp_path / "audit.sqlite3"

    audit([log], destination)

    correction, = iter_corrections(destination)
    assert (correction.unit, correction.delta, correction.baseline_line) == (
        Decimal("0.20"), Decimal("0.40"), 4,
    )


def test_global_baseline_when_room_has_only_free_singles(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-08-01 12:00:00,000 - INFO - [2] private-sender uid123456 赠送 star×1 (0.20)",
        "2026-08-01 12:00:01,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.00)",
        "2026-08-01 12:00:02,000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.20)",
    ])

    summary = audit([log], tmp_path / "audit.sqlite3")

    assert (summary["corrections"], summary["delta_rmb"]) == (1, "0.40")


@pytest.mark.parametrize("second", ["00", "02"])
def test_other_price_when_nearest_baselines_conflict(tmp_path: Path, second: str) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-08-01 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.10)",
        f"2026-08-01 12:00:{second},000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.20)",
        f"2026-08-01 12:00:{'00' if second == '00' else '01'},000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.10)",
    ])

    summary = audit([log], tmp_path / "audit.sqlite3")

    assert (summary["other_prices"], summary["corrections"], summary["missing_baselines"]) == (1, 0, 0)


def test_missing_report_when_no_paid_single_exists(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-08-01 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.00)",
        "2026-08-01 12:00:01,000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.10)",
        "2026-08-01 12:00:02,000 - INFO - [1] private-sender uid123456 赠送 star×4 (0.10)",
        "2026-08-01 12:00:03,000 - INFO - [1] private-sender uid123456 赠送 moon,star×2 (0.20)",
        "2026-08-01 12:00:04,000 - INFO - [1] private-sender uid123456 赠送 free×9 (0.00)",
    ])
    destination = tmp_path / "audit.sqlite3"

    summary = audit([log], destination)

    assert (summary["missing_baselines"], summary["missing_gifts"]) == (3, ["moon,star", "star"])
    with destination.with_suffix(".missing.csv").open(encoding="utf-8", newline="") as report:
        assert list(csv.reader(report)) == [["gift", "count"], ["moon,star", "1"], ["star", "2"]]


@pytest.mark.parametrize("reverse", [False, True])
def test_max_multiplicity_when_rotations_overlap_and_file_is_duplicate(
    tmp_path: Path, reverse: bool,
) -> None:
    baseline = "2026-08-01 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×1 (0.10)"
    batch = "2026-08-01 12:00:01,000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.10)"
    first = _log(tmp_path / "first.log", [baseline, batch, batch])
    second = _log(tmp_path / "second.log", [
        baseline, *[batch.replace("private-sender", "other-sender").replace("0.10", "0.1")] * 3,
    ])
    duplicate = tmp_path / "duplicate.log"
    duplicate.write_bytes(second.read_bytes())
    paths = [first, second, duplicate]
    destination = tmp_path / "audit.sqlite3"

    summary = audit(paths[::-1] if reverse else paths, destination)

    assert (summary["files"], summary["duplicate_files"], summary["gifts"],
            summary["corrections"], summary["delta_rmb"]) == (3, 1, 4, 3, "0.60")
    assert len({correction.event.event_id for correction in iter_corrections(destination)}) == 3


def test_summary_and_private_storage_when_logs_include_unrelated_lines(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "[1] private-sender uid123456 赠送 star×3 (0.10)",
        '2026-08-01 12:00:00,000 - INFO - 127.0.0.1 - "GET /gift HTTP/1.1" 200',
        "2026-08-01 12:00:01,123 - INFO - [1] private-sender uid123456 赠送 star×1 (0.10)",
        "2026-10-01 12:00:02,456 - INFO - [1] private-sender uid123456 赠送 star×3 (0.10)",
    ])
    destination = tmp_path / "audit.sqlite3"

    summary = audit([log], destination)

    assert read_summary(destination) == summary
    assert json.loads(destination.with_suffix(".summary.json").read_text(encoding="utf-8")) == summary
    assert (summary["gifts"], summary["first_timestamp"], summary["last_timestamp"]) == (
        2, "2026-08-01T12:00:01.123", "2026-10-01T12:00:02.456",
    )
    assert b"private-sender" not in destination.read_bytes()
    for path in [destination, destination.with_suffix(".summary.json"), destination.with_suffix(".missing.csv")]:
        assert path.stat().st_mode & 0o777 == 0o600


def test_existing_audit_when_reused_is_immutable(tmp_path: Path) -> None:
    destination = tmp_path / "audit.sqlite3"
    destination.write_bytes(b"existing private audit")

    with pytest.raises(RepairError):
        audit([], destination)

    assert destination.read_bytes() == b"existing private audit"


def test_invalid_gift_timestamp_when_parsed_fails_closed(tmp_path: Path) -> None:
    log = _log(tmp_path / "input.log", [
        "2026-02-30 12:00:00,000 - INFO - [1] private-sender uid123456 赠送 star×3 (0.10)",
    ])

    with pytest.raises(RepairError):
        audit([log], tmp_path / "audit.sqlite3")


def test_missing_audit_when_read_does_not_create_file(tmp_path: Path) -> None:
    destination = tmp_path / "absent.sqlite3"

    with pytest.raises(RepairError):
        read_summary(destination)

    assert not destination.exists()
