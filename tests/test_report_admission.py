from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, BoundedSemaphore, Event, Lock
from time import monotonic

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app import api_app, bootstrap, gift


def test_every_get_report_rejects_when_capacity_is_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: one permit is already held in the process-local report gate.
    gate = BoundedSemaphore(1)
    _ = gate.acquire(blocking=False)
    monkeypatch.setattr(api_app, "_report_gate", gate)
    monkeypatch.setattr(api_app, "REPORT_ACQUIRE_TIMEOUT_SECONDS", 0.01)
    paths = (
        "/gift",
        "/gift/by_month",
        "/gift/live_sessions?room_id=1",
        "/gift/attention?room_id=1",
        "/gift/sc?room_id=1",
    )

    # When: each of the five public GET reports is requested while saturated.
    with TestClient(gift.app) as client:
        responses = [client.get(path) for path in paths]
    gate.release()

    # Then: every route uses the same uncached 503 contract.
    assert all(response.status_code == 503 for response in responses)
    assert all(response.json() == {"error": "报表请求繁忙"} for response in responses)
    assert all(response.headers["retry-after"] == "1" for response in responses)
    assert all("cache-control" not in response.headers for response in responses)


def test_report_admission_bounds_concurrency_and_releases_after_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: ten synchronized requests and report work held behind a barrier.
    entered = 0
    maximum_active = 0
    state_lock = Lock()
    release_reports = Event()
    failing_request = Event()
    client = TestClient(gift.app)

    class ReportSession:
        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    def hold_report(_session: Session):
        nonlocal entered, maximum_active
        with state_lock:
            entered += 1
            maximum_active = max(maximum_active, entered)
            should_fail = failing_request.is_set()
        if should_fail:
            with state_lock:
                entered -= 1
            raise SQLAlchemyError("forced report failure")
        release_reports.wait(timeout=2)
        with state_lock:
            entered -= 1
        return type("Metadata", (), {"has_table": staticmethod(lambda _name: False)})()

    monkeypatch.setattr(api_app, "_resolved_session_factory", lambda: ReportSession)
    monkeypatch.setattr(api_app, "_resolved_room_ids_for_month", lambda: lambda *_args, **_kwargs: [])
    monkeypatch.setattr(api_app, "_report_table_metadata", hold_report)
    monkeypatch.setattr(api_app, "_report_gate", BoundedSemaphore(2), raising=False)
    monkeypatch.setattr(api_app, "REPORT_ACQUIRE_TIMEOUT_SECONDS", 0.1)
    start = Barrier(10)

    def request_report(_index: int):
        start.wait(timeout=2)
        return client.get("/gift")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(request_report, index) for index in range(10)]
        deadline = monotonic() + 2
        while monotonic() < deadline:
            responses = [future.result(timeout=1) for future in futures if future.done()]
            if any(response.status_code == 503 for response in responses):
                break
        release_reports.set()
        responses = [future.result(timeout=3) for future in futures]

    # Then: no more than two reports run, and overload is explicitly rejected uncached.
    rejected = [response for response in responses if response.status_code == 503]
    assert maximum_active == 2
    assert rejected
    assert rejected[0].headers["retry-after"] == "1"
    assert "cache-control" not in rejected[0].headers
    assert all(response.status_code in {200, 503} for response in responses)

    # When: report work raises after acquiring a permit, then a healthy request follows.
    failing_request.set()
    failed = client.get("/gift")
    failing_request.clear()
    release_reports.set()
    recovered = client.get("/gift")

    # Then: the error response is unchanged and the permit was released for the next request.
    assert failed.status_code == 500
    assert "cache-control" not in failed.headers
    assert recovered.status_code == 200
    assert "cache-control" not in recovered.headers
    client.close()


def test_pool_observer_repeats_at_info_level_and_cancels(monkeypatch, caplog) -> None:
    observations: list[tuple[str, int]] = []
    intervals: list[float] = []
    sleeps = 0

    def observe(label: str, level: int) -> None:
        observations.append((label, level))
        logging.getLogger(__name__).log(level, "pool observation")

    async def cancel_after_second_observation(_seconds: float) -> None:
        nonlocal sleeps
        intervals.append(_seconds)
        sleeps += 1
        if sleeps == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(bootstrap, "log_pool_status", observe)
    monkeypatch.setattr(bootstrap.asyncio, "sleep", cancel_after_second_observation)

    with caplog.at_level("INFO"), pytest.raises(asyncio.CancelledError):
        asyncio.run(bootstrap.pool_status_scheduler())

    assert observations == [("periodic", logging.INFO)] * 2
    assert intervals == [bootstrap.POOL_STATUS_INTERVAL_SECONDS] * 2
    assert all(seconds >= 300 for seconds in intervals)
    assert caplog.messages == ["pool observation", "pool observation"]
