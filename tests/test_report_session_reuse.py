from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import NoReturn, Protocol, TypedDict, override

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import JsonValue, TypeAdapter
from sqlalchemy import Column
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from app.models import RoomLiveStats
from app.repositories import live_stats
from app.repositories.tables import month_str


CURRENT_REPORT_KEYS = frozenset(
    {
        "room_id",
        "anchor_name",
        "attention",
        "status",
        "gift",
        "guard",
        "super_chat",
        "payer_count",
        "steel_coin_count",
        "blind_box_count",
        "blind_box_profit",
        "live_duration",
        "effective_days",
        "live_time",
        "title",
        "month",
        "guard_1",
        "guard_2",
        "guard_3",
        "fans_count",
        "current_concurrency",
        "whale_dependency",
        "danmaku",
    }
)

MONTH_REPORT_KEYS = CURRENT_REPORT_KEYS - {"current_concurrency"}

_REPORT_PAYLOADS: TypeAdapter[list[dict[str, JsonValue]]] = TypeAdapter(list[dict[str, JsonValue]])


class _ErrorPayload(TypedDict):
    error: str


_ERROR_PAYLOAD: TypeAdapter[_ErrorPayload] = TypeAdapter(_ErrorPayload)


class _GiftModule(Protocol):
    app: FastAPI
    RoomLiveStats: type[RoomLiveStats]


class _ResponseWithContent(Protocol):
    @property
    def content(self) -> bytes: ...


def _report_key_sets(response: _ResponseWithContent) -> list[frozenset[str]]:
    return [frozenset(item) for item in _REPORT_PAYLOADS.validate_json(response.content)]


def _error_payload(response: _ResponseWithContent) -> _ErrorPayload:
    return _ERROR_PAYLOAD.validate_json(response.content)


def _two_report_room_ids(
    _month: str,
    include_config: bool = True,
    session: Session | None = None,
    *,
    metadata=None,
) -> list[int]:
    _ = include_config
    _ = session
    _ = metadata
    return [101, 202]


def _one_report_room_id(
    _month: str,
    include_config: bool = True,
    session: Session | None = None,
    *,
    metadata=None,
) -> list[int]:
    _ = include_config
    _ = session
    _ = metadata
    return [101]


class _EmptyQuery:
    def filter_by(self, **_kwargs: int | str) -> _EmptyQuery:
        return self

    def filter(self, *_args: ColumnElement[bool]) -> _EmptyQuery:
        return self

    def first(self) -> None:
        return None

    def scalar(self) -> None:
        return None

    def tuples(self) -> _EmptyQuery:
        return self


class _BatchQuery(_EmptyQuery):
    def __init__(self, rows: list[tuple[int, int] | tuple[int, int, int]], session: _ReportRouteSession) -> None:
        self._rows: list[tuple[int, int] | tuple[int, int, int]] = rows
        self._session: _ReportRouteSession = session

    def group_by(self, *_args: Column[int]) -> _BatchQuery:
        self._session.group_by_calls += 1
        return self

    def all(self) -> list[tuple[int, int] | tuple[int, int, int]]:
        return self._rows


class _ReportRouteSession:
    def __init__(self) -> None:
        self.close_calls: int = 0
        self.rollback_calls: int = 0
        self.aggregate_batch_queries: int = 0
        self.steel_batch_queries: int = 0
        self.group_by_calls: int = 0

    def query(self, *args: Column[int]) -> _BatchQuery | _EmptyQuery:
        if args and args[0] is RoomLiveStats.room_id and len(args) == 3:
            self.aggregate_batch_queries += 1
            return _BatchQuery([(101, 3600, 0), (202, 7200, 1)], self)
        if args and args[0] is RoomLiveStats.room_id and len(args) == 2:
            self.steel_batch_queries += 1
            return _BatchQuery([(101, 3), (202, 7)], self)
        return _EmptyQuery()

    def connection(self) -> _ReportRouteSession:
        return self

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class _RepositoryQuery:
    def filter(self, *_args: ColumnElement[bool]) -> _RepositoryQuery:
        return self

    def one(self) -> tuple[int, int]:
        return 0, 0

    def scalar(self) -> int:
        return 0

    def tuples(self) -> _RepositoryQuery:
        return self


class _RepositorySession:
    def __init__(self) -> None:
        self.close_calls: int = 0
        self.rollback_calls: int = 0

    def query(self, *_args: Column[int]) -> _RepositoryQuery:
        return _RepositoryQuery()

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class _FailingRepositorySession(_RepositorySession):
    @override
    def query(self, *_args: Column[int]) -> _RepositoryQuery:
        raise SQLAlchemyError("forced repository failure")


class _CountingSessionFactory[T]:
    def __init__(self, session_type: Callable[[], T]) -> None:
        self._session_type: Callable[[], T] = session_type
        self.sessions: list[T] = []

    def __call__(self) -> T:
        session = self._session_type()
        self.sessions.append(session)
        return session


@pytest.fixture(autouse=True)
def _report_inspection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "app.api_app.inspect",
        lambda _connection: SimpleNamespace(
            has_table=lambda _name: False,
            get_columns=lambda _name: [],
        ),
    )


@pytest.mark.parametrize(
    ("path", "expected_keys"),
    [
        ("/gift", CURRENT_REPORT_KEYS),
        ("/gift/by_month", MONTH_REPORT_KEYS),
    ],
)
def test_report_routes_preserve_frozen_response_keys_when_repository_owns_aggregate_sessions(
    gift_module: _GiftModule, monkeypatch: pytest.MonkeyPatch, path: str, expected_keys: frozenset[str]
) -> None:
    # Given: a deterministic two-room route and repository-owned aggregate sessions.
    route_session = _ReportRouteSession()
    repository_sessions = _CountingSessionFactory(_RepositorySession)
    monkeypatch.setattr(gift_module, "Session", lambda: route_session)
    monkeypatch.setattr(live_stats, "Session", repository_sessions)
    monkeypatch.setattr(
        gift_module,
        "_room_ids_for_month",
        _two_report_room_ids,
    )

    # When: one public report route is requested through FastAPI.
    with TestClient(gift_module.app) as client:
        response = client.get(path)

    # Then: its frozen top-level item contract remains unchanged.
    assert response.status_code == 200
    assert _report_key_sets(response) == [expected_keys, expected_keys]


def test_aggregate_helpers_create_and_close_fallback_sessions_when_no_session_is_supplied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: aggregate helpers with a counting fallback Session factory.
    fallback_sessions = _CountingSessionFactory(_RepositorySession)
    monkeypatch.setattr(live_stats, "Session", fallback_sessions)
    month = month_str()

    # When: existing callers omit the optional Session argument.
    aggregate = live_stats.month_aggregate_for_month(RoomLiveStats, 101, month)
    steel_coin_count = live_stats.month_steel_coin_for_month(RoomLiveStats, 101, month)

    # Then: each legacy helper still owns and closes its own Session.
    assert aggregate == (0, 0)
    assert steel_coin_count == 0
    assert len(fallback_sessions.sessions) == 2
    assert [session.close_calls for session in fallback_sessions.sessions] == [1, 1]


@pytest.mark.parametrize("path", ["/gift", "/gift/by_month"])
def test_report_routes_reuse_their_session_and_load_aggregates_by_room_in_one_query_per_metric(
    gift_module: _GiftModule, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    # Given: a two-room route Session plus a factory that exposes nested repository Sessions.
    route_sessions = _CountingSessionFactory(_ReportRouteSession)
    repository_sessions = _CountingSessionFactory(_RepositorySession)
    monkeypatch.setattr(gift_module, "Session", route_sessions)
    monkeypatch.setattr(live_stats, "Session", repository_sessions)
    monkeypatch.setattr(
        gift_module,
        "_room_ids_for_month",
        _two_report_room_ids,
    )

    # When: the public report is invoked through FastAPI.
    with TestClient(gift_module.app) as client:
        response = client.get(path)

    # Then: one route Session serves set-based aggregates without repository-owned Sessions.
    route_session = route_sessions.sessions[0]
    assert response.status_code == 200
    assert len(route_sessions.sessions) == 1
    assert repository_sessions.sessions == []
    assert route_session.aggregate_batch_queries == 1
    assert route_session.steel_batch_queries == 1
    assert route_session.group_by_calls == 2
    assert route_session.rollback_calls == 0
    assert route_session.close_calls == 1


def test_aggregate_fallback_session_rolls_back_and_closes_when_querying_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: a legacy aggregate caller and a self-owned Session whose query fails.
    fallback_sessions = _CountingSessionFactory(_FailingRepositorySession)
    monkeypatch.setattr(live_stats, "Session", fallback_sessions)

    # When: the helper handles the SQLAlchemy failure without a caller-owned Session.
    result = live_stats.month_aggregate_for_month(RoomLiveStats, 101, month_str())

    # Then: the fallback Session is rolled back and closed before the default response is returned.
    assert result == (0, 0)
    assert fallback_sessions.sessions[0].rollback_calls == 1
    assert fallback_sessions.sessions[0].close_calls == 1


def test_report_route_rolls_back_and_closes_the_route_session_when_an_aggregate_raises(
    gift_module: _GiftModule, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a route Session and a monkey-patched aggregate classmethod that raises SQLAlchemyError.
    route_sessions = _CountingSessionFactory(_ReportRouteSession)
    monkeypatch.setattr(gift_module, "Session", route_sessions)
    monkeypatch.setattr(
        gift_module,
        "_room_ids_for_month",
        _one_report_room_id,
    )

    def raise_database_error(
        _cls: type[RoomLiveStats],
        _room_ids: list[int],
        _month: str,
        session: Session | None = None,
        metadata=None,
    ) -> NoReturn:
        _ = session
        _ = metadata
        raise SQLAlchemyError("forced aggregate failure")

    monkeypatch.setattr(
        gift_module.RoomLiveStats,
        "month_aggregate_for_month",
        classmethod(raise_database_error),
    )

    # When: the route observes the repository error through the public surface.
    with TestClient(gift_module.app) as client:
        response = client.get("/gift")

    # Then: the preserved SQLAlchemy error contract releases the route-owned Session.
    route_session = route_sessions.sessions[0]
    assert response.status_code == 500
    assert _error_payload(response) == {"error": "数据库查询失败"}
    assert route_session.rollback_calls == 1
    assert route_session.close_calls == 1
