from typing import Never, Self

from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from types import SimpleNamespace

from app import api_app, gift


class _EmptyQuery:
    def all(self) -> list[Never]:
        return []

    def filter(self, *_conditions: Never) -> Self:
        return self

    def filter_by(self, **_conditions: Never) -> Self:
        return self

    def first(self) -> None:
        return None

    def order_by(self, *_columns: Never) -> Self:
        return self

    def scalar(self) -> None:
        return None


class _EmptySession:
    def connection(self) -> Self:
        return self

    def close(self) -> None:
        return None

    def query(self, *_entities: Never) -> _EmptyQuery:
        return _EmptyQuery()

    def rollback(self) -> None:
        return None


def test_malformed_month_returns_stable_400_before_session_or_query(monkeypatch: MonkeyPatch) -> None:
    invalid_cases: tuple[tuple[str, dict[str, str], dict[str, str]], ...] = (
        ("/gift/by_month", {}, {"error": "month 参数无效，支持 YYYYMM 或 YYYY-MM"}),
        (
            "/gift/live_sessions",
            {"room_id": "111111"},
            {"error": "month 参数无效，支持 YYYYMM 或 YYYY-MM"},
        ),
        (
            "/gift/attention",
            {"room_id": "111111"},
            {"error": "month 参数无效，支持 YYYYMM 或 YYYY-MM"},
        ),
        (
            "/gift/sc",
            {"room_id": "111111"},
            {"error": "month 格式不正确，应为 YYYYMM 或 YYYY-MM"},
        ),
    )

    for bad_month in ("08", "2026-08-01", "not-a-month", "٢٠٢٦-٠٨", "000001", "999912"):
        for path, route_params, error_body in invalid_cases:
            session_calls = 0

            def forbidden_session() -> Never:
                nonlocal session_calls
                session_calls += 1
                raise AssertionError("malformed month must not create a Session")

            with monkeypatch.context() as patch:
                patch.setattr(gift, "Session", forbidden_session)

                # Given: malformed input and a Session factory that must remain untouched.
                request_params = {**route_params, "month": bad_month}

                # When: the public report route is requested.
                with TestClient(gift.app) as client:
                    response = client.get(path, params=request_params)

                # Then: route-specific stable errors return before database work.
                assert response.status_code == 400
                assert response.json() == error_body
                assert session_calls == 0


def test_by_month_normalizes_yyyy_dash_mm_before_room_lookup(monkeypatch: MonkeyPatch) -> None:
    observed_months: list[str] = []

    def empty_room_ids(
        month: str,
        include_config: bool = True,
        session: _EmptySession | None = None,
        *,
        metadata=None,
    ) -> list[int]:
        _ = include_config
        _ = session
        _ = metadata
        observed_months.append(month)
        return []

    monkeypatch.setattr(gift, "Session", _EmptySession)
    monkeypatch.setattr(gift, "_room_ids_for_month", empty_room_ids)
    monkeypatch.setattr(
        api_app,
        "inspect",
        lambda _connection: SimpleNamespace(
            has_table=lambda _name: False,
            get_columns=lambda _name: [],
        ),
    )

    # Given: equivalent compact and dashed historical month inputs.
    with TestClient(gift.app) as client:
        compact = client.get("/gift/by_month", params={"month": "190001"})
        dashed = client.get("/gift/by_month", params={"month": "1900-01"})

    # Then: both successful responses use the same canonical query month.
    assert compact.status_code == dashed.status_code == 200
    assert compact.json() == dashed.json() == []
    assert observed_months == ["190001", "190001"]


def test_room_reports_normalize_yyyy_dash_mm_in_response(monkeypatch: MonkeyPatch) -> None:
    def archive_table_missing(_name: str) -> bool:
        return False

    monkeypatch.setattr(gift, "Session", _EmptySession)
    monkeypatch.setattr(gift, "sc_log_table_exists", archive_table_missing)
    monkeypatch.setattr(
        api_app,
        "inspect",
        lambda _connection: SimpleNamespace(
            has_table=archive_table_missing,
            get_columns=lambda _name: [],
        ),
    )

    for path in ("/gift/live_sessions", "/gift/attention", "/gift/sc"):
        # Given: equivalent compact and dashed historical month inputs.
        with TestClient(gift.app) as client:
            compact = client.get(path, params={"room_id": "111111", "month": "190001"})
            dashed = client.get(path, params={"room_id": "111111", "month": "1900-01"})

        # Then: both successful responses expose the canonical month identically.
        assert compact.status_code == dashed.status_code == 200
        assert compact.json() == dashed.json()
        assert compact.json()["month"] == "190001"
