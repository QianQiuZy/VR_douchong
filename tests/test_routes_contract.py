"""Characterization: the seven FastAPI routes gift.py exposes.

Each test isolates the route to what a single FastAPI ``TestClient`` call
observes: HTTP status, JSON shape (top-level keys, per-item keys), and the
error strings the current implementation ships.  DB work is stubbed with
an in-memory session so no MySQL is touched.  Bilibili is never contacted.

Coverage matrix (mirrors api.md and gift.py lines 3151-3712):

  1. POST /add/room                 - 401 no key, 400 bad payload, 200/409 happy
  2. POST /delete/room              - 401 no key, 400 bad payload, 200/404 happy
  3. GET  /gift                     - list<obj>, current-month with current_concurrency
  4. GET  /gift/by_month            - list<obj>, historical dropped current_concurrency
  5. GET  /gift/live_sessions       - {room_id,month,sessions[]}, 400 on bad input
  6. GET  /gift/attention           - {room_id,month,attention[]}, 400 on bad month
  7. GET  /gift/sc                  - {room_id,month,list[]}, 400 on bad input
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient


class _StubQuery:
    """A minimal query stand-in whose terminals return no rows / defaults."""

    def filter_by(self, **_kwargs: Any) -> "_StubQuery":
        return self

    def filter(self, *_args: Any, **_kwargs: Any) -> "_StubQuery":
        return self

    def order_by(self, *_args: Any, **_kwargs: Any) -> "_StubQuery":
        return self

    def distinct(self) -> "_StubQuery":
        return self

    def all(self) -> list[Any]:
        return []

    def first(self) -> None:
        return None

    def scalar(self) -> None:
        return None

    def one(self) -> tuple[int, int]:
        return 0, 0


class _StubResult:
    def fetchall(self) -> list[Any]:
        return []

    def scalar(self) -> int:
        return 0

    def one(self) -> tuple[int, int]:
        return 0, 0


class _StubSession:
    def query(self, *_args: Any, **_kwargs: Any) -> _StubQuery:
        return _StubQuery()

    def execute(self, *_args: Any, **_kwargs: Any) -> _StubResult:
        return _StubResult()

    def rollback(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.fixture()
def client(gift_module) -> TestClient:
    return TestClient(gift_module.app)


@pytest.fixture()
def isolated_db(gift_module, monkeypatch):
    """Every ``Session()`` call inside a route returns the stub session."""
    monkeypatch.setattr(gift_module, "Session", lambda: _StubSession())
    monkeypatch.setattr(
        gift_module.RoomLiveStats,
        "month_aggregate_for_month",
        classmethod(lambda cls, room_id, month: (0, 0)),
    )
    monkeypatch.setattr(
        gift_module.RoomLiveStats,
        "month_steel_coin_for_month",
        classmethod(lambda cls, room_id, month: 0),
    )
    monkeypatch.setattr(gift_module, "sc_log_table_exists", lambda name: False)


class TestAddRoomRoute:
    def test_missing_api_key_returns_401_with_missing_key_error(self, client, gift_module):
        response = client.post("/add/room", json={"room_id": 12345, "room_anchors": "X"})
        assert response.status_code == 401
        assert response.json() == {"error": "缺少 API 密钥"}

    def test_wrong_api_key_returns_401_with_invalid_error(self, client, gift_module):
        response = client.post(
            "/add/room",
            json={"room_id": 12345, "room_anchors": "X"},
            headers={"X-API-Key": "wrong-key"},
        )
        assert response.status_code == 401
        assert response.json() == {"error": "API 密钥无效"}

    def test_missing_room_id_returns_400(self, client, gift_module):
        response = client.post(
            "/add/room",
            json={"room_anchors": "X"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必填"}

    def test_non_integer_room_id_returns_400(self, client, gift_module):
        response = client.post(
            "/add/room",
            json={"room_id": "not-a-number", "room_anchors": "X"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必须为整数"}

    def test_negative_room_id_returns_400(self, client, gift_module):
        response = client.post(
            "/add/room",
            json={"room_id": -1, "room_anchors": "X"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必须为正整数"}

    def test_missing_room_anchors_returns_400(self, client, gift_module):
        response = client.post(
            "/add/room",
            json={"room_id": 12345},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "room_anchors 必填"}

    def test_empty_room_anchors_returns_400(self, client, gift_module):
        response = client.post(
            "/add/room",
            json={"room_id": 12345, "room_anchors": "   "},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "room_anchors 必须为非空字符串"}

    def test_bearer_token_is_accepted_authentication(self, client, gift_module, monkeypatch):
        monkeypatch.setattr(gift_module, "add_room_async", lambda rid, name: None)
        response = client.post(
            "/add/room",
            json={"room_id": 12345, "room_anchors": "X"},
            headers={"Authorization": f"Bearer {gift_module.API_SECRET}"},
        )
        # MAIN_LOOP is None under tests -> 500 is the current documented failure mode.
        assert response.status_code == 500
        assert response.json() == {"error": "添加房间失败"}

    def test_body_api_key_is_accepted(self, client, gift_module, monkeypatch):
        monkeypatch.setattr(gift_module, "add_room_async", lambda rid, name: None)
        response = client.post(
            "/add/room",
            json={"room_id": 12345, "room_anchors": "X", "api_key": gift_module.API_SECRET},
        )
        assert response.status_code == 500
        assert response.json() == {"error": "添加房间失败"}

    def test_success_shape_from_add_room_async(self, client, gift_module, monkeypatch):
        monkeypatch.setattr(gift_module, "add_room_async", lambda rid, name: None)
        monkeypatch.setattr(
            gift_module,
            "_run_in_main_loop",
            lambda coro, timeout=30: (True, "房间已添加并启动任务"),
        )
        response = client.post(
            "/add/room",
            json={"room_id": 55555, "room_anchors": "Anchor55555"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 200
        assert response.json() == {"ok": True, "room_id": 55555, "message": "房间已添加并启动任务"}

    def test_conflict_returns_409_from_add_room_async(self, client, gift_module, monkeypatch):
        monkeypatch.setattr(gift_module, "add_room_async", lambda rid, name: None)
        monkeypatch.setattr(
            gift_module, "_run_in_main_loop", lambda coro, timeout=30: (False, "房间已存在")
        )
        response = client.post(
            "/add/room",
            json={"room_id": 55555, "room_anchors": "Anchor55555"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 409
        assert response.json() == {"ok": False, "room_id": 55555, "message": "房间已存在"}


class TestDeleteRoomRoute:
    def test_missing_api_key_returns_401(self, client, gift_module):
        response = client.post("/delete/room", json={"room_id": 12345, "room_anchors": "X"})
        assert response.status_code == 401
        assert response.json() == {"error": "缺少 API 密钥"}

    def test_wrong_api_key_returns_401(self, client, gift_module):
        response = client.post(
            "/delete/room",
            json={"room_id": 12345, "room_anchors": "X"},
            headers={"X-API-Key": "nope"},
        )
        assert response.status_code == 401
        assert response.json() == {"error": "API 密钥无效"}

    def test_missing_room_id_returns_400(self, client, gift_module):
        response = client.post(
            "/delete/room",
            json={"room_anchors": "X"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必填"}

    def test_success_returns_200_shape(self, client, gift_module, monkeypatch):
        monkeypatch.setattr(gift_module, "delete_room_async", lambda rid: None)
        monkeypatch.setattr(
            gift_module,
            "_run_in_main_loop",
            lambda coro, timeout=30: (True, "房间已删除并停止任务"),
        )
        response = client.post(
            "/delete/room",
            json={"room_id": 12345, "room_anchors": "X"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 200
        assert response.json() == {"ok": True, "room_id": 12345, "message": "房间已删除并停止任务"}

    def test_not_found_returns_404(self, client, gift_module, monkeypatch):
        monkeypatch.setattr(gift_module, "delete_room_async", lambda rid: None)
        monkeypatch.setattr(
            gift_module, "_run_in_main_loop", lambda coro, timeout=30: (False, "房间不存在")
        )
        response = client.post(
            "/delete/room",
            json={"room_id": 99999999, "room_anchors": "X"},
            headers={"X-API-Key": gift_module.API_SECRET},
        )
        assert response.status_code == 404
        assert response.json() == {"ok": False, "room_id": 99999999, "message": "房间不存在"}


class TestGiftCurrentMonthRoute:
    def test_empty_rooms_returns_empty_list(self, client, gift_module, isolated_db, monkeypatch):
        monkeypatch.setattr(gift_module, "_room_ids_for_month", lambda m, include_config=True: [])
        response = client.get("/gift")
        assert response.status_code == 200
        assert response.json() == []

    def test_one_room_returns_frozen_key_set_including_current_concurrency(
        self, client, gift_module, isolated_db, monkeypatch
    ):
        monkeypatch.setattr(gift_module, "_room_ids_for_month", lambda m, include_config=True: [111111])
        response = client.get("/gift")
        assert response.status_code == 200
        payload = response.json()
        assert isinstance(payload, list)
        assert len(payload) == 1
        item = payload[0]
        assert set(item.keys()) == {
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
        }
        assert item["room_id"] == 111111
        assert item["month"] == gift_module.month_str()
        assert item["status"] == 0
        assert item["current_concurrency"] is None
        assert item["live_duration"] == "00:00:00"


class TestGiftByMonthRoute:
    def test_current_month_shape_matches_gift(self, client, gift_module, isolated_db, monkeypatch):
        monkeypatch.setattr(gift_module, "_room_ids_for_month", lambda m, include_config=True: [111111])
        response = client.get("/gift/by_month")
        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        # by_month never emits current_concurrency (asymmetric with /gift).
        assert "current_concurrency" not in payload[0]
        assert set(payload[0].keys()) == {
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
        }

    def test_historical_month_uses_placeholder_live_time_and_null_metrics(
        self, client, gift_module, isolated_db, monkeypatch
    ):
        monkeypatch.setattr(gift_module, "_room_ids_for_month", lambda m, include_config=True: [111111])
        response = client.get("/gift/by_month", params={"month": "190001"})
        assert response.status_code == 200
        item = response.json()[0]
        assert item["month"] == "190001"
        assert item["live_time"] == "0000-00-00 00:00:00"
        assert item["title"] == ""
        assert item["status"] == 0
        assert item["guard_1"] is None
        assert item["guard_2"] is None
        assert item["guard_3"] is None
        assert item["fans_count"] is None


class TestGiftLiveSessionsRoute:
    def test_historical_15m_stats_query_uses_concrete_archive_table(
        self, gift_module, monkeypatch
    ):
        from app import api_app

        captured: dict[str, str] = {}

        class _Result:
            def fetchall(self) -> list[tuple[object, ...]]:
                return []

        class _Session:
            def execute(self, statement, _parameters):
                captured["sql"] = str(statement)
                return _Result()

        monkeypatch.setattr(gift_module, "sc_log_table_exists", lambda _name: True)

        api_app._session_15m_stats(_Session(), 14500, "202608")

        assert "FROM `live_session_15m_stats_202608`" in captured["sql"]

    def test_15m_stats_include_danmaku_count(self, gift_module):
        from app import api_app

        item = api_app._format_15m_stats((1, None, None, 1.0, 2.0, 3.0, 4, 5, 6, 7.0, 8, 9, 10))

        assert item["danmaku_count"] == 6
        assert item["avg_concurrency"] == 7.0
        assert item["max_concurrency"] == 8
        assert item["sample_count"] == 9
        assert item["payer_count"] == 10

    def test_missing_room_id_defaults_to_zero_and_returns_400(self, client, gift_module, isolated_db):
        response = client.get("/gift/live_sessions")
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必填且需为正整数"}

    def test_non_integer_room_id_returns_400(self, client, gift_module, isolated_db):
        response = client.get("/gift/live_sessions", params={"room_id": "abc"})
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 参数无效"}

    def test_negative_room_id_returns_400(self, client, gift_module, isolated_db):
        response = client.get("/gift/live_sessions", params={"room_id": "-5"})
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必填且需为正整数"}

    def test_current_month_empty_returns_wrapper_with_empty_sessions(
        self, client, gift_module, isolated_db
    ):
        response = client.get("/gift/live_sessions", params={"room_id": 111111})
        assert response.status_code == 200
        assert response.json() == {
            "room_id": 111111,
            "month": gift_module.month_str(),
            "sessions": [],
        }

    def test_historical_month_falls_back_when_archive_table_missing(
        self, client, gift_module, isolated_db
    ):
        response = client.get(
            "/gift/live_sessions", params={"room_id": 111111, "month": "190001"}
        )
        assert response.status_code == 200
        assert response.json() == {
            "room_id": 111111,
            "month": "190001",
            "sessions": [],
        }


class TestGiftAttentionRoute:
    def test_missing_room_id_returns_400(self, client, gift_module, isolated_db):
        response = client.get("/gift/attention")
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必填且需为正整数"}

    def test_bad_room_id_returns_400(self, client, gift_module, isolated_db):
        response = client.get("/gift/attention", params={"room_id": "abc"})
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 参数无效"}

    def test_bad_month_format_returns_400(self, client, gift_module, isolated_db):
        response = client.get(
            "/gift/attention", params={"room_id": 111111, "month": "not-a-month"}
        )
        assert response.status_code == 400
        assert response.json() == {"error": "month 参数无效，支持 YYYYMM 或 YYYY-MM"}

    def test_current_month_empty_returns_wrapper(self, client, gift_module, isolated_db):
        response = client.get("/gift/attention", params={"room_id": 111111})
        assert response.status_code == 200
        assert response.json() == {
            "room_id": 111111,
            "month": gift_module.month_str(),
            "attention": [],
        }

    def test_yyyy_dash_mm_normalises_to_yyyymm(self, client, gift_module, isolated_db):
        response = client.get("/gift/attention", params={"room_id": 111111, "month": "1999-05"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["month"] == "199905"
        assert payload["attention"] == []


class TestGiftScRoute:
    def test_missing_room_id_returns_400_必填(self, client, gift_module, isolated_db):
        response = client.get("/gift/sc")
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 参数必填"}

    def test_bad_room_id_returns_400_无效(self, client, gift_module, isolated_db):
        response = client.get("/gift/sc", params={"room_id": "abc"})
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 参数无效"}

    def test_negative_room_id_returns_400_正整数(self, client, gift_module, isolated_db):
        response = client.get("/gift/sc", params={"room_id": "-1"})
        assert response.status_code == 400
        assert response.json() == {"error": "room_id 必须为正整数"}

    def test_bad_month_returns_400_格式不正确(self, client, gift_module, isolated_db):
        response = client.get("/gift/sc", params={"room_id": 111111, "month": "not-a-month"})
        assert response.status_code == 400
        assert response.json() == {"error": "month 格式不正确，应为 YYYYMM 或 YYYY-MM"}

    def test_current_month_empty_returns_wrapper_with_list_key(
        self, client, gift_module, isolated_db
    ):
        response = client.get("/gift/sc", params={"room_id": 111111})
        assert response.status_code == 200
        assert response.json() == {
            "room_id": 111111,
            "month": gift_module.month_str(),
            "list": [],
        }

    def test_historical_month_falls_back_when_archive_missing(self, client, gift_module, isolated_db):
        response = client.get(
            "/gift/sc", params={"room_id": 111111, "month": "190001"}
        )
        assert response.status_code == 200
        assert response.json() == {
            "room_id": 111111,
            "month": "190001",
            "list": [],
        }
