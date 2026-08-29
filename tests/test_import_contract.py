"""Characterization: what MUST hold after `import gift`.

Anchors the pre-extraction module surface: module-level constants, the
gift-coin coin-value map, the env-driven poll cadence, and the
metadata-controlled DDL side effect.  Any Todo 2+ change that alters these
without updating the tests is a contract break.
"""

from __future__ import annotations

import os


class TestImportTimeConstants:
    def test_common_notice_gift_coin_map_is_frozen(self, gift_module):
        assert gift_module.COMMON_NOTICE_GIFT_COIN_MAP == {
            "干杯之旅": 10000,
            "启航之旅": 100000,
            "友谊的小船": 4900,
            "冲浪": 89900,
            "海湾之旅": 799900,
            "鸿运小电视": 1000000,
        }
        assert isinstance(gift_module.COMMON_NOTICE_GIFT_COIN_MAP, dict)

    def test_attention_daily_room_sleep_seconds_from_env(self, gift_module):
        assert isinstance(gift_module.ATTENTION_DAILY_ROOM_SLEEP_SECONDS, float)
        assert gift_module.ATTENTION_DAILY_ROOM_SLEEP_SECONDS == 1.0

    def test_app_host_and_port_from_env(self, gift_module):
        assert gift_module.APP_HOST == "0.0.0.0"
        assert gift_module.APP_PORT == 4666

    def test_api_secret_from_env(self, gift_module):
        assert gift_module.API_SECRET == "test-api-secret-abc123"

    def test_db_config_from_env(self, gift_module):
        assert gift_module.DB_CONFIG == {
            "host": "127.0.0.1",
            "user": "vr_test",
            "password": "vr_test",
            "db": "vr_test",
            "port": 3306,
        }

    def test_smtp_and_email_placeholders_are_empty_under_test(self, gift_module):
        assert gift_module.SMTP_HOST == ""
        assert gift_module.SMTP_PORT == 587
        assert gift_module.SMTP_USER == ""
        assert gift_module.SMTP_PASS == ""
        assert gift_module.EMAIL_FROM == ""
        assert gift_module.EMAIL_TO == ""

    def test_bili_cookies_base_shape(self, gift_module):
        assert set(gift_module.BILI_COOKIES_BASE.keys()) == {
            "SESSDATA",
            "bili_jct",
            "DedeUserID",
            "DedeUserID__ckMd5",
            "sid",
            "buvid3",
            "deviceFingerprint",
        }
        for value in gift_module.BILI_COOKIES_BASE.values():
            assert value == ""

    def test_bili_ticket_constants_frozen(self, gift_module):
        assert gift_module.BILI_TICKET_KEY == "XgwSnGZ1p"
        assert (
            gift_module.BILI_TICKET_URL
            == "https://api.bilibili.com/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket"
        )
        assert gift_module.BILI_TICKET_KEY_ID == "ec02"
        assert gift_module.BILI_TICKET is None
        assert gift_module.BILI_TICKET_EXPIRES is None

    def test_rooms_json_path_env_binding(self, gift_module):
        assert gift_module.ROOMS_JSON_PATH == os.environ["ROOMS_JSON_PATH"]

    def test_module_level_singletons_are_initialised(self, gift_module):
        assert isinstance(gift_module.ROOM_UIDS, dict)
        assert isinstance(gift_module.ROOM_CLIENTS, dict)
        assert isinstance(gift_module.LAST_RECONNECT, dict)
        assert isinstance(gift_module.CURRENT_SESSIONS, dict)
        assert gift_module.aiohttp_session is None
        assert gift_module.MAIN_LOOP is None

    def test_reconnect_daily_state_shape(self, gift_module):
        state = gift_module.RECONNECT_DAILY_STATE
        assert set(state.keys()) == {"date", "done"}
        assert state["date"] is None
        assert isinstance(state["done"], set)

    def test_cookie_alert_sent_starts_false(self, gift_module):
        assert gift_module.COOKIE_ALERT_SENT is False


class TestImportSideEffects:
    """DDL was issued (recorded), rooms were loaded, no live DB touched."""

    def test_metadata_create_all_is_not_called_during_import(self, import_ddl_calls):
        assert import_ddl_calls == []

    def test_metadata_remains_available_without_import_time_ddl(self, import_ddl_calls):
        assert import_ddl_calls == []
        assert sorted(__import__("app.gift", fromlist=["Base"]).Base.metadata.tables) == [
            "attention",
            "live_session",
            "room_blind_box_monthly",
            "room_info",
            "room_live_stats",
            "room_stats_monthly",
            "super_chat_log",
        ]

    def test_rooms_config_was_loaded_from_test_fixture(self, gift_module):
        assert gift_module.ROOM_IDS == [111111, 222222]
        assert gift_module.ROOM_ANCHORS == {
            111111: "TestAnchorAlpha",
            222222: "TestAnchorBeta",
        }

    def test_room_config_lock_is_a_threading_lock(self, gift_module):
        import threading

        assert isinstance(gift_module.ROOM_CONFIG_LOCK, type(threading.Lock()))

    def test_fastapi_app_disables_public_docs_endpoints(self, gift_module):
        app = gift_module.app
        assert app.docs_url is None
        assert app.redoc_url is None
        assert app.openapi_url is None

    def test_registered_routes_include_the_seven_frozen_paths(self, gift_module):
        recorded = {
            (route.path, tuple(sorted(route.methods)))
            for route in gift_module.app.routes
            if hasattr(route, "methods") and route.methods is not None
        }
        assert ("/add/room", ("POST",)) in recorded
        assert ("/delete/room", ("POST",)) in recorded
        assert ("/gift", ("GET",)) in recorded
        assert ("/gift/by_month", ("GET",)) in recorded
        assert ("/gift/live_sessions", ("GET",)) in recorded
        assert ("/gift/attention", ("GET",)) in recorded
        assert ("/gift/sc", ("GET",)) in recorded


class TestPublicSymbolsForArchiveCli:
    """migrate_sc_archive.py imports these four names from gift; do not rename."""

    def test_archive_super_chat_log_is_importable(self, gift_module):
        assert callable(gift_module.archive_super_chat_log)

    def test_archive_live_session_is_importable(self, gift_module):
        assert callable(gift_module.archive_live_session)

    def test_archive_room_live_stats_is_importable(self, gift_module):
        assert callable(gift_module.archive_room_live_stats)

    def test_normalize_month_code_is_importable(self, gift_module):
        assert callable(gift_module.normalize_month_code)

    def test_archive_attention_stays_module_private_to_scheduler(self, gift_module):
        # archive_attention exists but is NOT imported by migrate_sc_archive.py -
        # it must remain a scheduler-only concern per Todo 5's contract.
        assert callable(gift_module.archive_attention)
