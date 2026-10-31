"""Offline fixtures for VR_douchong's business and API regression tests.

Select an empty env file and synthetic room configuration before importing
the application. Reject unexpected table creation; replace database reflection
with an offline inspector. Integration tests explicitly restore the originals
for their disposable services. Fresh-process import checks live in
``test_import_contract.py`` and do not inherit these SQLAlchemy patches.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import schema as _sa_schema

# --------------------------------------------------------------------------- #
# 0. Paths.                                                                   #
# --------------------------------------------------------------------------- #
TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
FIXTURES_DIR = TESTS_DIR / "_fixtures"
FIXTURES_DIR.mkdir(exist_ok=True)


# --------------------------------------------------------------------------- #
# 1. Environment isolation - must happen BEFORE `import app.gift`.            #
#    app.gift reads .env at module load, so we make ENV_FILE point at a        #
#    guaranteed-empty stub and force placeholder DB config into os.environ.   #
# --------------------------------------------------------------------------- #
_EMPTY_ENV = FIXTURES_DIR / "empty.env"
if not _EMPTY_ENV.exists():
    _EMPTY_ENV.write_text("", encoding="utf-8")

_TEST_ROOMS_JSON = FIXTURES_DIR / "config" / "rooms.json"
_TEST_ROOMS_JSON.parent.mkdir(exist_ok=True)


def _write_baseline_rooms_json() -> None:
    """(Re)write the test rooms.json fixture with a deterministic baseline."""
    _TEST_ROOMS_JSON.write_text(
        json.dumps(
            {
                "room_ids": [111111, 222222],
                "room_anchors": {
                    "111111": "TestAnchorAlpha",
                    "222222": "TestAnchorBeta",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


_write_baseline_rooms_json()

# Force the following env vars *before* app.gift loads.  Real values (from the
# user's .env) never enter this test process because ENV_FILE points at the
# empty stub above and load_env_file skips keys already present in os.environ.
_TEST_ENV = {
    "ENV_FILE": str(_EMPTY_ENV),
    # Legacy report characterization is deliberately offline; new cache tests
    # explicitly enable the production cache-only middleware with isolated Redis.
    "API_CACHE_ENABLED": "0",
    "DB_HOST": "127.0.0.1",
    "DB_USER": "vr_test",
    "DB_PASSWORD": "vr_test",
    "DB_NAME": "vr_test",
    "DB_PORT": "3306",
    "SMTP_HOST": "",
    "SMTP_PORT": "587",
    "SMTP_USER": "",
    "SMTP_PASS": "",
    "EMAIL_FROM": "",
    "EMAIL_TO": "",
    "APP_HOST": "0.0.0.0",
    "APP_PORT": "4666",
    "API_SECRET": "test-api-secret-abc123",
    "ATTENTION_DAILY_ROOM_SLEEP_SECONDS": "1",
    "SESSDATA_VALUE": "",
    "BILI_JCT_VALUE": "",
    "DEDEUSERID_VALUE": "",
    "DEDEUSERID_CKMD5_VALUE": "",
    "SID_VALUE": "",
    "BUVID3_VALUE": "",
    "DEVICE_FP_VALUE": "",
    "ROOMS_JSON_PATH": str(_TEST_ROOMS_JSON),
}
for _k, _v in _TEST_ENV.items():
    os.environ[_k] = _v

# Production stores local, naive MySQL DATETIME values in Asia/Shanghai.
# Pin the test process's wall-clock convention independently of the runner host.
os.environ["TZ"] = "Asia/Shanghai"
if hasattr(time, "tzset"):
    time.tzset()

# Make repo root importable so `import app.gift` works from within tests/.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# --------------------------------------------------------------------------- #
# 2. Reject unexpected DDL; integration tests use the saved original.       #
# --------------------------------------------------------------------------- #
_ORIG_CREATE_ALL = _sa_schema.MetaData.create_all


def _block_create_all(self: _sa_schema.MetaData, *args: Any, **kwargs: Any) -> None:
    raise AssertionError("table creation requires an explicit disposable test database")


_sa_schema.MetaData.create_all = _block_create_all  # type: ignore[assignment]


# --------------------------------------------------------------------------- #
# 3. Keep schema reflection offline for tests without disposable services.   #
# --------------------------------------------------------------------------- #
class _StubInspector:
    """Drop-in Inspector that never connects and always raises for reflection."""

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    def has_table(self, table_name: str, schema: str | None = None) -> bool:
        return False

    def get_columns(self, table_name: str, schema: str | None = None, **_kw: Any) -> list[dict[str, Any]]:
        raise SQLAlchemyError(
            f"[test-isolation] Inspector.get_columns({table_name!r}) is stubbed; "
            "no live DB is available under characterization tests."
        )

    def __getattr__(self, name: str) -> Any:
        raise SQLAlchemyError(
            f"[test-isolation] Inspector.{name} is stubbed under characterization tests."
        )


_ORIG_INSPECT = sqlalchemy.inspect


def _test_inspect(target: Any) -> Any:
    # Any Engine-like target must never touch the network from tests.
    if hasattr(target, "raw_connection") or hasattr(target, "connect"):
        return _StubInspector(target)
    return _ORIG_INSPECT(target)


sqlalchemy.inspect = _test_inspect  # type: ignore[assignment]


# --------------------------------------------------------------------------- #
# 4. Import app.gift once, expose the module for every downstream test.       #
# --------------------------------------------------------------------------- #
def _import_gift_module():
    """Import app.gift under the isolation patches installed above."""
    if "app.gift" in sys.modules:
        del sys.modules["app.gift"]
    return importlib.import_module("app.gift")


gift = _import_gift_module()


@pytest.fixture(scope="session")
def gift_module():
    """Session-scoped fixture yielding the imported gift module."""
    return gift


@pytest.fixture(autouse=True)
def isolated_end_snapshots(monkeypatch):
    """Interruption snapshots must not leak between lifecycle scenarios."""
    from app import runtime_state

    monkeypatch.setattr(runtime_state, "SESSION_END_SNAPSHOTS", {})


@pytest.fixture(autouse=True)
def isolated_room_visibility(monkeypatch):
    """Every public GET checks Redis policy, including offline legacy reports."""
    import fakeredis

    from app import room_visibility

    client = fakeredis.FakeRedis()
    store = room_visibility.RoomVisibilityStore(client, prefix="test:visibility")
    monkeypatch.setattr(room_visibility, "_store", store)
    yield store
    client.close()


@pytest.fixture(autouse=True)
def isolated_metric_redis_clients(monkeypatch):
    """Default offline tests must not read or write the host's metric databases."""
    import fakeredis

    from app import redis_metrics, whale_metrics

    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_metrics, "_client", client)
    monkeypatch.setattr(whale_metrics, "_client", client)
    yield
    client.close()


@pytest.fixture()
def isolated_rooms_json(tmp_path, monkeypatch):
    """Per-test rooms.json path with baseline content, restored automatically."""
    rooms_path = tmp_path / "rooms.json"
    rooms_path.write_text(
        json.dumps(
            {
                "room_ids": [111111, 222222],
                "room_anchors": {
                    "111111": "TestAnchorAlpha",
                    "222222": "TestAnchorBeta",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    # Both the environment variable and the module-level constant must point
    # at the tmp file, because app.gift caches the resolved path at import.
    monkeypatch.setenv("ROOMS_JSON_PATH", str(rooms_path))
    monkeypatch.setattr(gift, "ROOMS_JSON_PATH", str(rooms_path))
    return rooms_path


@pytest.fixture()
def clean_room_registry(monkeypatch):
    """Snapshot & restore ROOM_IDS / ROOM_ANCHORS around a test."""
    original_ids = list(gift.ROOM_IDS)
    original_anchors = dict(gift.ROOM_ANCHORS)
    try:
        gift.ROOM_IDS.clear()
        gift.ROOM_ANCHORS.clear()
        yield
    finally:
        gift.ROOM_IDS.clear()
        gift.ROOM_IDS.extend(original_ids)
        gift.ROOM_ANCHORS.clear()
        gift.ROOM_ANCHORS.update(original_anchors)
