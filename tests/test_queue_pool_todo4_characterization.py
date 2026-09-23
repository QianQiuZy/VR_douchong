from __future__ import annotations

import asyncio
from types import SimpleNamespace

from sqlalchemy.pool import QueuePool

from app import bootstrap, database, gift


def test_default_engine_pool_contract_is_current_sqlalchemy_default() -> None:
    # Given: the import-time engine built by the isolated test configuration.
    pool = database.engine.pool

    # When: its QueuePool settings are observed without checking out a connection.
    # Then: the implicit pre-Todo-4 defaults are pinned.
    assert isinstance(pool, QueuePool)
    assert pool.size() == 5
    assert pool._max_overflow == 10
    assert pool._timeout == 30
    assert pool._recycle == 1800
    assert pool._pre_ping is True


def test_archive_scheduler_current_dispatch_order(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_to_thread(job, _month):
        calls.append(job.__name__)

    monkeypatch.setattr(bootstrap.asyncio, "to_thread", fake_to_thread)

    # Given: archive thread dispatch is replaced with a side-effect-free recorder.
    # When: one monthly archive operation is run directly.
    asyncio.run(bootstrap._archive_month("202601"))

    # Then: the four monthly jobs precede the live-session archive.
    assert calls == [
        "archive_super_chat_log",
        "archive_room_live_stats",
        "archive_attention",
        "archive_whale_month",
        "archive_live_session",
    ]


def test_historical_15m_metadata_currently_controls_null_columns(monkeypatch) -> None:
    from app import api_app

    captured: dict[str, str] = {}

    class Result:
        def fetchall(self) -> list[tuple[int, ...]]:
            return []

    class Session:
        def execute(self, statement, _parameters):
            captured["sql"] = str(statement)
            return Result()

    monkeypatch.setattr(gift, "sc_log_table_exists", lambda _table_name: True)
    monkeypatch.setattr(
        api_app,
        "inspect",
        lambda _engine: SimpleNamespace(get_columns=lambda _table_name: []),
    )

    # Given: an old archive table with no optional danmaku columns.
    # When: its 15-minute metadata is rendered.
    api_app._session_15m_stats(Session(), 99, "202601")

    # Then: the existing response-shaping SQL selects null placeholders.
    assert "NULL AS captain_danmaku_count" in captured["sql"]
    assert "NULL AS normal_danmaku_count" in captured["sql"]
