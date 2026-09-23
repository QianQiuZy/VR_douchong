from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy.schema import CreateTable

from app import gift
from app.database import Base


def test_historical_gift_route_reflects_archive_on_its_existing_session(monkeypatch) -> None:
    # Given: a real SQLite schema and a QueuePool with one available connection.
    engine = create_engine(
        "sqlite://",
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
        connect_args={"check_same_thread": False},
    )
    with engine.begin() as connection:
        for table in Base.metadata.sorted_tables:
            connection.execute(CreateTable(table))
        connection.execute(
            text(
                "CREATE TABLE room_live_stats_190001 "
                "(room_id INTEGER, date DATE, duration INTEGER, steel_coin_count INTEGER)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO room_live_stats_190001 "
                "(room_id, date, duration, steel_coin_count) VALUES (111111, '1900-01-01', 7200, 3)"
            )
        )
    monkeypatch.setattr(gift, "Session", sessionmaker(bind=engine))

    # When: the historical endpoint enumerates room rows, then reflects and queries the archive.
    with TestClient(gift.app) as client:
        response = client.get("/gift/by_month", params={"month": "190001"})

    # Then: the route uses the already-checked-out Session connection and returns archive values.
    assert response.status_code == 200
    room = next(item for item in response.json() if item["room_id"] == 111111)
    assert room["live_duration"] == "02:00:00"
    assert room["effective_days"] == 1
    assert room["steel_coin_count"] == 3
    assert isinstance(engine.pool, QueuePool)
    assert engine.pool.checkedout() == 0
    engine.dispose()
