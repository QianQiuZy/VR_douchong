from __future__ import annotations

from app import bilibili_gateway, event_ingestion, monitoring_jobs


class TestTodo4ModuleContract:
    def test_gateway_ingestion_and_jobs_are_importable(self):
        assert bilibili_gateway is not None
        assert event_ingestion is not None
        assert monitoring_jobs is not None
