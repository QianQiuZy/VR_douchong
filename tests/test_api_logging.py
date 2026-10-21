"""Offline regressions for BaoTa log separation and lossless size rotation."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
import threading
from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from app import bootstrap, config, logging_setup, monitoring_jobs, runtime_state
from app.api_cache import ApiCache
from app.api_cache_http import CacheMiddleware
from app.api_cache_store import CacheUnavailable
from app.api_request_logging import RequestLoggingMiddleware
from app.logging_setup import API_LOG_MAX_BYTES, ApiLogHandler, api_log_context, configure_logging


@pytest.fixture
def routed_logs(tmp_path, monkeypatch):
    """Isolate global logging changes, including Uvicorn's own handler state."""
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    names = ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi")
    saved_loggers = {
        name: (logger.handlers[:], logger.level, logger.propagate)
        for name in names
        for logger in [logging.getLogger(name)]
    }
    console = io.StringIO()
    root.handlers = [logging.StreamHandler(console)]
    path = tmp_path / "api.log"
    monkeypatch.setattr(config, "API_LOG_PATH", path)
    monkeypatch.setattr(config, "API_LOG_PROJECT_NAME", "VR_douchong")
    try:
        handler = configure_logging()
        yield SimpleNamespace(path=path, console=console, handler=handler)
    finally:
        for handler in root.handlers:
            handler.close()
        root.handlers, root.level = saved_handlers, saved_level
        for name, (handlers, level, propagate) in saved_loggers.items():
            logger = logging.getLogger(name)
            logger.handlers, logger.level, logger.propagate = handlers, level, propagate


def access_rows(path):
    return [
        json.loads(line.split(" - ", 4)[4])
        for line in path.read_text(encoding="utf-8").splitlines()
        if " - app.api_request_logging - " in line
    ]


def test_api_routing_is_idempotent_and_context_does_not_leak(routed_logs):
    assert configure_logging() is routed_logs.handler
    shared = logging.getLogger("app.database")
    shared.info("collector-pool")
    logging.getLogger("app.event_ingestion").info("gift-record")
    logging.getLogger("app.api_app").error("route-error")
    logging.getLogger("app.api_cache_store").error("cache-error")
    logging.getLogger("uvicorn.error").error("server-error")
    with api_log_context("request-123"):
        shared.warning("request-pool")
    shared.info("collector-after-request")
    console = routed_logs.console.getvalue()
    api = routed_logs.path.read_text(encoding="utf-8")
    for message in ("collector-pool", "gift-record", "collector-after-request"):
        assert message in console and message not in api
    for message in ("route-error", "cache-error", "server-error", "request-pool"):
        assert api.count(message) == 1 and message not in console
    assert "request_id=request-123" in api
    assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+08:00", api)


def test_real_50_mib_boundary_rotates_existing_file_without_deleting_history(tmp_path):
    path = tmp_path / "api.log"
    with path.open("wb") as stream:
        stream.write(b"old-log\n")
        stream.truncate(API_LOG_MAX_BYTES - 1)
    handler = ApiLogHandler(path, "VR_douchong")
    try:
        handler.handle(logging.makeLogRecord({"msg": "new-log"}))
    finally:
        handler.close()
    archives = list((tmp_path / "api-history_logs").glob("*.log"))
    assert len(archives) == 1
    assert re.fullmatch(r"VR_douchong_\d{4}-\d{2}-\d{2}_\d{6}_log\.log", archives[0].name)
    assert archives[0].stat().st_size == API_LOG_MAX_BYTES - 1
    with archives[0].open("rb") as stream:
        assert stream.read(8) == b"old-log\n"
    assert path.read_text(encoding="utf-8") == "new-log\n"


def test_utf8_rotation_and_same_second_archives_preserve_every_record(tmp_path, monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 9, 19, 40, 2, tzinfo=tz)

    monkeypatch.setattr(logging_setup, "datetime", FixedDateTime)
    path = tmp_path / "api.log"
    handler = ApiLogHandler(path, "VR_douchong", max_bytes=20)
    messages = [f"日志{i}" for i in range(10)]
    try:
        for message in messages:
            handler.handle(logging.makeLogRecord({"msg": message}))
    finally:
        handler.close()
    archives = sorted((tmp_path / "api-history_logs").glob("*.log"))
    assert len(archives) == 4
    assert (archives[0].parent / "VR_douchong_2026-10-09_194002_log.log").is_file()
    files = [*archives, path]
    assert all(file.stat().st_size < 20 for file in files)
    assert sorted(line for file in files for line in file.read_text().splitlines()) == messages


@pytest.mark.parametrize("status", [200, 400, 429, 503])
def test_cache_short_circuits_are_logged_once(status, routed_logs, monkeypatch):
    monkeypatch.setattr(config, "API_CACHE_ENABLED", True)

    def read(*args):
        if status == 503:
            raise CacheUnavailable("offline-cache-failure")
        import gzip

        return gzip.compress(b"[]")

    store = SimpleNamespace(
        allow_ip=lambda ip: status != 429,
        read=read,
        faults=SimpleNamespace(record=lambda *args: None),
    )
    app = FastAPI()
    app.state.api_cache = SimpleNamespace(store=store)
    app.add_middleware(CacheMiddleware, owner=app)
    url = "/gift/sc" if status == 400 else "/gift?month=202610&token=hidden-value"
    with TestClient(RequestLoggingMiddleware(app), client=("127.0.0.1", 12345)) as client:
        response = client.get(url, headers={"EO-Connecting-IP": "203.0.113.5"})
    assert response.status_code == status
    rows = access_rows(routed_logs.path)
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == status and row["completed"] is True
    assert row["client_ip"] == "203.0.113.5" and row["duration_ms"] >= 0
    assert row["request_time"].endswith("+08:00")
    assert "hidden-value" not in routed_logs.path.read_text()


def test_routing_errors_and_auth_secrets_are_not_logged(routed_logs):
    app = FastAPI()

    @app.get("/failure")
    def failure():
        raise RuntimeError("offline error")

    @app.post("/manage")
    def manage():
        return {"ok": True}

    with TestClient(RequestLoggingMiddleware(app), raise_server_exceptions=False) as client:
        assert client.get("/unknown").status_code == 404
        assert client.get("/manage").status_code == 405
        assert client.get("/failure").status_code == 500
        assert client.post(
            "/manage?room_id=123&api_key=query-secret&month=202610%0Aforged",
            headers={"X-API-Key": "header-secret", "Authorization": "Bearer auth-secret"},
            json={"api_key": "body-secret"},
        ).status_code == 200
    rows = access_rows(routed_logs.path)
    assert [row["status"] for row in rows] == [404, 405, 500, 200]
    text = routed_logs.path.read_text()
    assert len(text.splitlines()) == 4
    assert all(secret not in text for secret in (
        "query-secret", "header-secret", "auth-secret", "body-secret"
    ))
    assert len(set(re.findall(r"request_id=(\w+)", text))) == 4


def test_stream_duration_includes_last_body_and_incomplete_responses(routed_logs):
    async def scenario():
        async def receive():
            return {"type": "http.request"}

        async def send(message):
            pass

        scope = {
            "type": "http", "method": "GET", "path": "/stream",
            "headers": [], "client": ("203.0.113.1", 12345),
        }

        async def stream(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"first", "more_body": True})
            assert access_rows(routed_logs.path) == []
            await asyncio.sleep(0.02)
            await send({"type": "http.response.body", "body": b"last"})
            assert len(access_rows(routed_logs.path)) == 1

        await RequestLoggingMiddleware(stream)(scope, receive, send)

        async def broken(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            raise RuntimeError("stream broke")

        with pytest.raises(RuntimeError, match="stream broke"):
            await RequestLoggingMiddleware(broken)(scope, receive, send)

    asyncio.run(scenario())
    rows = access_rows(routed_logs.path)
    assert len(rows) == 2
    assert rows[0]["completed"] is True and rows[0]["duration_ms"] >= 20
    assert rows[1]["completed"] is False and rows[1]["error_type"] == "RuntimeError"


def test_context_follows_worker_and_main_loop_without_tagging_collector(routed_logs):
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def serve_loop():
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()

    thread = threading.Thread(target=serve_loop)
    thread.start()
    assert ready.wait(5)

    async def shared_work(message):
        logging.getLogger("app.room_lifecycle").warning(message)

    async def request():
        with api_log_context("bridge-request"):
            await asyncio.to_thread(logging.getLogger("app.database").warning, "worker-log")
            future = asyncio.run_coroutine_threadsafe(shared_work("bridge-log"), loop)
            await asyncio.wrap_future(future)
        await shared_work("collector-log")

    try:
        asyncio.run(request())
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()
    text = routed_logs.path.read_text()
    assert "worker-log" in text and "bridge-log" in text
    assert "collector-log" not in text and "collector-log" in routed_logs.console.getvalue()


def test_cache_builder_shared_logs_follow_api_context(routed_logs, monkeypatch):
    cache = ApiCache.__new__(ApiCache)
    monkeypatch.setattr(cache, "_run", lambda: logging.getLogger("app.database").error("builder-db"))
    cache.run()
    assert "builder-db" in routed_logs.path.read_text()
    assert "builder-db" not in routed_logs.console.getvalue()


def test_room_added_by_api_keeps_future_gifts_in_collector_log(routed_logs, monkeypatch):
    tasks = []

    async def collect():
        await asyncio.sleep(0)
        logging.getLogger("app.event_ingestion").info("new-room-gift")
        logging.getLogger("app.repositories.monthly").error("new-room-write-error")

    class OfflineClient:
        def __init__(self, *args, **kwargs):
            pass

        def set_handler(self, handler):
            pass

        def start(self):
            tasks.append(asyncio.create_task(collect()))

    monkeypatch.setattr(monitoring_jobs, "AuthenticatedLiveClient", OfflineClient)
    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {})
    monkeypatch.setattr(runtime_state, "LAST_RECONNECT", {})

    async def scenario():
        with api_log_context("add-room-request"):
            await monitoring_jobs.start_client(123)
        await asyncio.gather(*tasks)

    asyncio.run(scenario())
    assert "[connect]" in routed_logs.path.read_text()
    for message in ("new-room-gift", "new-room-write-error"):
        assert message in routed_logs.console.getvalue()
        assert message not in routed_logs.path.read_text()


def test_uvicorn_uses_the_outer_logger_without_default_access_duplicates(routed_logs, monkeypatch):
    import uvicorn

    captured = []
    monkeypatch.setattr(uvicorn.Server, "run", lambda self: captured.append(self.config))
    monkeypatch.setattr(bootstrap.api_app, "app", FastAPI())
    bootstrap._run_api_server()
    server_config = captured[0]
    assert server_config.log_config is None and server_config.access_log is False
    app = server_config.app.app

    @app.get("/logging-test-stream")
    def stream():
        return StreamingResponse(iter([b"first", b"last"]))

    with TestClient(server_config.app) as client:
        response = client.get("/logging-test-stream")
    assert response.content == b"firstlast"
    assert len(access_rows(routed_logs.path)) == 1
    assert " - uvicorn.access - " not in routed_logs.path.read_text()
