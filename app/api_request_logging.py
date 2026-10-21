"""ASGI access logging outside FastAPI, including cache and error responses."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime
from urllib.parse import parse_qsl

from starlette.requests import Request

from .api_cache_http import client_ip
from .logging_setup import SHANGHAI, api_log_context

logger = logging.getLogger(__name__)
QUERY_FIELDS = {"month", "room_id", "uid", "cache"}


def _query(scope) -> list[tuple[str, str]]:
    try:
        pairs = parse_qsl(
            scope.get("query_string", b"").decode("utf-8", "replace"), max_num_fields=64
        )
    except ValueError:
        return []
    return [(key, value[:128]) for key, value in pairs if key in QUERY_FIELDS]


class RequestLoggingMiddleware:
    """Measure through the last response body, excluding subsequent background work."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        started = time.perf_counter()
        request_time = datetime.now(SHANGHAI).isoformat(timespec="milliseconds")
        request_id = uuid.uuid4().hex
        status = None
        logged = False
        disconnected = False

        def record(completed: bool, error_type: str | None = None):
            nonlocal logged
            if logged:
                return
            logged = True
            fields = {
                "request_time": request_time,
                "client_ip": client_ip(Request(scope)),
                "method": scope["method"][:16],
                "path": scope.get("path", "")[:2048],
                "query": _query(scope),
                "status": status,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                "completed": completed,
                "disconnected": disconnected,
            }
            if error_type:
                fields["error_type"] = error_type
            # JSON escapes newlines/control characters; no headers, body or raw query.
            logger.info(json.dumps(fields, ensure_ascii=False))

        async def receive_logged():
            nonlocal disconnected
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected = True
            return message

        async def send_logged(message):
            nonlocal status
            await send(message)
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                record(completed=True)

        with api_log_context(request_id):
            try:
                await self.app(scope, receive_logged, send_logged)
            except BaseException as exc:
                record(completed=False, error_type=type(exc).__name__)
                raise
            finally:
                record(completed=False)
