"""Redis-backed publication policy; collection and complete snapshots stay intact."""

from __future__ import annotations

import gzip
import json

import anyio
import redis
from starlette.responses import JSONResponse, Response

from . import config
from .api_cache_http import GET_PATHS
from .api_cache_store import CacheUnavailable, FaultLog, db2_url


class RoomVisibilityStore:
    def __init__(self, client=None, prefix: str | None = None):
        self.client = (
            client
            if client is not None
            else redis.Redis.from_url(
                db2_url(config.REDIS_URL),
                decode_responses=False,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        )
        self.key = f"{prefix or config.API_CACHE_PREFIX}:hidden-rooms"
        self.faults = FaultLog()

    def hidden_rooms(self) -> set[int]:
        rooms = set()
        for value in self.client.smembers(self.key):
            raw = value.decode("ascii") if isinstance(value, bytes) else value
            if not isinstance(raw, str) or not raw.isascii() or not raw.isdecimal():
                raise CacheUnavailable("invalid hidden room identity")
            room_id = int(raw)
            if not 0 < room_id < 2**64 or str(room_id) != raw:
                raise CacheUnavailable("invalid hidden room identity")
            rooms.add(room_id)
        return rooms

    def set_hidden(self, room_id: int, hidden: bool) -> bool:
        operation = self.client.sadd if hidden else self.client.srem
        # No expiry: only an explicit restore removes the publication restriction.
        return bool(operation(self.key, str(room_id)))

    def close(self) -> None:
        self.client.close()


# Constructing the connection pool is offline. All I/O occurs in worker threads.
_store = RoomVisibilityStore()


def get_store() -> RoomVisibilityStore:
    return _store


def unavailable(exc: BaseException) -> JSONResponse:
    get_store().faults.record("room-visibility", exc)
    return JSONResponse(
        {"error": "房间可见状态暂不可用"},
        status_code=503,
        headers={"Retry-After": "1", "Cache-Control": "no-store"},
    )


def _filtered_response(path: str, start: dict, body: bytes, hidden: set[int]) -> Response:
    headers = {
        key.decode("latin-1"): value.decode("latin-1")
        for key, value in start["headers"]
        if key.lower() != b"content-length"
    }
    compressed = headers.get("content-encoding") == "gzip"
    payload = json.loads(gzip.decompress(body) if compressed else body)
    if path in {"/gift", "/gift/by_month"}:
        if not isinstance(payload, list):
            raise CacheUnavailable("invalid room summary")
        payload = [row for row in payload if row["room_id"] not in hidden]
    elif path == "/gift/entry":
        if payload["mode"] == "room":
            if payload["room_id"] in hidden:
                return _not_found()
        else:
            payload["items"] = [row for row in payload["items"] if row["room_id"] not in hidden]
    elif payload["room_id"] in hidden:
        return _not_found()
    headers["cache-control"] = "no-store"
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    if compressed:
        body = gzip.compress(body, compresslevel=1, mtime=0)
    return Response(body, headers=headers, media_type="application/json")


def _not_found() -> JSONResponse:
    return JSONResponse(
        {"error": "房间不存在"}, status_code=404, headers={"Cache-Control": "no-store"}
    )


class RoomVisibilityMiddleware:
    """Wrap both cached GETs and legacy SQL routes, even when caching is disabled."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "").rstrip("/")
        if scope["type"] != "http" or scope["method"] != "GET" or path not in GET_PATHS:
            return await self.app(scope, receive, send)
        try:
            hidden = await anyio.to_thread.run_sync(get_store().hidden_rooms)
        except (redis.RedisError, CacheUnavailable, ValueError, TypeError) as exc:
            return await unavailable(exc)(scope, receive, send)
        if not hidden:
            return await self.app(scope, receive, send)

        start = None
        chunks = []

        async def filtered_send(message):
            nonlocal start
            if message["type"] == "http.response.start" and message["status"] == 200:
                start = message
            elif message["type"] == "http.response.body" and start is not None:
                chunks.append(message.get("body", b""))
                if not message.get("more_body", False):
                    try:
                        response = await anyio.to_thread.run_sync(
                            _filtered_response, path, start, b"".join(chunks), hidden
                        )
                    except (
                        CacheUnavailable,
                        ValueError,
                        TypeError,
                        KeyError,
                        OSError,
                        EOFError,
                    ) as exc:
                        response = unavailable(exc)
                    await response(scope, receive, send)
            else:
                await send(message)

        await self.app(scope, receive, filtered_send)
