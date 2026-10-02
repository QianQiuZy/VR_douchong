"""Cache-only GET transport and per-IP admission for the seven public APIs."""

from __future__ import annotations

import gzip
import ipaddress
import math
from threading import BoundedSemaphore

import anyio
import redis
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from . import config
from .api_cache_store import ApiSqlScope, CacheUnavailable, api_sql_scope
from .repositories.tables import month_str, normalize_month_code

GET_PATHS = {
    "/gift",
    "/gift/by_month",
    "/gift/live_sessions",
    "/gift/attention",
    "/gift/sc",
}
API_PATHS = GET_PATHS | {"/add/room", "/delete/room"}


def client_ip(request: Request) -> str:
    peer = request.client.host if request.client else "unknown"
    # Uvicorn already handles trusted loopback XFF. Also support the EO-specific
    # header when ASGI is called directly behind the local Nginx proxy.
    candidates = [peer]
    if peer in {"127.0.0.1", "::1"}:
        candidates = [
            request.headers.get("eo-connecting-ip", ""),
            request.headers.get("x-real-ip", ""),
            peer,
        ]
    for value in candidates:
        try:
            address = ipaddress.ip_address(value)
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
            return str(address)
        except ValueError:
            continue
    return peer


def parameters(request: Request) -> tuple[str, int | None] | JSONResponse:
    path = request.url.path.rstrip("/")
    if path == "/gift":
        return month_str(), None
    rid = None
    if path != "/gift/by_month":
        raw = request.query_params.get("room_id")
        if path == "/gift/sc" and not raw:
            return JSONResponse({"error": "room_id 参数必填"}, status_code=400)
        try:
            rid = int(raw or "0")
        except ValueError:
            return JSONResponse({"error": "room_id 参数无效"}, status_code=400)
        if rid <= 0:
            message = (
                "room_id 必须为正整数"
                if path == "/gift/sc"
                else "room_id 必填且需为正整数"
            )
            return JSONResponse({"error": message}, status_code=400)
    raw_month = request.query_params.get("month")
    month = normalize_month_code(raw_month) if raw_month else month_str()
    if month is None:
        message = (
            "month 格式不正确，应为 YYYYMM 或 YYYY-MM"
            if path == "/gift/sc"
            else "month 参数无效，支持 YYYYMM 或 YYYY-MM"
        )
        return JSONResponse({"error": message}, status_code=400)
    return month, rid


def accepts_gzip(header: str) -> bool:
    quality: dict[str, float] = {}
    for token in header.lower().split(","):
        parts = token.strip().split(";")
        try:
            value = next(
                (
                    float(part.strip()[2:])
                    for part in parts[1:]
                    if part.strip().startswith("q=")
                ),
                1.0,
            )
        except ValueError:
            value = 0.0
        quality[parts[0]] = value if math.isfinite(value) and 0 <= value <= 1 else 0
    return quality.get("gzip", quality.get("*", 0)) > 0


class CacheMiddleware:
    def __init__(self, app, owner):
        self.app = app
        self.owner = owner
        self.gate = BoundedSemaphore(config.API_CACHE_MAX_INFLIGHT)

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "").rstrip("/")
        if (
            scope["type"] != "http"
            or path not in API_PATHS
            or not config.API_CACHE_ENABLED
        ):
            return await self.app(scope, receive, send)
        request = Request(scope)
        service = getattr(self.owner.state, "api_cache", None)
        try:
            if service is None:
                raise CacheUnavailable("cache lifecycle not started")
            allowed = await anyio.to_thread.run_sync(
                service.store.allow_ip, client_ip(request)
            )
        except (redis.RedisError, CacheUnavailable) as exc:
            return await self.failure(service, exc)(scope, receive, send)
        if not allowed:
            response = JSONResponse(
                {"error": "请求过于频繁"},
                status_code=429,
                headers={"Retry-After": "1", "Cache-Control": "no-store"},
            )
            return await response(scope, receive, send)
        if not self.gate.acquire(blocking=False):
            return await self.failure(
                service, CacheUnavailable("API in-flight capacity exhausted")
            )(scope, receive, send)
        try:
            return await self.serve(request, service, scope, receive, send)
        finally:
            self.gate.release()

    def failure(self, service, exc: BaseException) -> JSONResponse:
        if service is not None:
            service.store.faults.record("request-cache", exc)
        return JSONResponse(
            {"error": "接口缓存暂不可用"},
            status_code=503,
            headers={"Retry-After": "1", "Cache-Control": "no-store"},
        )

    async def serve(self, request, service, scope, receive, send):
        path = scope["path"].rstrip("/")
        if path in GET_PATHS and scope["method"] == "GET":
            try:
                parsed = parameters(request)
                if isinstance(parsed, JSONResponse):
                    response = parsed
                else:
                    month, rid = parsed
                    body = await anyio.to_thread.run_sync(
                        service.store.read, month, path, rid
                    )
                    headers = {"Cache-Control": "no-store", "Vary": "Accept-Encoding"}
                    if accepts_gzip(request.headers.get("accept-encoding", "")):
                        headers["Content-Encoding"] = "gzip"
                    else:
                        body = gzip.decompress(body)
                    response = Response(
                        body, media_type="application/json", headers=headers
                    )
            except (
                redis.RedisError,
                CacheUnavailable,
                ValueError,
                OSError,
                EOFError,
            ) as exc:
                response = self.failure(service, exc)
            return await response(scope, receive, send)
        budget_scope = ApiSqlScope(service.store)
        token = api_sql_scope.set(budget_scope)
        try:
            return await self.app(scope, receive, send)
        finally:
            budget_scope.active = False
            budget_scope.stop.set()
            api_sql_scope.reset(token)
