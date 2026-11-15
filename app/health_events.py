"""Process-local, bounded health history without URLs, bodies or credentials."""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import aiohttp

SHANGHAI = timezone(timedelta(hours=8))
WINDOW_SECONDS = 3600
UPSTREAM_PATHS = {
    "/room/v1/Room/get_status_info_by_uids": "live_status",
    "/room/v1/Room/get_info": "room_info",
    "/xlive/general-interface/v1/guard/GuardActive": "guard",
    "/xlive/general-interface/v1/rank/getFansMembersRank": "fans",
    "/xlive/general-interface/v1/rank/queryContributionRank": "concurrency",
    "/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket": "bili_ticket",
    "/xlive/web-room/v1/index/getDanmuInfo": "danmaku_info",
    "/x/web-interface/nav": "nav_wbi",
    "/x/frontend/finger/spi": "buvid",
    "/": "homepage",
}


def timestamp(value: float | None) -> str | None:
    return datetime.fromtimestamp(value, SHANGHAI).isoformat(timespec="milliseconds") if value else None


class HealthEvents:
    def __init__(self, clock=time.monotonic, wall_clock=time.time):
        self.clock, self.wall_clock = clock, wall_clock
        self.started = clock()
        self.started_at = wall_clock()
        self.lock = threading.Lock()
        self.buckets = deque()
        self.recent = deque(maxlen=200)
        self.upstream = {}
        self.upstream_observed = {}

    def _prune(self, now):
        while self.buckets and self.buckets[0][0] <= int(now) - WINDOW_SECONDS:
            self.buckets.popleft()

    def record(self, kind, target, *, room_id=None, reason=None, http_status=None,
               business_code=None, duration_ms=None, queue_ms=None):
        key = (kind, target, room_id, reason, http_status, business_code)
        with self.lock:
            now = self.clock()
            event = dict(kind=kind, target=target, room_id=room_id, reason=reason,
                         http_status=http_status, business_code=business_code,
                         at=timestamp(self.wall_clock()), duration_ms=duration_ms, queue_ms=queue_ms)
            self._prune(now)
            if not self.buckets or self.buckets[-1][0] != int(now):
                self.buckets.append((int(now), Counter()))
            self.buckets[-1][1][key] += 1
            self.recent.append((now, event))

    def upstream_result(self, observation, *, reason=None, http_status=None, business_code=None):
        if observation.recorded:
            return
        observation.recorded = True
        now = self.wall_clock()
        with self.lock:
            entry = self.upstream.setdefault(observation.target, {})
            entry["last_failure_at" if reason else "last_success_at"] = timestamp(now)
            entry["last_result"] = "failed" if reason else "ok"
            self.upstream_observed[observation.target] = self.clock()
        if reason:
            self.record(
                "upstream_api", observation.target, room_id=observation.room_id,
                reason=reason, http_status=http_status, business_code=business_code,
                duration_ms=round((self.clock() - observation.started) * 1000, 3),
                queue_ms=round(observation.queue_seconds * 1000, 3),
            )

    def snapshot(self):
        now = self.clock()
        with self.lock:
            self._prune(now)
            counts = Counter()
            for _, bucket in self.buckets:
                counts.update(bucket)
            totals = Counter()
            groups = []
            for (kind, target, room, reason, status, code), count in sorted(
                counts.items(), key=lambda item: str(item[0])
            ):
                totals[kind] += count
                groups.append(dict(kind=kind, target=target, room_id=room, reason=reason,
                                   http_status=status, business_code=code, count=count))
            return {
                "window_seconds": WINDOW_SECONDS,
                "resolution_seconds": 1,
                "observed_since": timestamp(self.started_at),
                "coverage_seconds": min(WINDOW_SECONDS, round(now - self.started, 3)),
                "full_window": now - self.started >= WINDOW_SECONDS,
                "totals": {kind: totals[kind] for kind in (
                    "room_disconnect", "room_planned_close", "upstream_api",
                    "local_api", "rate_limited",
                )},
                "groups": groups,
                "recent_events": [event.copy() for at, event in self.recent if at > now - WINDOW_SECONDS],
                "recent_events_limit": self.recent.maxlen,
                "upstream_endpoints": {
                    key: value.copy() for key, value in self.upstream.items()
                    if self.upstream_observed[key] > now - WINDOW_SECONDS
                },
            }


events = HealthEvents()


def observation(url):
    # Never retain a full URL, query, token, signature, error message or user ID.
    room = url.query.get("room_id")
    room_id = int(room) if room and room.isascii() and room.isdecimal() and len(room) <= 20 else None
    return SimpleNamespace(
        target=UPSTREAM_PATHS.get(url.path, "other_bilibili"), room_id=room_id,
        started=events.clock(), queue_seconds=0.0, queue_started=None, recorded=False,
    )


def trace_config():
    trace = aiohttp.TraceConfig()

    async def start(session, context, params):
        context.health = observation(params.url)
        if params.headers.get("Upgrade", "").lower() == "websocket":
            context.health.target = "websocket"

    async def queued(session, context, params):
        context.health.queue_started = events.clock()

    def finish_queue(context):
        if context.health.queue_started is not None:
            context.health.queue_seconds += events.clock() - context.health.queue_started
            context.health.queue_started = None

    async def dequeued(session, context, params):
        finish_queue(context)

    async def end(session, context, params):
        params.response._health_observation = context.health
        if params.response.status >= 400:
            events.upstream_result(context.health, reason="http_error", http_status=params.response.status)
        elif params.response.status == 101 or context.health.target == "homepage":
            # WS handshakes and the cookie initialization homepage do not have
            # JSON bodies; their recovery must clear the previous failure too.
            events.upstream_result(context.health, http_status=params.response.status)

    async def error(session, context, params):
        finish_queue(context)
        # Cancellation is normal when stopping clients or cancelling end reads.
        if isinstance(params.exception, Exception):
            events.upstream_result(context.health, reason=type(params.exception).__name__)

    trace.on_request_start.append(start)
    trace.on_connection_queued_start.append(queued)
    trace.on_connection_queued_end.append(dequeued)
    trace.on_request_end.append(end)
    trace.on_request_exception.append(error)
    return trace


class HealthClientResponse(aiohttp.ClientResponse):
    """Observe body-read/JSON failures too; trace end only means headers arrived."""

    def _result(self, **kwargs):
        item = getattr(self, "_health_observation", None)
        if item is not None:
            events.upstream_result(item, http_status=self.status, **kwargs)

    async def read(self):
        try:
            return await super().read()
        except Exception as exc:
            self._result(reason=type(exc).__name__)
            raise

    async def json(self, *args, **kwargs):
        try:
            payload = await super().json(*args, **kwargs)
        except Exception as exc:
            self._result(reason=type(exc).__name__)
            raise
        code = payload.get("code") if isinstance(payload, dict) else None
        if isinstance(code, int) and code != 0:
            self._result(reason="business_error", business_code=code)
        else:
            self._result()
        return payload


class HealthTrackingMiddleware:
    """Count locally observed availability failures outside cache/visibility."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        from .api_cache_http import API_PATHS

        path = scope.get("path", "").rstrip("/")
        if scope["type"] != "http" or path not in API_PATHS:
            return await self.app(scope, receive, send)
        status, completed, recorded = None, False, False
        started = events.clock()

        def record(reason):
            nonlocal recorded
            if not recorded:
                recorded = True
                events.record("rate_limited" if status == 429 else "local_api", path,
                              reason=reason, http_status=status,
                              duration_ms=round((events.clock() - started) * 1000, 3))

        async def track_send(message):
            nonlocal status, completed
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                completed = True
                if status == 429 or (status is not None and status >= 500):
                    record("http_error")

        try:
            await self.app(scope, receive, track_send)
        except BaseException as exc:
            if not completed:
                if status is None and isinstance(exc, Exception):
                    status = 500
                record(type(exc).__name__)
            raise
        finally:
            if not completed:
                record("incomplete_response")
