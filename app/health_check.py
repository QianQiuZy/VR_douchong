"""Cached public health snapshots; dependency probes never run in request paths."""

from __future__ import annotations

import asyncio
import threading
import time

import pymysql
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from .api_cache_store import redis_db_url
from . import config, database, health_events, room_config, runtime_state

SAMPLE_INTERVAL = 15
STALE_SECONDS = 45


def probe_database():
    # A dedicated bounded connection distinguishes database availability from
    # an exhausted application QueuePool; never wait on its 30-second checkout.
    with pymysql.connect(
        **config.DB_CONFIG, connect_timeout=2, read_timeout=2, write_timeout=2,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise RuntimeError("UnexpectedDatabaseProbeResult")


def pool_snapshot():
    pool = database.engine.pool
    result = {"status": "unknown", "type": type(pool).__name__}
    if all(hasattr(pool, name) for name in ("size", "checkedin", "checkedout", "overflow")):
        checked_out = pool.checkedout()
        capacity = config.DB_POOL_SIZE + config.DB_POOL_MAX_OVERFLOW
        result.update(
            status="exhausted" if checked_out >= capacity else "ok",
            size=pool.size(), checked_in=pool.checkedin(), checked_out=checked_out,
            overflow=pool.overflow(), max_overflow=config.DB_POOL_MAX_OVERFLOW,
            capacity=capacity, checkout_timeout_seconds=config.DB_POOL_TIMEOUT,
            recycle_seconds=config.DB_POOL_RECYCLE,
        )
    return result


def redis_clients():
    urls = {
        "metrics": config.REDIS_URL,
        "whale_db1": redis_db_url(config.REDIS_URL, 1),
        "api_db2": redis_db_url(config.REDIS_URL, 2),
        "danmaku_users_db3": redis_db_url(config.REDIS_URL, 3),
        "danmaku_rank_db4": redis_db_url(config.REDIS_URL, 4),
    }
    return {role: redis.Redis.from_url(
        url, socket_connect_timeout=2, socket_timeout=2,
        retry=Retry(NoBackoff(), 0), retry_on_timeout=False,
    ) for role, url in urls.items()}


async def collector_snapshot():
    # Runs on the collector loop: do not inspect its dictionaries/connector
    # from the separate Uvicorn thread while clients are being added/removed.
    rooms = []
    for room_id in room_config.get_room_ids():
        client = runtime_state.ROOM_CLIENTS.get(room_id)
        rooms.append({
            "room_id": room_id,
            "live_status": runtime_state.LAST_STATUS.get(room_id),
            **(client.health_snapshot() if client is not None else {"status": "not_started"}),
        })
    session = runtime_state.aiohttp_session
    connector = session.connector if session is not None and not session.closed else None
    http_pool = {"status": "uninitialized"}
    if connector is not None and not connector.closed:
        acquired = len(connector._acquired)
        waiting = sum(len(waiters) for waiters in connector._waiters.values())
        http_pool = {
            "status": "exhausted" if connector.limit and acquired >= connector.limit else "ok",
            "limit": connector.limit, "limit_per_host": connector.limit_per_host,
            "acquired": acquired, "waiting": waiting,
            "idle": sum(len(connections) for connections in connector._conns.values()),
        }
    queues = {
        name.lower(): getattr(runtime_state, name).qsize() for name in (
            "GUARD_FANS_QUEUE", "ATTENTION_QUEUE", "DAILY_ATTENTION_QUEUE",
            "DAILY_GUARD_QUEUE", "DAILY_FANS_QUEUE",
        )
    }
    connected = sum(room["status"] == "connected" for room in rooms)
    return {
        "status": "ok" if connected == len(rooms) else "degraded",
        "loop_responsive": True, "http_pool": http_pool, "queues": queues,
        "rooms": {"configured": len(rooms), "connected": connected,
                  "not_connected": len(rooms) - connected, "items": rooms},
    }


def probe_collector():
    loop = runtime_state.MAIN_LOOP
    if loop is None or loop.is_closed() or not loop.is_running():
        return {"status": "unavailable", "loop_responsive": False}
    coroutine = collector_snapshot()
    try:
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
    except RuntimeError:
        coroutine.close()
        raise
    try:
        return future.result(timeout=2)
    finally:
        if not future.done():
            future.cancel()


def measured(probe):
    started = time.monotonic()
    try:
        probe()
        result = {"status": "ok"}
    except Exception as exc:
        # Exceptions may contain credentials, SQL, signed URLs or user data.
        result = {"status": "unavailable", "error_type": type(exc).__name__}
    result["duration_ms"] = round((time.monotonic() - started) * 1000, 3)
    return result


def probe_redis(client):
    if not client.ping():
        raise RuntimeError("UnexpectedRedisProbeResult")


class HealthService:
    def __init__(self):
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.latest = None
        self.sampled = None

    def start(self):
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return
            self.stop.clear()
            self.latest, self.sampled = None, None
            self.thread = threading.Thread(target=self._run, name="health-probes", daemon=True)
            self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=12)

    def collect(self, clients):
        started = time.monotonic()
        dependencies = {"database": measured(probe_database)}
        dependencies["database_pool"] = pool_snapshot()
        dependencies["redis"] = {
            role: measured(lambda: probe_redis(client)) for role, client in clients.items()
        }
        try:
            collector = probe_collector()
        except Exception as exc:
            collector = {"status": "unavailable", "loop_responsive": False,
                         "error_type": type(exc).__name__}
        history = health_events.events.snapshot()
        endpoints = history["upstream_endpoints"]
        failing = [name for name, item in endpoints.items() if item["last_result"] == "failed"]
        healthy = (
            dependencies["database"]["status"] == "ok"
            and dependencies["database_pool"]["status"] == "ok"
            and all(item["status"] == "ok" for item in dependencies["redis"].values())
            and collector["status"] == "ok"
            and collector.get("http_pool", {}).get("status") == "ok"
            and not failing
        )
        return {
            "status": "ok" if healthy else "degraded", "ok": healthy,
            "checked_at": health_events.timestamp(time.time()),
            "sample_duration_ms": round((time.monotonic() - started) * 1000, 3),
            "dependencies": dependencies, "collector": collector,
            "upstream_api": {
                "status": "degraded" if failing else "ok" if endpoints else "unknown",
                "failing_endpoints": failing,
            },
            "last_hour": history,
        }

    def _run(self):
        clients = {}
        try:
            try:
                clients = redis_clients()
            except Exception as exc:
                with self.lock:
                    self.latest = {"status": "unavailable", "ok": False,
                                   "error_type": type(exc).__name__}
                    self.sampled = time.monotonic()
                return
            while not self.stop.is_set():
                try:
                    snapshot = self.collect(clients)
                except Exception as exc:
                    snapshot = {"status": "unavailable", "ok": False,
                                "error_type": type(exc).__name__}
                with self.lock:
                    self.latest, self.sampled = snapshot, time.monotonic()
                self.stop.wait(SAMPLE_INTERVAL)
        finally:
            for client in clients.values():
                client.close()

    def snapshot(self, cache=None):
        with self.lock:
            result = dict(self.latest or {"status": "starting", "ok": False})
            age = time.monotonic() - self.sampled if self.sampled is not None else None
        result.update(snapshot_age_seconds=round(age, 3) if age is not None else None,
                      sample_interval_seconds=SAMPLE_INTERVAL, stale_after_seconds=STALE_SECONDS)
        if age is not None and age > STALE_SECONDS:
            result.update(status="stale", ok=False)
        if config.API_CACHE_ENABLED:
            cache_ok = cache is not None and bool(cache.metrics.get("ready"))
            cache_age = max(0, time.monotonic() - cache.last_hot) if cache is not None else None
            snapshot_age = cache_age + cache.metrics.get("snapshot_age_seconds", 0) if cache is not None else None
            cache_ok = cache_ok and snapshot_age is not None and snapshot_age <= config.API_CACHE_MAX_AGE_SECONDS
            result["api_cache"] = {
                "status": "ok" if cache_ok else "unavailable",
                "last_refresh_age_seconds": round(cache_age, 3) if cache_age is not None and cache_age != float("inf") else None,
                "snapshot_age_seconds": round(snapshot_age, 3) if snapshot_age is not None and snapshot_age != float("inf") else None,
            }
            if not cache_ok and result["status"] == "ok":
                result.update(status="degraded", ok=False)
        else:
            result["api_cache"] = {"status": "disabled"}
        return result


service = HealthService()
