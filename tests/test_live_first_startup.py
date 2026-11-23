import asyncio
import json
from unittest.mock import AsyncMock

import anyio
import pytest

from app import bilibili_gateway, bootstrap, metrics_runtime, monitoring_jobs, runtime_state
from app.live_startup import status_batches
from app.blivedm.clients.ws_base import InitError


@pytest.fixture(autouse=True)
def isolate_startup(monkeypatch):
    for name in ("LAST_STATUS", "CURRENT_SESSIONS", "STREAM_STARTS", "LIVE_INFO", "CONCURRENCY_CACHE",
                 "PENDING_SESSION_ENDS", "LOCKED_ROOM_UNTIL", "FORCED_OFFLINE_AT"):
        monkeypatch.setattr(runtime_state, name, {})
    monkeypatch.setattr(runtime_state, "ROOMS_AWAITING_FIRST_AUTH", set())
    monkeypatch.setattr(runtime_state, "STARTUP_ATTENTION_READY", asyncio.Event())
    monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", asyncio.Queue())
    monkeypatch.setattr(runtime_state, "GUARD_FANS_QUEUE", asyncio.Queue())
    monkeypatch.setattr(metrics_runtime, "_buckets", {})
    monkeypatch.setattr(monitoring_jobs.LiveSession, "find_open_session", lambda room, **kwargs: None)
    monkeypatch.setattr(monitoring_jobs.LiveSession, "start_session", lambda room, *_: room)
    monkeypatch.setattr(runtime_state, "aiohttp_session", object())


@pytest.mark.parametrize("offline_status", [0, 2])
def test_offline_rooms_start_only_after_all_live_rooms_authenticate(monkeypatch: pytest.MonkeyPatch, offline_status: int) -> None:
    # Given: numeric room order puts an offline room before two live rooms.
    started: list[int] = []
    authenticated: list[int] = []
    offline_observations: list[list[int]] = []

    class Response:
        status = 200

        def raise_for_status(self) -> None:
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def json(self, **kwargs):
            return {"code": 0, "data": {"1": {"live_status": offline_status}, "2": {"live_status": 1}, "3": {"live_status": 1}}}

    class Session:
        calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            return Response()

    class Client:
        def __init__(self, room_id: int) -> None:
            self.room_id = room_id

        async def wait_connected(self) -> None:
            authenticated.append(self.room_id)

    clients: dict[int, Client] = {}

    async def start(room_id: int) -> None:
        if room_id in (20, 30):
            assert runtime_state.LAST_STATUS[room_id] == 1
            assert runtime_state.CURRENT_SESSIONS[room_id] == room_id
        started.append(room_id)
        clients[room_id] = Client(room_id)
        if room_id == 10:
            offline_observations.append(list(authenticated))

    async def stagger(_seconds: float) -> None:
        if _seconds and asyncio.current_task().get_coro().__name__ == "monitor_all_rooms_status":
            await asyncio.Event().wait()
        await anyio.lowlevel.checkpoint()

    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [10, 20, 30])
    monkeypatch.setattr(runtime_state, "ROOM_UIDS", {10: 1, 20: 2, 30: 3})
    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", clients)
    monkeypatch.setattr(runtime_state, "LAST_STATUS", {10: 0, 20: 0, 30: 0})
    monkeypatch.setattr(runtime_state, "aiohttp_session", Session())
    monkeypatch.setattr(monitoring_jobs, "start_client", start)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stagger)

    # When: the real startup connection loop runs.
    async def run():
        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(monitoring_jobs.run_clients_loop)
                await runtime_state.STARTUP_ATTENTION_READY.wait()
                tasks.cancel_scope.cancel()

    anyio.run(run)

    # Then: both live clients authenticate before any offline client starts.
    assert started == [20, 30, 10]
    assert offline_observations == [[20, 30]]
    assert runtime_state.LAST_STATUS == {10: 0, 20: 1, 30: 1}
    assert runtime_state.aiohttp_session.calls == 1


def test_status_snapshot_handles_missing_rooms_and_multiple_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: over 100 requested UIDs, and inactive/no-room entries omitted by the API.
    batch_sizes: list[int] = []

    class Response:
        status = 200

        def __init__(self, uids: list[int]) -> None:
            self.uids = uids

        def raise_for_status(self) -> None:
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def json(self, **kwargs):
            return {"code": 0, "data": {str(uid): {"live_status": 1} for uid in self.uids[:100] if uid in {1, 101, 125}}}

    class Session:
        def get(self, url, *, params, **kwargs):
            uids = [int(value) for _key, value in params]
            batch_sizes.append(len(uids))
            return Response(uids)

    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: list(range(1, 126)))
    monkeypatch.setattr(runtime_state, "ROOM_UIDS", {room: room for room in range(1, 126)})
    monkeypatch.setattr(runtime_state, "aiohttp_session", Session())

    # When: the real startup snapshot requests the full room list.
    async def collect():
        return {room async for batch in status_batches() for room, (_, info) in batch.items() if info}

    live_rooms = anyio.run(collect)

    # Then: missing inactive entries do not block startup or hide later live rooms.
    assert live_rooms == {1, 101, 125}
    assert batch_sizes == [100, 25]


def test_startup_waits_for_uid_and_all_auth_replies_before_refreshing_attention(monkeypatch, tmp_path):
    cache = tmp_path / "psp_rooms.uids.json"
    cache.write_text('{"10": 100, "20": false, "99": 990}', encoding="utf-8")
    monkeypatch.setattr(runtime_state, "ROOMS_JSON_PATH", str(tmp_path / "psp_rooms.json"))
    monkeypatch.setattr(runtime_state, "STARTUP_ATTENTION_READY", asyncio.Event())
    monkeypatch.setattr(runtime_state, "ROOM_UIDS", {})
    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {})
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [10, 20])

    async def scenario():
        offline_waiting = anyio.Event()
        offline_authenticated = anyio.Event()
        refresh_finished = anyio.Event()
        requested = []
        saved = []
        started = []

        cached_started = asyncio.Event()

        async def fetch(room_id):
            requested.append(room_id)
            if requested == [20]:
                return None
            if room_id == 20:
                await cached_started.wait()
            return {"uid": 200 if room_id == 20 else 999, "attention": room_id * 100}

        def save(room_id, *, attention):
            saved.append((room_id, attention))
            if room_id == 20:
                refresh_finished.set()

        async def status():
            yield {room: (uid, {"live_status": int(room == 10)}) for room, uid in runtime_state.ROOM_UIDS.items()}

        class Client:
            def __init__(self, room_id):
                self.room_id = room_id

            async def wait_connected(self):
                if self.room_id == 20:
                    offline_waiting.set()
                    await offline_authenticated.wait()

        async def start(room_id):
            started.append(room_id)
            if room_id == 10:
                cached_started.set()
            runtime_state.ROOM_CLIENTS[room_id] = Client(room_id)

        async def sleep(_seconds):
            await anyio.lowlevel.checkpoint()

        monkeypatch.setattr(bilibili_gateway, "_fetch_room_info", fetch)
        monkeypatch.setattr(bilibili_gateway.RoomInfo, "upsert", save)
        monkeypatch.setattr(bilibili_gateway, "fetch_guard_counts", AsyncMock(return_value=None))
        monkeypatch.setattr(bilibili_gateway, "fetch_fans_count", AsyncMock(return_value=None))
        monkeypatch.setattr(monitoring_jobs, "status_batches", status)
        monkeypatch.setattr(monitoring_jobs, "start_client", start)
        monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", sleep)

        # The cached live room was already on air before the process restart.
        monkeypatch.setattr(monitoring_jobs.LiveSession, "find_open_session", lambda room, **kw: (1, monitoring_jobs._now()))
        monkeypatch.setattr(monitoring_jobs, "restore_session", lambda *_: None)
        monitoring_jobs.load_cached_room_uids()
        await monitoring_jobs.init_uids_and_attention_once(max_rounds=1)
        assert runtime_state.ROOM_UIDS == {10: 100}
        assert started == []

        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(bootstrap._run_clients_and_attention, runtime_state.STARTUP_ATTENTION_READY)
                await offline_waiting.wait()
                assert started == [10, 20]
                assert requested == [20, 20]
                assert saved == []
                assert json.loads(cache.read_text(encoding="utf-8")) == {"10": 100, "20": 200, "99": 990}

                assert not await bilibili_gateway.fetch_room_info_and_update(10, update_uid=False)
                snapshot = runtime_state.SessionEndSnapshot(10, 1, monitoring_jobs._now())
                await monitoring_jobs.capture_end_snapshot(snapshot)
                assert snapshot.attention is None
                assert requested == [20, 20]

                offline_authenticated.set()
                await refresh_finished.wait()
                assert requested == [20, 20, 10, 20]
                assert saved == [(10, 1000), (20, 2000)]
                assert runtime_state.ROOM_UIDS == {10: 100, 20: 200}
                tasks.cancel_scope.cancel()

    anyio.run(scenario)


def test_first_uid_lookup_creates_cache_and_next_startup_reuses_it(monkeypatch, tmp_path):
    cache = tmp_path / "rooms.uids.json"
    monkeypatch.setattr(runtime_state, "ROOMS_JSON_PATH", str(tmp_path / "rooms.json"))
    monkeypatch.setattr(runtime_state, "STARTUP_ATTENTION_READY", asyncio.Event())
    monkeypatch.setattr(runtime_state, "ROOM_UIDS", {})
    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [10])
    fetch = AsyncMock(side_effect=[{"uid": -1}, {"uid": "100"}])
    monkeypatch.setattr(bilibili_gateway, "_fetch_room_info", fetch)

    async def scenario():
        await monitoring_jobs.init_uids_and_attention_once(max_rounds=1)
        assert not cache.exists()
        await monitoring_jobs.init_uid_and_attention_for_room(10)
        assert json.loads(cache.read_text(encoding="utf-8")) == {"10": 100}
        runtime_state.ROOM_UIDS.clear()
        monitoring_jobs.load_cached_room_uids()
        await monitoring_jobs.init_uids_and_attention_once()
        assert runtime_state.ROOM_UIDS == {10: 100}
        assert fetch.await_count == 2

    anyio.run(scenario)


def test_startup_timeout_releases_healthy_rooms_and_keeps_retrying(monkeypatch):
    async def scenario():
        ready = asyncio.Event()
        fail_first_attempt = asyncio.Event()
        retry_started = asyncio.Event()
        retry_authenticated = asyncio.Event()
        started = []
        requested = []
        start_snapshots = []
        daily_snapshots = []
        attention_queue = asyncio.Queue()
        daily_queue = asyncio.Queue()
        today = monitoring_jobs._now().date()
        for room_id in (10, 20, 30, 40):
            attention_queue.put_nowait((room_id, room_id, "start", today))
            daily_queue.put_nowait((room_id, today))

        class Client:
            def __init__(self, room_id, first_attempt):
                self.room_id = room_id
                self.first_attempt = first_attempt

            async def wait_connected(self):
                if self.room_id == 10 and self.first_attempt:
                    await fail_first_attempt.wait()
                    raise InitError("synthetic initialization failure")
                if self.room_id == 10:
                    retry_started.set()
                    await retry_authenticated.wait()

        async def start(room_id):
            runtime_state.ROOM_CLIENTS[room_id] = Client(room_id, room_id not in started)
            started.append(room_id)

        async def fetch(room_id):
            requested.append(room_id)
            return {10: {"attention": 17}, 20: {"attention": 123}, 30: {"attention": 0}, 40: {}}[room_id]

        async def stagger(_seconds):
            await anyio.lowlevel.checkpoint()

        async def refresh():
            for room_id in (10, 20):
                await bilibili_gateway.fetch_room_info_and_update(room_id, update_uid=False)
            await asyncio.Event().wait()

        monkeypatch.setattr(runtime_state, "ROOMS_AWAITING_FIRST_AUTH", set())
        monkeypatch.setattr(runtime_state, "STARTUP_ATTENTION_READY", ready)
        monkeypatch.setattr(runtime_state, "ROOM_UIDS", {rid: rid for rid in (10, 20, 30, 40)})
        monkeypatch.setattr(runtime_state, "CURRENT_SESSIONS", {rid: rid for rid in (10, 20, 30, 40)})
        monkeypatch.setattr(runtime_state, "LAST_STATUS", {10: 1, 20: 1})
        monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {})
        monkeypatch.setattr(runtime_state, "ATTENTION_QUEUE", attention_queue)
        monkeypatch.setattr(runtime_state, "DAILY_ATTENTION_QUEUE", daily_queue)
        monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [10, 20, 30, 40])
        async def statuses():
            yield {rid: (rid, {"live_status": int(rid in (10, 20))}) for rid in (10, 20, 30, 40)}

        monkeypatch.setattr(monitoring_jobs, "status_batches", statuses)
        monkeypatch.setattr(monitoring_jobs, "STARTUP_AUTH_WAIT_SECONDS", 0.05)
        monkeypatch.setattr(monitoring_jobs, "start_client", start)
        monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stagger)
        monkeypatch.setattr(monitoring_jobs, "refresh_attention_scheduler", refresh)
        monkeypatch.setattr(bilibili_gateway, "_fetch_room_info", fetch)
        monkeypatch.setattr(bilibili_gateway.RoomInfo, "upsert", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(monitoring_jobs.LiveSession, "update_start_attention", lambda *args: start_snapshots.append(args))
        monkeypatch.setattr(monitoring_jobs.Attention, "upsert_daily", lambda *args: daily_snapshots.append(args))

        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(bootstrap._run_clients_and_attention, ready)
                await anyio.lowlevel.checkpoint()
                assert not ready.is_set()
                assert requested == []

                await attention_queue.join()
                await daily_queue.join()
                assert started == [10, 20, 30, 40]
                assert start_snapshots == [(20, 123), (30, 0)]
                assert daily_snapshots == [(20, today, 123), (30, today, 0)]
                assert 10 not in requested

                fail_first_attempt.set()
                await retry_started.wait()
                assert started == [10, 20, 30, 40, 10]
                assert await bilibili_gateway.fetch_room_attention(10) is None
                retry_authenticated.set()
                await anyio.lowlevel.checkpoint()
                assert await bilibili_gateway.fetch_room_attention(10) == 17
                tasks.cancel_scope.cancel()

    anyio.run(scenario)


def test_newly_live_room_takes_next_slot_while_status_batch_is_retried(monkeypatch):
    async def scenario():
        first_started = asyncio.Event()
        reprioritized = asyncio.Event()
        ready = runtime_state.STARTUP_ATTENTION_READY
        started = []
        attempts = []
        # Room 101 belongs to another HTTP batch. All other rooms omitted by
        # a successful response remain eligible as ordinary offline candidates.
        rooms = list(range(1, 102))
        monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: rooms)
        monkeypatch.setattr(runtime_state, "ROOM_UIDS", {r: r for r in rooms})
        monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", {})

        class Response:
            def __init__(self, uids):
                self.uids = uids
                self.status = 503 if 101 in uids and 101 not in attempts else 200
                attempts.append(uids[0])

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def json(self, **kwargs):
                if self.uids == [101]:
                    return {"data": {"101": {"live_status": 1}}}
                return {"data": {"1": {"live_status": 1}, "99": {"live_status": int(first_started.is_set())}}}

        class Session:
            def get(self, url, *, params, **kwargs):
                return Response([int(uid) for _, uid in params])

        class Client:
            async def wait_connected(self):
                pass

        async def start(room):
            started.append(room)
            runtime_state.ROOM_CLIENTS[room] = Client()
            if room == 1:
                first_started.set()
            if room == 101:
                reprioritized.set()

        async def sleep(seconds):
            if asyncio.current_task().get_coro().__name__ == "monitor_all_rooms_status":
                if seconds:
                    await first_started.wait()
            elif started == [1] and attempts.count(101) < 2:
                while attempts.count(101) < 2:
                    await anyio.lowlevel.checkpoint()
            await anyio.lowlevel.checkpoint()

        monkeypatch.setattr(runtime_state, "aiohttp_session", Session())
        monkeypatch.setattr(monitoring_jobs, "start_client", start)
        monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", sleep)
        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(monitoring_jobs.run_clients_loop)
                await reprioritized.wait()
                await ready.wait()
                tasks.cancel_scope.cancel()
        assert started[:4] == [1, 99, 101, 2]
        assert len(set(started)) == len(started) == 101
        assert attempts[:4] == [1, 101, 1, 101]

    anyio.run(scenario)
