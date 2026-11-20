import json
from unittest.mock import AsyncMock

import anyio
import pytest

from app import bilibili_gateway, bootstrap, monitoring_jobs, runtime_state


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
        def get(self, *args, **kwargs):
            return Response()

    class Client:
        def __init__(self, room_id: int) -> None:
            self.room_id = room_id

        async def wait_connected(self) -> None:
            authenticated.append(self.room_id)

    clients: dict[int, Client] = {}

    async def start(room_id: int) -> None:
        started.append(room_id)
        clients[room_id] = Client(room_id)
        if room_id == 10:
            offline_observations.append(list(authenticated))

    async def stagger(_seconds: float) -> None:
        return None

    monkeypatch.setattr(monitoring_jobs.room_config, "get_room_ids", lambda: [10, 20, 30])
    monkeypatch.setattr(runtime_state, "ROOM_UIDS", {10: 1, 20: 2, 30: 3})
    monkeypatch.setattr(runtime_state, "ROOM_CLIENTS", clients)
    monkeypatch.setattr(runtime_state, "LAST_STATUS", {10: 0, 20: 0, 30: 0})
    monkeypatch.setattr(runtime_state, "aiohttp_session", Session())
    monkeypatch.setattr(monitoring_jobs, "start_client", start)
    monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", stagger)

    # When: the real startup connection loop runs.
    anyio.run(monitoring_jobs.run_clients_loop)

    # Then: both live clients authenticate before any offline client starts.
    assert started == [20, 30, 10]
    assert offline_observations == [[20, 30]]
    assert runtime_state.LAST_STATUS == {10: 0, 20: 0, 30: 0}


def test_status_snapshot_handles_missing_rooms_and_multiple_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.live_startup import fetch_live_room_ids

    # Given: over 100 requested UIDs, and inactive/no-room entries omitted by the API.
    batch_sizes: list[int] = []

    class Response:
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
    live_rooms = anyio.run(fetch_live_room_ids)

    # Then: missing inactive entries do not block startup or hide later live rooms.
    assert live_rooms == {1, 101, 125}
    assert batch_sizes == [100, 25]


def test_startup_waits_for_uid_and_all_auth_replies_before_refreshing_attention(monkeypatch, tmp_path):
    cache = tmp_path / "psp_rooms.uids.json"
    cache.write_text('{"10": 100, "20": false, "99": 990}', encoding="utf-8")
    monkeypatch.setattr(runtime_state, "ROOMS_JSON_PATH", str(tmp_path / "psp_rooms.json"))
    monkeypatch.setattr(runtime_state, "ROOMS_CONNECTING", True)
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

        async def fetch(room_id):
            requested.append(room_id)
            if requested == [20]:
                return None
            return {"uid": 200 if room_id == 20 else 999, "attention": room_id * 100}

        def save(room_id, *, attention):
            saved.append((room_id, attention))
            if room_id == 20:
                refresh_finished.set()

        async def status():
            assert runtime_state.ROOM_UIDS == {10: 100, 20: 200}
            return {10}

        class Client:
            def __init__(self, room_id):
                self.room_id = room_id

            async def wait_connected(self):
                if self.room_id == 20:
                    offline_waiting.set()
                    await offline_authenticated.wait()

        async def start(room_id):
            started.append(room_id)
            runtime_state.ROOM_CLIENTS[room_id] = Client(room_id)

        async def sleep(_seconds):
            await anyio.lowlevel.checkpoint()

        monkeypatch.setattr(bilibili_gateway, "_fetch_room_info", fetch)
        monkeypatch.setattr(bilibili_gateway.RoomInfo, "upsert", save)
        monkeypatch.setattr(bilibili_gateway, "fetch_guard_counts", AsyncMock(return_value=None))
        monkeypatch.setattr(bilibili_gateway, "fetch_fans_count", AsyncMock(return_value=None))
        monkeypatch.setattr(monitoring_jobs, "_read_room_attention", lambda _room_id: 123)
        monkeypatch.setattr(monitoring_jobs, "fetch_live_room_ids", status)
        monkeypatch.setattr(monitoring_jobs, "start_client", start)
        monkeypatch.setattr(monitoring_jobs.asyncio, "sleep", sleep)

        monitoring_jobs.load_cached_room_uids()
        await monitoring_jobs.init_uids_and_attention_once(max_rounds=1)
        assert runtime_state.ROOM_UIDS == {10: 100}
        assert started == []

        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(bootstrap._run_clients_and_attention)
                await offline_waiting.wait()
                assert started == [10, 20]
                assert requested == [20, 20]
                assert saved == []
                assert json.loads(cache.read_text(encoding="utf-8")) == {"10": 100, "20": 200, "99": 990}

                assert not await bilibili_gateway.fetch_room_info_and_update(10, update_uid=False)
                snapshot = runtime_state.SessionEndSnapshot(10, 1, monitoring_jobs._now())
                await monitoring_jobs.capture_end_snapshot(snapshot)
                assert snapshot.attention == 123
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
    monkeypatch.setattr(runtime_state, "ROOMS_CONNECTING", True)
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
