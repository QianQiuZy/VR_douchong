import anyio
import pytest

from app import monitoring_jobs, runtime_state


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
