import aiohttp
import anyio
import pytest
from anyio.lowlevel import checkpoint

from app.blivedm.clients.ws_base import AuthError, HeaderTuple, InitError, Operation
from app.live_client import AuthenticatedLiveClient


@pytest.mark.parametrize("body", [b'{"code":0}', b'{"code":-101}'])
def test_connection_gate_opens_only_for_successful_authentication(body: bytes) -> None:
    # Given: a client with a websocket transport, but no authentication reply yet.
    async def scenario() -> None:
        async with aiohttp.ClientSession() as session:
            client = AuthenticatedLiveClient(10, session=session)
            heartbeat_packets: list[bytes] = []

            class Websocket:
                async def send_bytes(self, packet: bytes) -> None:
                    heartbeat_packets.append(packet)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(client, "_websocket", Websocket())
                header = HeaderTuple(16 + len(body), 16, 1, Operation.AUTH_REPLY, 1)
                connected = anyio.Event()
                waiting = anyio.Event()

                async def wait() -> None:
                    waiting.set()
                    await client.wait_connected()
                    connected.set()

                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(wait)
                    await waiting.wait()
                    assert not connected.is_set()

                    # When: the real protocol parser receives the auth response.
                    if body == b'{"code":0}':
                        await client._parse_business_message(header, body)
                        await connected.wait()
                    else:
                        with pytest.raises(AuthError):
                            await client._parse_business_message(header, body)
                        await checkpoint()
                        tasks.cancel_scope.cancel()

                    # Then: only an accepted auth reply releases the startup gate.
                    assert connected.is_set() == (body == b'{"code":0}')
                    assert len(heartbeat_packets) == int(connected.is_set())

    anyio.run(scenario)


def test_client_stopped_before_authentication_releases_gate_with_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.blivedm import BLiveClient

    async def stopped(self) -> None:
        return None

    monkeypatch.setattr(BLiveClient, "_network_coroutine_wrapper", stopped)

    # Given: the underlying network worker terminates before authenticating.
    async def scenario() -> None:
        async with aiohttp.ClientSession() as session:
            client = AuthenticatedLiveClient(10, session=session)

            # When: the real readiness wrapper observes worker termination.
            await client._network_coroutine_wrapper()

            # Then: startup can retry instead of waiting forever on a dead worker.
            with pytest.raises(InitError):
                await client.wait_connected()

    anyio.run(scenario)
