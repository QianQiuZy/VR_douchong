"""Deterministic multi-room admission, circuit and credential-safe diagnostics."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import aiohttp

from app.blivedm.clients.danmaku import DanmakuRequestPolicy, get_request_policy
from app.blivedm.clients.web import BLiveClient, _log_failure


class Clock:
    def __init__(self, policy):
        self.time = 0.0
        self.waits = []
        policy._now = lambda: self.time
        policy._sleep = self.sleep

    async def sleep(self, delay):
        self.waits.append(delay)
        self.time += delay
        await asyncio.sleep(0)


class Response:
    status = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self):
        await asyncio.sleep(0)
        return self.payload


class DanmakuPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_wbi_refresh_does_not_shorten_http_start_spacing(self):
        async with aiohttp.ClientSession() as session:
            clients = [BLiveClient(room, uid=0, session=session) for room in [10, 20]]
            clock = Clock(clients[0]._danmaku_policy)
            starts = []

            def get(url, **kwargs):
                if url.endswith('/nav'):
                    clock.time += 10
                    return Response({'data': {'wbi_img': {
                        'img_url': 'https://example.invalid/' + 'a' * 32 + '.png',
                        'sub_url': 'https://example.invalid/' + 'b' * 32 + '.png',
                    }}})
                starts.append(clock.time)
                return Response({'code': 0, 'data': {
                    'host_list': [{'host': 'example.invalid', 'wss_port': 443}],
                    'token': 'synthetic-token',
                }})

            for client in clients:
                client._room_id = client.tmp_room_id
            with patch.object(session, 'get', get):
                self.assertEqual(await asyncio.gather(*(c._init_host_server() for c in clients)),
                                 [True, True])
            self.assertEqual(starts, [10, 13])

    async def test_transient_failure_after_352_keeps_recovery_alive(self):
        async with aiohttp.ClientSession() as session:
            client = BLiveClient(10, uid=0, session=session)
            client._room_id = 10
            Clock(client._danmaku_policy)
            attempts = []

            def get(url, **kwargs):
                if url.endswith('/nav'):
                    return Response({'data': {'wbi_img': {
                        'img_url': 'https://example.invalid/' + 'a' * 32 + '.png',
                        'sub_url': 'https://example.invalid/' + 'b' * 32 + '.png',
                    }}})
                attempts.append(url)
                if len(attempts) == 1:
                    return Response({'code': -352})
                if len(attempts) == 2:
                    raise asyncio.TimeoutError()
                return Response({'code': 0, 'data': {
                    'host_list': [{'host': 'example.invalid', 'wss_port': 443}],
                    'token': 'synthetic-token',
                }})

            original_sleep = asyncio.sleep

            async def sleep(delay):
                await original_sleep(0)

            with patch.object(session, 'get', get), patch.object(asyncio, 'sleep', sleep):
                self.assertTrue(await client._init_host_server())
            self.assertEqual(len(attempts), 3)

    async def test_all_sessions_share_three_second_http_admission(self):
        async with aiohttp.ClientSession() as first, aiohttp.ClientSession() as second:
            clients = [BLiveClient(10, uid=0, session=first),
                       BLiveClient(20, uid=0, session=second),
                       BLiveClient(30, uid=0, session=first)]
            policy = clients[0]._danmaku_policy
            self.assertIs(policy, get_request_policy())
            self.assertTrue(all(c._danmaku_policy is policy for c in clients))
            clock = Clock(policy)
            starts = []

            def get(url, **kwargs):
                starts.append(clock.time)
                return Response({'code': 0, 'data': {
                    'host_list': [{'host': 'example.invalid', 'wss_port': 443}],
                    'token': 'synthetic-token',
                }})

            for client in clients:
                client._room_id = client.tmp_room_id
                client._wbi_signer._wbi_key = 'synthetic-wbi-key'
            with patch.object(first, 'get', get), patch.object(second, 'get', get):
                self.assertEqual(await asyncio.gather(*(c._init_host_server() for c in clients)),
                                 [True, True, True])
            self.assertEqual(starts, [0, 3, 6])

    async def test_three_rooms_trip_circuit_and_only_one_probe_is_admitted(self):
        policy = DanmakuRequestPolicy()
        clock = Clock(policy)
        for room in [10, 20, 30]:
            async with policy.request():
                policy.started()
                policy.rejected(room)
        self.assertEqual(policy._open_until, 66)
        starts = []

        async def probe(fail):
            async with policy.request():
                starts.append(clock.time)
                policy.started()
                await asyncio.sleep(0)
                if fail:
                    policy.other_failure()
                else:
                    policy.success()

        await asyncio.gather(probe(True), probe(False), probe(False))
        self.assertEqual(starts, [66, 126, 129])
        self.assertEqual(policy._open_until, 0)

    async def test_old_failures_expire_and_success_resets_episode(self):
        policy = DanmakuRequestPolicy()
        clock = Clock(policy)
        policy.rejected(10)
        clock.time = 31
        policy.rejected(20)
        policy.rejected(30)
        self.assertEqual(policy._open_until, 0)
        policy.success()
        policy.rejected(40)
        self.assertEqual(set(policy._failures), {40})

    async def test_refreshed_signature_or_gaia_trips_without_waiting_for_three_rooms(self):
        for reason in ['refreshed', 'gaia']:
            with self.subTest(reason=reason):
                policy = DanmakuRequestPolicy()
                Clock(policy)
                policy.rejected(10, **{reason: True})
                self.assertEqual(policy._open_until, 60)

    async def test_cancelling_gate_wait_releases_lock_and_other_endpoints_still_work(self):
        async with aiohttp.ClientSession() as session:
            client = BLiveClient(10, uid=0, session=session)
            client._room_id = 10
            policy = client._danmaku_policy
            clock = Clock(policy)
            policy.rejected(10, gaia=True)
            waiting = asyncio.Event()

            async def wait(delay):
                waiting.set()
                await asyncio.Event().wait()

            policy._sleep = wait
            task = asyncio.create_task(client._init_host_server())
            try:
                await asyncio.wait_for(waiting.wait(), 1)
                with patch.object(session, 'get', return_value=Response({
                    'code': 0, 'data': {'room_id': 10, 'uid': 20},
                })):
                    self.assertTrue(await client._init_room_id_and_owner())
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            policy._sleep = clock.sleep
            async with policy.request():
                policy.started()
                policy.success()
            self.assertEqual(clock.time, 60)

    async def test_wbi_refresh_is_once_per_episode_and_rejects_stale_failures(self):
        async with aiohttp.ClientSession() as session:
            signer = BLiveClient(10, uid=0, session=session)._wbi_signer
            signer._wbi_key = 'first-key'
            signer.reject(0)
            self.assertEqual(signer.wbi_key, '')
            signer._wbi_key = 'refreshed-key'
            signer.generation = 1
            signer.reject(0)
            signer.reject(1)
            self.assertEqual(signer.wbi_key, 'refreshed-key')
            signer.accepted(1)
            signer.reject(1)
            self.assertEqual(signer.wbi_key, '')

    async def test_diagnostics_log_only_status_code_and_voucher_presence(self):
        response = SimpleNamespace(status=200, headers={'X-Bili-Gaia-Vvoucher': 'private-voucher'})
        with self.assertLogs('blivedm', level='WARNING') as captured:
            _log_failure('getDanmuInfo', response, {
                'code': -352, 'message': 'private-cookie', 'data': {'token': 'private-token'},
            }, 10)
            _log_failure('nav', response, {'code': 'private-code'})
        output = '\n'.join(captured.output)
        self.assertIn('code=-352', output)
        self.assertIn('http_status=200', output)
        self.assertIn('gaia_vvoucher=True', output)
        self.assertIn('code=invalid', output)
        self.assertNotIn('private-', output)
