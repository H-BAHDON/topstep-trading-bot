"""Exercise the real user-hub parser and timeout without network access."""
import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from websockets.exceptions import ConnectionClosed

from topstep_api import ProjectX, ProjectXError, RECORD_SEPARATOR


class AccountVerificationTests(unittest.IsolatedAsyncioTestCase):
    async def verify(self, frames, timeout=1):
        socket = SimpleNamespace(send=AsyncMock(), recv=AsyncMock(side_effect=frames))
        connection = Mock()
        connection.__aenter__ = AsyncMock(return_value=socket)
        connection.__aexit__ = AsyncMock(return_value=False)
        api = object.__new__(ProjectX)
        api.token = 'mock-token'
        api.accounts = AsyncMock(return_value=[{'id': 123, 'name': 'TEST-EXPRESS'}])
        with patch('topstep_api.websockets.connect', return_value=connection):
            return await api.verify_simulated_account(123, 'TEST-EXPRESS', timeout)

    def event(self, accounts):
        return json.dumps({'type': 1, 'target': 'GatewayUserAccount', 'arguments': [accounts]}) + RECORD_SEPARATOR

    async def test_reads_exact_simulated_account_from_snapshot(self):
        expected = {'id': 123, 'name': 'TEST-EXPRESS', 'simulated': True}
        result = await self.verify(['{}' + RECORD_SEPARATOR,
                                   self.event([{'id': 999, 'name': 'OTHER'}, expected])])
        self.assertEqual(result, expected)

    async def test_rejects_non_simulated_account(self):
        with self.assertRaisesRegex(ProjectXError, 'not independently verified'):
            await self.verify(['{}', self.event([{'id': 123, 'name': 'TEST-EXPRESS', 'simulated': False}])])

    async def test_timeout_is_translated_on_python39(self):
        with self.assertRaisesRegex(ProjectXError, 'timed out'):
            await self.verify(['{}', asyncio.TimeoutError()])

    async def test_zero_deadline_times_out_before_reading_snapshot(self):
        with self.assertRaisesRegex(ProjectXError, 'timed out'):
            await self.verify(['{}'], timeout=0)

    async def test_server_close_message_is_translated(self):
        with self.assertRaisesRegex(ProjectXError, 'User hub closed'):
            await self.verify(['{}', json.dumps({'type': 7}) + RECORD_SEPARATOR])

    async def test_socket_close_is_translated(self):
        with self.assertRaisesRegex(ProjectXError, 'connection closed'):
            await self.verify(['{}', ConnectionClosed(None, None)])

    async def test_deadline_does_not_reset_for_unrelated_messages(self):
        socket = SimpleNamespace(send=AsyncMock())
        frames = iter(['{}', self.event([{'id': 456, 'name': 'OTHER', 'simulated': True}])])
        async def receive():
            try:
                return next(frames)
            except StopIteration:
                await asyncio.Future()
        socket.recv = receive
        connection = Mock()
        connection.__aenter__ = AsyncMock(return_value=socket)
        connection.__aexit__ = AsyncMock(return_value=False)
        api = object.__new__(ProjectX)
        api.token = 'mock-token'
        api.accounts = AsyncMock(return_value=[{'id': 123, 'name': 'TEST-EXPRESS'}])
        with patch('topstep_api.websockets.connect', return_value=connection):
            with self.assertRaisesRegex(ProjectXError, 'timed out'):
                await asyncio.wait_for(api.verify_simulated_account(123, 'TEST-EXPRESS', .01), 1)

    async def test_fresh_rest_simulation_flag_verifies_without_hub(self):
        expected = {'id': 123, 'name': 'TEST-EXPRESS', 'simulated': True}
        api = object.__new__(ProjectX)
        api.token = 'mock-token'
        api.accounts = AsyncMock(return_value=[expected])
        with patch('topstep_api.websockets.connect') as connect:
            self.assertEqual(await api.verify_simulated_account(123, 'TEST-EXPRESS'), expected)
            connect.assert_not_called()
        api.accounts.assert_awaited_once()

    async def test_rest_rejects_false_or_non_boolean_simulation_flag(self):
        for value in (False, 'true', 1, None):
            api = object.__new__(ProjectX)
            api.token = 'mock-token'
            api.accounts = AsyncMock(return_value=[{'id': 123, 'name': 'TEST-EXPRESS', 'simulated': value}])
            with self.subTest(value=value), patch('topstep_api.websockets.connect') as connect:
                with self.assertRaises(ProjectXError):
                    await api.verify_simulated_account(123, 'TEST-EXPRESS')
                connect.assert_not_called()

    async def test_rest_rejects_missing_wrong_or_ambiguous_identity(self):
        valid = {'id': 123, 'name': 'TEST-EXPRESS', 'simulated': True}
        for rows in ([], [{**valid, 'name': 'OTHER'}], [valid, valid]):
            api = object.__new__(ProjectX)
            api.token = 'mock-token'
            api.accounts = AsyncMock(return_value=rows)
            with self.subTest(rows=rows), self.assertRaises(ProjectXError):
                await api.verify_simulated_account(123, 'TEST-EXPRESS')
