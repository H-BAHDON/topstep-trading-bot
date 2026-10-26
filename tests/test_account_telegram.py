from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from telegram_monitor import AccountTelegramMonitor, BrokerPnlSnapshot, PauseStateStore, TelegramConfig
from test_multi_account import manager
from test_execution import position, signal_result, stop


class AccountControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.store = PauseStateStore(self.root / 'controls.db')
        self.first, self.second = manager(), manager(2)
        self.first.account['_displayName'] = 'Express One'
        self.second.account['_displayName'] = 'Express Two'
        self.peers = []
        for candidate in (self.first, self.second):
            monitor = AccountTelegramMonitor(
                TelegramConfig('test-token', '123'), candidate, Mock(),
                self.root / f"trades_{candidate.account['id']}.json", client=Mock(),
            )
            monitor.send_to = AsyncMock(return_value=True)
            monitor.rate_limiter = Mock()
            monitor.rate_limiter.allow.return_value = True
            monitor.pause_store = self.store
            self.peers.append(monitor)
        for peer in self.peers:
            peer.peers = self.peers
        self.primary = self.peers[0]

    async def command(self, text, owner=123, chat_type='private'):
        await self.primary.process_update({
            'update_id': 10,
            'message': {'text': text, 'chat': {'id': owner, 'type': chat_type}},
        })

    async def test_controls_require_owner_and_private_chat(self):
        for command in ('/accounts', '/pause 2', '/resume 2', '/close 2', '/contracts 2 8', '/settings 2'):
            await self.command(command, owner=456)
        await self.command('/pause 2', chat_type='group')
        self.assertTrue(self.second.entries_enabled)
        self.assertEqual(self.second.config.contracts, 1)
        self.assertFalse(self.store.is_paused(2))
        self.assertFalse(self.second.api.close_calls)
        self.primary.send_to.assert_any_await('456', 'Not authorized')

    async def test_accounts_and_targeted_status_show_paused_state(self):
        await self.command('/pause Express Two')
        await self.command('/accounts')
        reply = self.primary.send_to.call_args.args[1]
        self.assertIn('Express One', reply)
        self.assertIn('Express Two', reply)
        self.assertIn('PAUSED', reply)
        await self.command('/status 2')
        self.assertIn('New entries: PAUSED', self.peers[1].send_to.call_args.args[1])

    async def test_pause_is_targeted_and_survives_reopening_store(self):
        await self.command('/pause Express Two')
        self.assertTrue(self.first.entries_enabled)
        self.assertFalse(self.second.entries_enabled)
        reopened = PauseStateStore(self.root / 'controls.db')
        self.assertTrue(reopened.is_paused(2))
        self.assertFalse(reopened.is_paused(self.first.account['id']))
        self.assertFalse(self.second.api.close_calls)
        await self.command('/resume 2')
        self.assertTrue(self.second.entries_enabled)
        self.assertFalse(reopened.is_paused(2))

    async def test_unknown_ambiguous_or_missing_target_has_no_side_effect(self):
        for command in ('/pause', '/close unknown', '/pause all', '/pause ' + self.first.account['name']):
            await self.command(command)
        self.assertTrue(self.first.entries_enabled)
        self.assertTrue(self.second.entries_enabled)
        self.assertFalse(self.first.api.close_calls)
        self.assertFalse(self.second.api.close_calls)

    async def test_telegram_bot_suffix_and_case_insensitive_names(self):
        await self.command('/pause@OurBot eXpReSs TwO')
        self.assertFalse(self.second.entries_enabled)
        self.assertTrue(self.first.entries_enabled)

    async def test_setting_changes_only_target_account_and_future_trades(self):
        self.second.active_trail_ticks = 4
        await self.command('/contracts Express Two 3')
        await self.command('/trail_activation 2 5')
        await self.command('/trail_distance 2 2')
        self.assertEqual(self.first.config.contracts, 1)
        self.assertEqual(self.second.config.contracts, 3)
        self.assertEqual(self.second.config.trail_activation_points, Decimal(5))
        self.assertEqual(self.second.config.trail_distance_ticks, 8)
        self.assertEqual(self.second.active_trail_ticks, 4)
        await self.command('/contracts 8')
        await self.command('/contracts 2 0')
        self.assertEqual(self.second.config.contracts, 3)

    async def test_resume_cannot_override_config_disabled_or_halted_accounts(self):
        for condition in ('configured_disabled', 'halted', 'daily_limit', 'orphan_order', 'unprotected'):
            with self.subTest(condition=condition):
                candidate = manager(2)
                candidate.entries_enabled = False
                candidate.runtime_paused = True
                self.peers[1].execution = candidate
                self.store.set_paused(2, True)
                if condition == 'configured_disabled':
                    candidate.configured_entries_enabled = False
                elif condition == 'halted':
                    candidate.halt('mock halt')
                elif condition == 'daily_limit':
                    candidate.api.trades_response = [{'profitAndLoss': -100}]
                elif condition == 'orphan_order':
                    candidate.api.open_orders_response = [{**stop(65, 1, 90), 'accountId': 2}]
                else:
                    candidate.api.positions_response = [position(1)]
                await self.command('/resume 2')
                self.assertFalse(candidate.entries_enabled)
                self.assertTrue(self.store.is_paused(2))

    async def test_close_pauses_target_confirms_flat_and_cleans_its_stop(self):
        candidate = self.second
        candidate.api.positions_response = [position(1)]
        candidate.api.open_orders_response = [{**stop(65, 1, 90), 'accountId': 2}]
        await candidate.startup_check()
        candidate.api.positions_response = []
        candidate.api.open_positions_sequence = [[position(1)], [], []]
        await self.command('/close Express Two')
        self.assertEqual(candidate.api.close_calls, [{'accountId': 2, 'contractId': candidate.contract['id']}])
        self.assertEqual(candidate.api.cancel_calls, [{'accountId': 2, 'orderId': 65}])
        self.assertFalse(candidate.entries_enabled)
        self.assertTrue(self.store.is_paused(2))
        self.assertTrue(self.first.entries_enabled)
        self.assertFalse(self.first.api.close_calls)
        self.assertIn('Confirmed flat', self.peers[1].send_to.call_args.args[1])

    async def test_uncertain_close_is_not_blindly_repeated(self):
        self.second.api.positions_response = [position(1)]
        self.second.api.close_position_once = AsyncMock(side_effect=TimeoutError('mock timeout'))
        await self.command('/close 2')
        await self.command('/close 2')
        self.second.api.close_position_once.assert_awaited_once()
        self.assertTrue(self.store.is_paused(2))
        self.assertFalse(self.second.entries_enabled)

    async def test_flat_account_with_untracked_orders_is_not_reported_clean(self):
        self.second.api.open_orders_response = [{**stop(88, 1, 90), 'accountId': 2}]
        await self.command('/close 2')
        self.assertIn('still has positions or orders', self.primary.send_to.call_args.args[1])
        self.assertFalse(self.second.api.close_calls)
        self.assertFalse(self.second.api.cancel_calls)
        self.assertFalse(self.second.entries_enabled)

    async def test_resume_store_failure_does_not_enable_entries(self):
        await self.command('/pause 2')
        self.store.set_paused = Mock(side_effect=OSError('disk unavailable'))
        await self.command('/resume 2')
        self.assertFalse(self.second.entries_enabled)

    async def test_pause_store_failure_still_blocks_entries_in_memory(self):
        self.store.set_paused = Mock(side_effect=OSError('disk unavailable'))
        await self.command('/pause 2')
        self.assertFalse(self.second.entries_enabled)
        self.assertIn('could not be saved', self.primary.send_to.call_args.args[1])

    async def test_pause_prevents_an_entry_waiting_on_execution_lock(self):
        candidate = self.second
        await candidate.lock.acquire()
        persist = lambda paused: self.store.set_paused(2, paused)
        pause = asyncio.create_task(candidate.pause_entries(persist))
        await asyncio.sleep(0)
        pending_signal = asyncio.create_task(candidate.signal(signal_result(), entry_eligible=True, market_fresh=True))
        await asyncio.sleep(0)
        candidate.lock.release()
        await pause
        self.assertEqual(await pending_signal, 'ACCOUNT_PAUSED')
        self.assertFalse(candidate.api.place_calls)

    async def test_rate_limit_prevents_mutation(self):
        self.primary.rate_limiter.allow.return_value = False
        await self.command('/pause 2')
        self.assertTrue(self.second.entries_enabled)

    async def test_combined_pnl_sums_accounts_and_refuses_partial_data(self):
        now = datetime.now(timezone.utc)
        for peer, daily, total in zip(self.peers, (10, -2), (100, 40)):
            peer.refresh_broker_pnl = AsyncMock(return_value=True)
            peer.broker_pnl = BrokerPnlSnapshot((), {}, Decimal(daily), Decimal(total), now)
        await self.command('/profit all')
        reply = self.primary.send_to.call_args.args[1]
        self.assertIn('Combined P&L', reply)
        self.assertIn('Today realised: +$8', reply)
        self.assertIn('Total realised: +$140', reply)
        self.peers[1].refresh_broker_pnl.return_value = False
        await self.command('/profit all')
        self.assertIn('Data unavailable', self.primary.send_to.call_args.args[1])

    async def test_invalid_saved_pause_state_is_rejected(self):
        with self.store.database() as connection:
            connection.execute('INSERT INTO pauses VALUES (2, 5)')
        with self.assertRaises(ValueError):
            self.store.is_paused(2)
