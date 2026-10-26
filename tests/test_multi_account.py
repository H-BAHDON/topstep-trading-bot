from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from execution import ExecutionManager
from main import (
    AccountTelegramMonitor, account_trading_config, async_main, configured_accounts,
    dispatch_signal, load_account_configuration, load_environment, run_bot_tasks,
    verify_configured_account,
)
from telegram_monitor import TelegramMonitor
from topstep_api import ProjectXError
from test_execution import ACCOUNT, CONTRACT, FakeApi, config, position, signal_result, stop


def settings(account_id=12345678, **changes):
    value = {
        'name': f'Account {account_id}', 'account_id': account_id,
        'account_type': 'practice', 'account_name': ACCOUNT['name'], 'enabled': True,
    }
    value.update(changes)
    return value


def manager(account_id=12345678, kind='practice', **changes):
    account = {**ACCOUNT, 'id': account_id}
    return ExecutionManager(
        FakeApi(), account, CONTRACT, config(**changes),
        approved_account=(account_id, account['name'], kind),
    )


class AccountConfigurationTests(unittest.TestCase):
    def load(self, accounts):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'accounts.json'
            path.write_text(json.dumps({'accounts': accounts}))
            return load_account_configuration(path)

    def test_multiple_accounts_do_not_write_process_account_identity(self):
        with patch.dict(os.environ, {'TOPSTEP_ACCOUNT_ID': '999'}, clear=True):
            result = self.load([settings(), settings(2, account_type='express')])
            self.assertEqual(len(result), 2)
            self.assertEqual(os.environ['TOPSTEP_ACCOUNT_ID'], '999')
            self.assertNotIn('VERIFIED_PRACTICE_ACCOUNT_ID', os.environ)

    def test_existing_migrated_practice_schema_is_supported(self):
        entry = settings()
        del entry['account_name']
        del entry['account_type']
        entry.update(verified_practice_account_id=entry['account_id'],
                     verified_practice_account_name=ACCOUNT['name'],
                     practice_account_simulated_verified=True)
        result = self.load([entry])[0]
        self.assertEqual(result['account_type'], 'practice')
        self.assertEqual(result['account_name'], ACCOUNT['name'])

    def test_invalid_and_ambiguous_configuration_is_rejected(self):
        cases = [[], [settings(), settings()],
                 [settings(), settings(2, name=settings()['name'])]]
        for changes in (
            {'account_id': True}, {'account_id': 0}, {'account_id': '123'},
            {'enabled': 'false'}, {'name': ''}, {'account_name': ''},
            {'account_type': 'live'}, {'contracts': True}, {'contracts': 0},
            {'contracts': 1.5}, {'daily_loss_limit_enabled': 'false'},
            {'max_daily_loss_usd': 'NaN'}, {'max_daily_loss_usd': -1},
            {'max_daily_loss_usd': 'Infinity'}, {'max_daily_loss_usd': True},
            {'trail_activation_points': 'NaN'}, {'trail_activation_points': 0},
            {'trail_activation_points': '1.1'}, {'trail_activation_points': True},
            {'trail_distance_points': 'Infinity'}, {'trail_distance_points': -1},
            {'trail_distance_points': '1.1'}, {'trail_distance_points': False},
            {'verified_practice_account_id': 555},
            {'practice_account_simulated_verified': True},
        ):
            cases.append([settings(**changes)])
        for entries in cases:
            with self.subTest(entries=entries), self.assertRaises(ProjectXError):
                self.load(entries)

    def test_disabled_accounts_remain_available_for_position_management(self):
        entries = self.load([settings(), settings(2, enabled=False)])
        self.assertEqual([e['account_id'] for e in entries if e['enabled']], [12345678])
        self.assertEqual(len(self.load([settings(enabled=False)])), 1)

    def test_relative_file_is_resolved_against_selected_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'accounts.json').write_text(json.dumps({'accounts': [settings()]}))
            env = root / '.env'
            env.write_text('ACCOUNTS_FILE=accounts.json\n')
            with patch.dict(os.environ, {}, clear=True):
                load_environment(str(env))
                self.assertEqual(os.environ['ACCOUNTS_FILE'], str((root / 'accounts.json').resolve()))
                self.assertEqual(configured_accounts()[0]['account_id'], 12345678)

    def test_account_overrides_are_independent_and_preserve_shared_settings(self):
        base = config()
        first = account_trading_config(base, settings(
            contracts=2,
            max_daily_loss_usd='50.25',
            trail_activation_points='10',
            trail_distance_points='1.75',
        ))
        second = account_trading_config(base, settings(2, contracts=3, daily_loss_limit_enabled=False))
        self.assertEqual((first.contracts, second.contracts, base.contracts), (2, 3, 1))
        self.assertEqual(first.max_daily_loss_usd, Decimal('50.25'))
        self.assertFalse(second.daily_loss_limit_enabled)
        self.assertTrue(first.daily_loss_limit_enabled)
        self.assertEqual(first.stop_loss_points, base.stop_loss_points)
        self.assertEqual(first.trail_activation_points, Decimal('10'))
        self.assertEqual(first.trail_distance_ticks, 7)
        self.assertEqual(second.trail_activation_points, base.trail_activation_points)
        self.assertEqual(second.trail_distance_ticks, base.trail_distance_ticks)


class MultiAccountExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_account_blocks_entries_but_recovers_protected_positions(self):
        candidate = manager()
        candidate.entries_enabled = False
        candidate.api.positions_response = [position(1)]
        candidate.api.open_orders_response = [stop(65, 1, 90)]
        await candidate.startup_check()
        self.assertEqual(candidate.confirmed_stop_order_id, 65)
        status = await candidate.signal(signal_result(), entry_eligible=True, market_fresh=True)
        self.assertEqual(status, 'ACCOUNT_PAUSED')
        self.assertFalse(candidate.api.place_calls)
        candidate.monitor_once = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await candidate.monitor()
        candidate.monitor_once.assert_awaited_once()

    async def test_explicit_account_identities_are_independent_of_environment(self):
        first, second = manager(), manager(2)
        with patch.dict(os.environ, {'VERIFIED_PRACTICE_ACCOUNT_ID': '999'}, clear=True):
            self.assertTrue(first.account_eligible())
            self.assertTrue(second.account_eligible())
        second.account['id'] = first.account['id']
        self.assertFalse(second.account_eligible())

    async def test_express_requires_opt_in_and_verified_identity(self):
        express = manager(kind='express', practice_only=False)
        self.assertTrue(express.account_eligible())
        for field, value in [('id', 222), ('name', 'OTHER'), ('simulated', False),
                             ('_simulatedVerified', False), ('canTrade', False), ('isVisible', False)]:
            candidate = manager(kind='express', practice_only=False)
            candidate.account[field] = value
            self.assertFalse(candidate.account_eligible())
        self.assertFalse(manager(kind='express').account_eligible())
        self.assertFalse(manager(kind='live', practice_only=False).account_eligible())

    async def test_blocked_identity_cannot_close_or_monitor_an_account(self):
        candidate = manager(kind='express')
        candidate.api.open_positions = AsyncMock()
        result = signal_result()
        result = SimpleNamespace(signal='DATE_RANGE_EXIT')
        self.assertIn('SAFETY_BLOCKED', await candidate.signal(result, entry_eligible=True, market_fresh=True))
        await candidate.startup_check()
        await candidate.monitor()
        candidate.api.open_positions.assert_not_awaited()
        self.assertTrue(candidate.halted_reason)

    async def test_same_signal_reaches_two_accounts_with_separate_sizing(self):
        first, second = manager(contracts=1), manager(2, contracts=3)
        result = signal_result()
        statuses = await dispatch_signal([first, second], result, entry_eligible=True, market_fresh=True)
        self.assertEqual(set(statuses), {12345678, 2})
        for candidate, size in [(first, 1), (second, 3)]:
            self.assertEqual(len(candidate.api.place_calls), 1)
            payload = candidate.api.place_calls[0]
            self.assertEqual(payload['accountId'], candidate.account['id'])
            self.assertEqual(payload['size'], size)
            self.assertEqual(payload['stopLossBracket']['type'], 4)
        await dispatch_signal([first, second], result, entry_eligible=True, market_fresh=True)
        self.assertEqual(len(first.api.place_calls), 1)
        self.assertEqual(len(second.api.place_calls), 1)

    async def test_account_loss_limit_does_not_block_other_account(self):
        first, second = manager(), manager(2)
        first.api.trades_response = [{'accountId': first.account['id'], 'profitAndLoss': -100}]
        statuses = await dispatch_signal([first, second], signal_result(), entry_eligible=True, market_fresh=True)
        self.assertEqual(statuses[first.account['id']], 'DAILY_LOSS_BLOCKED')
        self.assertFalse(first.api.place_calls)
        self.assertEqual(len(second.api.place_calls), 1)
        self.assertIsNone(second.halted_reason)

    async def test_shared_api_keeps_account_history_and_pnl_queries_separate(self):
        api = FakeApi()
        api.open_positions = AsyncMock(return_value=[])
        api.open_orders = AsyncMock(return_value=[])
        api.orders = AsyncMock(return_value=[])
        async def trades(account_id, *args):
            return [{'accountId': account_id, 'profitAndLoss': -100 if account_id == 12345678 else 25}]
        api.trades = AsyncMock(side_effect=trades)
        first, second = manager(), manager(2)
        first.api = second.api = api
        statuses = await dispatch_signal([first, second], signal_result(), entry_eligible=True, market_fresh=True)
        self.assertEqual(statuses[first.account['id']], 'DAILY_LOSS_BLOCKED')
        self.assertEqual((first.realized_net, second.realized_net), (Decimal('-100'), Decimal('25')))
        self.assertEqual([call['accountId'] for call in api.place_calls], [2])
        self.assertEqual({call.args[0] for call in api.open_positions.await_args_list}, {12345678, 2})
        self.assertEqual({call.args[0] for call in api.trades.await_args_list}, {12345678, 2})
        self.assertEqual([call.args[0] for call in api.orders.await_args_list], [2])

    async def test_dispatch_is_concurrent_and_exceptions_are_isolated(self):
        first, second = manager(), manager(2)
        arrived = asyncio.Event()
        async def failing(*args, **kwargs):
            await asyncio.wait_for(arrived.wait(), 1)
            raise RuntimeError('mock network failure')
        async def healthy(*args, **kwargs):
            arrived.set()
            return 'ORDER_REJECTED'
        first.signal, second.signal = failing, healthy
        statuses = await dispatch_signal([first, second], signal_result(), entry_eligible=True, market_fresh=True)
        self.assertEqual(statuses, {12345678: 'UNCERTAIN_BLOCKED', 2: 'ORDER_REJECTED'})
        self.assertTrue(first.halted_reason)
        self.assertIsNone(second.halted_reason)

    async def test_cancellation_is_propagated(self):
        candidate = manager()
        candidate.signal = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await dispatch_signal([candidate], signal_result(), entry_eligible=True, market_fresh=True)
        self.assertIsNone(candidate.halted_reason)

    async def test_wrong_account_broker_responses_are_rejected(self):
        candidate = manager()
        candidate.api.positions_response = [{**position(1), 'accountId': 2}]
        with self.assertRaises(ProjectXError):
            await candidate.reconcile()
        candidate.api.trades_response = [{'accountId': 2, 'profitAndLoss': 50}]
        with self.assertRaises(ProjectXError):
            await candidate.daily_net_pnl()

    async def test_restart_recovers_distinct_stops_and_fill_prices(self):
        first, second = manager(), manager(2)
        for candidate, entry, stop_id in [(first, 100, 65), (second, 110, 66)]:
            candidate.api.positions_response = [{**position(1, entry), 'accountId': candidate.account['id']}]
            candidate.api.open_orders_response = [{**stop(stop_id, 1, entry - 10), 'accountId': candidate.account['id']}]
        await asyncio.gather(first.startup_check(), second.startup_check())
        self.assertEqual((first.confirmed_stop_order_id, second.confirmed_stop_order_id), (65, 66))
        self.assertEqual((first.confirmed_stop_price, second.confirmed_stop_price), (Decimal('90'), Decimal('100')))
        first.halt('mock account-specific halt')
        self.assertIsNone(second.halted_reason)

    async def test_verification_is_per_account_and_express_has_no_manual_fallback(self):
        api = SimpleNamespace(verify_simulated_account=AsyncMock(side_effect=ProjectXError('unavailable')))
        practice_settings = settings(verified_practice_account_id=ACCOUNT['id'],
                                     verified_practice_account_name=ACCOUNT['name'],
                                     practice_account_simulated_verified=True)
        result = await verify_configured_account(api, [ACCOUNT], practice_settings)
        self.assertTrue(result['_simulatedVerified'])
        result = await verify_configured_account(api, [ACCOUNT], settings(account_type='express'))
        self.assertFalse(result['_simulatedVerified'])
        with self.assertRaises(ProjectXError):
            await verify_configured_account(api, [ACCOUNT], settings(account_name='WRONG'))

    async def test_hub_must_return_exact_identity_and_simulated_status(self):
        for verified in [ACCOUNT, {**ACCOUNT, 'id': 2}, {**ACCOUNT, 'simulated': False}]:
            api = SimpleNamespace(verify_simulated_account=AsyncMock(return_value=verified))
            result = await verify_configured_account(api, [ACCOUNT], settings(account_type='express'))
            self.assertEqual(result['_simulatedVerified'], verified == ACCOUNT)

    async def test_worker_cancellation_completes_before_shutdown_returns(self):
        started, cleaned = asyncio.Event(), asyncio.Event()
        async def worker():
            started.set()
            try:
                await asyncio.Future()
            finally:
                cleaned.set()
        async def feed():
            await started.wait()
        await run_bot_tasks(feed(), [worker()])
        self.assertTrue(cleaned.is_set())

    async def test_background_failure_cancels_feed(self):
        cleaned = asyncio.Event()
        async def feed():
            try:
                await asyncio.Future()
            finally:
                cleaned.set()
        async def worker():
            raise RuntimeError('worker failed')
        with self.assertRaises(RuntimeError):
            await run_bot_tasks(feed(), [worker()])
        self.assertTrue(cleaned.is_set())

    async def test_main_initializes_all_accounts_and_releases_every_lock(self):
        accounts = [ACCOUNT, {**ACCOUNT, 'id': 2}, {**ACCOUNT, 'id': 3}]
        api = SimpleNamespace(login=AsyncMock(), accounts=AsyncMock(return_value=accounts),
                              verify_simulated_account=AsyncMock(side_effect=accounts),
                              mnq=AsyncMock(return_value={**CONTRACT, 'name': 'MNQ test'}), close=AsyncMock())
        feed = Mock()
        feed.bootstrap = AsyncMock(return_value=[])
        feed.market_is_fresh.return_value = True
        result = signal_result()
        strategy = Mock(name='shared strategy')
        strategy.on_candle.return_value = result
        async def feed_run(callback):
            await callback(result.candle, True)
        feed.run = feed_run
        executions = []
        def create_manager(*args, **kwargs):
            candidate = ExecutionManager(*args, **kwargs)
            candidate.startup_check = AsyncMock()
            candidate.monitor = AsyncMock()
            candidate.signal = AsyncMock(return_value='ORDER_REJECTED')
            executions.append(candidate)
            return candidate
        locks = [Mock(), Mock(), Mock()]
        entries = [settings(), settings(2), settings(3, enabled=False)]
        with (patch.dict(os.environ, {'TOPSTEP_USERNAME': 'mock', 'TOPSTEP_API_KEY': 'mock'}),
              patch('main.configured_accounts', return_value=entries),
              patch('main.TradingConfig.from_env', return_value=config()),
              patch('main.ProjectX', return_value=api),
              patch('main.MarketDataFeed', return_value=feed),
              patch('main.select_strategy', return_value=strategy),
              patch('main.ExecutionManager', side_effect=create_manager),
              patch('main.AccountInstanceLock', side_effect=locks),
              patch('main.PauseStateStore', return_value=SimpleNamespace(is_paused=lambda account_id: account_id == 2)),
              patch('main.AccountTelegramMonitor.from_env', return_value=None)):
            await async_main()
        self.assertEqual(len(executions), 3)
        self.assertTrue(executions[0].entries_enabled)
        self.assertFalse(executions[1].entries_enabled)
        self.assertTrue(executions[1].runtime_paused)
        self.assertTrue(executions[1].configured_entries_enabled)
        self.assertFalse(executions[2].entries_enabled)
        strategy.on_candle.assert_called_once()
        for candidate, lock in zip(executions, locks):
            candidate.startup_check.assert_awaited_once()
            candidate.signal.assert_awaited_once()
            lock.close.assert_called_once()
        api.close.assert_awaited_once()

    async def test_partial_startup_failure_releases_acquired_lock_and_api(self):
        api = SimpleNamespace(login=AsyncMock(), accounts=AsyncMock(return_value=[ACCOUNT]),
                              verify_simulated_account=AsyncMock(return_value=ACCOUNT),
                              close=AsyncMock())
        lock = Mock()
        with (patch.dict(os.environ, {'TOPSTEP_USERNAME': 'mock', 'TOPSTEP_API_KEY': 'mock'}),
              patch('main.configured_accounts', return_value=[settings(), settings(2)]),
              patch('main.TradingConfig.from_env', return_value=config()),
              patch('main.ProjectX', return_value=api),
              patch('main.select_strategy', return_value=Mock()),
              patch('main.AccountInstanceLock', return_value=lock)):
            with self.assertRaises(ProjectXError):
                await async_main()
        lock.close.assert_called_once()
        api.close.assert_awaited_once()


class TelegramRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_one_monitor_polls_and_help_reaches_all_accounts(self):
        peers = [object.__new__(AccountTelegramMonitor) for _ in range(2)]
        for peer in peers:
            peer.peers = peers
        with patch.object(TelegramMonitor, 'poll_commands', new_callable=AsyncMock) as poll:
            await peers[0].poll_commands()
            await peers[1].poll_commands()
            poll.assert_awaited_once()
        update = {'update_id': 1, 'message': {'text': '/help', 'chat': {'id': 123, 'type': 'private'}}}
        with patch.object(TelegramMonitor, 'process_update', new_callable=AsyncMock) as process:
            await peers[0].process_update(update)
            self.assertEqual(process.await_count, 2)

    async def test_unscoped_setting_commands_cannot_change_multiple_accounts(self):
        primary = object.__new__(AccountTelegramMonitor)
        primary.peers = [primary, Mock()]
        primary.config = SimpleNamespace(owner_chat_id='123')
        primary.rate_limiter = Mock()
        primary.rate_limiter.allow.return_value = True
        primary.send_to = AsyncMock()
        update = {'update_id': 7, 'message': {'text': '/contracts 10', 'chat': {'id': 123, 'type': 'private'}}}
        with patch.object(TelegramMonitor, 'process_update', new_callable=AsyncMock) as process:
            await primary.process_update(update)
            process.assert_not_awaited()
        self.assertEqual(primary.offset, 8)
        primary.send_to.assert_awaited_once()

    async def test_account_labels_are_included_in_messages(self):
        monitor = object.__new__(AccountTelegramMonitor)
        monitor.execution = SimpleNamespace(account=ACCOUNT)
        with patch.object(TelegramMonitor, 'send_to', new_callable=AsyncMock, return_value=True) as send:
            self.assertTrue(await monitor.send_to('123', 'Trade opened'))
            self.assertIn(str(ACCOUNT['id']), send.call_args.args[1])
            self.assertIn('Trade opened', send.call_args.args[1])
