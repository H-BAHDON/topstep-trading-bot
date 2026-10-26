import asyncio
import unittest
from collections import deque
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

from topstep_api import MarketDataFeed, ProjectX, RequestBudget, parse_timestamp
from test_multi_account import manager


class RequestBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_reads_share_spacing_but_mutations_have_reserved_capacity(self):
        budget = RequestBudget()
        clock = SimpleNamespace(value=100.0)
        clock.time = lambda: clock.value
        async def sleep(seconds):
            clock.value += seconds
        with patch('topstep_api.asyncio.get_running_loop', return_value=clock), patch('topstep_api.asyncio.sleep', side_effect=sleep):
            await budget.acquire()
            await budget.acquire()
            self.assertAlmostEqual(clock.value, 100.4)
            budget.requests = deque([clock.value] * 160)
            await budget.acquire(mutation=True)
            self.assertEqual(len(budget.requests), 161)
            await budget.acquire()
            self.assertGreaterEqual(clock.value, 160.4)

    async def test_provider_cooldown_applies_to_reads_and_mutations(self):
        budget = RequestBudget()
        clock = SimpleNamespace(value=100.0)
        clock.time = lambda: clock.value
        sleeps = []
        async def sleep(seconds):
            sleeps.append(seconds)
            clock.value += seconds
        with patch('topstep_api.asyncio.get_running_loop', return_value=clock), patch('topstep_api.asyncio.sleep', side_effect=sleep):
            budget.cooldown(16)
            await budget.acquire(mutation=True)
            self.assertEqual(clock.value, 116)
            budget.cooldown(2)
            budget.cooldown(1)
            await budget.acquire()
            self.assertEqual(clock.value, 118)
            self.assertEqual(sleeps, [16, 2])

    async def test_mutation_is_never_retried_on_429_and_sets_shared_cooldown(self):
        api = object.__new__(ProjectX)
        api.token = 'fake'
        api.request_budget = SimpleNamespace(acquire=AsyncMock(), cooldown=Mock())
        response = httpx.Response(429, headers={'Retry-After': '16'}, request=httpx.Request('POST', 'https://test.invalid/api/Order/place'))
        api.http = SimpleNamespace(post=AsyncMock(return_value=response))
        for method in (api.place_order_once, api.close_position_once, api.cancel_order_once):
            api.http.post.reset_mock()
            with self.assertRaises(httpx.HTTPStatusError):
                await method({'accountId': 2})
            api.http.post.assert_awaited_once()
        api.request_budget.cooldown.assert_called_with(16)

    async def test_read_retry_uses_shared_budget(self):
        api = object.__new__(ProjectX)
        api.token = 'fake'
        api.request_budget = SimpleNamespace(acquire=AsyncMock(), cooldown=Mock())
        request = httpx.Request('POST', 'https://test.invalid/api/Position/searchOpen')
        api.http = SimpleNamespace(post=AsyncMock(side_effect=[
            httpx.Response(429, request=request, headers={'Retry-After': '2'}),
            httpx.Response(200, request=request, json={'success': True, 'positions': []}),
        ]))
        with patch('topstep_api.asyncio.sleep', new_callable=AsyncMock):
            result = await api.request('/api/Position/searchOpen', {'accountId': 2})
        self.assertEqual(result['positions'], [])
        self.assertEqual(api.request_budget.acquire.await_count, 2)
        api.request_budget.cooldown.assert_called_once_with(2)


class MarketPayloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_dotnet_fractional_seconds_and_numeric_epochs_are_accepted(self):
        expected = datetime(2026, 10, 25, 22, 52, 23, 785123, tzinfo=timezone.utc)
        self.assertEqual(parse_timestamp('2026-10-25T22:52:23.7851234Z'), expected)
        self.assertEqual(parse_timestamp('2026-10-25T22:52:23.785123456+00:00'), expected)
        for value in (expected.timestamp(), expected.timestamp() * 1000):
            self.assertLess(abs((parse_timestamp(value) - expected).total_seconds()), .00001)

    async def test_trade_formats_update_live_price(self):
        for timestamp in ('2026-10-25T22:52:23.7851234Z', 1792968743, 1792968743000):
            feed = MarketDataFeed(Mock(), {'id': 'MNQ'}, False)
            await feed.update_live_candle({'timestamp': timestamp, 'price': 25000, 'volume': None})
            self.assertEqual(feed.last_price, 25000)
            self.assertEqual(feed.current.volume, 0)
            self.assertEqual(feed.unexpected_payload_count, 0)

    async def test_trade_update_publishes_validated_live_price(self):
        feed = MarketDataFeed(Mock(), {'id': 'MNQ'}, False)
        observed = []
        feed.add_live_price_handler(observed.append)
        await feed.update_live_candle({
            'timestamp': '2026-10-25T22:52:23Z',
            'price': 25000.25,
            'volume': 1,
        })
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]['lastPrice'], 25000.25)

    async def test_invalid_fields_are_still_rejected(self):
        for field in ({'price': True}, {'volume': True}, {'timestamp': True}, {'price': 'NaN'}, {'timestamp': 'bad'}):
            feed = MarketDataFeed(Mock(), {'id': 'MNQ'}, False)
            await feed.update_live_candle({'timestamp': '2026-10-25T22:52:23Z', 'price': 25000, 'volume': 1, **field})
            self.assertIsNone(feed.last_price)
            self.assertEqual(feed.unexpected_payload_count, 1)


class MonitoringVolumeTests(unittest.IsolatedAsyncioTestCase):
    async def test_flat_monitor_skips_trade_history_when_loss_limit_disabled(self):
        execution = manager(daily_loss_limit_enabled=False)
        execution.api.trades = AsyncMock(return_value=[])
        await execution.monitor_once()
        await execution.monitor_once()
        execution.api.trades.assert_not_awaited()

    async def test_background_loss_refresh_is_throttled_but_entries_are_fresh(self):
        execution = manager()
        execution.api.trades = AsyncMock(return_value=[])
        await execution.monitor_once()
        await execution.monitor_once()
        execution.api.trades.assert_awaited_once()
        execution.last_background_pnl_refresh -= 16
        await execution.monitor_once()
        self.assertEqual(execution.api.trades.await_count, 2)
        await execution.daily_net_pnl()
        self.assertEqual(execution.api.trades.await_count, 3)
