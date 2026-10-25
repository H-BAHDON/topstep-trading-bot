from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from backtests.ema12_hourly_trailing import compare_tradingview_signals, simulate_exit
from execution import ExecutionManager, TradingConfig
from main import select_strategy
from strategies.ema12_hourly_trailing import Ema12HourlyTrailingStrategy
from topstep_api import Candle

UTC = timezone.utc


def minute_bars(start: datetime, closes: list[float]) -> list[Candle]:
    return [
        Candle(start + timedelta(minutes=index), value, value + 1, value - 1, value, 1)
        for index, value in enumerate(closes)
    ]


class Ema12HourlyStrategyTests(unittest.IsolatedAsyncioTestCase):
    def test_completed_15_minute_signal_and_duplicate_suppression(self):
        strategy = Ema12HourlyTrailingStrategy(ema_length=2)
        # 02:30 UTC is 03:30 UK during BST; this bar closes at 03:45 UK.
        bars = minute_bars(datetime(2026, 6, 1, 2, 30, tzinfo=UTC), [100] * 14 + [103])
        results = [strategy.on_candle(bar) for bar in bars]
        self.assertTrue(all(result is None for result in results[:-1]))
        self.assertIsNone(results[-1].signal)  # first EMA seed equals the close
        self.assertEqual(results[-1].trend, "EQUAL")

    def test_long_short_equal_and_hour_filter_after_warmup(self):
        strategy = Ema12HourlyTrailingStrategy(ema_length=2)
        first = minute_bars(datetime(2026, 6, 1, 1, 30, tzinfo=UTC), [100] * 15)
        second = minute_bars(datetime(2026, 6, 1, 2, 30, tzinfo=UTC), [110] * 15)
        third = minute_bars(datetime(2026, 6, 1, 3, 30, tzinfo=UTC), [90] * 15)
        for bar in first:
            strategy.on_candle(bar)
        long_result = None
        for bar in second:
            long_result = strategy.on_candle(bar) or long_result
        self.assertEqual(long_result.signal, "LONG_HOURLY")
        short_result = None
        for bar in third:
            short_result = strategy.on_candle(bar) or short_result
        self.assertEqual(short_result.signal, "SHORT_HOURLY")
        self.assertIsNone(strategy.on_candle(third[-1]))

    def test_dst_uses_london_wall_clock(self):
        winter = Ema12HourlyTrailingStrategy(ema_length=2)
        summer = Ema12HourlyTrailingStrategy(ema_length=2)
        winter_result = summer_result = None
        for bar in minute_bars(datetime(2026, 1, 5, 3, 30, tzinfo=UTC), [100] * 15):
            winter_result = winter.on_candle(bar) or winter_result
        for bar in minute_bars(datetime(2026, 7, 5, 2, 30, tzinfo=UTC), [100] * 15):
            summer_result = summer.on_candle(bar) or summer_result
        self.assertEqual(winter_result.signal_close_time.hour, 3)
        self.assertEqual(summer_result.signal_close_time.hour, 3)

    def test_gap_does_not_create_partial_signal_candle(self):
        strategy = Ema12HourlyTrailingStrategy()
        bars = minute_bars(datetime(2026, 1, 5, 3, 30, tzinfo=UTC), [100] * 15)
        result = None
        for bar in bars[:7] + bars[8:]:
            result = strategy.on_candle(bar) or result
        self.assertIsNone(result)

    def test_selectable_and_default_configuration(self):
        environment = {
            "ACTIVE_STRATEGY": "ema12_hourly_trailing",
            "ENABLE_ORDER_EXECUTION": "false", "PRACTICE_ONLY": "true",
            "EMERGENCY_HALT": "true", "ENABLE_DAILY_LOSS_LIMIT": "true",
            "MAX_DAILY_LOSS_USD": "100", "MAX_OPEN_POSITIONS": "1",
            "BRACKET_MODE": "auto_oco",
        }
        with patch.dict(os.environ, environment, clear=True):
            strategy = select_strategy("ema12_hourly_trailing")
            config = TradingConfig.from_env()
        self.assertIsInstance(strategy, Ema12HourlyTrailingStrategy)
        self.assertEqual(config.contracts, 5)
        self.assertEqual(config.take_profit_points, Decimal("25"))
        self.assertEqual(config.stop_loss_points, Decimal("40"))
        self.assertEqual(config.trail_activation_points, Decimal("10"))
        self.assertEqual(config.trail_distance_ticks, 20)

    async def test_auto_oco_entry_has_long_and_short_protection(self):
        config = TradingConfig(
            False, True, 5, "ema12_hourly_trailing", Decimal("40"),
            Decimal("25"), None, Decimal("10"), 20, "native_delayed",
            True, Decimal("100"), 1, "auto_oco", True, .5, (),
        )
        account = {"id": 1}
        contract = {"id": "MNQ", "tickSize": .25, "symbolId": "F.US.MNQ"}
        manager = ExecutionManager(object(), account, contract, config)
        long_payload = manager.build_entry_payload(0, "long")
        short_payload = manager.build_entry_payload(1, "short")
        self.assertEqual(long_payload["stopLossBracket"]["ticks"], -160)
        self.assertEqual(long_payload["takeProfitBracket"]["ticks"], 100)
        self.assertEqual(short_payload["stopLossBracket"]["ticks"], 160)
        self.assertEqual(short_payload["takeProfitBracket"]["ticks"], -100)


class Ema12HourlyBacktestTests(unittest.TestCase):
    def test_long_and_short_exits_and_trailing_never_moves_backwards(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        long_bars = [Candle(start, 100, 111, 107, 110, 1), Candle(start, 110, 116, 110, 111, 1)]
        short_bars = [Candle(start, 100, 93, 89, 90, 1), Candle(start, 90, 90, 84, 89, 1)]
        self.assertEqual(simulate_exit("LONG", 100, long_bars).price, 111)
        self.assertEqual(simulate_exit("SHORT", 100, short_bars).price, 89)

    def test_take_profit_hard_stop_and_intrabar_disclosure(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        target = simulate_exit("LONG", 100, [Candle(start, 100, 125, 99, 124, 1)])
        stop = simulate_exit("SHORT", 100, [Candle(start, 100, 141, 99, 140, 1)])
        ambiguous = simulate_exit("LONG", 100, [Candle(start, 100, 112, 104, 110, 1)])
        self.assertEqual(target.reason, "TAKE_PROFIT")
        self.assertEqual(stop.reason, "HARD_STOP")
        self.assertTrue(ambiguous.ambiguous_intrabar)

    def test_candle_by_candle_tradingview_comparison(self):
        stamp = datetime(2026, 1, 1, 3, 30, tzinfo=UTC)
        self.assertEqual(compare_tradingview_signals({stamp: "LONG"}, {stamp: "LONG"}), [])
        differences = compare_tradingview_signals({stamp: "LONG"}, {stamp: "SHORT"})
        self.assertIn("python='LONG' tradingview='SHORT'", differences[0])


if __name__ == "__main__":
    unittest.main()
