from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from execution import TradingConfig
from main import select_strategy
from strategies.ema_cross import EmaCrossStrategy
from strategies.ema_touch_reentry import EmaTouchReentryStrategy
from topstep_api import Candle, ProjectXError

UTC = timezone.utc
BASE = datetime(2026, 6, 5, tzinfo=UTC)


def candle(minute: int, close: float, low: float | None = None, high: float | None = None) -> Candle:
    return Candle(
        BASE + timedelta(minutes=minute), close,
        high if high is not None else close,
        low if low is not None else close,
        close, 1,
    )


class EmaTouchReentryTests(unittest.TestCase):
    def test_recursive_pine_ema_and_crossovers(self):
        strategy = EmaTouchReentryStrategy(fast_length=2, slow_length=4)
        first = strategy.on_candle(candle(1, 100))
        self.assertEqual(first.ema20, 100)
        self.assertEqual(first.ema50, 100)
        crossed = strategy.on_candle(candle(2, 110))
        self.assertEqual(crossed.signal, "LONG_CROSS")
        self.assertAlmostEqual(crossed.ema20, 106.6666666667)
        self.assertAlmostEqual(crossed.ema50, 104.0)
        crossed_down = strategy.on_candle(candle(3, 80))
        self.assertEqual(crossed_down.signal, "SHORT_CROSS")

    def test_blue_touch_is_fresh_and_rearms_after_non_touch(self):
        strategy = EmaTouchReentryStrategy(enable_cross_entries=False)
        strategy.fast_ema, strategy.slow_ema = 100.0, 90.0
        first = strategy.on_candle(candle(1, 101, 99, 102))
        self.assertEqual(first.signal, "LONG_TOUCH")
        self.assertIsNone(strategy.on_candle(candle(2, 101, 99, 102)).signal)
        self.assertIsNone(strategy.on_candle(candle(3, 110, 109, 111)).signal)
        strategy.fast_ema, strategy.slow_ema = 100.0, 90.0
        self.assertEqual(
            strategy.on_candle(candle(4, 101, 99, 102)).signal,
            "LONG_TOUCH",
        )

    def test_red_touch_short_and_position_blocks_touch_reentry(self):
        strategy = EmaTouchReentryStrategy(enable_cross_entries=False)
        strategy.fast_ema, strategy.slow_ema = 90.0, 100.0
        result = strategy.on_candle(candle(1, 99, 98, 101), position_type=1)
        self.assertIsNone(result.signal)
        strategy.on_candle(candle(2, 80, 79, 81))
        strategy.fast_ema, strategy.slow_ema = 90.0, 100.0
        result = strategy.on_candle(candle(3, 99, 98, 101), position_type=None)
        self.assertEqual(result.signal, "SHORT_TOUCH")

    def test_conflicting_blue_and_red_touches_are_skipped(self):
        strategy = EmaTouchReentryStrategy(enable_cross_entries=False)
        strategy.fast_ema, strategy.slow_ema = 100.0, 105.0
        result = strategy.on_candle(candle(1, 102, 99, 106))
        self.assertIsNone(result.signal)

    def test_cross_has_precedence_and_can_request_reversal(self):
        strategy = EmaTouchReentryStrategy(fast_length=2, slow_length=4)
        strategy.fast_ema, strategy.slow_ema = 100.0, 101.0
        result = strategy.on_candle(candle(1, 110, 90, 115), position_type=2)
        self.assertEqual(result.signal, "LONG_CROSS")

    def test_historical_and_duplicate_candles_never_emit(self):
        strategy = EmaTouchReentryStrategy(fast_length=2, slow_length=4)
        history = [candle(1, 100), candle(2, 110)]
        strategy.initialize(history)
        self.assertIsNone(strategy.on_candle(history[-1]))
        result = strategy.on_candle(candle(3, 80))
        self.assertEqual(result.signal, "SHORT_CROSS")

    def test_date_filter_is_start_inclusive_end_exclusive_and_exits(self):
        strategy = EmaTouchReentryStrategy(
            fast_length=2,
            slow_length=4,
            use_date_filter=True,
            start_date=BASE + timedelta(minutes=2),
            end_date=BASE + timedelta(minutes=4),
        )
        strategy.fast_ema, strategy.slow_ema = 100.0, 101.0
        before = strategy.on_candle(candle(1, 110))
        self.assertIsNone(before.signal)
        at_end = strategy.on_candle(candle(4, 110), position_type=1)
        self.assertEqual(at_end.signal, "DATE_RANGE_EXIT")
        self.assertIsNone(strategy.on_candle(candle(5, 110), position_type=1).signal)

    def test_strategy_selection_is_independent(self):
        original = select_strategy("ema_cross")
        with patch.dict(
            os.environ,
            {
                "EMA_TOUCH_FAST_LENGTH": "10",
                "EMA_TOUCH_SLOW_LENGTH": "30",
                "EMA_TOUCH_ENABLE_TOUCH_ENTRIES": "false",
            },
            clear=False,
        ):
            touch = select_strategy("ema_touch_reentry")
        self.assertIsInstance(original, EmaCrossStrategy)
        self.assertIsInstance(touch, EmaTouchReentryStrategy)
        self.assertEqual((touch.fast_length, touch.slow_length), (10, 30))
        self.assertFalse(touch.enable_touch_entries)

    def test_invalid_date_configuration_fails_closed(self):
        with patch.dict(
            os.environ,
            {"EMA_TOUCH_USE_DATE_FILTER": "true", "EMA_TOUCH_START_DATE": "", "EMA_TOUCH_END_DATE": ""},
            clear=False,
        ):
            with self.assertRaises(ValueError):
                select_strategy("ema_touch_reentry")

    def test_pine_exit_defaults_are_independent(self):
        environment = {
            "ACTIVE_STRATEGY": "ema_touch_reentry",
            "ENABLE_ORDER_EXECUTION": "false",
            "PRACTICE_ONLY": "true",
            "EMERGENCY_HALT": "true",
            "ENABLE_DAILY_LOSS_LIMIT": "true",
            "MAX_DAILY_LOSS_USD": "100",
            "MAX_OPEN_POSITIONS": "1",
            "BRACKET_MODE": "auto_oco",
        }
        with patch.dict(os.environ, environment, clear=True):
            config = TradingConfig.from_env()
        self.assertEqual(config.contracts, 1)
        self.assertEqual(str(config.stop_loss_points), "3.0")
        self.assertEqual(str(config.trail_activation_points), "20.0")
        self.assertEqual(config.trail_distance_ticks, 80)
        self.assertNotIn("STOP_LOSS_POINTS must be a positive number", config.errors)


if __name__ == "__main__":
    unittest.main()
