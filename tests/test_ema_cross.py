from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from strategies.ema_cross import EmaCrossStrategy
from topstep_api import Candle

UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)


def candle(
    offset: int,
    close: float,
    *,
    high: float | None = None,
    low: float | None = None,
) -> Candle:
    high = close if high is None else high
    low = close if low is None else low
    return Candle(START + timedelta(minutes=offset), close, high, low, close, 1)


class EmaCrossStrategyTests(unittest.TestCase):
    def test_crossover_detection(self) -> None:
        strategy = EmaCrossStrategy()
        result = strategy.evaluate_completed(
            candle(1, 101),
            ema20=101,
            ema50=100,
            previous_ema20=99,
            previous_ema50=100,
        )
        self.assertEqual(result.signal, "LONG_CROSS")

        result = strategy.evaluate_completed(
            candle(2, 99),
            ema20=99,
            ema50=100,
            previous_ema20=101,
            previous_ema50=100,
        )
        self.assertEqual(result.signal, "SHORT_CROSS")

    def test_bearish_pullback_confirmation_only_once(self) -> None:
        strategy = EmaCrossStrategy()
        armed = strategy.evaluate_completed(
            candle(1, 106, high=109, low=104),
            ema20=105,
            ema50=110,
            previous_ema20=105,
            previous_ema50=110,
        )
        self.assertIsNone(armed.signal)
        self.assertTrue(strategy.bearish_pullback_armed)

        confirmed = strategy.evaluate_completed(
            candle(2, 104, high=108, low=103),
            ema20=105,
            ema50=110,
            previous_ema20=105,
            previous_ema50=110,
        )
        self.assertEqual(confirmed.signal, "SHORT_PULLBACK")
        self.assertFalse(strategy.bearish_pullback_armed)

        repeated = strategy.evaluate_completed(
            candle(3, 103, high=107, low=102),
            ema20=105,
            ema50=110,
            previous_ema20=105,
            previous_ema50=110,
        )
        self.assertIsNone(repeated.signal)

    def test_bullish_pullback_confirmation(self) -> None:
        strategy = EmaCrossStrategy()
        strategy.evaluate_completed(
            candle(1, 104, high=106, low=101),
            ema20=105,
            ema50=100,
            previous_ema20=105,
            previous_ema50=100,
        )
        self.assertTrue(strategy.bullish_pullback_armed)

        result = strategy.evaluate_completed(
            candle(2, 106, high=107, low=102),
            ema20=105,
            ema50=100,
            previous_ema20=105,
            previous_ema50=100,
        )
        self.assertEqual(result.signal, "LONG_PULLBACK")

    def test_ema50_wick_touch_invalidates_pullback(self) -> None:
        strategy = EmaCrossStrategy()
        strategy.evaluate_completed(
            candle(1, 106, high=109, low=104),
            ema20=105,
            ema50=110,
            previous_ema20=105,
            previous_ema50=110,
        )
        result = strategy.evaluate_completed(
            candle(2, 104, high=110, low=103),
            ema20=105,
            ema50=110,
            previous_ema20=105,
            previous_ema50=110,
        )
        self.assertIsNone(result.signal)
        self.assertFalse(strategy.bearish_pullback_armed)

    def test_duplicate_completed_candle_is_ignored(self) -> None:
        strategy = EmaCrossStrategy()
        history = [candle(index, 100 + index / 10) for index in range(50)]
        strategy.initialize(history)
        new_candle = candle(50, 106)
        self.assertIsNotNone(strategy.on_candle(new_candle))
        self.assertIsNone(strategy.on_candle(new_candle))


if __name__ == "__main__":
    unittest.main()