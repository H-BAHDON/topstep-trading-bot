from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from main import select_strategy
from strategies.ema_cross import EmaCrossStrategy
from strategies.test_long import TestLongStrategy
from topstep_api import Candle

UTC = timezone.utc


def candle(minute: int) -> Candle:
    timestamp = datetime(2026, 10, 14, 12, 0, tzinfo=UTC) + timedelta(minutes=minute)
    price = 30000.0 + minute
    return Candle(timestamp, price, price + 1, price - 1, price, 1)


class TestLongStrategyTests(unittest.TestCase):
    def test_alternates_long_wait_short_wait(self) -> None:
        strategy = TestLongStrategy()
        results = [strategy.on_candle(candle(index)) for index in range(1, 8)]
        self.assertEqual(
            [result.signal for result in results],
            [
                "LONG_CROSS",
                None,
                "SHORT_CROSS",
                None,
                "LONG_CROSS",
                None,
                "SHORT_CROSS",
            ],
        )

    def test_waiting_candles_never_emit(self) -> None:
        strategy = TestLongStrategy()
        results = [strategy.on_candle(candle(index)) for index in range(1, 9)]
        self.assertTrue(all(results[index].signal is None for index in (1, 3, 5, 7)))

    def test_historical_candles_do_not_advance_sequence(self) -> None:
        strategy = TestLongStrategy()
        strategy.initialize(candle(index) for index in range(1, 101))
        first = strategy.on_candle(candle(101))
        second = strategy.on_candle(candle(102))
        third = strategy.on_candle(candle(103))
        self.assertEqual(
            [first.signal, second.signal, third.signal],
            ["LONG_CROSS", None, "SHORT_CROSS"],
        )
        self.assertEqual(strategy.live_candle_count, 3)

    def test_duplicate_candle_is_ignored_without_advancing(self) -> None:
        strategy = TestLongStrategy()
        first = candle(1)
        self.assertEqual(strategy.on_candle(first).signal, "LONG_CROSS")
        self.assertIsNone(strategy.on_candle(first))
        self.assertIsNone(strategy.on_candle(candle(2)).signal)
        self.assertEqual(strategy.on_candle(candle(3)).signal, "SHORT_CROSS")
        self.assertEqual(strategy.live_candle_count, 3)

    def test_reconnection_replay_does_not_advance_sequence(self) -> None:
        strategy = TestLongStrategy()
        self.assertEqual(strategy.on_candle(candle(1)).signal, "LONG_CROSS")
        self.assertIsNone(strategy.on_candle(candle(2)).signal)
        replay = strategy.on_candle(candle(3), emit_signal=False)
        self.assertIsNone(replay.signal)
        self.assertEqual(strategy.live_candle_count, 2)
        self.assertEqual(strategy.on_candle(candle(4)).signal, "SHORT_CROSS")

    def test_unavailable_entry_is_skipped_and_not_queued(self) -> None:
        strategy = TestLongStrategy()
        skipped_long = strategy.on_candle(candle(1), entry_available=False)
        self.assertIsNone(skipped_long.signal)
        self.assertIsNone(strategy.on_candle(candle(2)).signal)
        self.assertEqual(strategy.on_candle(candle(3)).signal, "SHORT_CROSS")

        self.assertIsNone(
            strategy.on_candle(candle(4), entry_available=False).signal
        )
        skipped_long = strategy.on_candle(candle(5), entry_available=False)
        self.assertIsNone(skipped_long.signal)
        self.assertIsNone(strategy.on_candle(candle(6)).signal)
        self.assertEqual(strategy.on_candle(candle(7)).signal, "SHORT_CROSS")

    def test_position_or_pending_order_suppresses_signal(self) -> None:
        for unavailable_reason in ("position", "pending_order"):
            with self.subTest(reason=unavailable_reason):
                strategy = TestLongStrategy()
                result = strategy.on_candle(candle(1), entry_available=False)
                self.assertIsNone(result.signal)
                self.assertEqual(strategy.live_candle_count, 1)



    def test_sequence_continues_without_a_trade_count_limit(self) -> None:
        strategy = TestLongStrategy()
        results = [strategy.on_candle(candle(index)) for index in range(1, 22)]
        signals = [result.signal for result in results if result.signal is not None]
        self.assertEqual(len(signals), 11)
        self.assertEqual(
            signals,
            [
                "LONG_CROSS", "SHORT_CROSS", "LONG_CROSS", "SHORT_CROSS",
                "LONG_CROSS", "SHORT_CROSS", "LONG_CROSS", "SHORT_CROSS",
                "LONG_CROSS", "SHORT_CROSS", "LONG_CROSS",
            ],
        )

    def test_strategy_selection_switches_without_changing_ema(self) -> None:
        self.assertIsInstance(select_strategy("test_long"), TestLongStrategy)
        self.assertIsInstance(select_strategy("ema_cross"), EmaCrossStrategy)


if __name__ == "__main__":
    unittest.main()
