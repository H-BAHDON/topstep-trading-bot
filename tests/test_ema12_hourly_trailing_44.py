from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from execution import ENTRY_SIGNALS, TradingConfig
from main import select_strategy
from strategies.ema12_hourly_trailing_44 import Ema12HourlyTrailing44Strategy
from topstep_api import Candle

UTC = timezone.utc


def bars(start: datetime, count: int, price: float = 110) -> list[Candle]:
    return [
        Candle(
            start + timedelta(minutes=index),
            price,
            price,
            price,
            price,
            1,
        )
        for index in range(count)
    ]


class Ema12HourlyTrailing44Tests(unittest.TestCase):
    def test_emits_at_44_from_completed_minutes_through_43(self) -> None:
        strategy = Ema12HourlyTrailing44Strategy(ema_length=2)
        strategy.ema = 100
        results = [
            strategy.on_candle(candle)
            for candle in bars(datetime(2026, 1, 1, 0, 30, tzinfo=UTC), 14)
        ]

        self.assertTrue(all(result is None for result in results[:-1]))
        self.assertEqual(results[-1].signal, "LONG_HOURLY_44")
        self.assertEqual(results[-1].signal_close_time.minute, 44)
        self.assertTrue(results[-1].partial)

    def test_does_not_emit_again_at_45(self) -> None:
        strategy = Ema12HourlyTrailing44Strategy(ema_length=2)
        strategy.ema = 100
        results = []
        for candle in bars(datetime(2026, 1, 1, 0, 30, tzinfo=UTC), 15):
            result = strategy.on_candle(candle)
            if result is not None:
                results.append(result)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].signal, "LONG_HOURLY_44")

    def test_missing_completed_minute_suppresses_signal(self) -> None:
        strategy = Ema12HourlyTrailing44Strategy()
        strategy.ema = 100
        for index, candle in enumerate(
            bars(datetime(2026, 1, 1, 0, 30, tzinfo=UTC), 14)
        ):
            if index != 7:
                self.assertIsNone(strategy.on_candle(candle))

    def test_registration_execution_direction_and_configuration(self) -> None:
        with patch.dict(
            os.environ,
            {"ACTIVE_STRATEGY": "ema12_hourly_trailing_44"},
            clear=True,
        ):
            strategy = select_strategy("ema12_hourly_trailing_44")
            config = TradingConfig.from_env()

        self.assertIsInstance(strategy, Ema12HourlyTrailing44Strategy)
        self.assertEqual(config.strategy_name, "ema12_hourly_trailing_44")
        self.assertEqual(ENTRY_SIGNALS["LONG_HOURLY_44"], 0)
        self.assertEqual(ENTRY_SIGNALS["SHORT_HOURLY_44"], 1)
        with self.assertRaises(ValueError):
            Ema12HourlyTrailing44Strategy(timeframe_minutes=5)


if __name__ == "__main__":
    unittest.main()
