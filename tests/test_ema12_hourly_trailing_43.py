from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from strategies.ema12_hourly_trailing_43 import Ema12HourlyTrailing43Strategy
from topstep_api import Candle

UTC = timezone.utc


class Ema12HourlyTrailing43Tests(unittest.TestCase):
    def test_emits_from_completed_minutes_through_42_only(self) -> None:
        strategy = Ema12HourlyTrailing43Strategy(ema_length=2)
        warm_start = datetime(2026, 1, 1, 0, 15, tzinfo=UTC)
        for index in range(15):
            value = 100.0
            strategy.on_candle(
                Candle(warm_start + timedelta(minutes=index), value, value, value, value, 1)
            )

        start = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
        result = None
        for index in range(13):
            value = 110.0
            result = strategy.on_candle(
                Candle(start + timedelta(minutes=index), value, value, value, value, 1)
            ) or result

        self.assertIsNotNone(result)
        self.assertEqual(result.signal, "LONG_HOURLY_43")
        self.assertEqual(result.signal_close_time.minute, 43)
        self.assertTrue(result.partial)

    def test_does_not_emit_again_at_45(self) -> None:
        strategy = Ema12HourlyTrailing43Strategy(ema_length=2)
        start = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
        results = []
        for index in range(15):
            result = strategy.on_candle(
                Candle(start + timedelta(minutes=index), 100, 100, 100, 100, 1)
            )
            if result is not None:
                results.append(result)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].signal_close_time.minute, 43)


if __name__ == "__main__":
    unittest.main()
