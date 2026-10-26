from __future__ import annotations

import unittest
from datetime import datetime, time, timedelta, timezone

from strategies.ema12_hourly_trailing_43 import Ema12HourlyTrailing43Strategy
from topstep_api import Candle

UTC = timezone.utc


class Ema12HourlyTrailing43Tests(unittest.TestCase):
    def test_overnight_window_accepts_signals_and_excludes_break(self):
        for hour, expected in ((23, True), (0, True), (12, True), (21, True), (22, False)):
            strategy = Ema12HourlyTrailing43Strategy(start_time=time(23), end_time=time(22))
            strategy.ema = 100
            candle = Candle(datetime(2026, 1, 1, hour, 30, tzinfo=UTC), 110, 110, 110, 110, 1)
            with self.subTest(hour=hour):
                result = strategy._evaluate_partial(candle, emit_signal=True)
                self.assertEqual(result.signal is not None, expected)

    def test_exact_overnight_boundaries_and_london_summer_time(self):
        strategy = Ema12HourlyTrailing43Strategy(start_time=time(23), end_time=time(22))
        for stamp, expected in (
            ('2026-01-01T22:59:59+00:00', False),
            ('2026-01-01T23:00:00+00:00', True),
            ('2026-01-02T21:59:59+00:00', True),
            ('2026-01-02T22:00:00+00:00', False),
            ('2026-07-01T22:00:00+00:00', True),
            ('2026-07-01T21:00:00+00:00', False),
        ):
            with self.subTest(stamp=stamp):
                self.assertEqual(strategy.within_trading_hours(datetime.fromisoformat(stamp)), expected)

    def test_daytime_window_uses_exact_end_time(self):
        strategy = Ema12HourlyTrailing43Strategy(start_time=time(9), end_time=time(20))
        self.assertTrue(strategy.within_trading_hours(datetime(2026, 1, 1, 19, 43, tzinfo=UTC)))
        self.assertFalse(strategy.within_trading_hours(datetime(2026, 1, 1, 20, 43, tzinfo=UTC)))

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
