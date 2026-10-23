"""Early :43 variant of the hourly EMA strategy.

Unlike the :45 strategy, this evaluates the in-progress 15-minute candle
using completed one-minute bars from :30 through :42.  The decision is made
at :43 UK time and can differ from the final :45 candle signal.
"""
from __future__ import annotations

from datetime import timedelta

from strategies.ema12_hourly_trailing import (
    UTC,
    Ema12HourlyTrailingStrategy,
    HourlyEmaResult,
)
from topstep_api import Candle


class Ema12HourlyTrailing43Strategy(Ema12HourlyTrailingStrategy):
    name = "ema12_hourly_trailing_43"

    def _partial_candle(self) -> Candle | None:
        expected_size = 13  # completed minutes :30 through :42
        if self._bucket_start is None or len(self._bucket) != expected_size:
            return None
        expected = [
            self._bucket_start + timedelta(minutes=index)
            for index in range(expected_size)
        ]
        if [item.time.astimezone(UTC) for item in self._bucket] != expected:
            return None
        return Candle(
            self._bucket_start,
            self._bucket[0].open,
            max(item.high for item in self._bucket),
            min(item.low for item in self._bucket),
            self._bucket[-1].close,
            sum(item.volume for item in self._bucket),
        )

    def _evaluate_partial(
        self, candle: Candle, *, emit_signal: bool
    ) -> HourlyEmaResult:
        alpha = 2.0 / (self.ema_length + 1.0)
        projected_ema = (
            candle.close
            if self.ema is None
            else alpha * candle.close + (1.0 - alpha) * self.ema
        )
        signal_time = (candle.time + timedelta(minutes=13)).astimezone(
            self.timezone
        )
        within_hours = self.start_time.hour <= signal_time.hour <= self.end_time.hour
        signal = None
        if emit_signal and within_hours and candle.time != self.last_signal_candle_time:
            if candle.close > projected_ema:
                signal = "LONG_HOURLY_43"
            elif candle.close < projected_ema:
                signal = "SHORT_HOURLY_43"
            self.last_signal_candle_time = candle.time
        trend = (
            "ABOVE" if candle.close > projected_ema
            else "BELOW" if candle.close < projected_ema
            else "EQUAL"
        )
        return HourlyEmaResult(
            candle, projected_ema, signal, signal_time, trend, partial=True
        )

    def on_candle(
        self, candle: Candle, *, emit_signal: bool = True, **_: object
    ) -> HourlyEmaResult | None:
        timestamp = candle.time.astimezone(UTC)
        if self.last_processed_time is not None and timestamp <= self.last_processed_time:
            return None
        self.last_processed_time = timestamp
        bucket_start = self._quarter_start(timestamp)
        if bucket_start != self._bucket_start:
            self._bucket_start = bucket_start
            self._bucket = []
        self._bucket.append(candle)

        if (
            bucket_start.minute == 30
            and timestamp == bucket_start + timedelta(minutes=12)
        ):
            partial = self._partial_candle()
            return (
                self._evaluate_partial(partial, emit_signal=emit_signal)
                if partial is not None else None
            )
        if timestamp == bucket_start + timedelta(minutes=self.timeframe_minutes - 1):
            completed = self._complete_bucket()
            self._bucket = []
            if completed is not None:
                # Update the permanent EMA from the final 15-minute close, but
                # never emit the original :45 signal in this strategy.
                self._evaluate(completed, emit_signal=False)
        return None
