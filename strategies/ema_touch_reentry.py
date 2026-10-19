"""Pine-compatible EMA cross and fresh EMA-touch re-entry signals."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable

from strategies.ema_cross import StrategyResult
from topstep_api import Candle


class EmaTouchReentryStrategy:
    name = "ema_touch_reentry"

    def __init__(self, *, fast_length=20, slow_length=50, enable_cross_entries=True,
                 enable_touch_entries=True, touch_blue_ema=True, touch_red_ema=True,
                 use_date_filter=False, start_date=None, end_date=None):
        self.fast_length = fast_length
        self.slow_length = slow_length
        self.enable_cross_entries = enable_cross_entries
        self.enable_touch_entries = enable_touch_entries
        self.touch_blue_ema = touch_blue_ema
        self.touch_red_ema = touch_red_ema
        self.use_date_filter = use_date_filter
        if use_date_filter and (start_date is None or end_date is None):
            raise ValueError("date filtering requires start_date and end_date")
        if start_date is not None and end_date is not None and start_date >= end_date:
            raise ValueError("start_date must be earlier than end_date")
        self.start_date = start_date
        self.end_date = end_date
        self.last_processed_time = None
        self.fast_ema = None
        self.slow_ema = None
        self.blue_touch_armed = True
        self.red_touch_armed = True
        self.date_exit_emitted = False

    def initialize(self, candles: Iterable[Candle]) -> None:
        for candle in sorted(candles, key=lambda item: item.time):
            self.on_candle(candle, emit_signal=False, position_type=None)

    @staticmethod
    def next_ema(previous, close, length):
        # Pine ta.ema uses the first non-na source as its recursive seed.
        return close if previous is None else (2.0 / (length + 1.0)) * close + (1.0 - 2.0 / (length + 1.0)) * previous

    def in_date_range(self, timestamp: datetime) -> bool:
        return not self.use_date_filter or (
            self.start_date is not None and self.end_date is not None
            and self.start_date <= timestamp < self.end_date
        )

    def on_candle(self, candle: Candle, *, emit_signal=True, position_type=None, **_):
        if self.last_processed_time is not None and candle.time <= self.last_processed_time:
            return None
        self.last_processed_time = candle.time
        previous_fast, previous_slow = self.fast_ema, self.slow_ema
        self.fast_ema = self.next_ema(self.fast_ema, candle.close, self.fast_length)
        self.slow_ema = self.next_ema(self.slow_ema, candle.close, self.slow_length)

        blue_touched = (
            self.enable_touch_entries and self.touch_blue_ema
            and candle.low <= self.fast_ema <= candle.high
        )
        red_touched = (
            self.enable_touch_entries and self.touch_red_ema
            and candle.low <= self.slow_ema <= candle.high
        )
        blue_new = blue_touched and self.blue_touch_armed
        red_new = red_touched and self.red_touch_armed
        self.blue_touch_armed = not blue_touched
        self.red_touch_armed = not red_touched

        crossed_up = previous_fast is not None and previous_slow is not None and previous_fast <= previous_slow and self.fast_ema > self.slow_ema
        crossed_down = previous_fast is not None and previous_slow is not None and previous_fast >= previous_slow and self.fast_ema < self.slow_ema
        signal = None
        in_range = self.in_date_range(candle.time)
        if emit_signal and self.use_date_filter and self.end_date is not None and candle.time >= self.end_date and position_type is not None and not self.date_exit_emitted:
            signal = "DATE_RANGE_EXIT"
            self.date_exit_emitted = True
        elif emit_signal and in_range:
            if self.enable_cross_entries and crossed_up:
                signal = "LONG_CROSS"
            elif self.enable_cross_entries and crossed_down:
                signal = "SHORT_CROSS"
            elif self.enable_touch_entries and position_type is None:
                long_touch = ((blue_new and candle.close > self.fast_ema) or (red_new and candle.close > self.slow_ema))
                short_touch = ((blue_new and candle.close < self.fast_ema) or (red_new and candle.close < self.slow_ema))
                if long_touch and not short_touch:
                    signal = "LONG_TOUCH"
                elif short_touch and not long_touch:
                    signal = "SHORT_TOUCH"
        trend = "BULLISH" if self.fast_ema > self.slow_ema else "BEARISH" if self.fast_ema < self.slow_ema else "FLAT"
        return StrategyResult(candle, self.fast_ema, self.slow_ema, trend, signal)

