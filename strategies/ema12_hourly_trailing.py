"""Hourly EMA signal strategy built from completed one-minute candles.

The market-data layer supplies completed one-minute MNQ bars.  This strategy
builds exact 15-minute bars and evaluates only bars whose close is at ``:45``
in the configured IANA timezone.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

from topstep_api import Candle

UTC = timezone.utc


@dataclass(frozen=True)
class HourlyEmaResult:
    candle: Candle
    ema12: float
    signal: str | None
    signal_close_time: datetime
    trend: str
    partial: bool = False

    # Keep the execution/logging result interface compatible without changing
    # either existing EMA strategy.
    @property
    def ema20(self) -> float:
        return self.ema12

    @property
    def ema50(self) -> float:
        return self.ema12


class Ema12HourlyTrailingStrategy:
    name = "ema12_hourly_trailing"

    def __init__(
        self,
        *,
        ema_length: int = 12,
        timeframe_minutes: int = 15,
        start_time: time = time(0, 0),
        end_time: time = time(20, 0),
        timezone_name: str = "Europe/London",
    ) -> None:
        if ema_length <= 0:
            raise ValueError("EMA length must be positive")
        if timeframe_minutes <= 0 or 60 % timeframe_minutes:
            raise ValueError("timeframe must be a positive divisor of 60 minutes")
        if start_time.minute or end_time.minute:
            raise ValueError("start and end times must be whole hours")
        self.ema_length = ema_length
        self.timeframe_minutes = timeframe_minutes
        self.start_time = start_time
        self.end_time = end_time
        self.timezone_name = timezone_name
        self.timezone = ZoneInfo(timezone_name)
        self.ema: float | None = None
        self.last_processed_time: datetime | None = None
        self.last_signal_candle_time: datetime | None = None
        self._bucket_start: datetime | None = None
        self._bucket: list[Candle] = []

    def within_trading_hours(self, timestamp: datetime) -> bool:
        clock = timestamp.astimezone(self.timezone).time()
        if self.start_time > self.end_time:
            return clock >= self.start_time or clock < self.end_time
        return self.start_time <= clock < self.end_time

    @staticmethod
    def parse_clock(value: str) -> time:
        try:
            parsed = time.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"invalid clock time {value!r}; expected HH:MM") from exc
        if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
            raise ValueError(f"invalid clock time {value!r}; expected HH:MM")
        return parsed

    def initialize(self, candles: Iterable[Candle]) -> None:
        for candle in sorted(candles, key=lambda item: item.time):
            self.on_candle(candle, emit_signal=False)

    def _quarter_start(self, timestamp: datetime) -> datetime:
        utc = timestamp.astimezone(UTC)
        minute = utc.minute - utc.minute % self.timeframe_minutes
        return utc.replace(minute=minute, second=0, microsecond=0)

    def _complete_bucket(self) -> Candle | None:
        if self._bucket_start is None or len(self._bucket) != self.timeframe_minutes:
            return None
        expected = [
            self._bucket_start + timedelta(minutes=index)
            for index in range(self.timeframe_minutes)
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

    def _evaluate(self, candle: Candle, *, emit_signal: bool) -> HourlyEmaResult:
        alpha = 2.0 / (self.ema_length + 1.0)
        self.ema = (
            candle.close
            if self.ema is None
            else alpha * candle.close + (1.0 - alpha) * self.ema
        )
        close_time = (candle.time + timedelta(minutes=self.timeframe_minutes)).astimezone(
            self.timezone
        )
        within_hours = self.within_trading_hours(close_time)
        scheduled = close_time.minute == 45 and within_hours
        signal = None
        if emit_signal and scheduled and candle.time != self.last_signal_candle_time:
            if candle.close > self.ema:
                signal = "LONG_HOURLY"
            elif candle.close < self.ema:
                signal = "SHORT_HOURLY"
            self.last_signal_candle_time = candle.time
        trend = "ABOVE" if candle.close > self.ema else "BELOW" if candle.close < self.ema else "EQUAL"
        return HourlyEmaResult(candle, self.ema, signal, close_time, trend)

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
        if timestamp != bucket_start + timedelta(minutes=self.timeframe_minutes - 1):
            return None
        completed = self._complete_bucket()
        self._bucket = []
        return self._evaluate(completed, emit_signal=emit_signal) if completed else None
