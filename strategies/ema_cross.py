"""EMA 20/50 crossover and pullback strategy for completed candles."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Iterable

from topstep_api import Candle


@dataclass(frozen=True)
class StrategyResult:
    candle: Candle
    ema20: float
    ema50: float
    trend: str
    signal: str | None


class EmaCrossStrategy:
    name = "ema_cross"

    def __init__(self) -> None:
        self.candles: deque[Candle] = deque(maxlen=300)
        self.last_processed_time = None
        self.bearish_pullback_armed = False
        self.bullish_pullback_armed = False

    @staticmethod
    def ema_series(values: list[float], period: int) -> list[float | None]:
        """Match the existing TradingView-style SMA seed and recursive EMA."""
        result: list[float | None] = [None] * len(values)
        if len(values) < period:
            return result
        value = sum(values[:period]) / period
        result[period - 1] = value
        alpha = 2.0 / (period + 1.0)
        for index in range(period, len(values)):
            value = alpha * values[index] + (1.0 - alpha) * value
            result[index] = value
        return result

    def initialize(self, candles: Iterable[Candle]) -> None:
        """Warm indicators and state without returning historical signals."""
        for candle in sorted(candles, key=lambda item: item.time):
            self.on_candle(candle, emit_signal=False)

    def on_candle(
        self, candle: Candle, *, emit_signal: bool = True
    ) -> StrategyResult | None:
        if self.last_processed_time is not None and candle.time <= self.last_processed_time:
            return None
        self.last_processed_time = candle.time
        self.candles.append(candle)

        closes = [item.close for item in self.candles]
        fast = self.ema_series(closes, 20)
        slow = self.ema_series(closes, 50)
        if fast[-1] is None or slow[-1] is None:
            return None

        previous_fast = fast[-2] if len(fast) > 1 else None
        previous_slow = slow[-2] if len(slow) > 1 else None
        return self.evaluate_completed(
            candle,
            float(fast[-1]),
            float(slow[-1]),
            None if previous_fast is None else float(previous_fast),
            None if previous_slow is None else float(previous_slow),
            emit_signal=emit_signal,
        )

    def evaluate_completed(
        self,
        candle: Candle,
        ema20: float,
        ema50: float,
        previous_ema20: float | None,
        previous_ema50: float | None,
        *,
        emit_signal: bool = True,
    ) -> StrategyResult:
        trend = "BULLISH" if ema20 > ema50 else "BEARISH" if ema20 < ema50 else "FLAT"
        signal = None

        crossed_up = (
            previous_ema20 is not None
            and previous_ema50 is not None
            and previous_ema20 <= previous_ema50
            and ema20 > ema50
        )
        crossed_down = (
            previous_ema20 is not None
            and previous_ema50 is not None
            and previous_ema20 >= previous_ema50
            and ema20 < ema50
        )

        if crossed_up:
            signal = "LONG_CROSS"
            self.bearish_pullback_armed = False
            self.bullish_pullback_armed = False
        elif crossed_down:
            signal = "SHORT_CROSS"
            self.bearish_pullback_armed = False
            self.bullish_pullback_armed = False
        elif trend == "BEARISH":
            self.bullish_pullback_armed = False
            if candle.high >= ema50:
                self.bearish_pullback_armed = False
            elif self.bearish_pullback_armed and candle.close < ema20:
                signal = "SHORT_PULLBACK"
                self.bearish_pullback_armed = False
            elif candle.close > ema20 and candle.high < ema50:
                self.bearish_pullback_armed = True
        elif trend == "BULLISH":
            self.bearish_pullback_armed = False
            if candle.low <= ema50:
                self.bullish_pullback_armed = False
            elif self.bullish_pullback_armed and candle.close > ema20:
                signal = "LONG_PULLBACK"
                self.bullish_pullback_armed = False
            elif candle.close < ema20 and candle.low > ema50:
                self.bullish_pullback_armed = True
        else:
            self.bearish_pullback_armed = False
            self.bullish_pullback_armed = False

        if not emit_signal:
            signal = None
        return StrategyResult(candle, ema20, ema50, trend, signal)