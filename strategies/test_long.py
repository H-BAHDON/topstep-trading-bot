"""Deterministic alternating LONG/SHORT live-candle test strategy."""
from __future__ import annotations

from typing import Iterable

from strategies.base import StrategyResult
from topstep_api import Candle


class TestLongStrategy:
    """Emit LONG, WAIT, SHORT, WAIT on unique eligible live candles."""

    __test__ = False
    name = "test_long"

    def __init__(self) -> None:
        self.last_processed_time = None
        self.live_candle_count = 0

    def initialize(self, candles: Iterable[Candle]) -> None:
        """Record historical timestamps without advancing the live sequence."""
        for candle in sorted(candles, key=lambda item: item.time):
            self.on_candle(candle, emit_signal=False)

    def on_candle(
        self,
        candle: Candle,
        *,
        emit_signal: bool = True,
        entry_available: bool = True,
    ) -> StrategyResult | None:
        if self.last_processed_time is not None and candle.time <= self.last_processed_time:
            return None
        self.last_processed_time = candle.time

        signal = None
        if emit_signal:
            self.live_candle_count += 1
            sequence_index = self.live_candle_count - 1
            is_signal_candle = sequence_index % 2 == 0
            if is_signal_candle and entry_available:
                signal = (
                    "LONG_CROSS"
                    if (sequence_index // 2) % 2 == 0
                    else "SHORT_CROSS"
                )

        return StrategyResult(
            candle=candle,
            ema20=float(candle.close),
            ema50=float(candle.close),
            trend="TEST_ALTERNATING",
            signal=signal,
        )
