"""Deterministic fixed-bracket MNQ quick-scalp strategy."""
from __future__ import annotations

from typing import Iterable

from strategies.ema_cross import StrategyResult
from topstep_api import Candle


class QuickScalpStrategy:
    """Alternate LONG/SHORT on unique eligible live one-minute candles."""

    name = "quick_scalp"

    def __init__(self, entry_mode: str = "alternating") -> None:
        mode = entry_mode.strip().lower()
        if mode not in {"alternating", "every_candle"}:
            raise ValueError(
                "ENTRY_MODE must be alternating or every_candle"
            )
        self.entry_mode = mode
        self.last_processed_time = None
        self.next_direction = 0
        self.pending_candle_time = None

    def initialize(self, candles: Iterable[Candle]) -> None:
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
        if emit_signal and entry_available:
            signal = "LONG_CROSS" if self.next_direction == 0 else "SHORT_CROSS"
            self.pending_candle_time = candle.time

        return StrategyResult(
            candle=candle,
            ema20=float(candle.close),
            ema50=float(candle.close),
            trend=f"QUICK_SCALP_{self.entry_mode.upper()}",
            signal=signal,
        )
    def entry_accepted(self, candle_time) -> None:
        """Advance direction only after ProjectX accepts and verifies the entry."""
        if self.pending_candle_time == candle_time:
            self.next_direction = 1 - self.next_direction
            self.pending_candle_time = None