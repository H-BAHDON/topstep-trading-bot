"""Deterministic bar-by-bar validation for ema12_hourly_trailing.

This intentionally reports ambiguous intrabar exits instead of claiming that
OHLC history proves whether activation, target, or the trailing stop happened
first.  Live execution delegates stop movement to the broker using tick data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping

from topstep_api import Candle


@dataclass(frozen=True)
class SimulatedExit:
    reason: str
    price: float
    ambiguous_intrabar: bool = False


def simulate_exit(
    side: str,
    entry: float,
    bars: Iterable[Candle],
    *,
    take_profit: float = 25,
    hard_stop: float = 40,
    activation: float = 10,
    trail_distance: float = 5,
) -> SimulatedExit | None:
    """Conservative OHLC simulation; stop wins when event order is unknowable."""
    trailing = False
    best = entry
    stop = entry - hard_stop if side == "LONG" else entry + hard_stop
    target = entry + take_profit if side == "LONG" else entry - take_profit
    for bar in bars:
        if side == "LONG":
            old_stop = stop
            hit_old_stop = bar.low <= old_stop
            hit_target = bar.high >= target
            activates = bar.high >= entry + activation
            candidate = bar.high - trail_distance
            ambiguous = (activates and bar.low <= candidate) or (hit_target and hit_old_stop)
            if hit_old_stop:
                return SimulatedExit("HARD_STOP" if not trailing else "TRAIL", old_stop, ambiguous)
            if hit_target:
                return SimulatedExit("TAKE_PROFIT", target, ambiguous)
            if activates:
                trailing = True
                best = max(best, bar.high)
                stop = max(stop, best - trail_distance)
                if bar.low <= stop:
                    return SimulatedExit("TRAIL", stop, True)
        else:
            old_stop = stop
            hit_old_stop = bar.high >= old_stop
            hit_target = bar.low <= target
            activates = bar.low <= entry - activation
            candidate = bar.low + trail_distance
            ambiguous = (activates and bar.high >= candidate) or (hit_target and hit_old_stop)
            if hit_old_stop:
                return SimulatedExit("HARD_STOP" if not trailing else "TRAIL", old_stop, ambiguous)
            if hit_target:
                return SimulatedExit("TAKE_PROFIT", target, ambiguous)
            if activates:
                trailing = True
                best = min(best, bar.low)
                stop = min(stop, best + trail_distance)
                if bar.high >= stop:
                    return SimulatedExit("TRAIL", stop, True)
    return None


def compare_tradingview_signals(
    python_signals: Mapping[datetime, str],
    tradingview_signals: Mapping[datetime, str],
) -> list[str]:
    """Return candle-level differences for exported TradingView signals."""
    differences: list[str] = []
    for timestamp in sorted(set(python_signals) | set(tradingview_signals)):
        python_value = python_signals.get(timestamp)
        tv_value = tradingview_signals.get(timestamp)
        if python_value != tv_value:
            differences.append(
                f"{timestamp.isoformat()}: python={python_value!r} tradingview={tv_value!r}"
            )
    return differences
