"""Shared result type for strategies and the execution layer."""
from __future__ import annotations

from dataclasses import dataclass

from topstep_api import Candle


@dataclass(frozen=True)
class StrategyResult:
    candle: Candle
    ema20: float
    ema50: float
    trend: str
    signal: str | None
