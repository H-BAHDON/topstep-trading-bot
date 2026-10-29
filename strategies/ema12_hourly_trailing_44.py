"""Evaluate the hourly EMA setup at :44 using completed :30-:43 bars.

The final :44 one-minute bar is not used because it is still forming when the
decision is made. The completed :45 close updates the permanent EMA without
emitting a second signal.
"""
from __future__ import annotations

from strategies.ema12_hourly_trailing_43 import Ema12HourlyTrailing43Strategy


class Ema12HourlyTrailing44Strategy(Ema12HourlyTrailing43Strategy):
    name = "ema12_hourly_trailing_44"
    partial_minutes = 14
    signal_bucket_minute = 30
    signal_minute = 44

    def __init__(self, **kwargs):
        if kwargs.get("timeframe_minutes", 15) != 15:
            raise ValueError(":44 strategy requires a 15-minute timeframe")
        super().__init__(**kwargs)
