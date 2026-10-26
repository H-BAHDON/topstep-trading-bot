"""Evaluate at :55 using completed :45–:54 minutes of the current 15M candle.

The final :00 close updates the permanent EMA without emitting another signal.
"""
from __future__ import annotations

from strategies.ema12_hourly_trailing_43 import Ema12HourlyTrailing43Strategy


class Ema12HourlyTrailing55Strategy(Ema12HourlyTrailing43Strategy):
    name = 'ema12_hourly_trailing_55'
    partial_minutes = 10
    signal_bucket_minute = 45
    signal_minute = 55

    def __init__(self, **kwargs):
        if kwargs.get('timeframe_minutes', 15) != 15:
            raise ValueError(':55 strategy requires a 15-minute timeframe')
        super().__init__(**kwargs)
