import unittest
from datetime import datetime, time, timedelta, timezone
from unittest.mock import patch
import os

from execution import ENTRY_SIGNALS, TradingConfig
from main import load_environment, select_strategy
from strategies.ema12_hourly_trailing_55 import Ema12HourlyTrailing55Strategy
from topstep_api import Candle


class Hourly55Tests(unittest.TestCase):
    def bars(self, start, count, price=110):
        return [Candle(start+timedelta(minutes=i),price,price,price,price,1) for i in range(count)]

    def test_only_emits_at_55_and_updates_ema_at_00(self):
        s=Ema12HourlyTrailing55Strategy()
        s.ema=100
        start=datetime(2026,1,1,0,30,tzinfo=timezone.utc)
        results=[]
        for bar in self.bars(start,30):
            result=s.on_candle(bar)
            if result:
                results.append(result)
        self.assertEqual(len(results),1)
        self.assertEqual(results[0].signal_close_time.minute,55)
        self.assertEqual(results[0].signal,'LONG_HOURLY_55')
        self.assertGreater(s.ema,100)
        self.assertIsNone(s.on_candle(self.bars(start+timedelta(minutes=24),1)[0]))

    def test_missing_minute_suppresses_signal(self):
        s=Ema12HourlyTrailing55Strategy()
        s.ema=100
        bars=self.bars(datetime(2026,1,1,0,45,tzinfo=timezone.utc),10)
        for index,bar in enumerate(bars):
            if index!=4:
                self.assertIsNone(s.on_candle(bar))

    def test_overnight_hours_and_short_direction(self):
        for hour, expected in ((23,'SHORT_HOURLY_55'),(0,'SHORT_HOURLY_55'),(21,'SHORT_HOURLY_55'),(22,None)):
            s=Ema12HourlyTrailing55Strategy(start_time=time(23),end_time=time(22))
            s.ema=120
            result=None
            for bar in self.bars(datetime(2026,1,1,hour,45,tzinfo=timezone.utc),10):
                result=s.on_candle(bar) or result
            self.assertIsNotNone(result)
            self.assertEqual(result.signal,expected)

    def test_registration_and_execution_settings(self):
        with patch.dict(os.environ, {'ACTIVE_STRATEGY':'ema12_hourly_trailing_55'},clear=True):
            s=select_strategy('ema12_hourly_trailing_55')
            c=TradingConfig.from_env()
        self.assertIsInstance(s,Ema12HourlyTrailing55Strategy)
        self.assertEqual(c.stop_loss_points,40)
        self.assertEqual(c.take_profit_points,25)
        self.assertEqual(ENTRY_SIGNALS['LONG_HOURLY_55'],0)
        self.assertEqual(ENTRY_SIGNALS['SHORT_HOURLY_55'],1)
        with self.assertRaises(ValueError):
            Ema12HourlyTrailing55Strategy(timeframe_minutes=5)

    def test_bootstrap_does_not_emit_historical_signals(self):
        s=Ema12HourlyTrailing55Strategy()
        s.initialize(self.bars(datetime(2026,1,1,0,30,tzinfo=timezone.utc),30))
        self.assertIsNotNone(s.ema)
        self.assertIsNone(s.on_candle(self.bars(datetime(2026,1,1,0,54,tzinfo=timezone.utc),1)[0]))
