from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from execution import ExecutionManager, LONG_POSITION, SHORT_POSITION
from main import AccountInstanceLock, load_environment, select_strategy
from strategies.quick_scalp import QuickScalpStrategy
from test_execution import ACCOUNT, CONTRACT, FakeApi, config, position, stop
from topstep_api import Candle, ProjectXError

UTC = timezone.utc


def candle(minute: int) -> Candle:
    return Candle(datetime(2026, 10, 15, 12, minute, tzinfo=UTC), 100, 101, 99, 100, 1)


def scalp_config(**changes):
    values = {
        "strategy_name": "quick_scalp",
        "contracts": 150,
        "stop_loss_points": Decimal("10"),
        "take_profit_points": Decimal("2"),
        "broker_max_contracts": 150,
        "trail_activation_points": Decimal(0),
        "trail_distance_ticks": 0,
        "trail_management_mode": "fixed_oco",
    }
    values.update(changes)
    return config(**values)


def take_profit(order_id: int, side: int, price: float, size: int = 1) -> dict:
    return {
        "id": order_id,
        "accountId": ACCOUNT["id"],
        "contractId": CONTRACT["id"],
        "creationTimestamp": "2026-10-14T14:54:03.000Z",
        "status": 1,
        "type": 1,
        "side": side,
        "size": size,
        "limitPrice": price,
        "stopPrice": None,
    }


class QuickScalpStrategyTests(unittest.TestCase):
    def test_buy_sell_sequence_advances_only_after_accepted_entry(self) -> None:
        strategy = QuickScalpStrategy("alternating")
        signals = []
        for minute in range(1, 5):
            result = strategy.on_candle(candle(minute))
            signals.append(result.signal)
            strategy.entry_accepted(result.candle.time)
        self.assertEqual(signals, ["LONG_CROSS", "SHORT_CROSS", "LONG_CROSS", "SHORT_CROSS"])

    def test_open_position_skips_candles_without_advancing_or_queueing(self) -> None:
        strategy = QuickScalpStrategy("alternating")
        first = strategy.on_candle(candle(1))
        strategy.entry_accepted(first.candle.time)
        self.assertIsNone(strategy.on_candle(candle(2), entry_available=False).signal)
        self.assertIsNone(strategy.on_candle(candle(3), entry_available=False).signal)
        resumed = strategy.on_candle(candle(4), entry_available=True)
        self.assertEqual(resumed.signal, "SHORT_CROSS")

    def test_rejected_entry_keeps_direction_for_next_unique_candle(self) -> None:
        strategy = QuickScalpStrategy("alternating")
        self.assertEqual(strategy.on_candle(candle(1)).signal, "LONG_CROSS")
        self.assertEqual(strategy.on_candle(candle(2)).signal, "LONG_CROSS")
        strategy.entry_accepted(candle(2).time)
        self.assertEqual(strategy.on_candle(candle(3)).signal, "SHORT_CROSS")

    def test_historical_duplicate_and_unavailable_entries_are_not_replayed(self) -> None:
        strategy = QuickScalpStrategy("every_candle")
        strategy.initialize([candle(1), candle(2)])
        first = strategy.on_candle(candle(3), entry_available=False)
        duplicate = strategy.on_candle(candle(3), entry_available=True)
        next_result = strategy.on_candle(candle(4), entry_available=True)
        self.assertIsNone(first.signal)
        self.assertIsNone(duplicate)
        self.assertEqual(next_result.signal, "LONG_CROSS")

    def test_invalid_entry_mode_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            QuickScalpStrategy("random")


class QuickScalpExecutionTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, api=None, **changes) -> ExecutionManager:
        return ExecutionManager(api or FakeApi(), ACCOUNT, CONTRACT, scalp_config(**changes))

    def test_long_and_short_payloads_have_fixed_oco_and_150_contracts(self) -> None:
        manager = self.manager()
        long_payload = manager.build_entry_payload(0, "long")
        short_payload = manager.build_entry_payload(1, "short")
        self.assertEqual(long_payload["size"], 150)
        self.assertEqual(short_payload["size"], 150)
        self.assertEqual(long_payload["stopLossBracket"], {"ticks": -40, "type": 4})
        self.assertEqual(short_payload["stopLossBracket"], {"ticks": 40, "type": 4})
        self.assertEqual(long_payload["takeProfitBracket"], {"ticks": 8, "type": 1})
        self.assertEqual(short_payload["takeProfitBracket"], {"ticks": -8, "type": 1})
        self.assertIsNone(long_payload["trailPrice"])

    def test_actual_fill_drives_two_point_target_and_ten_point_stop(self) -> None:
        manager = self.manager()
        self.assertEqual(manager.initial_take_profit_price(Decimal("30285.50"), LONG_POSITION), Decimal("30287.50"))
        self.assertEqual(manager.initial_protective_stop_price(Decimal("30285.50"), LONG_POSITION), Decimal("30275.50"))
        self.assertEqual(manager.initial_take_profit_price(Decimal("30285.50"), SHORT_POSITION), Decimal("30283.50"))
        self.assertEqual(manager.initial_protective_stop_price(Decimal("30285.50"), SHORT_POSITION), Decimal("30295.50"))
        self.assertEqual(manager.take_profit_ticks, 8)
        self.assertEqual(manager.stop_ticks, 40)
        point_value = Decimal(str(CONTRACT["tickValue"])) / Decimal(str(CONTRACT["tickSize"]))
        self.assertEqual(Decimal("2") * Decimal("150") * point_value, Decimal("600.0"))

    def test_configured_broker_maximum_blocks_oversize_without_reducing(self) -> None:
        manager = self.manager(contracts=151)
        self.assertIn(
            "CONTRACTS exceeds BROKER_MAX_MNQ_CONTRACTS",
            manager.readiness_errors(),
        )
        allowed = self.manager(contracts=150)
        self.assertNotIn(
            "CONTRACTS exceeds BROKER_MAX_MNQ_CONTRACTS",
            allowed.readiness_errors(),
        )
        self.assertEqual(allowed.build_entry_payload(0, "max")["size"], 150)

    async def test_fast_filled_oco_round_trip_does_not_halt(self) -> None:
        api = FakeApi()
        tag = "mnq-long_cross-fast"
        entry = {
            "id": 500, "accountId": ACCOUNT["id"], "contractId": CONTRACT["id"],
            "creationTimestamp": "2026-10-15T15:00:02+00:00", "status": 2,
            "type": 2, "side": 0, "size": 150, "fillVolume": 150,
            "filledPrice": 100, "customTag": tag,
        }
        hard = stop(501, 1, 90, size=150)
        hard.update({"status": 3, "customTag": f"{tag}-SL"})
        target = take_profit(502, 1, 102, size=150)
        target.update({"status": 2, "fillVolume": 150, "filledPrice": 102, "customTag": f"{tag}-TP"})
        api.orders_response = [entry, hard, target]
        api.positions_response = []
        api.open_orders_response = []
        api.trades_response = [
            {"id": 1, "orderId": 500, "accountId": ACCOUNT["id"], "contractId": CONTRACT["id"], "side": 0, "size": 150, "price": 100, "profitAndLoss": None, "fees": 54, "voided": False},
            {"id": 2, "orderId": 502, "accountId": ACCOUNT["id"], "contractId": CONTRACT["id"], "side": 1, "size": 150, "price": 102, "profitAndLoss": 600, "fees": 54, "voided": False},
        ]
        manager = self.manager(api)
        result = await manager.verify_entry(500, tag, set())
        self.assertEqual(result, "FILLED_PROTECTED")
        self.assertIsNone(manager.halted_reason)
        self.assertEqual(manager.last_positions, [])
        self.assertIn(502, manager.processed_exit_order_ids)

    async def test_terminal_unfilled_entry_does_not_create_protection_halt(self) -> None:
        api = FakeApi()
        api.orders_response = [{
            "id": 510, "accountId": ACCOUNT["id"], "contractId": CONTRACT["id"],
            "creationTimestamp": "2026-10-15T15:00:02+00:00", "status": 3,
            "type": 2, "side": 0, "size": 150, "fillVolume": 0,
            "filledPrice": None, "customTag": "cancelled-entry",
        }]
        manager = self.manager(api)
        result = await manager.verify_entry(510, "cancelled-entry", set())
        self.assertEqual(result, "ORDER_NOT_FILLED")
        self.assertIsNone(manager.halted_reason)

    async def test_partial_fill_requires_matching_stop_and_take_profit_sizes(self) -> None:
        api = FakeApi()
        tag = "quick-partial"
        api.orders_response = [{
            "id": 200,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "creationTimestamp": "2026-10-14T14:54:02.500Z",
            "status": 3,
            "type": 2,
            "side": 0,
            "size": 150,
            "fillVolume": 25,
            "filledPrice": 100,
            "customTag": tag,
        }]
        api.positions_response = [position(LONG_POSITION, 100, 25)]
        api.open_orders_response = [
            stop(201, 1, 90, size=25),
            take_profit(202, 1, 102, size=25),
        ]
        manager = self.manager(api)
        result = await manager.verify_entry(200, tag, set())
        self.assertEqual(result, "FILLED_PROTECTED")
        self.assertEqual(manager.confirmed_position_size, 25)
        self.assertEqual(manager.confirmed_stop_order_id, 201)
        self.assertEqual(manager.confirmed_take_profit_order_id, 202)

    async def test_oco_closure_cancels_only_remaining_tracked_exit(self) -> None:
        api = FakeApi()
        hard_stop = stop(301, 1, 90, size=150)
        api.open_orders_response = [hard_stop]
        api.orders_response = [{
            **take_profit(302, 1, 102, size=150),
            "status": 2,
            "filledPrice": 102,
        }]
        api.trades_response = [{
            "orderId": 302,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "profitAndLoss": 600,
            "fees": 75,
            "voided": False,
        }]
        manager = self.manager(api)
        manager.last_positions = [position(LONG_POSITION, 100, 150)]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 150
        manager.confirmed_stop_order_id = 301
        manager.confirmed_take_profit_order_id = 302
        result = await manager.monitor_once()
        self.assertEqual(result, "POSITION_CLOSED")
        self.assertEqual(api.cancel_calls, [{"accountId": ACCOUNT["id"], "orderId": 301}])
        self.assertEqual(api.open_orders_response, [])

    async def test_broker_rejection_is_reported_without_split_or_reduction(self) -> None:
        api = FakeApi()
        api.place_response = {
            "success": False,
            "errorCode": 4,
            "errorMessage": "Maximum position size exceeded",
            "orderId": 400,
        }
        manager = self.manager(api)
        from test_execution import signal_result
        result = await manager.signal(
            signal_result(), entry_eligible=True, market_fresh=True
        )
        self.assertEqual(result, "ORDER_REJECTED")
        self.assertEqual(len(api.place_calls), 1)
        self.assertEqual(api.place_calls[0]["size"], 150)


class QuickScalpConfigurationTests(unittest.TestCase):
    def test_strategy_selection_preserves_original_and_supports_quick_scalp(self) -> None:
        self.assertEqual(select_strategy("test_long").name, "test_long")
        quick = select_strategy("quick_scalp", "every_candle")
        self.assertIsInstance(quick, QuickScalpStrategy)
        self.assertEqual(quick.entry_mode, "every_candle")

    def test_selected_environment_overrides_base_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            selected = Path(directory) / ".env.scalp"
            selected.write_text(
                "ACTIVE_STRATEGY=quick_scalp\nCONTRACTS=150\nENTRY_MODE=alternating\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                loaded = load_environment(str(selected))
                self.assertEqual(loaded, selected.resolve())
                self.assertEqual(os.environ["ACTIVE_STRATEGY"], "quick_scalp")
                self.assertEqual(os.environ["CONTRACTS"], "150")

    def test_account_instance_lock_prevents_two_bots(self) -> None:
        lock = AccountInstanceLock(987654321)
        try:
            with self.assertRaises(ProjectXError):
                AccountInstanceLock(987654321)
        finally:
            lock.close()
        replacement = AccountInstanceLock(987654321)
        replacement.close()


if __name__ == "__main__":
    unittest.main()