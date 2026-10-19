from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, timezone
from decimal import Decimal

from execution import (
    LONG_POSITION,
    POSITION_BRACKETS_API_UNSUPPORTED,
    SHORT_POSITION,
    ExecutionManager,
    TradingConfig,
)
from strategies.ema_cross import StrategyResult
from topstep_api import Candle, ProjectXError

UTC = timezone.utc
CONTRACT = {
    "id": "CON.F.US.MNQ.TEST",
    "symbolId": "F.US.MNQ",
    "tickSize": 0.25,
    "tickValue": 0.5,
}
ACCOUNT = {
    "id": 12345678,
    "name": "PRAC-V2-TEST-ACCOUNT",
    "canTrade": True,
    "isVisible": True,
    "simulated": True,
    "_simulatedVerified": True,
}


def config(**changes) -> TradingConfig:
    values = {
        "enabled": True,
        "practice_only": True,
        "contracts": 1,
        "strategy_name": "test_long",
        "stop_loss_points": Decimal("10"),
        "take_profit_points": None,
        "broker_max_contracts": None,
        "trail_activation_points": Decimal("3"),
        "trail_distance_ticks": 4,
        "trail_management_mode": "native_delayed",
        "daily_loss_limit_enabled": True,
        "max_daily_loss_usd": Decimal("100"),
        "max_open_positions": 1,
        "bracket_mode": "auto_oco",
        "emergency_halt": False,
        "execution_poll_interval_seconds": 0.5,
        "errors": (),
    }
    values.update(changes)
    return TradingConfig(**values)


def signal_result(signal: str = "LONG_CROSS") -> StrategyResult:
    bar = Candle(
        datetime(2026, 10, 14, 13, 45, tzinfo=UTC),
        30600, 30601, 30599, 30600.25, 100,
    )
    return StrategyResult(bar, 30602.5, 30610.75, "BEARISH", signal)


def position(kind: int, entry: float = 100, size: int = 1) -> dict:
    return {
        "id": 11,
        "contractId": CONTRACT["id"],
        "type": kind,
        "size": size,
        "averagePrice": entry,
    }


def stop(
    order_id: int, side: int, price: float, size: int = 1
) -> dict:
    return {
        "id": order_id,
        "accountId": ACCOUNT["id"],
        "contractId": CONTRACT["id"],
        "creationTimestamp": "2026-10-14T14:54:02.600Z",
        "type": 4,
        "side": side,
        "size": size,
        "stopPrice": price,
        "status": 1,
    }


def quote(
    best_bid: float, best_ask: float, *, age: float = 0,
    timestamp: str = "2026-10-14T14:54:10Z",
) -> dict:
    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "ageSeconds": age,
        "timestamp": timestamp,
    }


class FakeApi:
    def __init__(self) -> None:
        self.positions_response = []
        self.open_orders_response = []
        self.orders_response = []
        self.trades_response = []
        self.place_response = {
            "success": False, "errorCode": 2,
            "errorMessage": "mock rejection", "orderId": 42,
        }
        self.place_error: Exception | None = None
        self.place_calls: list[dict] = []
        self.cancel_calls: list[dict] = []
        self.cancel_response = {"success": True, "errorCode": 0}
        self.cancel_error: Exception | None = None
        self.apply_cancel_to_open_orders = True
        self.open_positions_sequence: list[list[dict]] = []
        self.open_orders_sequence: list[list[dict]] = []
        self.orders_sequence: list[list[dict]] = []
        self.trades_sequence: list[list[dict]] = []
        self.quote_response: dict | None = None
        self.close_calls: list[dict] = []
        self.close_response = {"success": True, "errorCode": 0}

    async def open_positions(self, account_id):
        if self.open_positions_sequence:
            return self.open_positions_sequence.pop(0)
        return self.positions_response

    async def open_orders(self, account_id):
        if self.open_orders_sequence:
            return self.open_orders_sequence.pop(0)
        return self.open_orders_response

    async def orders(self, account_id, start, end=None):
        if self.orders_sequence:
            return self.orders_sequence.pop(0)
        return self.orders_response

    async def trades(self, account_id, start, end=None):
        if self.trades_sequence:
            return self.trades_sequence.pop(0)
        return self.trades_response

    async def place_order_once(self, payload):
        self.place_calls.append(payload)
        if self.place_error:
            raise self.place_error
        return self.place_response

    def quote_snapshot(self):
        return self.quote_response

    async def close_position_once(self, payload):
        self.close_calls.append(payload)
        return self.close_response
    async def cancel_order_once(self, payload):
        self.cancel_calls.append(payload)
        if self.cancel_error:
            raise self.cancel_error
        if self.cancel_response.get("success") and self.apply_cancel_to_open_orders:
            self.open_orders_response = [
                order for order in self.open_orders_response
                if order.get("id") != payload["orderId"]
            ]
        return self.cancel_response



class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.account_environment = patch.dict(
            os.environ,
            {
                "VERIFIED_PRACTICE_ACCOUNT_ID": str(ACCOUNT["id"]),
                "VERIFIED_PRACTICE_ACCOUNT_NAME": ACCOUNT["name"],
            },
        )
        self.account_environment.start()
        self.addCleanup(self.account_environment.stop)

    def manager(
        self, api: FakeApi | None = None, **config_changes
    ) -> ExecutionManager:
        return ExecutionManager(
            api or FakeApi(), ACCOUNT, CONTRACT, config(**config_changes)
        )

    def test_poll_interval_defaults_to_half_second(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            parsed = TradingConfig.from_env()
        self.assertEqual(parsed.execution_poll_interval_seconds, 0.5)

    async def test_monitor_uses_configured_half_second_interval(self) -> None:
        manager = self.manager()
        with (
            patch.object(
                manager, "monitor_once", AsyncMock(return_value="FLAT")
            ),
            patch(
                "execution.asyncio.sleep",
                AsyncMock(side_effect=asyncio.CancelledError),
            ) as sleep_mock,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await manager.monitor()
        sleep_mock.assert_awaited_once_with(0.5)

    def test_practice_account_restriction(self) -> None:
        self.assertTrue(ExecutionManager.practice_account_eligible(ACCOUNT))
        unsafe_accounts = (
            {**ACCOUNT, "id": 28504417},
            {**ACCOUNT, "name": "PRAC-V2-647233-OTHER"},
            {**ACCOUNT, "canTrade": False},
            {**ACCOUNT, "isVisible": False},
            {**ACCOUNT, "simulated": False},
            {**ACCOUNT, "_simulatedVerified": False},
        )
        for account in unsafe_accounts:
            with self.subTest(account=account):
                self.assertFalse(
                    ExecutionManager.practice_account_eligible(account)
                )

    def test_practice_account_identity_is_read_from_environment(self) -> None:
        updated_account = {
            **ACCOUNT,
            "id": 87654321,
            "name": "PRAC-V2-UPDATED-ACCOUNT",
        }
        with patch.dict(
            os.environ,
            {
                "VERIFIED_PRACTICE_ACCOUNT_ID": str(updated_account["id"]),
                "VERIFIED_PRACTICE_ACCOUNT_NAME": updated_account["name"],
            },
        ):
            self.assertTrue(
                ExecutionManager.practice_account_eligible(updated_account)
            )
            self.assertFalse(
                ExecutionManager.practice_account_eligible(ACCOUNT)
            )

    def test_explicit_verification_requires_exact_identity_and_confirmation(self) -> None:
        self.assertTrue(
            ExecutionManager.explicit_verification_valid(
                ACCOUNT,
                str(ACCOUNT["id"]),
                ACCOUNT["name"],
                True,
            )
        )
        self.assertFalse(
            ExecutionManager.explicit_verification_valid(
                ACCOUNT,
                str(ACCOUNT["id"]),
                ACCOUNT["name"],
                False,
            )
        )
        self.assertFalse(
            ExecutionManager.explicit_verification_valid(
                ACCOUNT,
                str(ACCOUNT["id"]),
                "PRAC-V2-647233-WRONG",
                True,
            )
        )
        self.assertFalse(
            ExecutionManager.explicit_verification_valid(
                ACCOUNT,
                "99999999",
                ACCOUNT["name"],
                True,
            )
        )

    def test_rest_identity_without_independent_proof_fails_closed(self) -> None:
        rest_only = {
            "id": ACCOUNT["id"],
            "name": ACCOUNT["name"],
            "canTrade": True,
            "isVisible": True,
        }
        self.assertFalse(
            ExecutionManager.practice_account_eligible(rest_only)
        )
        self.assertFalse(
            ExecutionManager.practice_account_eligible(
                {**rest_only, "simulated": True}
            )
        )

    def test_bracket_mode_and_tick_conversion(self) -> None:
        manager = self.manager()
        self.assertEqual(manager.stop_ticks, 40)
        self.assertEqual(manager.activation_ticks, 12)
        self.assertEqual(manager.trail_ticks, 4)
        self.assertEqual(manager.readiness_errors(), [])
        blocked = self.manager()
        blocked.config = config(bracket_mode="position", errors=())
        self.assertIn(
            POSITION_BRACKETS_API_UNSUPPORTED, blocked.readiness_errors()
        )

    def test_position_brackets_cannot_build_auto_oco_entry_payload(self) -> None:
        manager = ExecutionManager(
            FakeApi(), ACCOUNT, CONTRACT, config(bracket_mode="position")
        )
        with self.assertRaisesRegex(ProjectXError, "BRACKET_MODE=position"):
            manager.build_entry_payload(0, "position-bracket-test")

    async def test_disabled_signal_logs_exact_reason(self) -> None:
        manager = ExecutionManager(
            FakeApi(), ACCOUNT, CONTRACT, config(enabled=False)
        )
        with self.assertLogs(level="WARNING") as captured:
            outcome = await manager.signal(
                signal_result(), entry_eligible=True, market_fresh=True
            )
        self.assertEqual(
            outcome, "DISABLED reasons=ORDER_EXECUTION_DISABLED"
        )
        self.assertIn(
            "reasons=ORDER_EXECUTION_DISABLED", "\n".join(captured.output)
        )

    async def test_disabled_signal_logs_secondary_safety_blockers(self) -> None:
        manager = ExecutionManager(
            FakeApi(),
            ACCOUNT,
            CONTRACT,
            config(
                enabled=False,
                bracket_mode="position",
                emergency_halt=True,
            ),
        )
        with self.assertLogs(level="WARNING") as captured:
            outcome = await manager.signal(
                signal_result(), entry_eligible=True, market_fresh=True
            )
        for reason in (
            "ORDER_EXECUTION_DISABLED",
            "BRACKET_MODE_POSITION_UNSUPPORTED",
            "EMERGENCY_HALT_ACTIVE",
        ):
            self.assertIn(reason, outcome)
            self.assertIn(reason, "\n".join(captured.output))

    async def test_safety_block_logs_all_exact_reasons(self) -> None:
        manager = ExecutionManager(
            FakeApi(),
            ACCOUNT,
            CONTRACT,
            config(bracket_mode="position", emergency_halt=True),
        )
        with self.assertLogs(level="WARNING") as captured:
            outcome = await manager.signal(
                signal_result(), entry_eligible=True, market_fresh=True
            )
        self.assertIn("BRACKET_MODE_POSITION_UNSUPPORTED", outcome)
        self.assertIn("EMERGENCY_HALT_ACTIVE", outcome)
        log = "\n".join(captured.output)
        self.assertIn("SIGNAL SAFETY BLOCKED", log)
        self.assertIn("BRACKET_MODE_POSITION_UNSUPPORTED", log)
        self.assertIn("EMERGENCY_HALT_ACTIVE", log)

    async def test_position_brackets_block_long_and_short_before_order_api(self) -> None:
        for signal in ("LONG_CROSS", "SHORT_CROSS"):
            with self.subTest(signal=signal):
                api = FakeApi()
                manager = ExecutionManager(
                    api,
                    ACCOUNT,
                    CONTRACT,
                    config(
                        bracket_mode="position",
                        trail_management_mode="native_delayed",
                    ),
                )
                outcome = await manager.signal(
                    signal_result(signal),
                    entry_eligible=True,
                    market_fresh=True,
                )
                self.assertIn(
                    "BRACKET_MODE_POSITION_UNSUPPORTED", outcome
                )
                self.assertEqual(api.place_calls, [])

    def test_configured_entry_quantities_for_long_and_short(self) -> None:
        for quantity in (1, 5, 10, 25):
            manager = ExecutionManager(
                FakeApi(), ACCOUNT, CONTRACT, config(contracts=quantity)
            )
            self.assertNotIn(
                "CONTRACTS must be a positive integer",
                manager.readiness_errors(),
            )
            for side, expected_ticks in ((0, -40), (1, 40)):
                with self.subTest(quantity=quantity, side=side):
                    payload = manager.build_entry_payload(
                        side, f"quantity-{quantity}-{side}"
                    )
                    self.assertEqual(payload["size"], quantity)
                    self.assertEqual(
                        payload["stopLossBracket"],
                        {"ticks": expected_ticks, "type": 4},
                    )

    def test_native_trailing_uses_actual_position_quantity(self) -> None:
        for quantity in (1, 5, 10, 25):
            manager = ExecutionManager(
                FakeApi(), ACCOUNT, CONTRACT, config(contracts=25)
            )
            for position_type, side, last_price in (
                (LONG_POSITION, 1, Decimal("103")),
                (SHORT_POSITION, 0, Decimal("97")),
            ):
                with self.subTest(
                    quantity=quantity, position_type=position_type
                ):
                    payload = manager.native_trailing_payload(
                        position(position_type, 100, quantity),
                        last_price,
                    )
                    self.assertEqual(payload["size"], quantity)
                    self.assertEqual(payload["side"], side)

    def test_invalid_contract_quantities_fail_validation(self) -> None:
        for raw in ("0", "-1", "1.5", "invalid", ""):
            with self.subTest(raw=raw):
                with patch.dict(
                    os.environ, {"CONTRACTS": raw}, clear=True
                ):
                    parsed = TradingConfig.from_env()
                self.assertEqual(parsed.contracts, 0)
                self.assertIn(
                    "CONTRACTS must be an integer >= 1", parsed.errors
                )
        for invalid in (0, -1, 1.5, True):
            with self.subTest(invalid=invalid):
                manager = ExecutionManager(
                    FakeApi(), ACCOUNT, CONTRACT,
                    config(contracts=invalid),
                )
                self.assertIn(
                    "CONTRACTS must be a positive integer",
                    manager.readiness_errors(),
                )

    async def test_terminal_partial_fill_uses_actual_position_and_stop_size(self) -> None:
        api = FakeApi()
        tag = "partial-entry"
        api.orders_response = [{
            "id": 90,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "creationTimestamp": "2026-10-14T14:54:02.500Z",
            "status": 3,
            "type": 2,
            "side": 0,
            "size": 25,
            "fillVolume": 5,
            "filledPrice": 30423.5,
            "customTag": tag,
        }]
        api.positions_response = [position(LONG_POSITION, 30423.5, 5)]
        api.open_orders_response = [
            stop(91, 1, 30413.5, size=5)
        ]
        manager = ExecutionManager(
            api, ACCOUNT, CONTRACT, config(contracts=25)
        )

        outcome = await manager.verify_entry(90, tag, set())

        self.assertEqual(outcome, "FILLED_PROTECTED")
        self.assertEqual(manager.confirmed_position_size, 5)
        self.assertEqual(manager.confirmed_stop_order_id, 91)

    async def test_filled_entry_already_flat_does_not_persistently_halt(self) -> None:
        api = FakeApi()
        tag = "fast-flat-entry"
        api.orders_response = [{
            "id": 93,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "creationTimestamp": "2026-10-14T14:54:02.500Z",
            "status": 2,
            "type": 2,
            "side": 0,
            "size": 1,
            "fillVolume": 1,
            "filledPrice": 30423.5,
            "customTag": tag,
        }]
        api.positions_response = []
        api.open_orders_response = []
        manager = self.manager(api)

        loop = Mock()
        loop.time.side_effect = [0, 0, 21]
        with (
            patch("execution.asyncio.get_running_loop", return_value=loop),
            patch("execution.asyncio.sleep", AsyncMock()),
        ):
            outcome = await manager.verify_entry(93, tag, set())

        self.assertEqual(outcome, "FILLED_FLAT")
        self.assertIsNone(manager.halted_reason)

    async def test_broker_position_limit_rejection_is_not_locally_capped(self) -> None:
        api = FakeApi()
        api.place_response = {
            "success": False,
            "errorCode": 4,
            "errorMessage": "mock broker position limit",
            "orderId": 92,
        }
        manager = ExecutionManager(
            api, ACCOUNT, CONTRACT, config(contracts=25)
        )
        outcome = await manager.signal(
            signal_result("SHORT_CROSS"),
            entry_eligible=True,
            market_fresh=True,
        )
        self.assertEqual(outcome, "ORDER_REJECTED")
        self.assertEqual(api.place_calls[0]["size"], 25)
        self.assertIsNone(manager.halted_reason)

    async def test_position_size_change_detects_oversized_tracked_exit(self) -> None:
        api = FakeApi()
        pos = position(LONG_POSITION, 100, 5)
        hard = stop(93, 1, 90, size=5)
        oversized_trailing = {
            **stop(94, 1, 99, size=10),
            "type": 5,
            "stopPrice": 99,
            "trailPrice": 1,
        }
        api.positions_response = [pos]
        api.open_orders_response = [hard, oversized_trailing]
        manager = ExecutionManager(
            api, ACCOUNT, CONTRACT, config(contracts=10)
        )
        manager.last_positions = [pos]
        manager.confirmed_stop_order_id = 93
        manager.native_trailing_order_id = 94
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 10

        outcome = await manager.monitor_once()

        self.assertEqual(outcome, "PROTECTIVE_EXIT_SIZE_MISMATCH")
        self.assertIsNotNone(manager.halted_reason)
        self.assertIn("positionSize=5", manager.halted_reason)
        self.assertIn("orderSize=10", manager.halted_reason)

    async def test_closed_partial_position_cancels_oversized_exit_by_exact_id(self) -> None:
        api = FakeApi()
        oversized_hard = stop(95, 1, 90, size=25)
        api.open_orders_response = [oversized_hard]
        api.orders_response = [{
            **stop(96, 1, 103, size=5),
            "type": 5,
            "status": 2,
            "stopPrice": 103,
            "trailPrice": 1,
            "filledPrice": 103,
        }]
        api.trades_response = [{
            "orderId": 96,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "profitAndLoss": 10,
            "fees": 1,
            "voided": False,
        }]
        manager = ExecutionManager(
            api, ACCOUNT, CONTRACT, config(contracts=25)
        )
        manager.last_positions = [position(LONG_POSITION, 100, 5)]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 5
        manager.confirmed_stop_order_id = 95
        manager.native_trailing_order_id = 96

        outcome = await manager.monitor_once()

        self.assertEqual(outcome, "POSITION_CLOSED")
        self.assertEqual(
            api.cancel_calls,
            [{"accountId": ACCOUNT["id"], "orderId": 95}],
        )
        self.assertEqual(api.open_orders_response, [])

    def test_only_native_delayed_execution_mode_is_supported(self) -> None:
        manager = ExecutionManager(
            FakeApi(), ACCOUNT, CONTRACT,
            config(trail_management_mode="hard_stop_only"),
        )
        self.assertIn(
            "TRAIL_MANAGEMENT_MODE must be native_delayed",
            manager.readiness_errors(),
        )

    async def test_long_native_trailing_activates_at_three_points(self) -> None:
        api = FakeApi()
        pos = position(LONG_POSITION, 100)
        hard = stop(61, 1, 90)
        native = {
            **stop(62, 1, 102),
            "type": 5,
            "stopPrice": None,
            "trailPrice": 1,
        }
        api.positions_response = [pos]
        api.open_orders_response = [hard]
        api.quote_response = {
            "lastPrice": 103, "bestBid": 102.75, "bestAsk": 103.25,
            "ageSeconds": 0, "timestamp": "2026-10-14T14:54:10Z",
        }
        api.place_response = {
            "success": True, "errorCode": 0,
            "errorMessage": None, "orderId": 62,
        }
        api.open_orders_sequence = [[hard], [hard, native]]
        manager = self.manager(api)
        manager.last_positions = [pos]
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "NATIVE_TRAILING_ACTIVE")
        payload = api.place_calls[0]
        self.assertEqual(payload["type"], 5)
        self.assertEqual(payload["side"], 1)
        self.assertEqual(payload["trailPrice"], 102.0)
        self.assertEqual(manager.native_trailing_order_id, 62)

    async def test_short_native_trailing_activates_at_three_points(self) -> None:
        api = FakeApi()
        pos = position(SHORT_POSITION, 100)
        hard = stop(71, 0, 110)
        native = {
            **stop(72, 0, 98),
            "type": 5,
            "stopPrice": None,
            "trailPrice": 1,
        }
        api.positions_response = [pos]
        api.open_orders_response = [hard]
        api.quote_response = {
            "lastPrice": 97, "bestBid": 96.75, "bestAsk": 97.25,
            "ageSeconds": 0, "timestamp": "2026-10-14T14:54:10Z",
        }
        api.place_response = {
            "success": True, "errorCode": 0,
            "errorMessage": None, "orderId": 72,
        }
        api.open_orders_sequence = [[hard], [hard, native]]
        manager = self.manager(api)
        manager.last_positions = [pos]
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "NATIVE_TRAILING_ACTIVE")
        payload = api.place_calls[0]
        self.assertEqual(payload["side"], 0)
        self.assertEqual(payload["trailPrice"], 98.0)

    async def test_one_tick_mismatch_closure_clears_scoped_halt_and_allows_next_entry(self) -> None:
        api = FakeApi()
        pos = {
            **position(SHORT_POSITION, 30233.75),
            "id": 877998769,
        }
        hard = stop(3641447123, 0, 30243.75)
        native_open = {
            **stop(3641449862, 0, 30230.5),
            "type": 5,
            "stopPrice": 30230.5,
            "trailPrice": 0.25,
            "customTag": "mnq-trail-position-877998769",
        }
        api.positions_response = [pos]
        api.open_orders_response = [hard]
        api.quote_response = {
            "lastPrice": 30229.5,
            "bestBid": 30229.25,
            "bestAsk": 30229.75,
            "ageSeconds": 0,
            "timestamp": "2026-10-14T16:23:41Z",
        }
        api.place_response = {
            "success": True,
            "errorCode": 0,
            "errorMessage": None,
            "orderId": 3641449862,
        }
        api.open_positions_sequence = [[pos], [pos]]
        api.open_orders_sequence = [[hard], [hard, native_open]]
        manager = self.manager(api)
        manager.last_positions = [pos]

        with self.assertLogs(level="WARNING") as mismatch_log:
            activation = await manager.monitor_once()
        self.assertEqual(
            activation,
            "NATIVE_TRAILING_DISTANCE_MISMATCH_POSITION_ACTIVE",
        )
        payload = api.place_calls[0]
        self.assertEqual(payload["side"], 0)
        self.assertEqual(payload["trailPrice"], 30230.5)
        self.assertEqual(
            manager.trailing_requested_distance, Decimal("1.00")
        )
        self.assertEqual(
            manager.trailing_actual_distance, Decimal("0.25")
        )
        self.assertIsNone(manager.halted_reason)
        log = "\n".join(mismatch_log.output)
        self.assertIn("requestedTicks=4", log)
        self.assertIn("actualTicks=1", log)

        # Reproduce the old persistent halt so closure must clear it too.
        manager.halted_reason = (
            "native trailing distance mismatch: positionId=877998769 "
            "orderId=3641449862 expected=1.00 actual=0.25"
        )
        manager.halted_code = "NATIVE_TRAILING_DISTANCE_MISMATCH"
        api.positions_response = []
        api.open_orders_response = [hard]
        api.orders_response = [{
            **native_open,
            "status": 2,
            "filledPrice": 30230.5,
        }]
        api.trades_response = [{
            "orderId": 3641449862,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "profitAndLoss": 6.64,
            "fees": 0.50,
            "voided": False,
        }]
        closed = await manager.monitor_once()
        self.assertEqual(closed, "POSITION_CLOSED")
        self.assertEqual(
            api.cancel_calls,
            [{"accountId": ACCOUNT["id"], "orderId": 3641447123}],
        )
        self.assertIsNone(manager.halted_reason)
        self.assertIsNone(manager.trailing_distance_mismatch_position_id)

        api.place_response = {
            "success": False,
            "errorCode": 2,
            "errorMessage": "next mocked entry reached broker",
            "orderId": 99,
        }
        next_result = StrategyResult(
            Candle(
                datetime(2026, 10, 14, 16, 24, tzinfo=UTC),
                30218, 30219, 30217, 30218.5, 100,
            ),
            30218.5,
            30218.5,
            "TEST_ALTERNATING",
            "LONG_CROSS",
        )
        next_outcome = await manager.signal(
            next_result, entry_eligible=True, market_fresh=True
        )
        self.assertEqual(next_outcome, "ORDER_REJECTED")
        self.assertEqual(api.place_calls[-1]["side"], 0)

    async def test_server_price_movement_mismatch_keeps_open_position_resolved(self) -> None:
        api = FakeApi()
        pos = position(LONG_POSITION, 100)
        hard = stop(61, 1, 90)
        native = {
            **stop(62, 1, 102),
            "type": 5,
            "stopPrice": 102,
            "trailPrice": 0.25,
        }
        api.positions_response = [pos]
        api.open_orders_response = [hard]
        api.quote_response = {
            "lastPrice": 103,
            "bestBid": 102.75,
            "bestAsk": 103.25,
            "ageSeconds": 0,
            "timestamp": "2026-10-14T14:54:10Z",
        }
        api.place_response = {
            "success": True, "errorCode": 0,
            "errorMessage": None, "orderId": 62,
        }
        api.open_positions_sequence = [[pos], [pos]]
        api.open_orders_sequence = [[hard], [hard, native]]
        manager = self.manager(api)
        manager.last_positions = [pos]
        outcome = await manager.monitor_once()
        self.assertEqual(
            outcome,
            "NATIVE_TRAILING_DISTANCE_MISMATCH_POSITION_ACTIVE",
        )
        self.assertIsNone(manager.halted_reason)

        # The position still blocks another entry; its mismatch is not ignored.
        blocked = await manager.signal(
            signal_result("SHORT_CROSS"),
            entry_eligible=True,
            market_fresh=True,
        )
        self.assertEqual(blocked, "POSITION_EXISTS")
        self.assertEqual(len(api.place_calls), 1)

    async def test_native_trailing_waits_before_three_points(self) -> None:
        api = FakeApi()
        pos = position(LONG_POSITION, 100)
        api.positions_response = [pos]
        api.open_orders_response = [stop(61, 1, 90)]
        api.quote_response = {
            "lastPrice": 102.75, "bestBid": 102.5, "bestAsk": 103,
            "ageSeconds": 0, "timestamp": "2026-10-14T14:54:10Z",
        }
        manager = self.manager(api)
        manager.last_positions = [pos]
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "WAITING_FOR_TRAIL_ACTIVATION")
        self.assertEqual(api.place_calls, [])

    async def test_native_trailing_is_submitted_only_once_per_position(self) -> None:
        api = FakeApi()
        pos = position(LONG_POSITION, 100)
        hard = stop(61, 1, 90)
        api.positions_response = [pos]
        api.open_orders_response = [hard]
        api.quote_response = {
            "lastPrice": 103, "bestBid": 102.75, "bestAsk": 103.25,
            "ageSeconds": 0, "timestamp": "2026-10-14T14:54:10Z",
        }
        api.place_response = {
            "success": False, "errorCode": 2,
            "errorMessage": "mock trailing rejection", "orderId": 62,
        }
        manager = self.manager(api)
        manager.last_positions = [pos]
        first = await manager.monitor_once()
        second = await manager.monitor_once()
        self.assertEqual(first, "NATIVE_TRAILING_REJECTED_HARD_STOP_ACTIVE")
        self.assertEqual(second, "NATIVE_TRAILING_ALREADY_ATTEMPTED")
        self.assertEqual(len(api.place_calls), 1)

    async def test_reconnect_does_not_resubmit_prior_trailing_attempt(self) -> None:
        api = FakeApi()
        pos = position(LONG_POSITION, 100)
        api.positions_response = [pos]
        api.open_orders_response = [stop(61, 1, 90)]
        api.quote_response = {
            "lastPrice": 103, "bestBid": 102.75, "bestAsk": 103.25,
            "ageSeconds": 0, "timestamp": "2026-10-14T14:54:10Z",
        }
        api.orders_response = [{
            "id": 62,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "type": 5,
            "side": 1,
            "size": 1,
            "status": 5,
            "customTag": "mnq-trail-position-11",
        }]
        manager = self.manager(api)
        manager.last_positions = [pos]
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "NATIVE_TRAILING_PRIOR_ATTEMPT_FOUND")
        self.assertEqual(api.place_calls, [])

    async def test_long_entry_sends_negative_stop_ticks(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        outcome = await manager.signal(
            signal_result("LONG_CROSS"),
            entry_eligible=True,
            market_fresh=True,
        )
        self.assertEqual(outcome, "ORDER_REJECTED")
        payload = api.place_calls[0]
        self.assertEqual(payload["side"], 0)
        self.assertEqual(payload["stopLossBracket"], {"ticks": -40, "type": 4})
        self.assertIsNone(payload["takeProfitBracket"])

    async def test_short_entry_sends_positive_stop_ticks(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        outcome = await manager.signal(
            signal_result("SHORT_CROSS"),
            entry_eligible=True,
            market_fresh=True,
        )
        self.assertEqual(outcome, "ORDER_REJECTED")
        payload = api.place_calls[0]
        self.assertEqual(payload["side"], 1)
        self.assertEqual(payload["stopLossBracket"], {"ticks": 40, "type": 4})
        self.assertIsNone(payload["takeProfitBracket"])

    async def test_invalid_bracket_sign_is_blocked_before_submission(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        invalid_payload = {
            "side": 0,
            "stopLossBracket": {"ticks": 40, "type": 4},
        }
        with patch.object(
            manager, "build_entry_payload", return_value=invalid_payload
        ):
            outcome = await manager.signal(
                signal_result("LONG_CROSS"),
                entry_eligible=True,
                market_fresh=True,
            )
        self.assertEqual(outcome, "BRACKET_INVALID")
        self.assertEqual(api.place_calls, [])
        self.assertIsNotNone(manager.halted_reason)

    async def test_new_long_initial_stop_is_exactly_ten_points_below_entry(self) -> None:
        api = FakeApi()
        api.orders_response = [
            {
                "id": 3640834174,
                "accountId": ACCOUNT["id"],
                "contractId": CONTRACT["id"],
                "creationTimestamp": "2026-10-14T14:54:02.500Z",
                "status": 2,
                "type": 2,
                "side": 0,
                "fillVolume": 1,
                "filledPrice": 30423.5,
                "customTag": "test-entry",
            }
        ]
        api.positions_response = [position(1, 30423.5)]
        api.open_orders_response = [stop(3640834175, 1, 30413.5)]
        manager = self.manager(api)
        outcome = await manager.verify_entry(
            3640834174, "test-entry", set()
        )
        self.assertEqual(outcome, "FILLED_PROTECTED")
        self.assertEqual(manager.confirmed_stop_order_id, 3640834175)
        self.assertEqual(manager.confirmed_stop_price, Decimal("30413.5"))
        self.assertEqual(
            Decimal(str(api.positions_response[0]["averagePrice"]))
            - manager.confirmed_stop_price,
            Decimal("10.0"),
        )
        self.assertIsNone(manager.halted_reason)

    async def test_new_short_initial_stop_is_exactly_ten_points_above_entry(self) -> None:
        api = FakeApi()
        api.orders_response = [
            {
                "id": 70,
                "accountId": ACCOUNT["id"],
                "contractId": CONTRACT["id"],
                "creationTimestamp": "2026-10-14T14:54:02.500Z",
                "status": 2,
                "type": 2,
                "side": 1,
                "fillVolume": 1,
                "filledPrice": 30423.5,
                "customTag": "short-entry",
            }
        ]
        api.positions_response = [position(2, 30423.5)]
        api.open_orders_response = [stop(71, 0, 30433.5)]
        manager = self.manager(api)
        outcome = await manager.verify_entry(70, "short-entry", set())
        self.assertEqual(outcome, "FILLED_PROTECTED")
        self.assertEqual(manager.confirmed_stop_price, Decimal("30433.5"))
        self.assertEqual(
            manager.confirmed_stop_price
            - Decimal(str(api.positions_response[0]["averagePrice"])),
            Decimal("10.0"),
        )
    async def test_already_modified_stop_cannot_pass_initial_stop_check(self) -> None:
        api = FakeApi()
        api.orders_response = [
            {
                "id": 3640834174,
                "accountId": ACCOUNT["id"],
                "contractId": CONTRACT["id"],
                "creationTimestamp": "2026-10-14T14:54:02.500Z",
                "status": 2,
                "type": 2,
                "side": 0,
                "fillVolume": 1,
                "filledPrice": 30423.5,
                "customTag": "test-entry",
            }
        ]
        api.positions_response = [position(1, 30423.5)]
        api.open_orders_response = [stop(3640834175, 1, 30424.75)]
        manager = self.manager(api)
        outcome = await manager.verify_entry(
            3640834174, "test-entry", set()
        )
        self.assertEqual(outcome, "INITIAL_PROTECTION_MISMATCH")
        self.assertEqual(manager.halted_code, "INITIAL_PROTECTION_MISMATCH")

    def test_pre_entry_stop_id_is_never_associated_with_new_position(self) -> None:
        manager = self.manager()
        old_stop = stop(99, 1, 90)
        selected = manager.find_protective_stop(
            position(1), [old_stop], excluded_order_ids={99}
        )
        self.assertIsNone(selected)

    def test_directional_initial_protective_stop_prices(self) -> None:
        manager = self.manager()
        self.assertEqual(
            manager.initial_protective_stop_price(30605, 1),
            Decimal("30595"),
        )
        self.assertEqual(
            manager.initial_protective_stop_price(30605, 2),
            Decimal("30615"),
        )




    async def test_reconnect_resumes_from_broker_stop(self) -> None:
        api = FakeApi()
        api.positions_response = [position(2, 30605)]
        api.open_orders_response = [stop(24, 0, 30615)]
        manager = self.manager(api)
        await manager.startup_check()
        self.assertIsNone(manager.halted_reason)
        self.assertEqual(manager.confirmed_stop_price, Decimal("30615"))
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "WAITING_FOR_FRESH_QUOTE")
        self.assertEqual(api.place_calls, [])
        self.assertEqual(api.cancel_calls, [])












    async def test_position_closure_resets_trailing_state(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        manager.last_positions = [position(1)]
        manager.confirmed_stop_price = Decimal("103")
        manager.confirmed_stop_order_id = 27
        api.orders_response = [
            {
                **stop(27, 1, 103),
                "status": 2,
                "filledPrice": 103,
                "updateTimestamp": "2026-10-14T13:50:00Z",
            }
        ]
        api.trades_response = [
            {
                "orderId": 27, "profitAndLoss": 6, "fees": 1,
                "voided": False,
            }
        ]
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "POSITION_CLOSED")
        self.assertIsNone(manager.confirmed_stop_price)


    async def test_unconfirmed_closure_halts(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        manager.last_positions = [position(1)]
        manager.confirmed_stop_order_id = 40
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "CLOSURE_UNCONFIRMED")
        self.assertIsNotNone(manager.halted_reason)
    async def test_unconfirmed_closure_logs_broker_order_status_evidence(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        manager.last_positions = [position(LONG_POSITION, 100)]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 71
        api.orders_response = [
            {
                **stop(71, 1, 90),
                "status": 3,
                "updateTimestamp": "2026-10-14T15:38:50Z",
            }
        ]
        with self.assertLogs(level="ERROR") as captured:
            outcome = await manager.monitor_once()
        self.assertEqual(outcome, "CLOSURE_UNCONFIRMED")
        evidence = "\n".join(captured.output)
        self.assertIn("CLOSURE RECONCILIATION EVIDENCE", evidence)
        self.assertIn("trackedHardStopOrderId=71", evidence)
        self.assertIn("'status': 3", evidence)
        self.assertNotIn("Authorization", evidence)

    async def test_long_position_keeps_single_hard_stop_without_native_oco(self) -> None:
        api = FakeApi()
        api.positions_response = [position(LONG_POSITION, 100)]
        api.open_orders_response = [stop(61, 1, 90)]
        manager = self.manager(api)
        manager.last_positions = api.positions_response
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "WAITING_FOR_FRESH_QUOTE")
        self.assertEqual(manager.confirmed_stop_order_id, 61)
        self.assertEqual(api.place_calls, [])

    async def test_short_position_keeps_single_hard_stop_without_native_oco(self) -> None:
        api = FakeApi()
        api.positions_response = [position(SHORT_POSITION, 100)]
        api.open_orders_response = [stop(62, 0, 110)]
        manager = self.manager(api)
        manager.last_positions = api.positions_response
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "WAITING_FOR_FRESH_QUOTE")
        self.assertEqual(manager.confirmed_stop_order_id, 62)
        self.assertEqual(api.place_calls, [])

    def test_native_distance_is_four_ticks(self) -> None:
        manager = self.manager()
        self.assertEqual(manager.trail_ticks, 4)
        self.assertEqual(
            Decimal(manager.trail_ticks) * manager.tick_size,
            Decimal("1.00"),
        )

    async def test_existing_native_trailing_is_reconciled_without_duplicate(self) -> None:
        api = FakeApi()
        hard_stop = stop(63, 1, 90)
        native = {
            **stop(64, 1, 99),
            "type": 5,
            "stopPrice": None,
            "trailPrice": 1,
        }
        api.positions_response = [position(LONG_POSITION, 100)]
        api.open_orders_response = [hard_stop, native]
        manager = self.manager(api)
        manager.last_positions = api.positions_response
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "NATIVE_TRAILING_ACTIVE")
        self.assertEqual(manager.native_trailing_order_id, 64)
        self.assertEqual(api.place_calls, [])

    async def test_position_close_cancels_only_tracked_orphan_and_confirms(self) -> None:
        api = FakeApi()
        tracked = stop(65, 1, 90)
        unrelated = stop(66, 1, 80)
        api.open_orders_response = [tracked, unrelated]
        manager = self.manager(api)
        manager.last_positions = [position(LONG_POSITION, 100)]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 65
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "CLOSURE_UNCONFIRMED")
        self.assertEqual(api.cancel_calls, [{"accountId": ACCOUNT["id"], "orderId": 65}])
        self.assertEqual([o["id"] for o in api.open_orders_response], [66])

    async def test_reconcile_does_not_erase_position_transition_state(self) -> None:
        api = FakeApi()
        tracked_position = {
            **position(LONG_POSITION, 30285.5),
            "id": 878027738,
        }
        manager = self.manager(api)
        manager.last_positions = [tracked_position]
        api.positions_response = []
        api.open_orders_response = [stop(3641573662, 1, 30275.5)]

        positions, orders = await manager.reconcile()

        self.assertEqual(positions, [])
        self.assertEqual([order["id"] for order in orders], [3641573662])
        self.assertEqual(manager.last_positions, [tracked_position])

    async def test_immediate_trailing_fill_cleans_exact_hard_stop_after_nested_reconcile(self) -> None:
        api = FakeApi()
        tracked_position = {
            **position(LONG_POSITION, 30285.5),
            "id": 878027738,
        }
        hard = stop(3641573662, 1, 30275.5)
        trailing_fill = {
            **stop(3641577626, 1, 30288.5),
            "type": 5,
            "status": 2,
            "stopPrice": 30288.5,
            "trailPrice": 0.25,
            "filledPrice": 30288.25,
        }
        manager = self.manager(api)
        manager.last_positions = [tracked_position]
        manager.confirmed_position_id = 878027738
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 3641573662
        manager.native_trailing_order_id = 3641577626

        # Reproduce the nested activation reconciliation that first observes flat.
        api.positions_response = []
        api.open_orders_response = [hard]
        await manager.reconcile()
        self.assertEqual(manager.last_positions, [tracked_position])

        api.orders_response = [trailing_fill]
        api.trades_response = [{
            "orderId": 3641577626,
            "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"],
            "profitAndLoss": 5.5,
            "fees": 0.36,
            "voided": False,
        }]
        outcome = await manager.monitor_once()

        self.assertEqual(outcome, "POSITION_CLOSED")
        self.assertEqual(
            api.cancel_calls,
            [{"accountId": ACCOUNT["id"], "orderId": 3641573662}],
        )
        self.assertEqual(api.open_orders_response, [])
        self.assertEqual(manager.last_positions, [])

    async def test_trailing_fill_cancels_remaining_tracked_hard_stop(self) -> None:
        api = FakeApi()
        hard = stop(80, 1, 90)
        api.open_orders_response = [hard]
        manager = self.manager(api)
        manager.last_positions = [position(LONG_POSITION, 100)]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 80
        manager.native_trailing_order_id = 81
        api.orders_response = [{
            **stop(81, 1, 103),
            "type": 5,
            "status": 2,
            "stopPrice": None,
            "trailPrice": 1,
            "filledPrice": 103,
        }]
        api.trades_response = [{
            "orderId": 81, "accountId": ACCOUNT["id"],
            "contractId": CONTRACT["id"], "profitAndLoss": 6,
            "fees": 1, "voided": False,
        }]
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "POSITION_CLOSED")
        self.assertEqual(
            api.cancel_calls,
            [{"accountId": ACCOUNT["id"], "orderId": 80}],
        )
        self.assertEqual(manager.processed_exit_order_ids, {81})

    async def test_orphan_cancellation_rejection_halts(self) -> None:
        api = FakeApi()
        api.open_orders_response = [stop(67, 1, 90)]
        api.cancel_response = {
            "success": False,
            "errorCode": 3,
            "errorMessage": "mock cancel rejection",
        }
        manager = self.manager(api)
        manager.last_positions = [position(LONG_POSITION, 100)]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 67
        outcome = await manager.monitor_once()
        self.assertEqual(outcome, "ORPHAN_CANCELLATION_UNCERTAIN")
        self.assertIsNotNone(manager.halted_reason)

    async def test_native_exit_fill_is_processed_once(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        manager.last_positions = [position(LONG_POSITION, 100)]
        manager.native_trailing_order_id = 68
        native_fill = {
            **stop(68, 1, 103),
            "type": 5,
            "status": 2,
            "filledPrice": 103,
        }
        api.orders_response = [native_fill]
        api.trades_response = [
            {"orderId": 68, "profitAndLoss": 6, "fees": 1, "voided": False}
        ]
        first = await manager.monitor_once()
        duplicate = manager.record_exit_fill(native_fill, api.trades_response[0])
        self.assertEqual(first, "POSITION_CLOSED")
        self.assertFalse(duplicate)
        self.assertEqual(manager.processed_exit_order_ids, {68})

    async def test_duplicate_signal_and_risk_limit(self) -> None:
        api = FakeApi()
        manager = self.manager(api)
        await manager.signal(signal_result(), entry_eligible=True, market_fresh=True)
        duplicate = await manager.signal(
            signal_result(), entry_eligible=True, market_fresh=True
        )
        self.assertEqual(duplicate, "DUPLICATE_BLOCKED")
        self.assertEqual(len(api.place_calls), 1)

        loss_api = FakeApi()
        loss_api.trades_response = [
            {"profitAndLoss": -99, "fees": 1, "voided": False}
        ]
        loss_manager = self.manager(loss_api)
        blocked = await loss_manager.signal(
            signal_result("SHORT_PULLBACK"),
            entry_eligible=True,
            market_fresh=True,
        )
        self.assertEqual(blocked, "DAILY_LOSS_BLOCKED")
        self.assertEqual(loss_api.place_calls, [])

    async def test_disabled_daily_loss_limit_allows_entry(self) -> None:
        api = FakeApi()
        api.trades_response = [
            {"profitAndLoss": -99, "fees": 1, "voided": False}
        ]
        manager = self.manager(api, daily_loss_limit_enabled=False)

        result = await manager.signal(
            signal_result("SHORT_PULLBACK"),
            entry_eligible=True,
            market_fresh=True,
        )

        self.assertNotEqual(result, "DAILY_LOSS_BLOCKED")
        self.assertEqual(len(api.place_calls), 1)

    def test_daily_loss_limit_toggle_is_read_from_env(self) -> None:
        required = {
            "ENABLE_ORDER_EXECUTION": "false",
            "PRACTICE_ONLY": "true",
            "EMERGENCY_HALT": "true",
            "CONTRACTS": "1",
            "STOP_LOSS_POINTS": "10",
            "TRAIL_ACTIVATION_POINTS": "3",
            "TRAIL_DISTANCE_TICKS": "4",
            "TRAIL_MANAGEMENT_MODE": "native_delayed",
            "MAX_DAILY_LOSS_USD": "100",
            "MAX_OPEN_POSITIONS": "1",
            "BRACKET_MODE": "auto_oco",
        }
        with patch.dict(
            os.environ,
            {**required, "ENABLE_DAILY_LOSS_LIMIT": "true"},
            clear=True,
        ):
            self.assertTrue(TradingConfig.from_env().daily_loss_limit_enabled)
        with patch.dict(
            os.environ,
            {**required, "ENABLE_DAILY_LOSS_LIMIT": "false"},
            clear=True,
        ):
            self.assertFalse(TradingConfig.from_env().daily_loss_limit_enabled)

    async def test_ema_touch_cross_reversal_confirms_flat_and_cleans_stop(self) -> None:
        api = FakeApi()
        old_position = position(SHORT_POSITION)
        old_stop = stop(700, 0, 110)
        api.open_positions_sequence = [[old_position], [], []]
        api.open_orders_sequence = [[old_stop], [old_stop], [], []]
        manager = self.manager(api, strategy_name="ema_touch_reentry")
        manager.last_positions = [old_position]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = SHORT_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 700
        manager.confirmed_stop_price = Decimal("110")

        status = await manager.signal(
            signal_result("LONG_CROSS"),
            entry_eligible=True,
            market_fresh=True,
        )

        self.assertEqual(status, "ORDER_REJECTED")
        self.assertEqual(
            api.close_calls,
            [{"accountId": ACCOUNT["id"], "contractId": CONTRACT["id"]}],
        )
        self.assertEqual(api.cancel_calls, [{"accountId": ACCOUNT["id"], "orderId": 700}])
        self.assertEqual(api.place_calls[0]["side"], 0)

    async def test_ema_touch_never_reverses_same_direction(self) -> None:
        api = FakeApi()
        api.positions_response = [position(LONG_POSITION)]
        manager = self.manager(api, strategy_name="ema_touch_reentry")
        status = await manager.signal(
            signal_result("LONG_CROSS"),
            entry_eligible=True,
            market_fresh=True,
        )
        self.assertEqual(status, "POSITION_EXISTS")
        self.assertEqual(api.close_calls, [])
        self.assertEqual(api.place_calls, [])

    async def test_ema_touch_date_exit_closes_but_does_not_enter(self) -> None:
        api = FakeApi()
        old_position = position(LONG_POSITION)
        old_stop = stop(701, 1, 90)
        api.open_positions_sequence = [[old_position], []]
        api.open_orders_sequence = [[old_stop], [old_stop], []]
        manager = self.manager(api, strategy_name="ema_touch_reentry")
        manager.last_positions = [old_position]
        manager.confirmed_position_id = 11
        manager.confirmed_position_type = LONG_POSITION
        manager.confirmed_position_size = 1
        manager.confirmed_stop_order_id = 701
        manager.confirmed_stop_price = Decimal("90")

        status = await manager.signal(
            signal_result("DATE_RANGE_EXIT"),
            entry_eligible=True,
            market_fresh=True,
        )

        self.assertEqual(status, "POSITION_CLOSED")
        self.assertEqual(len(api.close_calls), 1)
        self.assertEqual(api.place_calls, [])

if __name__ == "__main__":
    unittest.main()















