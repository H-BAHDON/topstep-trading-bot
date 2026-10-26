"""Risk-gated, practice-only ProjectX execution with verified hard-stop protection."""
from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any
from zoneinfo import ZoneInfo

from strategies.base import StrategyResult
from topstep_api import ProjectX, ProjectXError, now_utc

UTC = timezone.utc
CHICAGO = ZoneInfo("America/Chicago")
ENTRY_SIGNALS = {
    "LONG_CROSS": 0,
    "LONG_PULLBACK": 0,
    "LONG_TOUCH": 0,
    "SHORT_CROSS": 1,
    "SHORT_PULLBACK": 1,
    "SHORT_TOUCH": 1,
    "LONG_HOURLY": 0,
    "SHORT_HOURLY": 1,
    "LONG_HOURLY_43": 0,
    "SHORT_HOURLY_43": 1,
    "LONG_HOURLY_55": 0,
    "SHORT_HOURLY_55": 1,
}
OPEN_OR_PENDING_STATUSES = {1, 6}
FILLED_STATUS = 2
REJECTED_STATUS = 5
LONG_POSITION = 1
SHORT_POSITION = 2
LIMIT_ORDER = 1
STOP_ORDER = 4
TRAILING_STOP_ORDER = 5
POSITION_BRACKETS_API_UNSUPPORTED = (
    "BRACKET_MODE=position is blocked: the immediate API hard-stop bracket "
    "requires Auto OCO mode; ProjectX rejects bracket fields in Position mode"
)
@dataclass(frozen=True)
class TradingConfig:
    enabled: bool
    practice_only: bool
    contracts: int
    strategy_name: str
    stop_loss_points: Decimal
    take_profit_points: Decimal | None
    broker_max_contracts: int | None
    trail_activation_points: Decimal
    trail_distance_ticks: int
    trail_management_mode: str
    daily_loss_limit_enabled: bool
    max_daily_loss_usd: Decimal
    max_open_positions: int
    bracket_mode: str
    emergency_halt: bool
    execution_poll_interval_seconds: float
    errors: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "TradingConfig":
        errors: list[str] = []

        def boolean(name: str, required: bool | None = None) -> bool:
            raw = os.getenv(name)
            valid = {"true", "false", "1", "0", "yes", "no", "on", "off"}
            if raw is None or raw.strip().lower() not in valid:
                errors.append(f"{name} is missing or invalid")
                return False
            value = raw.strip().lower() in {"true", "1", "yes", "on"}
            if required is not None and value is not required:
                errors.append(f"{name} must be {str(required).lower()}")
            return value

        def integer(name: str, minimum: int = 1) -> int:
            try:
                value = int(os.environ[name])
                if value < minimum:
                    raise ValueError
                return value
            except (KeyError, ValueError):
                errors.append(f"{name} must be an integer >= {minimum}")
                return 0

        def decimal(name: str) -> Decimal:
            try:
                value = Decimal(os.environ[name])
                if not value.is_finite() or value <= 0:
                    raise InvalidOperation
                return value
            except (KeyError, InvalidOperation):
                errors.append(f"{name} must be a positive number")
                return Decimal(0)

        try:
            poll_interval = float(
                os.getenv("EXECUTION_POLL_INTERVAL_SECONDS", "0.5")
            )
            if not 0 < poll_interval < float("inf"):
                raise ValueError
        except ValueError:
            errors.append(
                "EXECUTION_POLL_INTERVAL_SECONDS must be a positive number"
            )
            poll_interval = 0.5

        enabled = boolean("ENABLE_ORDER_EXECUTION")
        practice_only = boolean("PRACTICE_ONLY")
        emergency_halt = boolean("EMERGENCY_HALT")
        daily_loss_limit_enabled = boolean("ENABLE_DAILY_LOSS_LIMIT")
        bracket_mode = os.getenv("BRACKET_MODE", "").strip().lower()
        if bracket_mode not in {"auto_oco", "position"}:
            errors.append("BRACKET_MODE must be auto_oco or position")
        elif bracket_mode == "position":
            errors.append(POSITION_BRACKETS_API_UNSUPPORTED)

        strategy_name = os.getenv(
            "ACTIVE_STRATEGY", "ema12_hourly_trailing"
        ).strip().lower()
        contracts = 0
        stop_loss_points = Decimal(0)
        if strategy_name == "quick_scalp":
            contracts = integer("CONTRACTS")
            stop_loss_points = decimal("STOP_LOSS_POINTS")
            trail_management_mode = "fixed_oco"
            take_profit_points = decimal("TAKE_PROFIT_POINTS")
            broker_max_contracts = integer("BROKER_MAX_MNQ_CONTRACTS")
            trail_activation_points = Decimal(0)
            trail_distance_ticks = 0
        elif strategy_name == "ema_touch_reentry":
            try:
                contracts = int(os.getenv("EMA_TOUCH_CONTRACTS", "1"))
                if contracts <= 0:
                    raise ValueError
            except ValueError:
                errors.append("EMA_TOUCH_CONTRACTS must be an integer >= 1")
                contracts = 0
            try:
                stop_loss_points = Decimal(
                    os.getenv("EMA_TOUCH_HARD_STOP_POINTS", "3.0")
                )
                trail_activation_points = Decimal(
                    os.getenv("EMA_TOUCH_TRAIL_ACTIVATION_POINTS", "20.0")
                )
                trail_distance_points = Decimal(
                    os.getenv("EMA_TOUCH_TRAIL_DISTANCE_POINTS", "20.0")
                )
                if any(
                    not value.is_finite() or value <= 0
                    for value in (
                        stop_loss_points,
                        trail_activation_points,
                        trail_distance_points,
                    )
                ):
                    raise InvalidOperation
            except InvalidOperation:
                errors.append("EMA touch exit points must be positive numbers")
                stop_loss_points = Decimal(0)
                trail_activation_points = Decimal(0)
                trail_distance_points = Decimal(0)
            distance_ticks = trail_distance_points / Decimal("0.25")
            if distance_ticks != distance_ticks.to_integral_value():
                errors.append(
                    "EMA_TOUCH_TRAIL_DISTANCE_POINTS must convert to whole MNQ ticks"
                )
                trail_distance_ticks = 0
            else:
                trail_distance_ticks = int(distance_ticks)
            trail_management_mode = "native_delayed"
            take_profit_points = None
            broker_max_contracts = None
        elif strategy_name in {
            "ema12_hourly_trailing",
            "ema12_hourly_trailing_43",
            "ema12_hourly_trailing_55",
        }:
            def strategy_integer(name: str, default: str) -> int:
                try:
                    value = int(os.getenv(name, default))
                    if value <= 0:
                        raise ValueError
                    return value
                except ValueError:
                    errors.append(f"{name} must be an integer >= 1")
                    return 0

            def strategy_decimal(name: str, default: str) -> Decimal:
                try:
                    value = Decimal(os.getenv(name, default))
                    if not value.is_finite() or value <= 0:
                        raise InvalidOperation
                    return value
                except InvalidOperation:
                    errors.append(f"{name} must be a positive number")
                    return Decimal(0)

            contracts = strategy_integer("EMA12_HOURLY_CONTRACTS", "5")
            stop_loss_points = strategy_decimal(
                "EMA12_HOURLY_HARD_STOP_POINTS", "40"
            )
            take_profit_points = strategy_decimal(
                "EMA12_HOURLY_TAKE_PROFIT_POINTS", "25"
            )
            trail_activation_points = strategy_decimal(
                "EMA12_HOURLY_TRAIL_ACTIVATION_POINTS", "10"
            )
            trail_distance_points = strategy_decimal(
                "EMA12_HOURLY_TRAIL_DISTANCE_POINTS", "5"
            )
            distance_ticks = trail_distance_points / Decimal("0.25")
            if distance_ticks != distance_ticks.to_integral_value():
                errors.append(
                    "EMA12_HOURLY_TRAIL_DISTANCE_POINTS must convert to whole MNQ ticks"
                )
                trail_distance_ticks = 0
            else:
                trail_distance_ticks = int(distance_ticks)
            trail_management_mode = "native_delayed"
            broker_max_contracts = None
        else:
            contracts = integer("CONTRACTS")
            stop_loss_points = decimal("STOP_LOSS_POINTS")
            trail_management_mode = os.getenv(
                "TRAIL_MANAGEMENT_MODE", "native_delayed"
            ).strip().lower()
            if trail_management_mode != "native_delayed":
                errors.append("TRAIL_MANAGEMENT_MODE must be native_delayed")
            take_profit_points = None
            broker_max_contracts = None
            trail_activation_points = decimal("TRAIL_ACTIVATION_POINTS")
            trail_distance_ticks = integer("TRAIL_DISTANCE_TICKS")

        return cls(
            enabled=enabled,
            practice_only=practice_only,
            contracts=contracts,
            strategy_name=strategy_name,
            stop_loss_points=stop_loss_points,
            take_profit_points=take_profit_points,
            broker_max_contracts=broker_max_contracts,
            trail_activation_points=trail_activation_points,
            trail_distance_ticks=trail_distance_ticks,
            trail_management_mode=trail_management_mode,
            daily_loss_limit_enabled=daily_loss_limit_enabled,
            max_daily_loss_usd=decimal("MAX_DAILY_LOSS_USD"),
            max_open_positions=integer("MAX_OPEN_POSITIONS"),
            bracket_mode=bracket_mode,
            emergency_halt=emergency_halt,
            execution_poll_interval_seconds=poll_interval,
            errors=tuple(errors),
        )


class ExecutionManager:
    def __init__(
        self,
        api: ProjectX,
        account: dict[str, Any],
        contract: dict[str, Any],
        config: TradingConfig,
        quote_source: Any | None = None,
        event_sink: Any | None = None,
        approved_account: tuple[int, str, str] | None = None,
        entries_enabled: bool = True,
    ) -> None:
        self.api = api
        self.quote_source = quote_source or api
        self.event_sink = event_sink
        self.account = account
        # Immutable identity from configuration, independent of process-wide env.
        self.approved_account = approved_account
        self.entries_enabled = entries_enabled
        self.configured_entries_enabled = entries_enabled
        self.runtime_paused = False
        self.manual_close_uncertain = False
        self.contract = contract
        self.config = config
        self.lock = asyncio.Lock()
        self.halted_reason: str | None = None
        self.halted_code: str | None = None
        self.order_pending = False
        self.processed_candles: set[datetime] = set()
        self.last_positions: list[dict[str, Any]] = []
        self.realized_net = Decimal(0)
        self.last_background_pnl_refresh: float | None = None
        self.confirmed_stop_price: Decimal | None = None
        self.confirmed_stop_order_id: int | None = None
        self.confirmed_take_profit_order_id: int | None = None
        self.native_trailing_order_id: int | None = None
        self.native_trailing_active = False
        self.trailing_attempted_position_id: int | None = None
        self.trailing_distance_mismatch_position_id: int | None = None
        self.trailing_requested_distance: Decimal | None = None
        self.trailing_actual_distance: Decimal | None = None
        self.confirmed_position_id: int | None = None
        self.confirmed_position_type: int | None = None
        self.confirmed_position_size: int | None = None
        self.processed_exit_order_ids: set[int] = set()
        self.cancel_verify_attempts = 3
        self.cancel_verify_delay_seconds = 0.25
        self.stop_ticks = self.points_to_ticks(config.stop_loss_points)
        self.take_profit_ticks = (
            self.points_to_ticks(config.take_profit_points)
            if config.take_profit_points is not None
            else 0
        )
        self.activation_ticks = self.points_to_ticks(config.trail_activation_points)
        self.trail_ticks = config.trail_distance_ticks
        self.active_trail_activation_points: Decimal | None = None
        self.active_trail_ticks: int | None = None
        self.live_trailing_latched_position_id: int | None = None
        self.locally_opened_position_id: int | None = None

    async def update_runtime_setting(self, setting: str, value: str) -> str:
        """Update the settings used for future entries without restarting."""
        async with self.lock:
            if setting == "contracts":
                try:
                    parsed_contracts = int(value)
                    if parsed_contracts <= 0:
                        raise ValueError
                except ValueError as exc:
                    raise ValueError("contracts must be a positive integer") from exc
                self.config = replace(self.config, contracts=parsed_contracts)
                logging.info(
                    "TELEGRAM SETTING UPDATED contracts=%d", parsed_contracts
                )
                return str(parsed_contracts)

            try:
                points = Decimal(value)
                if not points.is_finite() or points <= 0:
                    raise InvalidOperation
            except InvalidOperation as exc:
                raise ValueError("points must be a positive number") from exc
            ticks = self.points_to_ticks(points)
            if ticks <= 0:
                raise ValueError(
                    f"points must convert to whole {self.tick_size}-point MNQ ticks"
                )
            if setting == "trail_activation":
                self.config = replace(
                    self.config, trail_activation_points=points
                )
                self.activation_ticks = ticks
            elif setting == "trail_distance":
                self.config = replace(self.config, trail_distance_ticks=ticks)
                self.trail_ticks = ticks
            else:
                raise ValueError("unknown runtime setting")
            logging.info(
                "TELEGRAM SETTING UPDATED %s=%s points ticks=%d",
                setting,
                points,
                ticks,
            )
            return str(points)

    def _pause_entries(self, persist) -> None:
        self.entries_enabled = False
        self.runtime_paused = True
        persist(True)

    async def pause_entries(self, persist) -> str:
        async with self.lock:
            self._pause_entries(persist)
        return "Paused new entries. Open trades remain managed."

    async def resume_entries(self, persist) -> str:
        async with self.lock:
            if not self.configured_entries_enabled:
                raise ValueError("Account is disabled in accounts.json; enable it there and restart first")
            if not self.config.enabled or self.readiness_errors():
                raise ValueError("Account execution is disabled or safety checks are not ready")
            if self.halted_reason or self.manual_close_uncertain or self.order_pending:
                raise ValueError("Account is halted or has an uncertain/pending order; reconcile it before resuming")
            positions, orders = await self.reconcile()
            if len(positions) > 1 or (not positions and orders):
                raise ValueError("Unresolved broker positions or orders prevent resuming")
            if positions:
                if self.find_protective_stop(positions[0], orders) is None:
                    raise ValueError("Open position has no unique confirmed protective stop")
                if (self.config.take_profit_points is not None
                        and self.find_take_profit(positions[0], orders) is None):
                    raise ValueError("Open position has no unique confirmed take profit")
            net = await self.daily_net_pnl()
            if self.config.daily_loss_limit_enabled and net <= -self.config.max_daily_loss_usd:
                raise ValueError("Daily loss limit prevents resuming")
            persist(False)
            self.runtime_paused = False
            self.entries_enabled = True
        return "Resumed new entries. Existing risk and market checks still apply."

    async def close_account(self, persist) -> str:
        """Pause first; never blindly repeat an uncertain market close."""
        async with self.lock:
            self._pause_entries(persist)
            if not self.account_eligible() or not self.config.enabled:
                raise ValueError("Account identity or execution permission prevents closing")
            positions, orders = await self.reconcile()
            if len(positions) > 1 or self.order_pending:
                raise ValueError("Multiple positions or a pending entry require broker review; account stays paused")
            if positions:
                if self.manual_close_uncertain:
                    raise ValueError("Previous close is unconfirmed; inspect broker state before another close")
                self.manual_close_uncertain = True
                if not await self.close_strategy_position(positions[0], "TELEGRAM_CLOSE"):
                    raise ValueError("Close was not confirmed; account stays paused. Inspect broker state")
            elif not await self.cancel_and_confirm_orphan_exits(orders):
                raise ValueError("Exit cleanup was not confirmed; account stays paused")
            remaining_positions, remaining_orders = await self.reconcile()
            if remaining_positions or remaining_orders:
                raise ValueError("Broker still has positions or orders; account stays paused for review")
            self.manual_close_uncertain = False
            self.clear_tracked_position()
        return "Confirmed flat with no remaining strategy-contract orders. Account stays paused."

    def runtime_settings(self) -> dict[str, str | int]:
        return {
            "contracts": self.config.contracts,
            "trail_activation": str(self.config.trail_activation_points),
            "trail_distance": str(Decimal(self.trail_ticks) * self.tick_size),
        }

    @property
    def position_trail_activation_points(self) -> Decimal:
        return (
            self.active_trail_activation_points
            if self.active_trail_activation_points is not None
            else self.config.trail_activation_points
        )

    @property
    def position_trail_ticks(self) -> int:
        return (
            self.active_trail_ticks
            if self.active_trail_ticks is not None
            else self.trail_ticks
        )

    def emit_event(self, event: str, **payload: Any) -> None:
        if self.event_sink is None:
            return
        try:
            self.event_sink.notify(event, **payload)
        except Exception:
            logging.exception("Optional monitoring event failed event=%s", event)

    @staticmethod
    def intended_practice_account() -> tuple[int | None, str]:
        """Return the exact practice-account identity configured in the environment."""
        raw_id = os.getenv("VERIFIED_PRACTICE_ACCOUNT_ID", "").strip()
        name = os.getenv("VERIFIED_PRACTICE_ACCOUNT_NAME", "").strip()
        try:
            account_id = int(raw_id)
        except ValueError:
            account_id = None
        return account_id, name

    @staticmethod
    def explicit_verification_valid(
        account: dict[str, Any],
        verified_id: str | None,
        verified_name: str | None,
        confirmed: bool,
    ) -> bool:
        intended_id, intended_name = ExecutionManager.intended_practice_account()
        return (
            confirmed is True
            and intended_id is not None
            and bool(intended_name)
            and verified_id == str(intended_id)
            and verified_name == intended_name
            and str(account.get("id")) == verified_id
            and account.get("name") == verified_name
        )

    @staticmethod
    def practice_account_eligible(account: dict[str, Any]) -> bool:
        """Require exact identity plus independent user-hub simulation proof."""
        intended_id, intended_name = ExecutionManager.intended_practice_account()
        return (
            intended_id is not None
            and bool(intended_name)
            and account.get("id") == intended_id
            and account.get("name") == intended_name
            and account.get("canTrade") is True
            and account.get("isVisible") is True
            and account.get("simulated") is True
            and account.get("_simulatedVerified") is True
        )

    @property
    def tick_size(self) -> Decimal:
        return Decimal(str(self.contract.get("tickSize", 0)))

    def points_to_ticks(self, points: Decimal) -> int:
        try:
            ticks = points / self.tick_size
        except (InvalidOperation, ZeroDivisionError):
            return 0
        if ticks != ticks.to_integral_value() or ticks <= 0:
            return 0
        return int(ticks)

    def signed_stop_ticks(self, entry_side: int) -> int:
        if entry_side == 0:
            return -self.stop_ticks
        if entry_side == 1:
            return self.stop_ticks
        raise ProjectXError(f"invalid entry side: {entry_side}")

    def signed_take_profit_ticks(self, entry_side: int) -> int:
        if entry_side == 0:
            return self.take_profit_ticks
        if entry_side == 1:
            return -self.take_profit_ticks
        raise ProjectXError(f"invalid entry side: {entry_side}")

    @staticmethod
    def validate_stop_bracket_sign(entry_side: int, ticks: int) -> None:
        if entry_side == 0 and ticks >= 0:
            raise ProjectXError(
                "LONG stop-loss bracket ticks must be less than zero"
            )
        if entry_side == 1 and ticks <= 0:
            raise ProjectXError(
                "SHORT stop-loss bracket ticks must be greater than zero"
            )
        if entry_side not in {0, 1}:
            raise ProjectXError(f"invalid entry side: {entry_side}")

    def initial_protective_stop_price(
        self, entry_price: float | Decimal, position_type: int
    ) -> Decimal:
        entry = Decimal(str(entry_price))
        if position_type == LONG_POSITION:
            return self.quantize_stop(
                entry - self.config.stop_loss_points, LONG_POSITION
            )
        if position_type == SHORT_POSITION:
            return self.quantize_stop(
                entry + self.config.stop_loss_points, SHORT_POSITION
            )
        raise ProjectXError(f"invalid position type: {position_type}")

    def initial_take_profit_price(
        self, entry_price: float | Decimal, position_type: int
    ) -> Decimal:
        if self.config.take_profit_points is None:
            raise ProjectXError("take profit is not configured")
        entry = Decimal(str(entry_price))
        if position_type == LONG_POSITION:
            return self.quantize_stop(
                entry + self.config.take_profit_points, SHORT_POSITION
            )
        if position_type == SHORT_POSITION:
            return self.quantize_stop(
                entry - self.config.take_profit_points, LONG_POSITION
            )
        raise ProjectXError(f"invalid position type: {position_type}")

    def build_entry_payload(
        self, entry_side: int, tag: str
    ) -> dict[str, Any]:
        if self.config.bracket_mode != "auto_oco":
            raise ProjectXError(POSITION_BRACKETS_API_UNSUPPORTED)
        bracket_ticks = self.signed_stop_ticks(entry_side)
        self.validate_stop_bracket_sign(entry_side, bracket_ticks)
        take_profit = (
            {
                "ticks": self.signed_take_profit_ticks(entry_side),
                "type": LIMIT_ORDER,
            }
            if self.config.take_profit_points is not None
            else None
        )
        return {
            "accountId": int(self.account["id"]),
            "contractId": self.contract["id"],
            "type": 2,
            "side": entry_side,
            "size": self.config.contracts,
            "limitPrice": None,
            "stopPrice": None,
            "trailPrice": None,
            "customTag": tag,
            "stopLossBracket": {
                "ticks": bracket_ticks,
                "type": STOP_ORDER,
            },
            "takeProfitBracket": take_profit,
        }

    def readiness_errors(self) -> list[str]:
        errors = list(self.config.errors)
        if (
            self.config.bracket_mode == "position"
            and POSITION_BRACKETS_API_UNSUPPORTED not in errors
        ):
            errors.append(POSITION_BRACKETS_API_UNSUPPORTED)
        if self.config.strategy_name == "quick_scalp":
            if self.config.trail_management_mode != "fixed_oco":
                errors.append("Quick Scalp requires fixed Auto OCO exits")
            if min(self.stop_ticks, self.take_profit_ticks) <= 0:
                errors.append("stop and take-profit points must convert to whole positive ticks")
            if (
                self.config.broker_max_contracts is None
                or self.config.contracts > self.config.broker_max_contracts
            ):
                errors.append("CONTRACTS exceeds BROKER_MAX_MNQ_CONTRACTS")
        else:
            if self.config.trail_management_mode != "native_delayed":
                errors.append("TRAIL_MANAGEMENT_MODE must be native_delayed")
            if min(self.stop_ticks, self.activation_ticks, self.trail_ticks) <= 0:
                errors.append("stop and trailing points must convert to whole positive ticks")
            if (
                self.config.take_profit_points is not None
                and self.take_profit_ticks <= 0
            ):
                errors.append("take-profit points must convert to whole positive ticks")
        if not self.account_eligible():
            errors.append(
                "configured account is unverified, mismatched, unavailable, or blocked by PRACTICE_ONLY"
                if self.approved_account is not None else
                "selected account does not exactly match the intended practice account or lacks independent simulated-status verification"
            )
        if self.contract.get("symbolId") != "F.US.MNQ":
            errors.append("selected contract is not MNQ")
        if (
            not isinstance(self.config.contracts, int)
            or isinstance(self.config.contracts, bool)
            or self.config.contracts <= 0
        ):
            errors.append("CONTRACTS must be a positive integer")
        if self.config.max_open_positions != 1:
            errors.append("MAX_OPEN_POSITIONS must be 1")
        if self.config.emergency_halt:
            errors.append("EMERGENCY_HALT is active")
        return errors

    def account_eligible(self) -> bool:
        if self.approved_account is None:
            return self.practice_account_eligible(self.account)
        account_id, name, kind = self.approved_account
        return (
            kind in {"practice", "express"}
            and (kind == "practice" or not self.config.practice_only)
            and self.account.get("id") == account_id
            and self.account.get("name") == name
            and self.account.get("canTrade") is True
            and self.account.get("isVisible") is True
            and self.account.get("simulated") is True
            and self.account.get("_simulatedVerified") is True
        )

    @staticmethod
    def readiness_reason_code(error: str) -> str:
        mappings = (
            ("BRACKET_MODE=position", "BRACKET_MODE_POSITION_UNSUPPORTED"),
            ("EMERGENCY_HALT is active", "EMERGENCY_HALT_ACTIVE"),
            ("intended practice account", "PRACTICE_ACCOUNT_UNVERIFIED"),
            ("configured account is unverified", "ACCOUNT_UNVERIFIED"),
            ("selected contract is not MNQ", "INVALID_CONTRACT"),
            ("CONTRACTS must be a positive integer", "INVALID_CONTRACT_SIZE"),
            ("MAX_OPEN_POSITIONS must be 1", "INVALID_POSITION_LIMIT"),
            ("TRAIL_MANAGEMENT_MODE", "INVALID_TRAIL_MODE"),
            ("fixed Auto OCO", "INVALID_EXIT_MODE"),
            ("BROKER_MAX_MNQ_CONTRACTS", "BROKER_POSITION_LIMIT_EXCEEDED"),
            ("whole positive ticks", "INVALID_TICK_CONFIGURATION"),
        )
        for fragment, code in mappings:
            if fragment in error:
                return code
        return ExecutionManager.halt_reason_code(error)

    @property
    def safety_ready(self) -> bool:
        return not self.readiness_errors() and not self.halted_reason

    @property
    def execution_ready(self) -> bool:
        return self.config.enabled and self.safety_ready

    @staticmethod
    def halt_reason_code(reason: str) -> str:
        normalized = reason.lower()
        mappings = (
            ("reconciliation failed", "BROKER_RECONCILIATION_FAILED"),
            ("position closed but broker-side exit", "BROKER_CLOSURE_UNCONFIRMED"),
            ("position remains open and protective stop", "PROTECTIVE_STOP_MISSING_POSITION_OPEN"),
            ("modified protective stop state remained inconsistent", "BROKER_STATE_INCONSISTENT"),
            ("daily net p&l limit", "DAILY_LOSS_LIMIT_REACHED"),
            ("protective stop missing", "PROTECTIVE_STOP_MISSING"),
            ("no unique confirmed protective stop", "PROTECTIVE_STOP_UNCONFIRMED"),
            ("fill or hard protective stop", "ENTRY_PROTECTION_UNCONFIRMED"),
            ("new protective stop association", "PROTECTIVE_STOP_ASSOCIATION_UNVERIFIED"),
            ("initial protective stop price mismatch", "INITIAL_PROTECTION_MISMATCH"),
            ("order state uncertain", "ORDER_STATE_UNCERTAIN"),
            ("market data is stale", "MARKET_DATA_STALE"),
        )
        for fragment, code in mappings:
            if fragment in normalized:
                return code
        code = re.sub(r"[^A-Z0-9]+", "_", reason.upper()).strip("_")
        return code[:96] or "UNSPECIFIED_SAFETY_HALT"

    def halt(self, reason: str) -> None:
        if self.halted_reason is None:
            self.halted_reason = reason
            self.halted_code = self.halt_reason_code(reason)
            logging.critical(
                "NEW ORDERS HALTED accountId=%s reason=%s detail=%s. "
                "Existing broker stop remains active; inspect the position "
                "and orders in TopstepX.",
                self.account["id"], self.halted_code,
                reason,
            )

    @staticmethod
    def trading_day_start(now: datetime | None = None) -> datetime:
        current = (now or now_utc()).astimezone(CHICAGO)
        boundary = datetime.combine(current.date(), time(17, 0), CHICAGO)
        if current < boundary:
            boundary -= timedelta(days=1)
        return boundary.astimezone(UTC)

    async def daily_net_pnl(self) -> Decimal:
        trades = await self.api.trades(
            int(self.account["id"]), self.trading_day_start()
        )
        net = Decimal(0)
        for trade in trades:
            self.validate_record_account(trade)
            if trade.get("voided"):
                continue
            net += Decimal(str(trade.get("profitAndLoss") or 0))
            net -= Decimal(str(trade.get("fees") or 0))
        self.realized_net = net
        return net

    def custom_tag(self, result: StrategyResult) -> str:
        stamp = result.candle.time.astimezone(UTC).strftime("%Y%m%d%H%M")
        return f"mnq-{result.signal.lower()}-{stamp}"

    def mnq_state(
        self,
        positions: list[dict[str, Any]],
        orders: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return (
            [
                position
                for position in positions
                if position.get("contractId") == self.contract["id"]
            ],
            [
                order
                for order in orders
                if order.get("contractId") == self.contract["id"]
                and order.get("status") in OPEN_OR_PENDING_STATUSES
            ],
        )

    async def reconcile(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        account_id = int(self.account["id"])
        positions, orders = await asyncio.gather(
            self.api.open_positions(account_id),
            self.api.open_orders(account_id),
        )
        for record in [*positions, *orders]:
            self.validate_record_account(record)
        return self.mnq_state(positions, orders)

    def validate_record_account(self, record: dict[str, Any]) -> None:
        """Reject a misrouted broker response rather than managing another account."""
        if "accountId" in record and record["accountId"] != self.account["id"]:
            raise ProjectXError("Broker response contains a different account ID")

    def matching_exit_orders(
        self,
        position: dict[str, Any],
        orders: list[dict[str, Any]],
        order_type: int,
        excluded_order_ids: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        expected_side = 1 if position.get("type") == LONG_POSITION else 0
        return [
            order for order in orders
            if order.get("type") == order_type
            and order.get("side") == expected_side
            and order.get("size") == position.get("size")
            and order.get("accountId") == self.account.get("id")
            and order.get("contractId") == self.contract.get("id")
            and int(order.get("id", -1)) not in (excluded_order_ids or set())
        ]

    def find_protective_stop(
        self,
        position: dict[str, Any],
        orders: list[dict[str, Any]],
        excluded_order_ids: set[int] | None = None,
    ) -> dict[str, Any] | None:
        candidates = self.matching_exit_orders(
            position, orders, STOP_ORDER, excluded_order_ids
        )
        if len(candidates) != 1 or candidates[0].get("stopPrice") is None:
            return None
        return candidates[0]

    def find_take_profit(
        self, position: dict[str, Any], orders: list[dict[str, Any]],
        excluded_order_ids: set[int] | None = None,
    ) -> dict[str, Any] | None:
        candidates = self.matching_exit_orders(
            position, orders, LIMIT_ORDER, excluded_order_ids
        )
        if len(candidates) != 1 or candidates[0].get("limitPrice") is None:
            return None
        return candidates[0]

    def find_native_trailing(
        self, position: dict[str, Any], orders: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        candidates = self.matching_exit_orders(
            position, orders, TRAILING_STOP_ORDER
        )
        return candidates[0] if len(candidates) == 1 else None

    async def startup_check(self) -> None:
        if self.approved_account is not None and not self.account_eligible():
            self.halt("configured account is unverified or blocked by PRACTICE_ONLY")
            return
        errors = self.readiness_errors()
        if errors:
            logging.warning("Execution not ready accountId=%s: %s", self.account["id"], "; ".join(errors))
        try:
            positions, orders = await self.reconcile()
            net = await self.daily_net_pnl()
        except Exception as exc:
            self.halt(f"startup reconciliation failed: {type(exc).__name__}")
            return

        if len(positions) > 1:
            self.halt("more than one MNQ position exists")
        elif positions:
            position = positions[0]
            self.last_positions = [position]
            stop = self.find_protective_stop(position, orders)
            take_profit = (
                self.find_take_profit(position, orders)
                if self.config.take_profit_points is not None else None
            )
            if stop is None:
                self.halt("existing MNQ position has no unique confirmed protective stop")
            elif self.config.take_profit_points is not None and take_profit is None:
                self.halt("existing position has no unique confirmed take profit")
            else:
                self.confirmed_stop_price = Decimal(str(stop["stopPrice"]))
                self.confirmed_stop_order_id = int(stop["id"])
                self.confirmed_take_profit_order_id = (
                    int(take_profit["id"]) if take_profit is not None else None
                )
                self.confirmed_position_id = int(position["id"])
                self.confirmed_position_type = int(position["type"])
                self.confirmed_position_size = int(position["size"])
                self.active_trail_activation_points = (
                    self.config.trail_activation_points
                )
                self.active_trail_ticks = self.trail_ticks
                if self.config.trail_management_mode == "native_delayed":
                    trailing_candidates = self.matching_exit_orders(
                        position, orders, TRAILING_STOP_ORDER
                    )
                    if len(trailing_candidates) > 1:
                        self.halt("multiple native trailing exits found on reconnect")
                    elif trailing_candidates:
                        self.native_trailing_order_id = int(
                            trailing_candidates[0]["id"]
                        )
                        self.native_trailing_active = True
                        self.trailing_attempted_position_id = self.confirmed_position_id
                logging.warning(
                    "Resuming protected MNQ position id=%s averagePrice=%s "
                    "hardStopOrderId=%s takeProfitOrderId=%s trailingOrderId=%s",
                    position.get("id"), position.get("averagePrice"), stop.get("id"),
                    self.confirmed_take_profit_order_id,
                    self.native_trailing_order_id,
                )
        elif orders:
            self.halt("orphaned open or pending MNQ order exists")

        if (
            self.config.daily_loss_limit_enabled
            and net <= -self.config.max_daily_loss_usd
        ):
            self.halt(f"daily net P&L limit reached ({net:.2f} USD)")
        logging.info(
            "Execution readiness enabled=%s safetyReady=%s ready=%s bracketMode=%s "
            "stopTicks=%d activationTicks=%d trailTicks=%d trailMode=%s "
            "pollIntervalSeconds=%.3f dailyNet=%.2f",
            self.config.enabled,
            self.safety_ready,
            self.execution_ready,
            self.config.bracket_mode or "UNCONFIRMED",
            self.stop_ticks,
            self.activation_ticks,
            self.trail_ticks,
            self.config.trail_management_mode,
            self.config.execution_poll_interval_seconds,
            net,
        )

    def clear_tracked_position(self) -> None:
        self.confirmed_stop_price = None
        self.confirmed_stop_order_id = None
        self.confirmed_take_profit_order_id = None
        self.native_trailing_order_id = None
        self.native_trailing_active = False
        self.trailing_attempted_position_id = None
        self.confirmed_position_id = None
        self.confirmed_position_type = None
        self.confirmed_position_size = None
        self.active_trail_activation_points = None
        self.active_trail_ticks = None
        self.live_trailing_latched_position_id = None
        self.locally_opened_position_id = None
        self.last_positions = []

    async def close_strategy_position(
        self, position: dict[str, Any], reason: str
    ) -> bool:
        """Close, confirm flat, then remove only the tracked protective exits."""
        position_id = int(position["id"])
        logging.info(
            "STRATEGY POSITION CLOSE REQUEST reason=%s positionId=%s direction=%s size=%s",
            reason,
            position_id,
            position.get("type"),
            position.get("size"),
        )
        try:
            response = await self.api.close_position_once(
                {
                    "accountId": int(self.account["id"]),
                    "contractId": self.contract["id"],
                }
            )
        except Exception as exc:
            self.halt(
                f"strategy position close state uncertain: positionId={position_id} "
                f"error={type(exc).__name__}"
            )
            return False
        if not response.get("success"):
            logging.error(
                "STRATEGY POSITION CLOSE REJECTED reason=%s positionId=%s "
                "code=%s message=%r",
                reason,
                position_id,
                response.get("errorCode"),
                response.get("errorMessage"),
            )
            return False
        for attempt in range(1, 5):
            positions, orders = await self.reconcile()
            if not positions:
                if not await self.cancel_and_confirm_orphan_exits(orders):
                    return False
                logging.info(
                    "STRATEGY POSITION CLOSE CONFIRMED reason=%s positionId=%s attempt=%s",
                    reason,
                    position_id,
                    attempt,
                )
                self.clear_tracked_position()
                return True
            if attempt < 4:
                await asyncio.sleep(0.25)
        self.halt(
            f"strategy position close not confirmed: positionId={position_id}"
        )
        return False

    async def strategy_date_exit(
        self, result: StrategyResult, *, entry_eligible: bool
    ) -> str:
        logging.info(
            "SIGNAL DATE_RANGE_EXIT candle=%s", result.candle.time.isoformat()
        )
        if not self.config.enabled:
            return "DISABLED reasons=ORDER_EXECUTION_DISABLED"
        if not entry_eligible:
            return "RECONNECT_REPLAY_BLOCKED"
        if self.halted_reason or self.readiness_errors():
            return "SAFETY_BLOCKED"
        async with self.lock:
            positions, _ = await self.reconcile()
            if not positions:
                return "FLAT"
            if len(positions) != 1:
                self.halt("date-range exit found unexpected MNQ position count")
                return "POSITION_UNCERTAIN"
            return (
                "POSITION_CLOSED"
                if await self.close_strategy_position(
                    positions[0], "DATE_RANGE_EXIT"
                )
                else "CLOSE_UNCONFIRMED"
            )
    async def signal(
        self,
        result: StrategyResult,
        *,
        entry_eligible: bool,
        market_fresh: bool,
    ) -> str:
        if self.approved_account is not None and not self.account_eligible():
            return "SAFETY_BLOCKED reasons=ACCOUNT_UNVERIFIED"
        if result.signal == "DATE_RANGE_EXIT":
            return await self.strategy_date_exit(
                result, entry_eligible=entry_eligible
            )
        if result.signal not in ENTRY_SIGNALS:
            return "NO_SIGNAL"
        if not self.entries_enabled:
            return "ACCOUNT_PAUSED"
        logging.info(
            "SIGNAL %s candle=%s price=%.2f",
            result.signal,
            result.candle.time.isoformat(),
            result.candle.close,
        )
        if not self.config.enabled:
            readiness_errors = self.readiness_errors()
            reason_codes = ["ORDER_EXECUTION_DISABLED"] + [
                self.readiness_reason_code(error) for error in readiness_errors
            ]
            logging.warning(
                "SIGNAL BLOCKED signal=%s reasons=%s details=%s",
                result.signal,
                ",".join(reason_codes),
                ["ENABLE_ORDER_EXECUTION is false", *readiness_errors],
            )
            return "DISABLED reasons=" + ",".join(reason_codes)
        if result.candle.time in self.processed_candles:
            return "DUPLICATE_BLOCKED"
        self.processed_candles.add(result.candle.time)
        if not entry_eligible:
            return "RECONNECT_REPLAY_BLOCKED"
        if not market_fresh:
            logging.warning(
                "SIGNAL SKIPPED signal=%s candle=%s reason=MARKET_DATA_STALE; "
                "waiting for a fresh live quote without halting future entries",
                result.signal,
                result.candle.time.isoformat(),
            )
            return "STALE_MARKET_BLOCKED"
        if self.halted_reason:
            code = self.halted_code or self.halt_reason_code(self.halted_reason)
            logging.warning(
                "SIGNAL HALTED signal=%s reason=%s detail=%s "
                "lastPositionIds=%s protectiveStopOrderId=%s protectiveStopPrice=%s",
                result.signal,
                code,
                self.halted_reason,
                [position.get("id") for position in self.last_positions],
                self.confirmed_stop_order_id,
                self.confirmed_stop_price,
            )
            return f"HALTED reason={code}"
        readiness_errors = self.readiness_errors()
        if readiness_errors:
            reason_codes = [
                self.readiness_reason_code(error) for error in readiness_errors
            ]
            logging.warning(
                "SIGNAL SAFETY BLOCKED signal=%s reasons=%s details=%s",
                result.signal,
                ",".join(reason_codes),
                readiness_errors,
            )
            return "SAFETY_BLOCKED reasons=" + ",".join(reason_codes)

        async with self.lock:
            if not self.entries_enabled:
                return "ACCOUNT_PAUSED"
            if self.order_pending:
                return "PENDING_BLOCKED"
            try:
                positions, open_orders = await self.reconcile()
                net = await self.daily_net_pnl()
            except Exception as exc:
                self.halt(f"pre-entry reconciliation failed: {type(exc).__name__}")
                return "UNCERTAIN_BLOCKED"
            if (
                self.config.daily_loss_limit_enabled
                and net <= -self.config.max_daily_loss_usd
            ):
                self.halt(f"daily net P&L limit reached ({net:.2f} USD)")
                return "DAILY_LOSS_BLOCKED"
            if positions:
                side = ENTRY_SIGNALS[result.signal]
                desired_type = LONG_POSITION if side == 0 else SHORT_POSITION
                may_reverse = (
                    self.config.strategy_name == "ema_touch_reentry"
                    and result.signal in {"LONG_CROSS", "SHORT_CROSS"}
                    and len(positions) == 1
                    and int(positions[0].get("type", 0)) != desired_type
                )
                if not may_reverse:
                    return "POSITION_EXISTS"
                if not await self.close_strategy_position(
                    positions[0], f"{result.signal}_REVERSAL"
                ):
                    return "REVERSAL_CLOSE_UNCONFIRMED"
                positions, open_orders = await self.reconcile()
                if positions or open_orders:
                    self.halt("reversal cleanup left an unresolved position or order")
                    return "REVERSAL_STATE_UNCERTAIN"
            if open_orders:
                return "ORDER_EXISTS"

            tag = self.custom_tag(result)
            existing = await self.api.orders(
                int(self.account["id"]), self.trading_day_start()
            )
            for record in existing:
                self.validate_record_account(record)
            if any(order.get("customTag") == tag for order in existing):
                return "DUPLICATE_BLOCKED"

            side = ENTRY_SIGNALS[result.signal]
            try:
                self.active_trail_activation_points = (
                    self.config.trail_activation_points
                )
                self.active_trail_ticks = self.trail_ticks
                payload = self.build_entry_payload(side, tag)
                self.validate_stop_bracket_sign(
                    side, int(payload["stopLossBracket"]["ticks"])
                )
            except (ProjectXError, KeyError, TypeError, ValueError) as exc:
                self.halt(f"invalid protective bracket payload: {exc}")
                return "BRACKET_INVALID"
            logging.info(
                "ENTRY REQUEST accountId=%s signal=%s side=%s size=%s tag=%s hardStopTicks=%d",
                self.account["id"], result.signal,
                "BUY" if side == 0 else "SELL",
                payload["size"],
                tag,
                payload["stopLossBracket"]["ticks"],
            )
            self.order_pending = True
            try:
                response = await self.api.place_order_once(payload)
            except Exception as exc:
                self.halt(f"order state uncertain: {type(exc).__name__}")
                return "ORDER_UNCERTAIN"
            finally:
                if self.halted_reason:
                    self.order_pending = False

            if not response.get("success"):
                self.order_pending = False
                logging.error(
                    "ORDER REJECTED orderId=%s code=%s message=%r",
                    response.get("orderId"),
                    response.get("errorCode"),
                    response.get("errorMessage"),
                )
                if response.get("errorCode") in {6, 7}:
                    self.halt("broker returned pending or unknown entry status")
                    return "ORDER_UNCERTAIN"
                return "ORDER_REJECTED"

            order_id = response.get("orderId")
            logging.info("ORDER ACCEPTED orderId=%s tag=%s", order_id, tag)
            try:
                return await self.verify_entry(
                    order_id, tag, {int(order["id"]) for order in open_orders}
                )
            finally:
                self.order_pending = False

    async def verify_fast_oco_closure(
        self, entry: dict[str, Any], orders: list[dict[str, Any]],
        open_orders: list[dict[str, Any]], tag: str,
        pre_entry_order_ids: set[int],
    ) -> bool:
        """Confirm a fixed-OCO trade that opened and closed between REST snapshots."""
        if self.config.take_profit_points is None:
            return False
        account_id = int(self.account["id"])
        related = [
            order for order in orders
            if order.get("accountId") == account_id
            and order.get("contractId") == self.contract["id"]
            and int(order.get("id") or 0) not in pre_entry_order_ids
            and order.get("customTag") in {f"{tag}-SL", f"{tag}-TP"}
        ]
        stops = [order for order in related if order.get("type") == STOP_ORDER]
        targets = [order for order in related if order.get("type") == LIMIT_ORDER]
        if len(stops) != 1 or len(targets) != 1:
            return False
        stop, target = stops[0], targets[0]
        filled = [order for order in (stop, target) if order.get("status") == FILLED_STATUS]
        if len(filled) > 1:
            self.halt(
                f"both OCO exits filled after entry orderId={entry.get('id')} "
                f"exitOrderIds={[order.get('id') for order in filled]}"
            )
            return False
        sibling = target if filled and filled[0] is stop else stop
        if len(filled) != 1 or sibling.get("status") != 3:
            return False
        related_ids = {int(stop["id"]), int(target["id"])}
        if any(int(order.get("id") or 0) in related_ids for order in open_orders):
            return False
        trades = await self.api.trades(
            account_id, now_utc() - timedelta(minutes=10)
        )
        entry_trade = next(
            (trade for trade in trades if trade.get("orderId") == entry.get("id") and not trade.get("voided")),
            None,
        )
        exit_order = filled[0]
        exit_trade = next(
            (trade for trade in trades if trade.get("orderId") == exit_order.get("id")
             and trade.get("profitAndLoss") is not None and not trade.get("voided")),
            None,
        )
        if entry_trade is None or exit_trade is None:
            return False
        size = int(entry.get("fillVolume") or 0)
        if size <= 0 or int(entry_trade.get("size") or 0) != size or int(exit_trade.get("size") or 0) != size:
            return False
        position_type = LONG_POSITION if int(entry.get("side")) == 0 else SHORT_POSITION
        entry_price = Decimal(str(entry_trade["price"]))
        expected_stop = self.initial_protective_stop_price(entry_price, position_type)
        expected_target = self.initial_take_profit_price(entry_price, position_type)
        if (
            Decimal(str(stop.get("stopPrice"))) != expected_stop
            or Decimal(str(target.get("limitPrice"))) != expected_target
        ):
            self.halt(
                "completed OCO protection prices do not match actual entry: "
                f"entry={entry_price} expectedStop={expected_stop} actualStop={stop.get('stopPrice')} "
                f"expectedTarget={expected_target} actualTarget={target.get('limitPrice')}"
            )
            return False
        self.confirmed_stop_order_id = int(stop["id"])
        self.confirmed_take_profit_order_id = int(target["id"])
        logging.info(
            "FAST OCO ROUND TRIP CONFIRMED entryOrderId=%s entryPrice=%s "
            "stopOrderId=%s stopStatus=%s takeProfitOrderId=%s takeProfitStatus=%s "
            "filledExitOrderId=%s accountFlat=true",
            entry.get("id"), entry_price, stop.get("id"), stop.get("status"),
            target.get("id"), target.get("status"), exit_order.get("id"),
        )
        self.emit_event("trade_opened", position_id=None, order_id=int(entry["id"]))
        self.record_exit_fill(exit_order, exit_trade)
        self.confirmed_stop_price = None
        self.confirmed_stop_order_id = None
        self.confirmed_take_profit_order_id = None
        self.confirmed_position_id = None
        self.confirmed_position_type = None
        self.confirmed_position_size = None
        self.locally_opened_position_id = None
        self.last_positions = []
        return True

    async def verify_entry(
        self, order_id: Any, tag: str, pre_entry_order_ids: set[int]
    ) -> str:
        deadline = asyncio.get_running_loop().time() + 20
        account_id = int(self.account["id"])
        while asyncio.get_running_loop().time() < deadline:
            try:
                orders, positions, open_orders = await asyncio.gather(
                    self.api.orders(account_id, now_utc() - timedelta(minutes=10)),
                    self.api.open_positions(account_id),
                    self.api.open_orders(account_id),
                )
            except Exception:
                await asyncio.sleep(1)
                continue
            entry = next(
                (
                    order
                    for order in orders
                    if order.get("id") == order_id or order.get("customTag") == tag
                ),
                None,
            )
            if entry and entry.get("status") == REJECTED_STATUS:
                logging.error("ORDER REJECTED after acceptance orderId=%s", order_id)
                return "ORDER_REJECTED"
            position = next(
                (
                    item
                    for item in positions
                    if item.get("contractId") == self.contract["id"]
                ),
                None,
            )
            entry_status = entry.get("status") if entry else None
            fill_volume = int(entry.get("fillVolume") or 0) if entry else 0
            entry_fill_is_final = (
                entry_status == FILLED_STATUS
                or (
                    entry_status in {3, 4}
                    and fill_volume > 0
                )
            )
            if entry and entry_status in {3, 4, REJECTED_STATUS} and fill_volume == 0:
                logging.warning(
                    "ENTRY TERMINATED UNFILLED orderId=%s status=%s; no position or protection required",
                    order_id, entry_status,
                )
                return "ORDER_NOT_FILLED"
            if entry and entry_fill_is_final and not position:
                try:
                    if await self.verify_fast_oco_closure(
                        entry, orders, open_orders, tag, pre_entry_order_ids
                    ):
                        return "FILLED_PROTECTED"
                except Exception:
                    logging.exception(
                        "Fast OCO closure verification failed orderId=%s", order_id
                    )
            if entry and entry_fill_is_final and position:
                _, mnq_orders = self.mnq_state(positions, open_orders)
                stop = self.find_protective_stop(
                    position, mnq_orders, pre_entry_order_ids
                )
                take_profit = (
                    self.find_take_profit(position, mnq_orders, pre_entry_order_ids)
                    if self.config.take_profit_points is not None else None
                )
                protection_complete = stop is not None and (
                    self.config.take_profit_points is None
                    or take_profit is not None
                )
                if protection_complete:
                    brackets = [stop]
                    if take_profit is not None:
                        brackets.append(take_profit)
                    try:
                        entry_created = datetime.fromisoformat(
                            str(entry["creationTimestamp"]).replace("Z", "+00:00")
                        )
                        bracket_times = [
                            datetime.fromisoformat(
                                str(item["creationTimestamp"]).replace("Z", "+00:00")
                            )
                            for item in brackets
                        ]
                    except (KeyError, TypeError, ValueError):
                        self.halt(
                            "new protective bracket association cannot be verified: "
                            "missing or invalid creation timestamp"
                        )
                        return "PROTECTIVE_STOP_ASSOCIATION_UNVERIFIED"
                    if any(created < entry_created for created in bracket_times):
                        self.halt(
                            "new protective bracket association rejected: "
                            f"entryOrderId={order_id} bracketOrderIds="
                            f"{[item['id'] for item in brackets]}"
                        )
                        return "PROTECTIVE_STOP_ASSOCIATION_REJECTED"
                    entry_price = Decimal(str(position["averagePrice"]))
                    actual_stop = Decimal(str(stop["stopPrice"]))
                    expected_stop = self.initial_protective_stop_price(
                        entry_price, int(position["type"])
                    )
                    if actual_stop != expected_stop:
                        self.confirmed_stop_price = actual_stop
                        self.confirmed_stop_order_id = int(stop["id"])
                        self.last_positions = [position]
                        self.halt(
                            "initial protective stop price mismatch: "
                            f"entry={entry_price} expected={expected_stop} "
                            f"actual={actual_stop} stopOrderId={stop['id']}"
                        )
                        return "INITIAL_PROTECTION_MISMATCH"
                    if take_profit is not None:
                        actual_target = Decimal(str(take_profit["limitPrice"]))
                        expected_target = self.initial_take_profit_price(
                            entry_price, int(position["type"])
                        )
                        if actual_target != expected_target:
                            self.halt(
                                "initial take profit price mismatch: "
                                f"entry={entry_price} expected={expected_target} "
                                f"actual={actual_target} orderId={take_profit['id']}"
                            )
                            return "INITIAL_TAKE_PROFIT_MISMATCH"
                        self.confirmed_take_profit_order_id = int(take_profit["id"])
                    self.confirmed_stop_price = actual_stop
                    self.confirmed_stop_order_id = int(stop["id"])
                    self.last_positions = [position]
                    self.confirmed_position_id = int(position["id"])
                    self.confirmed_position_type = int(position["type"])
                    self.confirmed_position_size = int(position["size"])
                    self.locally_opened_position_id = int(position["id"])
                    logging.info(
                        "FILL AND INITIAL PROTECTION CONFIRMED entryOrderId=%s "
                        "positionId=%s entryPrice=%s requestedSize=%s fillVolume=%s "
                        "positionSize=%s stopOrderId=%s stopPrice=%s "
                        "takeProfitOrderId=%s takeProfitPrice=%s",
                        order_id, position.get("id"), entry_price,
                        self.config.contracts, fill_volume, position.get("size"),
                        stop.get("id"), actual_stop,
                        take_profit.get("id") if take_profit else None,
                        take_profit.get("limitPrice") if take_profit else None,
                    )
                    self.emit_event(
                        "trade_opened",
                        position_id=int(position["id"]),
                        order_id=int(order_id),
                    )
                    return "FILLED_PROTECTED"
            await asyncio.sleep(1)

        flat_after_fill = (
            'entry_fill_is_final' in locals()
            and entry_fill_is_final
            and 'position' in locals()
            and position is None
            and 'open_orders' in locals()
            and not self.mnq_state([], open_orders)[1]
        )
        (logging.warning if flat_after_fill else logging.error)(
            "ENTRY VERIFICATION TIMEOUT orderId=%s entryStatus=%s fillVolume=%s "
            "positionIds=%s openOrderIds=%s",
            order_id, entry_status if 'entry_status' in locals() else None,
            fill_volume if 'fill_volume' in locals() else None,
            [item.get("id") for item in positions] if 'positions' in locals() else [],
            [item.get("id") for item in open_orders] if 'open_orders' in locals() else [],
        )
        if flat_after_fill:
            logging.warning(
                "ENTRY FILLED BUT ACCOUNT IS FLAT orderId=%s fillVolume=%s; "
                "no MNQ position or open exit order remains, so new orders are not halted",
                order_id,
                fill_volume,
            )
            self.clear_tracked_position()
            return "FILLED_FLAT"
        self.halt("fill or hard protective stop could not be confirmed")
        return "PROTECTION_UNCERTAIN"

    def quantize_stop(self, value: Decimal, position_type: int) -> Decimal:
        units = value / self.tick_size
        rounding = ROUND_FLOOR if position_type == LONG_POSITION else ROUND_CEILING
        return units.to_integral_value(rounding=rounding) * self.tick_size

    def record_exit_fill(
        self,
        exit_order: dict[str, Any],
        matching_trade: dict[str, Any] | None,
    ) -> bool:
        order_id = int(exit_order["id"])
        if order_id in self.processed_exit_order_ids:
            logging.info("PROTECTIVE STOP FILL ALREADY PROCESSED orderId=%s", order_id)
            return False
        self.processed_exit_order_ids.add(order_id)
        realized = None
        if matching_trade is not None and matching_trade.get("profitAndLoss") is not None:
            realized = Decimal(str(matching_trade["profitAndLoss"])) - Decimal(
                str(matching_trade.get("fees") or 0)
            )
        logging.info(
            "PROTECTIVE EXIT FILLED orderId=%s exitPrice=%s realizedNetPnl=%s USD; "
            "POSITION CLOSED",
            order_id,
            exit_order.get("filledPrice"),
            f"{realized:.2f}" if realized is not None else "UNAVAILABLE",
        )
        self.emit_event(
            "trade_closed",
            order_id=order_id,
            position_id=self.confirmed_position_id,
            pnl=realized,
            timestamp=now_utc(),
        )
        return True

    def associated_exit_order_ids(self) -> set[int]:
        return {
            order_id
            for order_id in (
                self.confirmed_stop_order_id,
                self.confirmed_take_profit_order_id,
                self.native_trailing_order_id,
            )
            if order_id is not None
        }

    def is_tracked_exit_order(self, order: dict[str, Any]) -> bool:
        expected_side = (
            1 if self.confirmed_position_type == LONG_POSITION else 0
            if self.confirmed_position_type == SHORT_POSITION else None
        )
        try:
            order_id = int(order.get("id"))
            size = int(order.get("size"))
        except (TypeError, ValueError):
            return False
        return (
            order_id in self.associated_exit_order_ids()
            and order.get("accountId") == self.account.get("id")
            and order.get("contractId") == self.contract.get("id")
            and order.get("type") in {LIMIT_ORDER, STOP_ORDER, TRAILING_STOP_ORDER}
            and order.get("side") == expected_side
            and size > 0
            and order.get("status") in OPEN_OR_PENDING_STATUSES
        )

    def tracked_exit_quantity_mismatch(
        self,
        position: dict[str, Any],
        orders: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        expected_size = int(position["size"])
        tracked_ids = self.associated_exit_order_ids()
        for order in orders:
            try:
                order_id = int(order.get("id"))
                order_size = int(order.get("size"))
            except (TypeError, ValueError):
                continue
            if (
                order_id in tracked_ids
                and order.get("accountId") == self.account.get("id")
                and order.get("contractId") == self.contract.get("id")
                and order.get("status") in OPEN_OR_PENDING_STATUSES
                and order_size != expected_size
            ):
                return order
        return None

    async def cancel_and_confirm_orphan_exits(
        self, open_orders: list[dict[str, Any]]
    ) -> bool:
        tracked_open = [
            order for order in open_orders if self.is_tracked_exit_order(order)
        ]
        if not tracked_open:
            return True
        account_id = int(self.account["id"])
        for order in tracked_open:
            order_id = int(order["id"])
            logging.warning(
                "ORPHAN EXIT CANCEL REQUEST positionId=%s orderId=%s type=%s",
                self.confirmed_position_id,
                order_id,
                order.get("type"),
            )
            try:
                response = await self.api.cancel_order_once(
                    {"accountId": account_id, "orderId": order_id}
                )
            except Exception as exc:
                self.halt(
                    "orphan exit cancellation state uncertain: "
                    f"orderId={order_id} error={type(exc).__name__}"
                )
                return False
            if not response.get("success"):
                self.halt(
                    "orphan exit cancellation rejected: "
                    f"orderId={order_id} code={response.get('errorCode')} "
                    f"message={response.get('errorMessage')!r}"
                )
                return False
            confirmed = False
            for attempt in range(1, self.cancel_verify_attempts + 1):
                try:
                    current = await self.api.open_orders(account_id)
                except Exception as exc:
                    self.halt(
                        "orphan exit cancellation verification failed: "
                        f"orderId={order_id} error={type(exc).__name__}"
                    )
                    return False
                if not any(
                    candidate.get("id") == order_id
                    and candidate.get("accountId") == account_id
                    and candidate.get("contractId") == self.contract.get("id")
                    for candidate in current
                ):
                    confirmed = True
                    logging.info(
                        "ORPHAN EXIT CANCELLATION CONFIRMED orderId=%s attempt=%d",
                        order_id,
                        attempt,
                    )
                    break
                if attempt < self.cancel_verify_attempts:
                    await asyncio.sleep(self.cancel_verify_delay_seconds)
            if not confirmed:
                self.halt(
                    "orphan exit cancellation could not be confirmed: "
                    f"orderId={order_id}"
                )
                return False
        return True

    @staticmethod
    def broker_order_diagnostic(order: dict[str, Any]) -> dict[str, Any]:
        fields = (
            "id", "accountId", "contractId", "creationTimestamp",
            "updateTimestamp", "status", "type", "side", "size",
            "limitPrice", "stopPrice", "trailPrice", "filledPrice", "fillVolume", "customTag",
        )
        return {field: order.get(field) for field in fields}

    @staticmethod
    def broker_trade_diagnostic(trade: dict[str, Any]) -> dict[str, Any]:
        fields = (
            "id", "orderId", "accountId", "contractId", "creationTimestamp",
            "side", "size", "price", "profitAndLoss", "fees", "voided",
        )
        return {field: trade.get(field) for field in fields}

    async def log_position_closure(self) -> bool:
        account_id = int(self.account["id"])
        start = now_utc() - timedelta(hours=2)
        orders, trades = await asyncio.gather(
            self.api.orders(account_id, start),
            self.api.trades(account_id, start),
        )
        exit_order = next(
            (
                order
                for order in reversed(orders)
                if order.get("id") in self.associated_exit_order_ids()
                and order.get("accountId") == self.account.get("id")
                and order.get("contractId") == self.contract["id"]
                and order.get("status") == FILLED_STATUS
                and order.get("type") in {LIMIT_ORDER, STOP_ORDER, TRAILING_STOP_ORDER}
            ),
            None,
        )
        if exit_order is None:
            relevant_orders = [
                self.broker_order_diagnostic(order)
                for order in orders
                if order.get("accountId") == self.account.get("id")
                and order.get("contractId") == self.contract["id"]
                and (
                    order.get("id") in self.associated_exit_order_ids()
                    or order.get("type") in {LIMIT_ORDER, STOP_ORDER, TRAILING_STOP_ORDER}
                )
            ]
            relevant_order_ids = {
                order.get("id") for order in orders
                if order.get("accountId") == self.account.get("id")
                and order.get("contractId") == self.contract["id"]
            }
            relevant_trades = [
                self.broker_trade_diagnostic(trade)
                for trade in trades
                if trade.get("accountId") == self.account.get("id")
                and trade.get("contractId") == self.contract["id"]
                and (
                    trade.get("orderId") in self.associated_exit_order_ids()
                    or trade.get("orderId") in relevant_order_ids
                )
            ]
            logging.error(
                "CLOSURE RECONCILIATION EVIDENCE positionId=%s direction=%s "
                "quantity=%s trackedHardStopOrderId=%s trackedTakeProfitOrderId=%s "
                "trackedTrailingOrderId=%s "
                "positionOpen=False orders=%s trades=%s",
                self.confirmed_position_id,
                self.confirmed_position_type,
                self.confirmed_position_size,
                self.confirmed_stop_order_id,
                self.confirmed_take_profit_order_id,
                self.native_trailing_order_id,
                relevant_orders,
                relevant_trades,
            )
            return False
        matching_trade = next(
            (
                trade
                for trade in reversed(trades)
                if trade.get("orderId") == exit_order.get("id")
                and not trade.get("voided")
            ),
            None,
        )
        self.record_exit_fill(exit_order, matching_trade)
        return True
    def native_trailing_payload(
        self, position: dict[str, Any], last_price: Decimal
    ) -> dict[str, Any]:
        position_type = int(position["type"])
        distance = Decimal(self.position_trail_ticks) * self.tick_size
        if position_type == LONG_POSITION:
            side = 1
            trail_price = self.quantize_stop(
                last_price - distance, LONG_POSITION
            )
        elif position_type == SHORT_POSITION:
            side = 0
            trail_price = self.quantize_stop(
                last_price + distance, SHORT_POSITION
            )
        else:
            raise ProjectXError(f"invalid position type: {position_type}")
        return {
            "accountId": int(self.account["id"]),
            "contractId": self.contract["id"],
            "type": TRAILING_STOP_ORDER,
            "side": side,
            "size": int(position["size"]),
            "limitPrice": None,
            "stopPrice": None,
            "trailPrice": float(trail_price),
            "customTag": f"mnq-trail-position-{int(position['id'])}",
            "stopLossBracket": None,
            "takeProfitBracket": None,
        }

    async def activate_native_trailing(
        self,
        position: dict[str, Any],
        orders: list[dict[str, Any]],
        observed_last_price: Decimal | None = None,
        skip_history_preflight: bool = False,
    ) -> str:
        position_id = int(position["id"])
        trailing_candidates = self.matching_exit_orders(
            position, orders, TRAILING_STOP_ORDER
        )
        if len(trailing_candidates) > 1:
            self.halt("multiple native trailing exits exist for one position")
            return "DUPLICATE_TRAILING_UNSAFE"
        if trailing_candidates:
            trailing = trailing_candidates[0]
            self.native_trailing_order_id = int(trailing["id"])
            self.native_trailing_active = True
            self.trailing_attempted_position_id = position_id
            return "NATIVE_TRAILING_ACTIVE"
        if self.trailing_attempted_position_id == position_id:
            return "NATIVE_TRAILING_ALREADY_ATTEMPTED"

        if observed_last_price is None:
            quote = self.quote_source.quote_snapshot()
            if quote is None or quote.get("lastPrice") is None:
                return "WAITING_FOR_FRESH_QUOTE"
            last_price = Decimal(str(quote["lastPrice"]))
        else:
            last_price = observed_last_price
        entry_price = Decimal(str(position["averagePrice"]))
        favorable_points = (
            last_price - entry_price
            if int(position["type"]) == LONG_POSITION
            else entry_price - last_price
        )
        if favorable_points < self.position_trail_activation_points:
            return "WAITING_FOR_TRAIL_ACTIVATION"

        custom_tag = f"mnq-trail-position-{position_id}"
        if not skip_history_preflight:
            prior_orders = await self.api.orders(
                int(self.account["id"]), self.trading_day_start()
            )
            prior = next(
                (
                    order for order in prior_orders
                    if order.get("customTag") == custom_tag
                ),
                None,
            )
            if prior is not None:
                self.trailing_attempted_position_id = position_id
                prior_id = prior.get("id")
                prior_status = prior.get("status")
                if prior_id is not None:
                    self.native_trailing_order_id = int(prior_id)
                logging.warning(
                    "NATIVE TRAILING NOT RESUBMITTED positionId=%s orderId=%s "
                    "brokerStatus=%s tag=%s",
                    position_id,
                    prior_id,
                    prior_status,
                    custom_tag,
                )
                if prior_status in OPEN_OR_PENDING_STATUSES:
                    return "NATIVE_TRAILING_ALREADY_SUBMITTED"
                if prior_status == FILLED_STATUS:
                    self.halt(
                        "native trailing is filled while its position remains open"
                    )
                    return "NATIVE_TRAILING_FILL_POSITION_MISMATCH"
                return "NATIVE_TRAILING_PRIOR_ATTEMPT_FOUND"

        # Re-read the stream snapshot immediately before submission. Recovered
        # positions retain the history preflight above; locally opened positions
        # use the in-memory latch and unique tag to take the direct fast path.
        submission_price = last_price
        submission_quote = self.quote_source.quote_snapshot()
        if submission_quote is not None and submission_quote.get("lastPrice") is not None:
            try:
                candidate = Decimal(str(submission_quote["lastPrice"]))
                if candidate.is_finite():
                    submission_price = candidate
            except (InvalidOperation, TypeError, ValueError):
                pass
        payload = self.native_trailing_payload(position, submission_price)
        self.trailing_attempted_position_id = position_id
        logging.info(
            "NATIVE TRAILING REQUEST positionId=%s direction=%s entryPrice=%s "
            "activationPrice=%s submissionPrice=%s favorablePoints=%s "
            "side=%s size=%s trailPrice=%s "
            "distanceTicks=%s hardStopOrderId=%s fastPath=%s",
            position_id,
            "LONG" if int(position["type"]) == LONG_POSITION else "SHORT",
            entry_price,
            last_price,
            submission_price,
            favorable_points,
            "SELL" if payload["side"] == 1 else "BUY",
            payload["size"],
            payload["trailPrice"],
            self.position_trail_ticks,
            self.confirmed_stop_order_id,
            skip_history_preflight,
        )
        try:
            response = await self.api.place_order_once(payload)
        except Exception as exc:
            self.halt(
                "native trailing order state uncertain: "
                f"positionId={position_id} error={type(exc).__name__}"
            )
            return "NATIVE_TRAILING_UNCERTAIN"
        logging.info(
            "NATIVE TRAILING RESPONSE positionId=%s orderId=%s success=%s "
            "code=%s message=%r",
            position_id,
            response.get("orderId"),
            response.get("success"),
            response.get("errorCode"),
            response.get("errorMessage"),
        )
        if not response.get("success"):
            if response.get("errorCode") in {6, 7}:
                self.halt("native trailing broker status is pending or unknown")
                return "NATIVE_TRAILING_UNCERTAIN"
            return "NATIVE_TRAILING_REJECTED_HARD_STOP_ACTIVE"

        order_id = int(response["orderId"])
        self.native_trailing_order_id = order_id
        for attempt in range(1, 4):
            current_positions, current_orders = await self.reconcile()
            if not current_positions:
                return "POSITION_CLOSED_DURING_TRAIL_ACTIVATION"
            confirmed = next(
                (
                    order for order in current_orders
                    if int(order.get("id", -1)) == order_id
                    and order.get("type") == TRAILING_STOP_ORDER
                    and order.get("side") == payload["side"]
                    and order.get("size") == payload["size"]
                ),
                None,
            )
            if confirmed:
                self.native_trailing_active = True
                self.emit_event(
                    "trailing_activated",
                    position_id=position_id,
                    order_id=order_id,
                )
                expected_distance = (
                    Decimal(self.position_trail_ticks) * self.tick_size
                )
                try:
                    broker_distance = Decimal(str(confirmed["trailPrice"]))
                except (KeyError, InvalidOperation):
                    self.halt(
                        "native trailing broker distance is missing or invalid: "
                        f"positionId={position_id} orderId={order_id}"
                    )
                    return "NATIVE_TRAILING_DISTANCE_UNCERTAIN"
                if broker_distance != expected_distance:
                    actual_ticks = broker_distance / self.tick_size
                    self.trailing_distance_mismatch_position_id = position_id
                    self.trailing_requested_distance = expected_distance
                    self.trailing_actual_distance = broker_distance
                    logging.warning(
                        "NATIVE TRAILING DISTANCE MISMATCH positionId=%s "
                        "orderId=%s requestedDistance=%s requestedTicks=%s "
                        "actualDistance=%s actualTicks=%s submittedTrailPrice=%s "
                        "brokerTrailPrice=%s brokerStopPrice=%s "
                        "positionScoped=true",
                        position_id,
                        order_id,
                        expected_distance,
                        self.position_trail_ticks,
                        broker_distance,
                        actual_ticks,
                        payload["trailPrice"],
                        confirmed.get("trailPrice"),
                        confirmed.get("stopPrice"),
                    )
                    return "NATIVE_TRAILING_DISTANCE_MISMATCH_POSITION_ACTIVE"
                logging.info(
                    "NATIVE TRAILING CONFIRMED positionId=%s orderId=%s "
                    "brokerTrailDistance=%s hardStopOrderId=%s "
                    "independentExitRace=true",
                    position_id,
                    order_id,
                    broker_distance,
                    self.confirmed_stop_order_id,
                )
                return "NATIVE_TRAILING_ACTIVE"
            if attempt < 3:
                await asyncio.sleep(0.25)
        self.halt(
            "native trailing accepted but open order could not be confirmed "
            f"positionId={position_id} orderId={order_id}"
        )
        return "NATIVE_TRAILING_UNCERTAIN"

    def observe_live_price(self, quote: dict[str, Any]) -> None:
        """Latch a qualifying stream price without waiting for REST polling."""
        if (
            not self.config.enabled
            or self.config.trail_management_mode != "native_delayed"
            or self.halted_reason
            or self.order_pending
            or self.native_trailing_active
            or self.trailing_attempted_position_id is not None
            or len(self.last_positions) != 1
            or self.confirmed_position_id is None
            or self.confirmed_stop_order_id is None
        ):
            return
        position = self.last_positions[0]
        position_id = int(position.get("id", -1))
        if (
            position_id != self.confirmed_position_id
            or self.live_trailing_latched_position_id == position_id
        ):
            return
        try:
            last_price = Decimal(str(quote["lastPrice"]))
            if not last_price.is_finite():
                return
            entry_price = Decimal(str(position["averagePrice"]))
            favorable_points = (
                last_price - entry_price
                if int(position["type"]) == LONG_POSITION
                else entry_price - last_price
            )
        except (KeyError, InvalidOperation, TypeError, ValueError):
            return
        if favorable_points < self.position_trail_activation_points:
            return

        # This method is called synchronously by the stream reader. Setting the
        # latch before scheduling makes a burst of qualifying ticks idempotent.
        self.live_trailing_latched_position_id = position_id
        logging.info(
            "LIVE TRAILING ACTIVATION OBSERVED positionId=%s lastPrice=%s "
            "favorablePoints=%s",
            position_id,
            last_price,
            favorable_points,
        )
        asyncio.create_task(
            self._activate_native_trailing_from_stream(position_id, last_price)
        )

    async def _activate_native_trailing_from_stream(
        self, position_id: int, observed_last_price: Decimal
    ) -> None:
        async with self.lock:
            if (
                self.halted_reason
                or self.order_pending
                or self.native_trailing_active
                or self.trailing_attempted_position_id is not None
                or len(self.last_positions) != 1
                or int(self.last_positions[0].get("id", -1)) != position_id
                or self.confirmed_position_id != position_id
                or self.confirmed_stop_order_id is None
            ):
                return
            try:
                outcome = await self.activate_native_trailing(
                    self.last_positions[0], [], observed_last_price,
                    skip_history_preflight=(
                        self.locally_opened_position_id == position_id
                    ),
                )
                logging.info(
                    "LIVE TRAILING ACTIVATION RESULT positionId=%s outcome=%s",
                    position_id,
                    outcome,
                )
            except Exception as exc:
                self.halt(
                    "live trailing activation failed: "
                    f"positionId={position_id} error={type(exc).__name__}"
                )

    async def monitor_once(self) -> str:
        async with self.lock:
            return await self._monitor_once_locked()

    async def _monitor_once_locked(self) -> str:
        if self.order_pending:
            return "ENTRY_VERIFICATION_PENDING"
        previous_had_position = bool(self.last_positions)
        positions, orders = await self.reconcile()
        now = asyncio.get_running_loop().time()
        if self.config.daily_loss_limit_enabled and (
            self.last_background_pnl_refresh is None
            or now - self.last_background_pnl_refresh >= 15
        ):
            net = await self.daily_net_pnl()
            self.last_background_pnl_refresh = now
            if net <= -self.config.max_daily_loss_usd:
                self.halt(f"daily net P&L limit reached ({net:.2f} USD)")
        if previous_had_position and not positions:
            account_id = int(self.account["id"])
            confirmation_positions = await self.api.open_positions(account_id)
            confirmed_mnq_positions, _ = self.mnq_state(
                confirmation_positions, []
            )
            if confirmed_mnq_positions:
                self.last_positions = confirmed_mnq_positions
                logging.info(
                    "POSITION CLOSURE NOT YET CONFIRMED positionId=%s",
                    self.confirmed_position_id,
                )
                return "POSITION_CLOSURE_NOT_CONFIRMED"

            fresh_open_orders = await self.api.open_orders(account_id)
            _, fresh_mnq_orders = self.mnq_state([], fresh_open_orders)
            logging.info(
                "POSITION CLOSURE CONFIRMED positionId=%s "
                "trackedHardStopOrderId=%s trackedTakeProfitOrderId=%s "
                "trackedTrailingOrderId=%s "
                "freshOpenOrderIds=%s",
                self.confirmed_position_id,
                self.confirmed_stop_order_id,
                self.confirmed_take_profit_order_id,
                self.native_trailing_order_id,
                [order.get("id") for order in fresh_mnq_orders],
            )
            if not await self.cancel_and_confirm_orphan_exits(
                fresh_mnq_orders
            ):
                return "ORPHAN_CANCELLATION_UNCERTAIN"
            try:
                closure_confirmed = await self.log_position_closure()
            except Exception:
                raise
            self.confirmed_stop_price = None
            self.confirmed_stop_order_id = None
            self.confirmed_take_profit_order_id = None
            closed_position_id = self.confirmed_position_id
            self.native_trailing_order_id = None
            self.native_trailing_active = False
            self.trailing_attempted_position_id = None
            self.live_trailing_latched_position_id = None
            self.locally_opened_position_id = None
            self.confirmed_position_id = None
            self.confirmed_position_type = None
            self.confirmed_position_size = None
            if not closure_confirmed:
                self.halt("position closed but broker-side exit could not be confirmed")
                return "CLOSURE_UNCONFIRMED"
            if (
                self.trailing_distance_mismatch_position_id
                == closed_position_id
            ):
                logging.info(
                    "NATIVE TRAILING DISTANCE MISMATCH RESOLVED "
                    "positionId=%s requestedDistance=%s actualDistance=%s "
                    "positionClosed=True remainingOrdersResolved=True",
                    closed_position_id,
                    self.trailing_requested_distance,
                    self.trailing_actual_distance,
                )
                self.trailing_distance_mismatch_position_id = None
                self.trailing_requested_distance = None
                self.trailing_actual_distance = None
            if (
                self.halted_reason
                and self.halted_reason.startswith(
                    "native trailing distance mismatch:"
                )
            ):
                logging.info(
                    "POSITION-SCOPED HALT CLEARED positionId=%s reason=%s",
                    closed_position_id,
                    self.halted_code,
                )
                self.halted_reason = None
                self.halted_code = None
            self.last_positions = []
            return "POSITION_CLOSED"
        if positions:
            if len(positions) != 1:
                self.halt("unexpected MNQ position count during monitoring")
                return "POSITION_UNCERTAIN"
            position = positions[0]
            self.last_positions = [position]
            size_mismatch = self.tracked_exit_quantity_mismatch(
                position, orders
            )
            if size_mismatch is not None:
                self.halt(
                    "tracked protective exit quantity mismatch: "
                    f"positionId={position.get('id')} "
                    f"positionSize={position.get('size')} "
                    f"orderId={size_mismatch.get('id')} "
                    f"orderSize={size_mismatch.get('size')}"
                )
                return "PROTECTIVE_EXIT_SIZE_MISMATCH"
            stop = self.find_protective_stop(position, orders)
            if stop is None:
                self.halt("protective hard stop missing or ambiguous")
                return "PROTECTION_UNCERTAIN"
            self.confirmed_stop_order_id = int(stop["id"])
            self.confirmed_stop_price = Decimal(str(stop["stopPrice"]))
            self.confirmed_position_id = int(position["id"])
            self.confirmed_position_type = int(position["type"])
            self.confirmed_position_size = int(position["size"])
            if self.config.take_profit_points is not None:
                take_profit = self.find_take_profit(position, orders)
                if take_profit is None:
                    self.halt("protective take profit missing or ambiguous")
                    return "PROTECTION_UNCERTAIN"
                self.confirmed_take_profit_order_id = int(take_profit["id"])
                if self.config.trail_management_mode == "fixed_oco":
                    return "FIXED_OCO_ACTIVE"
            if self.live_trailing_latched_position_id == int(position["id"]):
                return "LIVE_TRAILING_ACTIVATION_PENDING"
            return await self.activate_native_trailing(position, orders)
        self.last_positions = []
        return "FLAT"

    async def monitor(self) -> None:
        if self.approved_account is not None and not self.account_eligible():
            return
        if not self.config.enabled:
            return
        loop = asyncio.get_running_loop()
        previous_cycle_start: float | None = None
        while True:
            cycle_start = loop.time()
            if previous_cycle_start is not None:
                logging.debug(
                    "EXECUTION POLL CYCLE intervalSeconds=%.3f "
                    "configuredSleepSeconds=%.3f",
                    cycle_start - previous_cycle_start,
                    self.config.execution_poll_interval_seconds,
                )
            previous_cycle_start = cycle_start
            try:
                await self.monitor_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.halt(f"order-state monitoring failed: {type(exc).__name__}")
            await asyncio.sleep(
                self.config.execution_poll_interval_seconds
            )










