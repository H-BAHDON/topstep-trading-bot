"""TopstepX MNQ strategy runner with multi-account, risk-gated execution."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfoNotFoundError

from dotenv import dotenv_values, load_dotenv

from execution import (
    EntryPreparationBatch, ExecutionManager, PreparedEntry, TradingConfig,
)
from strategies.base import StrategyResult
from strategies.test_long import TestLongStrategy
from strategies.ema12_hourly_trailing import Ema12HourlyTrailingStrategy, HourlyEmaResult
from strategies.ema12_hourly_trailing_43 import Ema12HourlyTrailing43Strategy
from strategies.ema12_hourly_trailing_55 import Ema12HourlyTrailing55Strategy
from telegram_monitor import AccountTelegramMonitor, PauseStateStore
from topstep_api import MarketDataFeed, ProjectX, ProjectXError, env_bool


def configure_logging() -> None:
    log_directory = Path(__file__).resolve().parent / "logs"
    log_directory.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    formatter.converter = time.gmtime
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        log_directory / "bot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    root.addHandler(stream)
    root.addHandler(file_handler)
    # httpx logs complete Telegram URLs, which contain the bot token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

STRATEGIES = {
    "test_long": TestLongStrategy,
    "ema12_hourly_trailing": Ema12HourlyTrailingStrategy,
    "ema12_hourly_trailing_43": Ema12HourlyTrailing43Strategy,
    "ema12_hourly_trailing_55": Ema12HourlyTrailing55Strategy,
}


def select_strategy(name: str, entry_mode: str | None = None):
    strategy_class = STRATEGIES.get(name)
    if strategy_class is None:
        available = ", ".join(sorted(STRATEGIES))
        raise ProjectXError(
            f"Unknown ACTIVE_STRATEGY={name!r}; choose one of: {available}"
        )
    if strategy_class in {
        Ema12HourlyTrailingStrategy,
        Ema12HourlyTrailing43Strategy,
        Ema12HourlyTrailing55Strategy,
    }:
        try:
            ema_length = int(os.getenv("EMA12_HOURLY_EMA_LENGTH", "12"))
            timeframe = int(os.getenv("EMA12_HOURLY_TIMEFRAME_MINUTES", "15"))
            return strategy_class(
                ema_length=ema_length,
                timeframe_minutes=timeframe,
                start_time=strategy_class.parse_clock(
                    os.getenv("EMA12_HOURLY_START_TIME", "00:00")
                ),
                end_time=strategy_class.parse_clock(
                    os.getenv("EMA12_HOURLY_END_TIME", "20:00")
                ),
                timezone_name=os.getenv(
                    "EMA12_HOURLY_TIMEZONE", "Europe/London"
                ).strip(),
            )
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ProjectXError(f"Invalid ema12_hourly_trailing settings: {exc}") from exc
    return strategy_class()


def load_environment(path: str) -> Path:
    selected = Path(path).expanduser().resolve()
    if not selected.is_file():
        raise ProjectXError(f"Environment file does not exist: {selected}")
    default = Path(__file__).resolve().parent / ".env"
    if selected != default and default.is_file():
        load_dotenv(default, override=False)
    load_dotenv(selected, override=True)
    if os.getenv("ACCOUNTS_FILE", "").strip():
        account_path = Path(os.environ["ACCOUNTS_FILE"]).expanduser()
        if not account_path.is_absolute():
            source = selected if "ACCOUNTS_FILE" in dotenv_values(selected) else default
            account_path = source.parent / account_path
        load_account_configuration(account_path)
        os.environ["ACCOUNTS_FILE"] = str(account_path.resolve())
    return selected


def load_account_configuration(path: Path) -> list[dict]:
    """Validate all entries before contacting the broker. Never mutate account env."""
    try:
        document = json.loads(path.read_text(encoding="utf-8-sig"))
        accounts = document["accounts"]
        if not isinstance(accounts, list) or not accounts:
            raise ValueError("accounts must be a nonempty list")
        ids, names = set(), set()
        normalized = []
        for entry in accounts:
            if not isinstance(entry, dict):
                raise ValueError("each account must be an object")
            account = dict(entry)
            account_id = account["account_id"]
            if type(account_id) is not int or account_id <= 0 or account_id in ids:
                raise ValueError("account_id must be a unique positive integer")
            ids.add(account_id)
            if type(account.get("enabled")) is not bool:
                raise ValueError("enabled must be a boolean")
            name = account.get("name")
            if not isinstance(name, str) or not name.strip() or name.casefold() in names:
                raise ValueError("name must be a unique nonempty string")
            names.add(name.casefold())
            kind = account.setdefault("account_type", "practice")
            if kind not in {"practice", "express"}:
                raise ValueError("account_type must be practice or express; Live is unsupported")
            broker_name = account.get("account_name", account.get("verified_practice_account_name"))
            if not isinstance(broker_name, str) or not broker_name.strip():
                raise ValueError("account_name must contain the exact broker account name")
            account["account_name"] = broker_name
            verified_id = account.get("verified_practice_account_id")
            if verified_id is not None and (type(verified_id) is not int or verified_id != account_id):
                raise ValueError("verified practice account ID must match account_id")
            verified = account.get("practice_account_simulated_verified", False)
            if type(verified) is not bool:
                raise ValueError("practice_account_simulated_verified must be a boolean")
            if verified and (kind != "practice" or verified_id is None
                             or account.get("verified_practice_account_name") != broker_name):
                raise ValueError("explicit practice verification requires matching practice identity")
            if "contracts" in account and (type(account["contracts"]) is not int or account["contracts"] <= 0):
                raise ValueError("contracts must be a positive integer")
            if "daily_loss_limit_enabled" in account and type(account["daily_loss_limit_enabled"]) is not bool:
                raise ValueError("daily_loss_limit_enabled must be a boolean")
            if "trailing_enabled" in account and type(account["trailing_enabled"]) is not bool:
                raise ValueError("trailing_enabled must be a boolean")
            if "max_daily_loss_usd" in account:
                limit = Decimal(str(account["max_daily_loss_usd"]))
                if not limit.is_finite() or limit <= 0:
                    raise ValueError("max_daily_loss_usd must be a positive finite number")
            for field in (
                "stop_loss_points",
                "take_profit_points",
                "trail_activation_points",
                "trail_distance_points",
            ):
                if field not in account:
                    continue
                if isinstance(account[field], bool):
                    raise ValueError(f"{field} must be a positive finite number")
                points = Decimal(str(account[field]))
                if not points.is_finite() or points <= 0:
                    raise ValueError(f"{field} must be a positive finite number")
                ticks = points / Decimal("0.25")
                if ticks != ticks.to_integral_value():
                    raise ValueError(f"{field} must convert to whole 0.25-point MNQ ticks")
            normalized.append(account)
        return normalized
    except (OSError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
        raise ProjectXError(f"Invalid account configuration {path}: {exc}") from exc


def configured_accounts() -> list[dict]:
    if os.getenv("ACCOUNTS_FILE", "").strip():
        return load_account_configuration(Path(os.environ["ACCOUNTS_FILE"]))
    # Preserve deployments that still select a single practice account via .env.
    try:
        account_id = int(os.environ["TOPSTEP_ACCOUNT_ID"])
    except (KeyError, ValueError) as exc:
        raise ProjectXError("Set ACCOUNTS_FILE or TOPSTEP_ACCOUNT_ID") from exc
    return [{
        "name": "Practice", "enabled": True, "account_id": account_id,
        "account_type": "practice",
        "account_name": os.getenv("VERIFIED_PRACTICE_ACCOUNT_NAME", ""),
        "verified_practice_account_id": (
            account_id if os.getenv("VERIFIED_PRACTICE_ACCOUNT_ID") == str(account_id) else None
        ),
        "verified_practice_account_name": os.getenv("VERIFIED_PRACTICE_ACCOUNT_NAME", ""),
        "practice_account_simulated_verified": env_bool("PRACTICE_ACCOUNT_SIMULATED_VERIFIED"),
    }]


def account_trading_config(base: TradingConfig, settings: dict) -> TradingConfig:
    changes = {key: settings[key] for key in ("contracts", "daily_loss_limit_enabled") if key in settings}
    if "trailing_enabled" in settings:
        changes["trail_management_mode"] = (
            "native_delayed" if settings["trailing_enabled"] else "fixed_oco"
        )
    if "max_daily_loss_usd" in settings:
        changes["max_daily_loss_usd"] = Decimal(str(settings["max_daily_loss_usd"]))
    if "stop_loss_points" in settings:
        changes["stop_loss_points"] = Decimal(str(settings["stop_loss_points"]))
    if "take_profit_points" in settings:
        changes["take_profit_points"] = Decimal(
            str(settings["take_profit_points"])
        )
    if "trail_activation_points" in settings:
        changes["trail_activation_points"] = Decimal(
            str(settings["trail_activation_points"])
        )
    if "trail_distance_points" in settings:
        distance = Decimal(str(settings["trail_distance_points"]))
        changes["trail_distance_ticks"] = int(distance / Decimal("0.25"))
    return replace(base, **changes)


async def verify_configured_account(api: ProjectX, accounts: list[dict], settings: dict) -> dict:
    account = dict(select_explicit_account(accounts, str(settings["account_id"])))
    if account.get("name") != settings["account_name"]:
        raise ProjectXError(f"Account {settings['account_id']} does not match configured account_name")
    account["_displayName"] = settings["name"]
    account["_simulatedVerified"] = False
    try:
        verified = await api.verify_simulated_account(int(account["id"]), account["name"])
        if (verified.get("id") != account["id"] or verified.get("name") != account["name"]
                or verified.get("simulated") is not True):
            raise ProjectXError("Account verification returned an inconsistent identity or simulated status")
        account.update(verified)
        account["_simulatedVerified"] = True
    except ProjectXError as exc:
        # Only the existing explicitly verified practice identity has a fallback.
        if (settings["account_type"] == "practice"
                and settings.get("practice_account_simulated_verified") is True
                and settings.get("verified_practice_account_id") == account["id"]
                and settings.get("verified_practice_account_name") == account["name"]
                and account.get("simulated") is not False):
            account.update(simulated=True, _simulatedVerified=True,
                           _simulationVerificationSource="explicit_config")
            logging.warning("Account %s using explicit practice verification: %s", account["id"], exc)
        else:
            logging.error("Account %s simulation verification failed: %s", account["id"], exc)
    return account


async def dispatch_signal(executions: list[ExecutionManager], result, **kwargs) -> dict[int, str]:
    loop = asyncio.get_running_loop()
    signal_received = loop.time()
    logging.info(
        "SIGNAL RECEIVED signal=%s accountIds=%s monotonic=%.6f",
        result.signal, [execution.account["id"] for execution in executions],
        signal_received,
    )
    preparation_batch = EntryPreparationBatch(executions)

    async def prepare(execution):
        try:
            outcome = await execution.prepare_entry(
                result,
                preparation_batch=preparation_batch,
                signal_received_monotonic=signal_received,
                **kwargs,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            execution.halt("entry preparation failed; broker state must be reconciled")
            logging.exception("Entry preparation failed accountId=%s", execution.account["id"])
            outcome = "UNCERTAIN_BLOCKED"
        finally:
            await preparation_batch.participant_finished(execution)
        return execution, outcome

    preparation_tasks = [
        asyncio.create_task(prepare(execution)) for execution in executions
    ]
    try:
        prepared_results = await asyncio.gather(*preparation_tasks)
    except BaseException:
        for task in preparation_tasks:
            task.cancel()
        completed = await asyncio.gather(*preparation_tasks, return_exceptions=True)
        for item in completed:
            if (
                isinstance(item, tuple)
                and len(item) == 2
                and isinstance(item[1], PreparedEntry)
            ):
                item[0].abort_prepared_entry(item[1])
        raise

    async def submit(execution, prepared):
        try:
            status = await execution.submit_prepared_entry(prepared)
        except asyncio.CancelledError:
            raise
        except Exception:
            execution.halt("signal execution failed; broker state must be reconciled")
            logging.exception("Signal failed accountId=%s", execution.account["id"])
            status = "UNCERTAIN_BLOCKED"
        logging.info("Signal result accountId=%s status=%s", execution.account["id"], status)
        return int(execution.account["id"]), status

    statuses = {
        int(execution.account["id"]): outcome
        for execution, outcome in prepared_results
        if isinstance(outcome, str)
    }
    submissions = [
        submit(execution, outcome)
        for execution, outcome in prepared_results
        if isinstance(outcome, PreparedEntry)
    ]
    if submissions:
        statuses.update(dict(await asyncio.gather(*submissions)))
    for execution, outcome in prepared_results:
        if isinstance(outcome, str):
            logging.info(
                "Signal result accountId=%s status=%s",
                execution.account["id"], outcome,
            )
    return statuses


async def run_bot_tasks(feed_task, workers) -> None:
    """Drain all workers before releasing account locks or closing the API."""
    feed = asyncio.create_task(feed_task)
    tasks = [feed, *(asyncio.create_task(worker) for worker in workers)]
    pending = set(tasks)
    try:
        while not feed.done():
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        feed.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class AccountInstanceLock:
    def __init__(self, account_id: int) -> None:
        directory = Path(__file__).resolve().parent / "runtime"
        directory.mkdir(parents=True, exist_ok=True)
        self.handle = None
        try:
            self.handle = open(directory / f"account-{account_id}.lock", "a+b")
            self.handle.seek(0)
            if self.handle.read(1) == b"":
                self.handle.write(b"0")
                self.handle.flush()
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            if self.handle is not None:
                self.handle.close()
                self.handle = None
            raise ProjectXError(
                f"Another bot instance is already using account {account_id}"
            ) from exc

    def close(self) -> None:
        if self.handle is None or self.handle.closed:
            return
        self.handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()

def select_explicit_account(
    accounts: list[dict], configured_id: str | None
) -> dict:
    if not configured_id or not configured_id.strip():
        raise ProjectXError(
            "TOPSTEP_ACCOUNT_ID is required; automatic account fallback is disabled"
        )
    selected = next(
        (
            account
            for account in accounts
            if str(account.get("id")) == configured_id.strip()
        ),
        None,
    )
    if selected is None:
        raise ProjectXError(
            f"TOPSTEP_ACCOUNT_ID={configured_id!r} is not an active account"
        )
    return selected


def log_result(result: StrategyResult | HourlyEmaResult, execution_status: str) -> None:
    if isinstance(result, HourlyEmaResult):
        logging.info(
            "%s MNQ 15M %s signalTime=%s price=%.2f EMA%d=%.2f "
            "relation=%s signal=%s execution=%s",
            result.candle.time.strftime("[%H:%M]"),
            "PARTIAL" if result.partial else "CLOSED",
            result.signal_close_time.isoformat(),
            result.candle.close,
            12,
            result.ema12,
            result.trend,
            result.signal or "NONE",
            execution_status,
        )
        return
    logging.info(
        "%s MNQ CLOSED\n"
        "Price: %.2f\n"
        "EMA20: %.2f\n"
        "EMA50: %.2f\n"
        "Trend: %s\n"
        "Signal: %s\n"
        "Execution: %s",
        result.candle.time.strftime("[%H:%M]"),
        result.candle.close,
        result.ema20,
        result.ema50,
        result.trend,
        result.signal or "NONE",
        execution_status,
    )


async def async_main() -> None:
    username = os.getenv("TOPSTEP_USERNAME", "").strip()
    api_key = os.getenv("TOPSTEP_API_KEY", "").strip()
    if not username or not api_key:
        raise ProjectXError("Add TOPSTEP_USERNAME and TOPSTEP_API_KEY to .env")

    live = env_bool("PROJECTX_LIVE_DATA", False)
    strategy = select_strategy(
        os.getenv("ACTIVE_STRATEGY", "ema12_hourly_trailing").strip().lower(),
        os.getenv("ENTRY_MODE", "alternating"),
    )
    trading_config = TradingConfig.from_env()
    settings = configured_accounts()
    # Two state reads per account per cycle; leave capacity for entries, exit
    # reconciliation, history and Telegram even when every account is flat.
    trading_config = replace(trading_config, execution_poll_interval_seconds=max(
        trading_config.execution_poll_interval_seconds, len(settings) * 1.5,
    ))
    api = ProjectX(username, api_key)
    account_locks: list[AccountInstanceLock] = []
    try:
        await api.login()
        available_accounts = await api.accounts()
        selected_accounts = []
        for account_settings in settings:
            account = await verify_configured_account(api, available_accounts, account_settings)
            account_locks.append(AccountInstanceLock(int(account["id"])))
            selected_accounts.append(account)
            logging.info("Selected account id=%s name=%s", account["id"], account["name"])

        contract = await api.mnq(live)
        point_value = float(contract["tickValue"]) / float(contract["tickSize"])
        logging.info(
            "Selected contract id=%s name=%s tickSize=%s tickValue=%s pointValue=%.2f",
            contract["id"],
            contract["name"],
            contract["tickSize"],
            contract["tickValue"],
            point_value,
        )

        feed = MarketDataFeed(api, contract, live)
        historical = await feed.bootstrap()
        strategy.initialize(historical)
        logging.info(
            "Initialized %s with %d completed candles; historical signals suppressed",
            strategy.name,
            len(historical),
        )

        executions = [
            ExecutionManager(
                api, account, contract, account_trading_config(trading_config, account_settings),
                quote_source=feed,
                approved_account=(account_settings["account_id"], account_settings["account_name"],
                                  account_settings["account_type"]),
                entries_enabled=account_settings["enabled"],
            )
            for account, account_settings in zip(selected_accounts, settings)
        ]
        for execution in executions:
            feed.add_live_price_handler(execution.observe_live_price)
        pause_store = PauseStateStore(Path(__file__).resolve().parent / "runtime" / "account_controls.db")
        telegrams = []
        for execution in executions:
            execution.runtime_paused = pause_store.is_paused(execution.account["id"])
            execution.entries_enabled = execution.configured_entries_enabled and not execution.runtime_paused
            ledger = Path(__file__).resolve().parent / "data" / f"telegram_trades_{execution.account['id']}.json"
            telegram = AccountTelegramMonitor.from_env(execution, feed, ledger)
            if telegram is not None:
                execution.event_sink = telegram
                telegrams.append(telegram)
        for telegram in telegrams:
            telegram.peers = telegrams
            telegram.pause_store = pause_store
        await asyncio.gather(*(execution.startup_check() for execution in executions))

        async def on_closed_candle(candle, entry_eligible: bool) -> None:
            if isinstance(strategy, TestLongStrategy):
                result = strategy.on_candle(
                    candle,
                    emit_signal=entry_eligible,
                    entry_available=(
                        any(not execution.order_pending and not execution.last_positions
                            and not execution.halted_reason and execution.entries_enabled
                            for execution in executions)
                    ),
                )
            else:
                result = strategy.on_candle(
                    candle,
                    emit_signal=entry_eligible,
                )
            if result is None:
                return
            statuses = await dispatch_signal(
                executions, result,
                entry_eligible=entry_eligible,
                market_fresh=feed.market_is_fresh(),
            )
            log_result(result, str(statuses))

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, feed.stop.set)
            except NotImplementedError:
                pass

        workers = [
            *(execution.monitor() for execution in executions),
            *(telegram.run() for telegram in telegrams),
        ]
        await run_bot_tasks(feed.run(on_closed_candle), workers)
    finally:
        for account_lock in reversed(account_locks):
            account_lock.close()
        await api.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=".env", help="environment file to load")
    arguments = parser.parse_args()
    try:
        selected_environment = load_environment(arguments.env)
    except ProjectXError as exc:
        raise SystemExit(str(exc)) from exc
    configure_logging()
    logging.info("Loaded environment file %s", selected_environment)
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
    except ProjectXError as exc:
        logging.error("%s", exc)
        raise SystemExit(1) from exc





