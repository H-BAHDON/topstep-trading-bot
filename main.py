"""TopstepX MNQ strategy runner with practice-only, risk-gated execution."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfoNotFoundError

from dotenv import load_dotenv

from execution import ExecutionManager, TradingConfig
from strategies.base import StrategyResult
from strategies.test_long import TestLongStrategy
from strategies.ema12_hourly_trailing import Ema12HourlyTrailingStrategy, HourlyEmaResult
from strategies.ema12_hourly_trailing_43 import Ema12HourlyTrailing43Strategy
from telegram_monitor import TelegramMonitor
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
    return selected


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
    api = ProjectX(username, api_key)
    account_lock: AccountInstanceLock | None = None

    try:
        await api.login()
        account = select_explicit_account(
            await api.accounts(), os.getenv("TOPSTEP_ACCOUNT_ID")
        )
        try:
            verified_account = await api.verify_simulated_account(
                int(account["id"]), str(account["name"])
            )
            account = {
                **account,
                **verified_account,
                "_simulatedVerified": True,
            }
            logging.info(
                "Account simulated status independently verified through user hub"
            )
        except ProjectXError as exc:
            explicit_confirmed = (
                os.getenv("PRACTICE_ACCOUNT_SIMULATED_VERIFIED", "")
                .strip()
                .lower()
                == "true"
            )
            explicit_valid = ExecutionManager.explicit_verification_valid(
                account,
                os.getenv("VERIFIED_PRACTICE_ACCOUNT_ID"),
                os.getenv("VERIFIED_PRACTICE_ACCOUNT_NAME"),
                explicit_confirmed,
            )
            if explicit_valid:
                account = {
                    **account,
                    "simulated": True,
                    "_simulatedVerified": True,
                    "_simulationVerificationSource": "explicit_config",
                }
                logging.warning(
                    "Using explicit practice-account verification because "
                    "ProjectX user hub did not provide a snapshot: %s",
                    exc,
                )
            else:
                account = {**account, "_simulatedVerified": False}
                logging.error(
                    "Account simulation verification failed closed: %s. "
                    "Explicit verification is absent or mismatched.",
                    exc,
                )
        account_lock = AccountInstanceLock(int(account["id"] ))
        logging.info(
            "Selected account id=%s name=%s canTrade=%s visible=%s",
            account.get("id"),
            account.get("name"),
            account.get("canTrade"),
            account.get("isVisible"),
        )

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

        execution = ExecutionManager(
            api,
            account,
            contract,
            trading_config,
            quote_source=feed,
        )
        telegram = TelegramMonitor.from_env(
            execution,
            feed,
            Path(__file__).resolve().parent / "data" / "telegram_trades.json",
        )
        if telegram is not None:
            execution.event_sink = telegram
        await execution.startup_check()

        async def on_closed_candle(candle, entry_eligible: bool) -> None:
            if isinstance(strategy, TestLongStrategy):
                result = strategy.on_candle(
                    candle,
                    emit_signal=entry_eligible,
                    entry_available=(
                        not execution.order_pending
                        and not execution.last_positions
                    ),
                )
            else:
                result = strategy.on_candle(
                    candle,
                    emit_signal=entry_eligible,
                )
            if result is None:
                return
            status = await execution.signal(
                result,
                entry_eligible=entry_eligible,
                market_fresh=feed.market_is_fresh(),
            )
            log_result(result, status)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, feed.stop.set)
            except NotImplementedError:
                pass

        tasks = [
            feed.run(on_closed_candle),
            execution.monitor(),
        ]
        if telegram is not None:
            tasks.append(telegram.run())
        await asyncio.gather(*tasks)
    finally:
        if account_lock is not None:
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





