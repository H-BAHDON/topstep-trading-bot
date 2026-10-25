"""Optional public, read-only Telegram monitoring for the trading bot."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time as monotonic_time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

UTC = timezone.utc
CHICAGO = ZoneInfo("America/Chicago")
LONG_POSITION = 1
SHORT_POSITION = 2
SETTING_COMMANDS = {"/contracts", "/trail_activation", "/trail_distance"}
COMMANDS = {
    "/start", "/stop", "/status", "/profit", "/trades", "/stats", "/help",
    "/settings", *SETTING_COMMANDS,
}
HELP_TEXT = (
    "Available commands:\n"
    "/start - subscribe to trade notifications\n"
    "/stop - unsubscribe from trade notifications\n"
    "/status - bot and position status\n"
    "/profit - open, daily and total P&L\n"
    "/trades - last 10 completed trade results\n"
    "/stats - completed trade statistics\n"
    "/settings - current runtime trade settings\n"
    "/contracts N - set contracts for future trades\n"
    "/trail_activation N - set trailing activation points\n"
    "/trail_distance N - set trailing distance points\n"
    "/help - show this help"
)


def money(value: Decimal) -> str:
    value = value.quantize(Decimal("0.01"))
    sign = "+" if value >= 0 else "-"
    absolute = abs(value)
    if absolute == absolute.to_integral_value():
        return f"{sign}${absolute:.0f}"
    return f"{sign}${absolute:.2f}"


def trading_day_key(value: datetime | None = None) -> str:
    current = (value or datetime.now(UTC)).astimezone(CHICAGO)
    boundary = datetime.combine(current.date(), time(17, 0), CHICAGO)
    if current < boundary:
        boundary -= timedelta(days=1)
    return boundary.date().isoformat()


@dataclass(frozen=True)
class TelegramConfig:
    token: str
    owner_chat_id: str = ""

    @classmethod
    def from_env(cls) -> TelegramConfig | None:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            return None
        return cls(
            token=token,
            owner_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        )


class SubscriberStore:
    """SQLite-backed private-chat subscriptions."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.database() as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS subscribers ("
                "chat_id TEXT PRIMARY KEY, subscribed_at TEXT NOT NULL)"
            )

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=5)

    @contextmanager
    def database(self):
        database = self.connect()
        try:
            yield database
            database.commit()
        except Exception:
            database.rollback()
            raise
        finally:
            database.close()

    def add(self, chat_id: str) -> bool:
        with self.database() as database:
            cursor = database.execute(
                "INSERT OR IGNORE INTO subscribers(chat_id, subscribed_at) "
                "VALUES (?, ?)",
                (chat_id, datetime.now(UTC).isoformat()),
            )
            return cursor.rowcount == 1

    def remove(self, chat_id: str) -> bool:
        with self.database() as database:
            cursor = database.execute(
                "DELETE FROM subscribers WHERE chat_id = ?", (chat_id,)
            )
            return cursor.rowcount == 1

    def contains(self, chat_id: str) -> bool:
        with self.database() as database:
            row = database.execute(
                "SELECT 1 FROM subscribers WHERE chat_id = ?", (chat_id,)
            ).fetchone()
        return row is not None

    def all(self) -> list[str]:
        with self.database() as database:
            rows = database.execute(
                "SELECT chat_id FROM subscribers ORDER BY subscribed_at"
            ).fetchall()
        return [str(row[0]) for row in rows]


class CommandRateLimiter:
    def __init__(self, limit: int = 8, window_seconds: float = 60.0) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self.requests: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, chat_id: str, now: float | None = None) -> bool:
        current = monotonic_time.monotonic() if now is None else now
        requests = self.requests[chat_id]
        while requests and current - requests[0] >= self.window_seconds:
            requests.popleft()
        if len(requests) >= self.limit:
            return False
        requests.append(current)
        return True


class TradeLedger:
    """Persistent, broker-exit-ID-deduplicated realised trade history."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.records: list[dict[str, Any]] = []
        self.sent_events: set[str] = set()
        self.summary_days: set[str] = set()
        self.load()

    def load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.records = list(raw.get("trades", []))
            self.sent_events = set(raw.get("sentEvents", []))
            self.summary_days = set(raw.get("summaryDays", []))
        except FileNotFoundError:
            return
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            logging.exception("Telegram trade ledger could not be loaded; starting empty")

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = {
            "trades": self.records,
            "sentEvents": sorted(self.sent_events),
            "summaryDays": sorted(self.summary_days),
        }
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        temporary.replace(self.path)

    def add_trade(
        self, order_id: int, pnl: Decimal, timestamp: datetime | None = None
    ) -> bool:
        if any(int(item["orderId"]) == order_id for item in self.records):
            return False
        occurred = (timestamp or datetime.now(UTC)).astimezone(UTC)
        self.records.append(
            {
                "orderId": order_id,
                "pnl": str(pnl),
                "timestamp": occurred.isoformat(),
                "tradingDay": trading_day_key(occurred),
            }
        )
        self.save()
        return True

    def values(self) -> list[Decimal]:
        values: list[Decimal] = []
        for item in self.records:
            try:
                values.append(Decimal(str(item["pnl"])))
            except (KeyError, InvalidOperation):
                continue
        return values

    def day_values(self, day: str) -> list[Decimal]:
        values: list[Decimal] = []
        for item in self.records:
            if item.get("tradingDay") != day:
                continue
            try:
                values.append(Decimal(str(item["pnl"])))
            except (KeyError, InvalidOperation):
                continue
        return values


@dataclass(frozen=True)
class BrokerPnlSnapshot:
    completed: tuple[dict[str, Any], ...]
    daily_net: dict[str, Decimal]
    today_net: Decimal
    total_net: Decimal
    refreshed_at: datetime


class TelegramMonitor:
    """Public private-chat commands and queued subscriber notifications."""

    def __init__(
        self,
        config: TelegramConfig,
        execution: Any,
        feed: Any,
        ledger_path: Path,
        client: Any | None = None,
        subscriber_path: Path | None = None,
        rate_limiter: CommandRateLimiter | None = None,
    ) -> None:
        self.config = config
        self.execution = execution
        self.feed = feed
        self.ledger = TradeLedger(ledger_path)
        self.subscribers = SubscriberStore(
            subscriber_path or ledger_path.with_name("telegram_subscribers.db")
        )
        self.rate_limiter = rate_limiter or CommandRateLimiter()
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
        self.offset: int | None = None
        self.client = client
        self._owns_client = client is None
        self.running = False
        self.current_day = trading_day_key()
        self.broker_pnl: BrokerPnlSnapshot | None = None
        self.broker_pnl_lock = asyncio.Lock()

    @classmethod
    def from_env(
        cls, execution: Any, feed: Any, ledger_path: Path
    ) -> TelegramMonitor | None:
        config = TelegramConfig.from_env()
        if config is None:
            return None
        return cls(config, execution, feed, ledger_path)

    def notify(self, event: str, **payload: Any) -> None:
        try:
            self.queue.put_nowait((event, payload))
        except Exception:
            logging.exception("Telegram event queue failed event=%s", event)

    async def api_call(self, method: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        try:
            response = await self.client.post(
                f"https://api.telegram.org/bot{self.config.token}/{method}",
                json=payload,
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("ok"):
                logging.warning("Telegram API rejected method=%s", method)
                return None
            return body
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning(
                "Telegram API failure method=%s error=%s", method, type(exc).__name__
            )
            return None

    async def send_to(self, chat_id: str, text: str) -> bool:
        try:
            response = await self.client.post(
                f"https://api.telegram.org/bot{self.config.token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
            )
            status = int(getattr(response, "status_code", 200))
            body = response.json()
            if status == 403 or (
                status == 400
                and any(
                    phrase in str(body.get("description", "")).lower()
                    for phrase in ("chat not found", "deactivated", "blocked")
                )
            ):
                self.subscribers.remove(chat_id)
                logging.info("Removed unavailable Telegram subscriber")
                return False
            response.raise_for_status()
            if not body.get("ok"):
                logging.warning("Telegram send rejected for subscriber")
                return False
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Telegram delivery failed error=%s", type(exc).__name__)
            return False

    async def broadcast(self, text: str) -> tuple[int, int]:
        chat_ids = self.subscribers.all()
        if not chat_ids:
            return 0, 0
        semaphore = asyncio.Semaphore(10)

        async def deliver(chat_id: str) -> bool:
            async with semaphore:
                return await self.send_to(chat_id, text)

        results = await asyncio.gather(
            *(deliver(chat_id) for chat_id in chat_ids), return_exceptions=True
        )
        delivered = sum(result is True for result in results)
        return delivered, len(chat_ids)

    def event_key(self, event: str, payload: dict[str, Any]) -> str:
        identifier = (
            payload.get("order_id")
            if event == "trade_closed"
            else payload.get("position_id") or payload.get("order_id")
        )
        return f"{event}:{identifier}"

    async def handle_event(self, event: str, payload: dict[str, Any]) -> bool:
        key = self.event_key(event, payload)
        if event == "trade_closed":
            order_id = int(payload["order_id"])
            if not await self.refresh_broker_pnl(force=True) or self.broker_pnl is None:
                logging.warning("Telegram close notification delayed: ProjectX P&L unavailable")
                return False
            record = next(
                (item for item in reversed(self.broker_pnl.completed) if item["orderId"] == order_id),
                None,
            )
            if record is None:
                logging.warning(
                    "Telegram close notification delayed: broker completed trade missing orderId=%s",
                    order_id,
                )
                return False
            value = record["pnl"]
            self.ledger.add_trade(order_id, value, record["timestamp"])
            if key in self.ledger.sent_events:
                return True
            message = money(value)
        elif event == "trade_opened":
            if key in self.ledger.sent_events:
                return True
            message = "Trade opened"
        elif event == "trailing_activated":
            if key in self.ledger.sent_events:
                return True
            message = "Trailing stop activated"
        else:
            return True
        delivered, attempted = await self.broadcast(message)
        if delivered or attempted == 0 or not self.subscribers.all():
            self.ledger.sent_events.add(key)
            self.ledger.save()
            return True
        return False

    async def requeue_after(self, event: str, payload: dict[str, Any], delay: float) -> None:
        await asyncio.sleep(delay)
        self.notify(event, **payload)

    async def event_worker(self) -> None:
        while True:
            event, payload = await self.queue.get()
            try:
                completed = await self.handle_event(event, payload)
                attempt = int(payload.get("_telegram_attempt", 0))
                if not completed and attempt < 2:
                    retry = {**payload, "_telegram_attempt": attempt + 1}
                    asyncio.create_task(
                        self.requeue_after(event, retry, 2 ** attempt),
                        name=f"telegram-retry-{event}",
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Telegram notification processing failed event=%s", event)
            finally:
                self.queue.task_done()

    def position_snapshot(self) -> dict[str, Any] | None:
        positions = self.execution.last_positions
        return positions[0] if len(positions) == 1 else None

    def unrealized_pnl(self, position: dict[str, Any] | None) -> Decimal | None:
        if position is None:
            return Decimal(0)
        quote = self.feed.quote_snapshot()
        if not quote or quote.get("lastPrice") is None:
            return None
        entry = Decimal(str(position["averagePrice"]))
        last = Decimal(str(quote["lastPrice"]))
        size = Decimal(str(position["size"]))
        point_value = Decimal(str(self.execution.contract["tickValue"])) / Decimal(
            str(self.execution.contract["tickSize"])
        )
        points = last - entry
        if int(position["type"]) == SHORT_POSITION:
            points = -points
        return points * size * point_value

    @staticmethod
    def broker_completed_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Reconcile fills into flat-to-flat trades, including every fill fee."""
        ordered = sorted(
            (trade for trade in trades if not trade.get("voided")),
            key=lambda trade: (str(trade.get("creationTimestamp") or ""), int(trade.get("id") or 0)),
        )
        position = 0
        cycle_net = Decimal(0)
        completed: list[dict[str, Any]] = []
        for trade in ordered:
            side = trade.get("side")
            size = trade.get("size")
            if side not in (0, 1) or not isinstance(size, int) or isinstance(size, bool) or size <= 0:
                raise ValueError("broker trade has invalid side or size")
            previous = position
            position += size if side == 0 else -size
            cycle_net -= Decimal(str(trade.get("fees") or 0))
            if trade.get("profitAndLoss") is not None:
                cycle_net += Decimal(str(trade["profitAndLoss"]))
            if previous and position and (previous > 0) != (position > 0):
                raise ValueError("broker fills reverse through flat and cannot be reconciled safely")
            if previous and position == 0:
                completed.append({
                    "orderId": int(trade.get("orderId") or 0),
                    "pnl": cycle_net,
                    "timestamp": datetime.fromisoformat(
                        str(trade["creationTimestamp"]).replace("Z", "+00:00")
                    ),
                })
                cycle_net = Decimal(0)
        return completed

    async def refresh_broker_pnl(self, *, force: bool = False) -> bool:
        async with self.broker_pnl_lock:
            now = datetime.now(UTC)
            if (
                not force
                and self.broker_pnl is not None
                and (now - self.broker_pnl.refreshed_at).total_seconds() < 5
            ):
                return True
            try:
                account_id = int(self.execution.account["id"])
                accounts, trades = await asyncio.gather(
                    self.execution.api.accounts(),
                    self.execution.api.trades(account_id, datetime(2000, 1, 1, tzinfo=UTC), now),
                )
                matches = [item for item in accounts if int(item.get("id", -1)) == account_id]
                if len(matches) != 1:
                    raise ValueError("selected account was not uniquely returned by ProjectX")
                selected = [
                    item for item in trades
                    if int(item.get("accountId", -1)) == account_id and not item.get("voided")
                ]
                if len(selected) != len([item for item in trades if not item.get("voided")]):
                    raise ValueError("ProjectX trade response mixed account IDs")
                completed = self.broker_completed_trades(selected)
                total_net = sum(
                    (Decimal(str(item.get("profitAndLoss") or 0)) - Decimal(str(item.get("fees") or 0)) for item in selected),
                    Decimal(0),
                )
                daily_net: dict[str, Decimal] = {}
                for item in selected:
                    key = trading_day_key(datetime.fromisoformat(
                        str(item["creationTimestamp"]).replace("Z", "+00:00")
                    ))
                    daily_net[key] = daily_net.get(key, Decimal(0)) + (
                        Decimal(str(item.get("profitAndLoss") or 0))
                        - Decimal(str(item.get("fees") or 0))
                    )
                day = trading_day_key(now)
                today_net = daily_net.get(day, Decimal(0))
                local = sum(self.ledger.values(), Decimal(0))
                if local != total_net:
                    logging.warning(
                        "P&L discrepancy source=telegram_local_vs_projectx accountId=%s localNet=%s brokerNet=%s difference=%s",
                        account_id, local, total_net, local - total_net,
                    )
                self.broker_pnl = BrokerPnlSnapshot(tuple(completed), daily_net, today_net, total_net, now)
                return True
            except Exception as exc:
                self.broker_pnl = None
                logging.warning("ProjectX P&L refresh failed error=%s", type(exc).__name__)
                return False

    def command_response(self, command: str) -> str:
        position = self.position_snapshot()
        if command == "/status":
            if position is None:
                direction, contracts = "FLAT", 0
            else:
                direction = (
                    "LONG" if int(position["type"]) == LONG_POSITION else "SHORT"
                )
                contracts = int(position["size"])
            trailing = bool(
                position and getattr(self.execution, "native_trailing_active", False)
            )
            return (
                f"Running: {'YES' if self.running else 'NO'}\n"
                f"Position: {direction}\n"
                f"Contracts: {contracts}\n"
                f"Trailing activated: {'YES' if trailing else 'NO'}"
            )
        if command == "/profit":
            open_pnl = self.unrealized_pnl(position)
            open_text = money(open_pnl) if open_pnl is not None else "Data unavailable"
            if self.broker_pnl is None:
                return f"Open: {open_text}\nToday realised: Data unavailable\nTotal realised: Data unavailable"
            return (
                f"Open: {open_text}\n"
                f"Today realised: {money(self.broker_pnl.today_net)}\n"
                f"Total realised: {money(self.broker_pnl.total_net)}"
            )
        if command == "/trades":
            if self.broker_pnl is None:
                return "Data unavailable"
            values = [item["pnl"] for item in self.broker_pnl.completed[-10:]]
            if not values:
                return "No completed trades recorded"
            return "\n".join(
                f"{index}. {money(value)}"
                for index, value in enumerate(values, start=1)
            )
        if command == "/stats":
            if self.broker_pnl is None:
                return "Data unavailable"
            values = [item["pnl"] for item in self.broker_pnl.completed]
            wins = [value for value in values if value > 0]
            losses = [value for value in values if value < 0]
            count = len(values)
            win_rate = Decimal(len(wins) * 100) / Decimal(count) if count else Decimal(0)
            gross_profit = sum(wins, Decimal(0))
            gross_loss = abs(sum(losses, Decimal(0)))
            factor = (
                f"{gross_profit / gross_loss:.2f}"
                if gross_loss
                else ("INF" if gross_profit else "0.00")
            )
            average_win = gross_profit / len(wins) if wins else Decimal(0)
            average_loss = sum(losses, Decimal(0)) / len(losses) if losses else Decimal(0)
            return (
                f"Completed trades: {count}\n"
                f"Win rate: {win_rate:.2f}%\n"
                f"Profit factor: {factor}\n"
                f"Average winner: {money(average_win)}\n"
                f"Average loser: {money(average_loss)}"
            )
        if command == "/settings":
            values = self.execution.runtime_settings()
            return (
                f"Contracts: {values['contracts']}\n"
                f"Trail activation: {values['trail_activation']} points\n"
                f"Trail distance: {values['trail_distance']} points"
            )
        if command == "/help":
            return HELP_TEXT
        return HELP_TEXT

    async def process_update(self, update: dict[str, Any]) -> None:
        update_id = update.get("update_id")
        if isinstance(update_id, int):
            self.offset = update_id + 1
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat")
        if not isinstance(chat, dict) or chat.get("type") != "private":
            return
        chat_id_value = chat.get("id")
        if not isinstance(chat_id_value, (int, str)):
            return
        chat_id = str(chat_id_value)
        text = message.get("text")
        if not isinstance(text, str):
            return
        parts = text.strip().split()
        command = parts[0].split("@", 1)[0].lower() if parts else ""
        if command not in COMMANDS:
            return
        if command == "/stop":
            self.subscribers.remove(chat_id)
            await self.send_to(chat_id, "Notifications stopped. Send /start to subscribe again.")
            return
        if not self.rate_limiter.allow(chat_id):
            await self.send_to(chat_id, "Too many requests. Please wait and try again.")
            return
        if command in SETTING_COMMANDS or command == "/settings":
            if not self.config.owner_chat_id or chat_id != self.config.owner_chat_id:
                await self.send_to(chat_id, "Not authorized")
                return
            if command == "/settings":
                await self.send_to(chat_id, self.command_response(command))
                return
            if len(parts) != 2:
                await self.send_to(chat_id, f"Usage: {command} VALUE")
                return
            setting = command.removeprefix("/")
            try:
                updated = await self.execution.update_runtime_setting(
                    setting, parts[1]
                )
            except ValueError as exc:
                await self.send_to(chat_id, f"Invalid value: {exc}")
                return
            label = setting.replace("_", " ").title()
            suffix = "" if setting == "contracts" else " points"
            await self.send_to(
                chat_id,
                f"{label} updated to {updated}{suffix}. Applies to future trades.",
            )
            return
        if command == "/start":
            self.subscribers.add(chat_id)
            await self.send_to(
                chat_id,
                "Welcome to Topsteps Trading Bot.\n"
                "You are subscribed to trade notifications.\n\n"
                + HELP_TEXT,
            )
            return
        if command in {"/profit", "/trades", "/stats"}:
            await self.refresh_broker_pnl(force=True)
        await self.send_to(chat_id, self.command_response(command))

    async def poll_commands(self) -> None:
        backoff = 1.0
        while True:
            payload: dict[str, Any] = {
                "timeout": 25,
                "allowed_updates": ["message"],
            }
            if self.offset is not None:
                payload["offset"] = self.offset
            result = await self.api_call("getUpdates", payload)
            if result is None:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            backoff = 1.0
            for update in result.get("result", []):
                if isinstance(update, dict):
                    await self.process_update(update)

    async def send_daily_summary(self, completed_day: str) -> bool:
        if completed_day in self.ledger.summary_days:
            return False
        if not await self.refresh_broker_pnl(force=True) or self.broker_pnl is None:
            logging.warning("Telegram daily summary skipped: ProjectX P&L unavailable")
            return False
        values = [
            item["pnl"] for item in self.broker_pnl.completed
            if trading_day_key(item["timestamp"]) == completed_day
        ]
        wins = sum(1 for value in values if value > 0)
        rate = Decimal(wins * 100) / Decimal(len(values)) if values else Decimal(0)
        day_net = self.broker_pnl.daily_net.get(completed_day)
        if day_net is None:
            logging.warning("Telegram daily summary skipped: broker trading day unavailable")
            return False
        summary = (
            f"Daily realised: {money(day_net)}\n"
            f"Completed trades: {len(values)}\n"
            f"Win rate: {rate:.2f}%"
        )
        delivered, attempted = await self.broadcast(summary)
        if delivered or attempted == 0 or not self.subscribers.all():
            self.ledger.summary_days.add(completed_day)
            self.ledger.save()
            return True
        return False

    async def daily_summary_worker(self) -> None:
        while True:
            await asyncio.sleep(30)
            new_day = trading_day_key()
            if new_day == self.current_day:
                continue
            completed_day = self.current_day
            self.current_day = new_day
            await self.send_daily_summary(completed_day)

    async def run(self) -> None:
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=httpx.Timeout(35.0))
        self.running = True
        logging.info(
            "Public Telegram monitoring enabled subscribers=%d",
            len(self.subscribers.all()),
        )
        try:
            await asyncio.gather(
                self.poll_commands(), self.event_worker(), self.daily_summary_worker()
            )
        finally:
            self.running = False
            if self._owns_client and self.client is not None:
                await self.client.aclose()
