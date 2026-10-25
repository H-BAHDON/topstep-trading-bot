from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from telegram_monitor import (
    CommandRateLimiter,
    SubscriberStore,
    BrokerPnlSnapshot,
    TelegramConfig,
    TelegramMonitor,
    TradeLedger,
    trading_day_key,
)

UTC = timezone.utc


class FakeResponse:
    def __init__(self, body=None, status_code=200) -> None:
        self.body = body or {"ok": True, "result": {}}
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.body


class FakeClient:
    def __init__(self, fail=False, blocked_chat=None) -> None:
        self.fail = fail
        self.blocked_chat = str(blocked_chat) if blocked_chat is not None else None
        self.calls = []

    async def post(self, url, json):
        self.calls.append((url, json))
        if self.fail:
            raise RuntimeError("network unavailable")
        if self.blocked_chat == str(json.get("chat_id")):
            return FakeResponse(
                {"ok": False, "description": "Forbidden: bot was blocked by the user"},
                403,
            )
        return FakeResponse()


class FakeFeed:
    def __init__(self, price=103, connected=True) -> None:
        self.price = price
        self.connected = connected

    def quote_snapshot(self):
        return {"lastPrice": self.price}


class FakeBrokerApi:
    def __init__(self) -> None:
        self.account_id = 28648769
        self.trade_rows = []
        self.fail = False

    async def accounts(self):
        if self.fail:
            raise RuntimeError("broker unavailable")
        return [{"id": self.account_id, "balance": 152000}]

    async def trades(self, account_id, start, end=None):
        if self.fail:
            raise RuntimeError("broker unavailable")
        return list(self.trade_rows)


class FakeExecution:
    def __init__(self) -> None:
        self.last_positions = []
        self.native_trailing_order_id = None
        self.native_trailing_active = False
        self.trailing_attempted_position_id = None
        self.realized_net = Decimal("25")
        self.contract = {"tickSize": 0.25, "tickValue": 0.5}
        self.account = {"id": 28648769}
        self.api = FakeBrokerApi()
        self.settings = {
            "contracts": 5,
            "trail_activation": "25",
            "trail_distance": "1",
        }

    async def update_runtime_setting(self, setting, value):
        if setting == "contracts":
            parsed = int(value)
            if parsed <= 0:
                raise ValueError("contracts must be a positive integer")
            self.settings[setting] = parsed
            return str(parsed)
        parsed = Decimal(value)
        if parsed <= 0 or parsed % Decimal("0.25"):
            raise ValueError("points must convert to whole 0.25-point MNQ ticks")
        self.settings[setting] = str(parsed)
        return str(parsed)

    def runtime_settings(self):
        return dict(self.settings)


def private_update(chat_id, command, update_id=1):
    return {
        "update_id": update_id,
        "message": {
            "chat": {"id": chat_id, "type": "private"},
            "text": command,
        },
    }


class TelegramMonitorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        directory = Path(self.temporary.name)
        self.path = directory / "telegram_trades.json"
        self.subscriber_path = directory / "subscribers.db"
        self.client = FakeClient()
        self.execution = FakeExecution()
        self.feed = FakeFeed()
        self.monitor = TelegramMonitor(
            TelegramConfig("secret-token"),
            self.execution,
            self.feed,
            self.path,
            client=self.client,
            subscriber_path=self.subscriber_path,
        )
        self.monitor.running = True

    def broker_round_trip(self, order_id: int, pnl: Decimal, fees: Decimal = Decimal(0)) -> None:
        timestamp = datetime.now(UTC).isoformat()
        account_id = self.execution.account["id"]
        self.execution.api.trade_rows = [
            {"id": 1, "accountId": account_id, "creationTimestamp": timestamp, "side": 0, "size": 1, "profitAndLoss": None, "fees": fees, "voided": False, "orderId": order_id - 1},
            {"id": 2, "accountId": account_id, "creationTimestamp": timestamp, "side": 1, "size": 1, "profitAndLoss": pnl, "fees": 0, "voided": False, "orderId": order_id},
        ]

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_optional_configuration_requires_only_token(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(TelegramConfig.from_env())
        with patch.dict(
            os.environ,
            {"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_CHAT_ID": "legacy"},
            clear=True,
        ):
            self.assertEqual(
                TelegramConfig.from_env(), TelegramConfig("token", "legacy")
            )

    async def test_owner_can_update_runtime_settings_and_read_them(self) -> None:
        self.monitor.config = TelegramConfig("secret-token", "9001")
        await self.monitor.process_update(private_update(9001, "/contracts 10", 1))
        await self.monitor.process_update(
            private_update(9001, "/trail_activation 25", 2)
        )
        await self.monitor.process_update(
            private_update(9001, "/trail_distance 1", 3)
        )
        await self.monitor.process_update(private_update(9001, "/settings", 4))
        self.assertEqual(self.execution.settings["contracts"], 10)
        self.assertEqual(self.execution.settings["trail_activation"], "25")
        self.assertEqual(self.execution.settings["trail_distance"], "1")
        self.assertIn("Contracts: 10", self.client.calls[-1][1]["text"])

    async def test_non_owner_cannot_update_runtime_settings(self) -> None:
        self.monitor.config = TelegramConfig("secret-token", "9001")
        await self.monitor.process_update(private_update(9002, "/contracts 10"))
        self.assertEqual(self.execution.settings["contracts"], 5)
        self.assertEqual(self.client.calls[-1][1]["text"], "Not authorized")

    async def test_start_publicly_subscribes_and_persists_private_chat(self) -> None:
        await self.monitor.process_update(private_update(1001, "/start"))
        self.assertTrue(self.monitor.subscribers.contains("1001"))
        reply = self.client.calls[-1][1]["text"]
        self.assertIn("subscribed", reply.lower())
        self.assertIn("/status", reply)
        restarted = SubscriberStore(self.subscriber_path)
        self.assertTrue(restarted.contains("1001"))

    async def test_stop_unsubscribes_and_user_can_resubscribe(self) -> None:
        await self.monitor.process_update(private_update(1002, "/start", 1))
        await self.monitor.process_update(private_update(1002, "/stop", 2))
        self.assertFalse(self.monitor.subscribers.contains("1002"))
        await self.monitor.process_update(private_update(1002, "/start", 3))
        self.assertTrue(self.monitor.subscribers.contains("1002"))

    async def test_public_read_only_commands_do_not_require_subscription(self) -> None:
        for index, command in enumerate(
            ("/status", "/profit", "/trades", "/stats", "/help"), start=1
        ):
            await self.monitor.process_update(private_update(2001, command, index))
        self.assertFalse(self.monitor.subscribers.contains("2001"))
        self.assertEqual(len(self.client.calls), 5)
        self.assertIn("Running: YES", self.client.calls[0][1]["text"])
        self.assertIn("Available commands", self.client.calls[-1][1]["text"])

    async def test_group_subscriptions_and_trading_commands_are_ignored(self) -> None:
        group = {
            "update_id": 1,
            "message": {
                "chat": {"id": -1001, "type": "group"},
                "text": "/start",
            },
        }
        await self.monitor.process_update(group)
        await self.monitor.process_update(private_update(3001, "/buy"))
        await self.monitor.process_update(private_update(3001, "/close"))
        self.assertEqual(self.monitor.subscribers.all(), [])
        self.assertEqual(self.client.calls, [])

    def test_status_and_profit_use_current_broker_state_without_identifiers(self) -> None:
        self.execution.last_positions = [
            {"id": 7, "type": 1, "size": 5, "averagePrice": 100}
        ]
        self.execution.native_trailing_active = True
        status = self.monitor.command_response("/status")
        profit = self.monitor.command_response("/profit")
        self.assertIn("Position: LONG", status)
        self.assertIn("Contracts: 5", status)
        self.assertIn("Trailing activated: YES", status)
        self.assertIn("Open: +$30", profit)
        for secret in ("account", "token", "28648769", "CON.F"):
            self.assertNotIn(secret, (status + profit).lower())

    def test_trades_and_stats_only_expose_profit_values(self) -> None:
        now = datetime.now(UTC)
        self.monitor.broker_pnl = BrokerPnlSnapshot((
            {"orderId": 1, "pnl": Decimal("150"), "timestamp": now},
            {"orderId": 2, "pnl": Decimal("-50"), "timestamp": now},
            {"orderId": 3, "pnl": Decimal("100"), "timestamp": now},
        ), {trading_day_key(now): Decimal("200")}, Decimal("200"), Decimal("200"), now)
        trades = self.monitor.command_response("/trades")
        stats = self.monitor.command_response("/stats")
        self.assertEqual(trades, "1. +$150\n2. -$50\n3. +$100")
        self.assertNotIn("LONG", trades)
        self.assertNotIn("order", trades.lower())
        self.assertIn("Completed trades: 3", stats)
        self.assertIn("Win rate: 66.67%", stats)
        self.assertIn("Profit factor: 5.00", stats)
        self.assertIn("Average winner: +$125", stats)
        self.assertIn("Average loser: -$50", stats)

    async def test_notifications_broadcast_and_are_deduplicated_after_restart(self) -> None:
        self.monitor.subscribers.add("4001")
        self.monitor.subscribers.add("4002")
        close = {
            "position_id": 7,
            "order_id": 12,
            "pnl": Decimal("24.64"),
            "timestamp": datetime(2026, 10, 15, tzinfo=UTC),
        }
        self.broker_round_trip(12, Decimal("24.64"))
        await self.monitor.handle_event("trade_opened", {"position_id": 7})
        await self.monitor.handle_event("trailing_activated", {"position_id": 7})
        await self.monitor.handle_event("trade_closed", close)
        await self.monitor.handle_event("trade_closed", close)
        messages = [call[1]["text"] for call in self.client.calls]
        self.assertEqual(messages.count("Trade opened"), 2)
        self.assertEqual(messages.count("Trailing stop activated"), 2)
        self.assertEqual(messages.count("+$24.64"), 2)
        restarted = TradeLedger(self.path)
        self.assertEqual(len(restarted.records), 1)
        self.assertIn("trade_closed:12", restarted.sent_events)

    async def test_blocked_user_is_removed_without_affecting_other_subscribers(self) -> None:
        monitor = TelegramMonitor(
            TelegramConfig("secret-token"),
            self.execution,
            self.feed,
            self.path,
            client=FakeClient(blocked_chat="5001"),
            subscriber_path=self.subscriber_path,
        )
        monitor.subscribers.add("5001")
        monitor.subscribers.add("5002")
        completed = await monitor.handle_event("trade_opened", {"position_id": 9})
        self.assertTrue(completed)
        self.assertFalse(monitor.subscribers.contains("5001"))
        self.assertTrue(monitor.subscribers.contains("5002"))

    async def test_connection_failure_never_raises_or_marks_notification_sent(self) -> None:
        monitor = TelegramMonitor(
            TelegramConfig("secret-token"),
            self.execution,
            self.feed,
            self.path,
            client=FakeClient(fail=True),
            subscriber_path=self.subscriber_path,
        )
        monitor.subscribers.add("6001")
        self.broker_round_trip(88, Decimal("-12.50"))
        completed = await monitor.handle_event(
            "trade_closed",
            {"position_id": 7, "order_id": 88, "pnl": Decimal("999")},
        )
        self.assertFalse(completed)
        self.assertNotIn("trade_closed:88", monitor.ledger.sent_events)
        self.assertEqual(len(monitor.ledger.records), 1)

    async def test_daily_summary_broadcasts_once(self) -> None:
        self.monitor.subscribers.add("7001")
        self.monitor.subscribers.add("7002")
        timestamp = datetime.now(UTC)
        account_id = self.execution.account["id"]
        self.execution.api.trade_rows = [
            {"id": 1, "accountId": account_id, "creationTimestamp": timestamp.isoformat(), "side": 0, "size": 1, "profitAndLoss": None, "fees": 1, "voided": False, "orderId": 100},
            {"id": 2, "accountId": account_id, "creationTimestamp": timestamp.isoformat(), "side": 1, "size": 1, "profitAndLoss": 76, "fees": 0, "voided": False, "orderId": 101},
            {"id": 3, "accountId": account_id, "creationTimestamp": timestamp.isoformat(), "side": 1, "size": 1, "profitAndLoss": None, "fees": 1, "voided": False, "orderId": 102},
            {"id": 4, "accountId": account_id, "creationTimestamp": timestamp.isoformat(), "side": 0, "size": 1, "profitAndLoss": -24, "fees": 0, "voided": False, "orderId": 103},
        ]
        day = trading_day_key(timestamp)
        self.assertTrue(await self.monitor.send_daily_summary(day))
        self.assertFalse(await self.monitor.send_daily_summary(day))
        messages = [call[1]["text"] for call in self.client.calls]
        self.assertEqual(len(messages), 2)
        self.assertTrue(all("Daily realised: +$50" in text for text in messages))

    async def test_rate_limit_is_per_chat_and_stop_remains_available(self) -> None:
        monitor = TelegramMonitor(
            TelegramConfig("secret-token"),
            self.execution,
            self.feed,
            self.path,
            client=self.client,
            subscriber_path=self.subscriber_path,
            rate_limiter=CommandRateLimiter(limit=2, window_seconds=60),
        )
        await monitor.process_update(private_update(8001, "/start", 1))
        await monitor.process_update(private_update(8001, "/status", 2))
        await monitor.process_update(private_update(8001, "/stats", 3))
        self.assertIn("Too many requests", self.client.calls[-1][1]["text"])
        await monitor.process_update(private_update(8001, "/stop", 4))
        self.assertFalse(monitor.subscribers.contains("8001"))

    async def test_broker_pnl_includes_opening_and_closing_fees_and_partial_fills(self) -> None:
        account_id = self.execution.account["id"]
        timestamp = datetime.now(UTC).isoformat()
        self.execution.api.trade_rows = [
            {"id": 1, "accountId": account_id, "creationTimestamp": timestamp, "side": 0, "size": 60, "profitAndLoss": None, "fees": 30, "voided": False, "orderId": 10},
            {"id": 2, "accountId": account_id, "creationTimestamp": timestamp, "side": 0, "size": 90, "profitAndLoss": None, "fees": 45, "voided": False, "orderId": 10},
            {"id": 3, "accountId": account_id, "creationTimestamp": timestamp, "side": 1, "size": 75, "profitAndLoss": 300, "fees": 37.5, "voided": False, "orderId": 11},
            {"id": 4, "accountId": account_id, "creationTimestamp": timestamp, "side": 1, "size": 75, "profitAndLoss": 300, "fees": 37.5, "voided": False, "orderId": 11},
        ]
        self.assertTrue(await self.monitor.refresh_broker_pnl(force=True))
        self.assertEqual(self.monitor.broker_pnl.total_net, Decimal("450.0"))
        self.assertEqual(self.monitor.broker_pnl.completed[0]["pnl"], Decimal("450.0"))

    async def test_trading_day_boundary_uses_chicago_five_pm_and_selected_account(self) -> None:
        account_id = self.execution.account["id"]
        before = datetime(2026, 10, 15, 21, 59, tzinfo=UTC)
        after = datetime(2026, 10, 15, 22, 1, tzinfo=UTC)
        self.execution.api.trade_rows = [
            {"id": 1, "accountId": account_id, "creationTimestamp": before.isoformat(), "side": 0, "size": 1, "profitAndLoss": None, "fees": 1, "voided": False, "orderId": 20},
            {"id": 2, "accountId": account_id, "creationTimestamp": before.isoformat(), "side": 1, "size": 1, "profitAndLoss": 11, "fees": 0, "voided": False, "orderId": 21},
            {"id": 3, "accountId": account_id, "creationTimestamp": after.isoformat(), "side": 0, "size": 1, "profitAndLoss": None, "fees": 2, "voided": False, "orderId": 22},
            {"id": 4, "accountId": account_id, "creationTimestamp": after.isoformat(), "side": 1, "size": 1, "profitAndLoss": 22, "fees": 0, "voided": False, "orderId": 23},
        ]
        self.assertTrue(await self.monitor.refresh_broker_pnl(force=True))
        self.assertEqual(self.monitor.broker_pnl.daily_net[trading_day_key(before)], Decimal("10"))
        self.assertEqual(self.monitor.broker_pnl.daily_net[trading_day_key(after)], Decimal("20"))

    async def test_broker_account_mismatch_and_api_failure_show_unavailable(self) -> None:
        self.execution.api.account_id = 999
        self.assertFalse(await self.monitor.refresh_broker_pnl(force=True))
        self.assertIn("Data unavailable", self.monitor.command_response("/profit"))
        self.execution.api.account_id = self.execution.account["id"]
        self.execution.api.fail = True
        self.assertFalse(await self.monitor.refresh_broker_pnl(force=True))

    def test_short_unrealised_profit_uses_actual_position_size(self) -> None:
        self.feed.price = 98
        position = {"id": 8, "type": 2, "size": 10, "averagePrice": 100}
        self.assertEqual(self.monitor.unrealized_pnl(position), Decimal("40"))


if __name__ == "__main__":
    unittest.main()
