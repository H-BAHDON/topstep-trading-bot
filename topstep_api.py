"""ProjectX REST and SignalR market-data integration."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from urllib.parse import quote

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

API_URL = "https://api.topstepx.com"
MARKET_HUB = "wss://rtc.topstepx.com/hubs/market"
USER_HUB = "wss://rtc.topstepx.com/hubs/user"
RECORD_SEPARATOR = "\x1e"
MNQ_SYMBOL = "F.US.MNQ"
UTC = timezone.utc
ClosedCandleHandler = Callable[["Candle", bool], Awaitable[None]]


class ProjectXError(RuntimeError):
    pass


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def minute_start(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(second=0, microsecond=0)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    if value.lower().strip() in {"1", "true", "yes", "on"}:
        return True
    if value.lower().strip() in {"0", "false", "no", "off"}:
        return False
    raise ProjectXError(f"{name} must be true or false")


@dataclass(frozen=True)
class Candle:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_api(cls, value: dict[str, Any]) -> "Candle":
        return cls(
            minute_start(parse_timestamp(value["t"])),
            float(value["o"]),
            float(value["h"]),
            float(value["l"]),
            float(value["c"]),
            float(value["v"]),
        )


class ProjectX:
    def __init__(self, username: str, api_key: str) -> None:
        self.username = username
        self.api_key = api_key
        self.token: str | None = None
        self.http = httpx.AsyncClient(base_url=API_URL, timeout=20)

    async def close(self) -> None:
        await self.http.aclose()

    async def request(
        self, path: str, body: dict[str, Any], *, authenticated: bool = True
    ) -> dict[str, Any]:
        if authenticated and not self.token:
            await self.login()
        for attempt in range(6):
            headers = {"Accept": "text/plain", "Content-Type": "application/json"}
            if authenticated:
                headers["Authorization"] = f"Bearer {self.token}"
            try:
                response = await self.http.post(path, json=body, headers=headers)
                if response.status_code == 401 and authenticated and attempt == 0:
                    await self.login()
                    continue
                if response.status_code == 429:
                    delay = float(
                        response.headers.get("Retry-After", min(2**attempt, 30))
                    )
                    logging.warning("Rate limited; retrying in %.1fs", delay)
                    await asyncio.sleep(delay + random.random() / 2)
                    continue
                response.raise_for_status()
                data = response.json()
                if not data.get("success"):
                    raise ProjectXError(
                        f"{path} failed: code={data.get('errorCode')} "
                        f"message={data.get('errorMessage')!r}"
                    )
                return data
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt == 5:
                    raise ProjectXError(f"Network failure calling {path}: {exc}") from exc
                delay = min(2**attempt, 30) + random.random() / 2
                logging.warning("API network error; retrying in %.1fs", delay)
                await asyncio.sleep(delay)
        raise ProjectXError(f"Retry limit reached for {path}")

    async def login(self) -> None:
        data = await self.request(
            "/api/Auth/loginKey",
            {"userName": self.username, "apiKey": self.api_key},
            authenticated=False,
        )
        self.token = data.get("token")
        if not self.token:
            raise ProjectXError("Login returned no session token")
        logging.info("Authenticated with ProjectX")

    async def accounts(self) -> list[dict[str, Any]]:
        data = await self.request(
            "/api/Account/search", {"onlyActiveAccounts": True}
        )
        return data["accounts"]

    async def verify_simulated_account(
        self,
        account_id: int,
        account_name: str,
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        """Verify broker-reported simulated status from REST or the user hub."""
        if not self.token:
            raise ProjectXError("Cannot verify account without a session token")
        # SubscribeAccounts may emit updates without an initial account snapshot.
        # A fresh authenticated REST response can provide the same explicit flag.
        snapshot = await self.accounts()
        matches = [account for account in snapshot
                   if str(account.get("id")) == str(account_id)]
        if len(matches) != 1 or matches[0].get("name") != account_name:
            raise ProjectXError("Account identity missing or ambiguous in verification snapshot")
        verified = matches[0]
        if "simulated" in verified:
            if verified["simulated"] is not True:
                raise ProjectXError("Broker account is not explicitly verified as simulated")
            return verified
        url = f"{USER_HUB}?access_token={quote(self.token, safe='')}"
        try:
            async with websockets.connect(
                url,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as socket:
                await socket.send(
                    '{"protocol":"json","version":1}' + RECORD_SEPARATOR
                )
                reply = str(await asyncio.wait_for(socket.recv(), timeout=10))
                handshake = reply.split(RECORD_SEPARATOR)[0]
                if handshake and json.loads(handshake).get("error"):
                    raise ProjectXError(
                        "User hub handshake failed: "
                        + str(json.loads(handshake)["error"])
                    )
                invocation = {
                    "type": 1,
                    "invocationId": "account-verification",
                    "target": "SubscribeAccounts",
                    "arguments": [],
                }
                await socket.send(
                    json.dumps(invocation, separators=(",", ":"))
                    + RECORD_SEPARATOR
                )
                loop = asyncio.get_running_loop()
                deadline = loop.time() + timeout_seconds
                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
                    for frame in str(raw).split(RECORD_SEPARATOR):
                        if not frame:
                            continue
                        message = json.loads(frame)
                        if not isinstance(message, dict):
                            continue
                        if message.get("type") == 7:
                            raise ProjectXError(
                                "User hub closed during account verification"
                            )
                        if (
                            message.get("type") != 1
                            or message.get("target") != "GatewayUserAccount"
                        ):
                            continue
                        arguments = message.get("arguments", [])
                        if not isinstance(arguments, list) or not arguments:
                            continue
                        payloads = (
                            arguments[0]
                            if isinstance(arguments[0], list)
                            else [arguments[0]]
                        )
                        for account in payloads:
                            if not isinstance(account, dict):
                                continue
                            if (
                                str(account.get("id")) == str(account_id)
                                and account.get("name") == account_name
                            ):
                                if account.get("simulated") is not True:
                                    raise ProjectXError(
                                        "selected account is not independently "
                                        "verified as simulated"
                                    )
                                return account
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise ProjectXError(
                "timed out waiting for simulated-status account verification"
            ) from exc
        except ConnectionClosed as exc:
            raise ProjectXError("User hub connection closed during account verification") from exc
        raise ProjectXError(
            "matching account was not returned by the user verification hub"
        )
    async def mnq(self, live: bool) -> dict[str, Any]:
        data = await self.request(
            "/api/Contract/search", {"searchText": "MNQ", "live": live}
        )
        matches = [
            contract
            for contract in data["contracts"]
            if contract.get("activeContract")
            and contract.get("symbolId") == MNQ_SYMBOL
        ]
        if len(matches) != 1:
            ids = ", ".join(str(contract.get("id")) for contract in matches) or "none"
            raise ProjectXError(f"Expected one active MNQ contract; found {ids}")
        return matches[0]

    async def bars(
        self, contract_id: str, live: bool, limit: int = 300
    ) -> list[Candle]:
        end = now_utc()
        data = await self.request(
            "/api/History/retrieveBars",
            {
                "contractId": contract_id,
                "live": live,
                "startTime": (end - timedelta(days=10))
                .isoformat()
                .replace("+00:00", "Z"),
                "endTime": end.isoformat().replace("+00:00", "Z"),
                "unit": 2,
                "unitNumber": 1,
                "limit": limit,
                "includePartialBar": False,
            },
        )
        unique = {bar.time: bar for bar in map(Candle.from_api, data["bars"])}
        return sorted(unique.values(), key=lambda candle: candle.time)[-limit:]

    async def open_positions(self, account_id: int) -> list[dict[str, Any]]:
        data = await self.request(
            "/api/Position/searchOpen", {"accountId": account_id}
        )
        return data["positions"]

    async def open_orders(self, account_id: int) -> list[dict[str, Any]]:
        data = await self.request(
            "/api/Order/searchOpen", {"accountId": account_id}
        )
        return data["orders"]

    async def orders(
        self, account_id: int, start: datetime, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "accountId": account_id,
            "startTimestamp": start.astimezone(UTC).isoformat(),
            "endTimestamp": (end or now_utc()).astimezone(UTC).isoformat(),
        }
        data = await self.request("/api/Order/search", body)
        return data["orders"]

    async def trades(
        self, account_id: int, start: datetime, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "accountId": account_id,
            "startTimestamp": start.astimezone(UTC).isoformat(),
            "endTimestamp": (end or now_utc()).astimezone(UTC).isoformat(),
        }
        data = await self.request("/api/Trade/search", body)
        return data["trades"]

    async def place_order_once(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Submit exactly once. A timeout is uncertain and is never retried."""
        if not self.token:
            raise ProjectXError("Order blocked: no authenticated session")
        headers = {
            "Accept": "text/plain",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        try:
            response = await self.http.post(
                "/api/Order/place", json=payload, headers=headers
            )
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ProjectXError(
                "order state uncertain after network failure; no retry was attempted"
            ) from exc

    async def close_position_once(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Close one contract position once; uncertain responses are reconciled."""
        if not self.token:
            raise ProjectXError("Position close blocked: no authenticated session")
        headers = {
            "Accept": "text/plain",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        try:
            response = await self.http.post(
                "/api/Position/closeContract", json=payload, headers=headers
            )
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ProjectXError(
                "position close state uncertain; no retry was attempted"
            ) from exc
    async def cancel_order_once(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Cancel exactly once. An uncertain response is reconciled, never retried."""
        if not self.token:
            raise ProjectXError("Order cancellation blocked: no authenticated session")
        headers = {
            "Accept": "text/plain",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        try:
            response = await self.http.post(
                "/api/Order/cancel", json=payload, headers=headers
            )
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ProjectXError(
                "order cancellation state uncertain; no retry was attempted"
            ) from exc



class MarketDataFeed:
    """Maintain live MNQ state and emit only new REST-confirmed candles."""

    def __init__(
        self, api: ProjectX, contract: dict[str, Any], live: bool
    ) -> None:
        self.api = api
        self.contract = contract
        self.live = live
        self.candles: deque[Candle] = deque(maxlen=300)
        self.current: Candle | None = None
        self.last_price: float | None = None
        self.best_bid: float | None = None
        self.best_ask: float | None = None
        self.last_market_update: datetime | None = None
        self.last_quote_update: datetime | None = None
        self.stop = asyncio.Event()
        self.connected = False
        self.invocation = 0
        self.seen_trades: deque[tuple[Any, ...]] = deque()
        self.seen_trade_keys: set[tuple[Any, ...]] = set()
        self.max_seen_trades = 20_000
        self.unexpected_payload_count = 0

    async def bootstrap(self) -> list[Candle]:
        bars = await self.api.bars(self.contract["id"], self.live, 300)
        if len(bars) < 50:
            raise ProjectXError(
                f"At least 50 completed candles required; received {len(bars)}"
            )
        self.candles.extend(bars)
        logging.info("Downloaded %d completed one-minute candles", len(bars))
        return list(self.candles)

    async def reconcile(
        self, on_closed_candle: ClosedCandleHandler, limit: int = 10,
        *, entry_eligible: bool = True,
    ) -> None:
        fresh = await self.api.bars(self.contract["id"], self.live, limit)
        previous_latest = self.candles[-1].time
        existing = {candle.time: candle for candle in self.candles}
        for candle in fresh:
            existing[candle.time] = candle
        self.candles = deque(
            sorted(existing.values(), key=lambda candle: candle.time)[-300:],
            maxlen=300,
        )

        new_candles = sorted(
            (candle for candle in fresh if candle.time > previous_latest),
            key=lambda candle: candle.time,
        )
        for candle in new_candles:
            await on_closed_candle(candle, entry_eligible)
        if self.current is not None and self.current.time <= self.candles[-1].time:
            self.current = None

    async def reconcile_each_minute(
        self, on_closed_candle: ClosedCandleHandler
    ) -> None:
        while not self.stop.is_set():
            current = now_utc()
            target = minute_start(current) + timedelta(minutes=1, seconds=2)
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=max(0.1, (target - current).total_seconds())
                )
            except asyncio.TimeoutError:
                try:
                    await self.reconcile(on_closed_candle, entry_eligible=True)
                except Exception:
                    logging.exception(
                        "Closed-candle reconciliation failed; will retry next minute"
                    )

    async def invoke(self, socket: Any, target: str) -> None:
        self.invocation += 1
        message = {
            "type": 1,
            "invocationId": str(self.invocation),
            "target": target,
            "arguments": [self.contract["id"]],
        }
        await socket.send(
            json.dumps(message, separators=(",", ":")) + RECORD_SEPARATOR
        )

    def log_unexpected_payload(self, label: str, payload: Any) -> None:
        self.unexpected_payload_count += 1
        count = self.unexpected_payload_count
        if count > 5 and count % 100:
            return
        if isinstance(payload, dict):
            shape = f"dict keys={sorted(map(str, payload.keys()))}"
        elif isinstance(payload, list):
            item_types = sorted({type(item).__name__ for item in payload})
            shape = f"list length={len(payload)} itemTypes={item_types}"
        else:
            shape = type(payload).__name__
        logging.warning(
            "Ignoring malformed %s payload (%s, occurrence=%d)", label, shape, count
        )

    def individual_trades(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            trades = [item for item in payload if isinstance(item, dict)]
            if len(trades) != len(payload):
                self.log_unexpected_payload("GatewayTrade batch item", payload)
            return trades
        self.log_unexpected_payload("GatewayTrade", payload)
        return []

    def remember_trade(self, key: tuple[Any, ...]) -> bool:
        if key in self.seen_trade_keys:
            return False
        if len(self.seen_trades) >= self.max_seen_trades:
            self.seen_trade_keys.discard(self.seen_trades.popleft())
        self.seen_trades.append(key)
        self.seen_trade_keys.add(key)
        return True

    async def update_live_candle(self, trade: dict[str, Any]) -> None:
        try:
            raw_price = trade.get("price")
            raw_time = trade.get("timestamp")
            raw_volume = trade.get("volume", 0)
            if (
                raw_price is None
                or isinstance(raw_price, bool)
                or isinstance(raw_volume, bool)
                or not isinstance(raw_time, str)
            ):
                raise ValueError("missing or invalid trade fields")
            price, size = float(raw_price), float(raw_volume)
            if not math.isfinite(price) or not math.isfinite(size) or size < 0:
                raise ValueError("invalid price or volume")
            trade_time = parse_timestamp(raw_time)
        except (TypeError, ValueError, OverflowError):
            self.log_unexpected_payload("GatewayTrade fields", trade)
            return

        symbol_id = trade.get("symbolId")
        if symbol_id not in (None, MNQ_SYMBOL):
            self.log_unexpected_payload("GatewayTrade symbol", trade)
            return

        key = (symbol_id, raw_time, price, size, trade.get("type"))
        if not self.remember_trade(key):
            logging.debug("Ignoring duplicate GatewayTrade")
            return

        bucket = minute_start(trade_time)
        self.last_price = price
        self.last_market_update = now_utc()
        if self.current is None or bucket > self.current.time:
            self.current = Candle(bucket, price, price, price, price, size)
        elif bucket == self.current.time:
            self.current = Candle(
                bucket,
                self.current.open,
                max(self.current.high, price),
                min(self.current.low, price),
                price,
                self.current.volume + size,
            )
        else:
            logging.debug("Ignoring out-of-order trade for %s", bucket)
            return

        logging.debug(
            "MNQ tick timestamp=%s price=%.2f currentOHLC=%.2f/%.2f/%.2f/%.2f volume=%.0f",
            trade_time.isoformat().replace("+00:00", "Z"),
            price,
            self.current.open,
            self.current.high,
            self.current.low,
            self.current.close,
            self.current.volume,
        )

    async def stream_once(self) -> None:
        if not self.api.token:
            raise ProjectXError("Cannot connect market stream without a session token")
        url = (
            f"{MARKET_HUB}?access_token="
            f"{quote(self.api.token, safe='')}"
        )
        async with websockets.connect(
            url,
            open_timeout=15,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as socket:
            await socket.send(
                '{"protocol":"json","version":1}' + RECORD_SEPARATOR
            )
            reply = str(await asyncio.wait_for(socket.recv(), timeout=10))
            handshake = reply.split(RECORD_SEPARATOR)[0]
            if handshake and json.loads(handshake).get("error"):
                raise ProjectXError(
                    f"SignalR handshake failed: {json.loads(handshake)['error']}"
                )
            await self.invoke(socket, "SubscribeContractQuotes")
            await self.invoke(socket, "SubscribeContractTrades")
            self.connected = True
            logging.info("Connected to live MNQ market stream")

            async for raw in socket:
                for frame in str(raw).split(RECORD_SEPARATOR):
                    if not frame:
                        continue
                    try:
                        message = json.loads(frame)
                    except (json.JSONDecodeError, TypeError):
                        self.log_unexpected_payload("SignalR JSON", frame)
                        continue
                    if not isinstance(message, dict):
                        self.log_unexpected_payload("SignalR message", message)
                        continue
                    if message.get("type") == 7:
                        raise ProjectXError(
                            f"Market hub closed: {message.get('error', 'unknown reason')}"
                        )
                    if message.get("type") != 1:
                        continue
                    arguments = message.get("arguments", [])
                    if not isinstance(arguments, list):
                        self.log_unexpected_payload(
                            "SignalR arguments", arguments
                        )
                        continue
                    target = message.get("target")
                    if target == "GatewayQuote" and len(arguments) >= 2:
                        quote_data = arguments[1]
                        if isinstance(quote_data, dict):
                            try:
                                value = quote_data.get("lastPrice")
                                bid_value = quote_data.get("bestBid")
                                ask_value = quote_data.get("bestAsk")
                                if value is not None and not isinstance(value, bool):
                                    parsed = float(value)
                                    if math.isfinite(parsed):
                                        self.last_price = parsed
                                if (
                                    bid_value is not None
                                    and ask_value is not None
                                    and not isinstance(bid_value, bool)
                                    and not isinstance(ask_value, bool)
                                ):
                                    parsed_bid = float(bid_value)
                                    parsed_ask = float(ask_value)
                                    if (
                                        math.isfinite(parsed_bid)
                                        and math.isfinite(parsed_ask)
                                        and parsed_bid > 0
                                        and parsed_ask >= parsed_bid
                                    ):
                                        self.best_bid = parsed_bid
                                        self.best_ask = parsed_ask
                                        self.last_quote_update = now_utc()
                                self.last_market_update = now_utc()
                            except (TypeError, ValueError, OverflowError):
                                self.log_unexpected_payload(
                                    "GatewayQuote fields", quote_data
                                )
                        else:
                            self.log_unexpected_payload(
                                "GatewayQuote", quote_data
                            )
                    elif target == "GatewayTrade" and len(arguments) >= 2:
                        for trade in self.individual_trades(arguments[1]):
                            try:
                                await self.update_live_candle(trade)
                            except Exception:
                                logging.exception(
                                    "Ignoring unexpected GatewayTrade processing error"
                                )
                    elif target == "GatewayTrade":
                        self.log_unexpected_payload(
                            "GatewayTrade arguments", arguments
                        )

    async def stream_forever(
        self, on_closed_candle: ClosedCandleHandler
    ) -> None:
        backoff = 1.0
        while not self.stop.is_set():
            try:
                await self.stream_once()
                self.connected = False
                backoff = 1.0
            except asyncio.CancelledError:
                self.connected = False
                raise
            except Exception:
                self.connected = False
                logging.exception("Market stream disconnected")
                try:
                    await self.api.login()
                    await self.reconcile(
                        on_closed_candle, limit=300, entry_eligible=False
                    )
                except Exception:
                    logging.exception("Reconnect preparation failed")
                delay = min(backoff, 30.0) + random.random() / 2
                logging.info("Reconnecting market stream in %.1fs", delay)
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 30.0)

    def quote_snapshot(
        self, maximum_age_seconds: float = 3.0
    ) -> dict[str, Any] | None:
        if (
            self.best_bid is None
            or self.best_ask is None
            or self.last_quote_update is None
        ):
            return None
        age = (now_utc() - self.last_quote_update).total_seconds()
        if age > maximum_age_seconds:
            return None
        return {
            "lastPrice": self.last_price,
            "bestBid": self.best_bid,
            "bestAsk": self.best_ask,
            "ageSeconds": age,
            "timestamp": self.last_quote_update.isoformat(),
        }

    def market_is_fresh(self, maximum_age_seconds: float = 15.0) -> bool:
        if self.last_market_update is None:
            return False
        return (now_utc() - self.last_market_update).total_seconds() <= maximum_age_seconds

    async def run(self, on_closed_candle: ClosedCandleHandler) -> None:
        await asyncio.gather(
            self.stream_forever(on_closed_candle),
            self.reconcile_each_minute(on_closed_candle),
        )

