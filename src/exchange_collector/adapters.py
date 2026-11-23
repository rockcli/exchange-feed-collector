"""Official public REST and WebSocket adapters for Coinbase, Kraken, and OKX."""

from __future__ import annotations

import os
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import aiohttp

from exchange_collector.models import NormalizedTick, epoch_ms_to_utc_iso, utc_now_ms
from exchange_collector.resilience import AsyncRateLimiter


class ExchangeRateLimitError(RuntimeError):
    """Raised when an exchange REST endpoint returns HTTP 429 or explicit rate limit."""

    def __init__(self, exchange: str, retry_after_seconds: float = 5.0) -> None:
        self.exchange = exchange
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"{exchange} rate limited; retry after {retry_after_seconds:.1f}s")


def resolve_proxy_url(target_url: str | None = None) -> str | None:
    """Resolve HTTPS/HTTP proxy from environment or OS settings, respecting localhost bypass."""
    if target_url:
        host = urllib.parse.urlparse(target_url).hostname or ""
        if host in {"127.0.0.1", "localhost", "::1"} or urllib.request.proxy_bypass(host):
            return None
    for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        value = os.environ.get(key)
        if value:
            return value
    proxies = urllib.request.getproxies()
    return proxies.get("https") or proxies.get("http")


def parse_iso_to_ms(value: str) -> int:
    """Parse RFC3339/ISO-8601 UTC timestamp (including nanosecond precision) into epoch ms."""
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    if "." in cleaned:
        head, tail = cleaned.split(".", 1)
        tz_idx = max(tail.find("+"), tail.find("-"))
        if tz_idx == -1:
            frac = tail[:6].ljust(6, "0")
            cleaned = f"{head}.{frac}+00:00"
        else:
            frac = tail[:tz_idx][:6].ljust(6, "0")
            tz_part = tail[tz_idx:]
            cleaned = f"{head}.{frac}{tz_part}"
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def normalize_symbol(exchange: str, raw_symbol: str) -> tuple[str, str, str]:
    """Normalize venue-specific pair names while preserving explicit quote currency.

    Examples:
        - Coinbase `BTC-USD` -> (`BTC-USD`, `BTC`, `USD`)
        - Kraken `XBT/USD`, `XXBTZUSD`, `BTC/USD` -> (`BTC-USD`, `BTC`, `USD`)
        - OKX `BTC-USDT` -> (`BTC-USDT`, `BTC`, `USDT`)
    """
    token = raw_symbol.strip().upper()
    if exchange == "kraken" and token in {"XBT/USD", "XBTUSD", "XXBTZUSD", "BTC/USD"}:
        return ("BTC-USD", "BTC", "USD")
    if "-" in token:
        base, quote = token.split("-", 1)
        base = "BTC" if base == "XBT" else base
        return (f"{base}-{quote}", base, quote)
    if "/" in token:
        base, quote = token.split("/", 1)
        base = "BTC" if base == "XBT" else base
        return (f"{base}-{quote}", base, quote)
    raise ValueError(f"Unsupported symbol format {raw_symbol!r} for exchange {exchange!r}")


def _to_optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _to_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def build_tick(
    *,
    exchange: str,
    raw_symbol: str,
    price: float,
    bid: float | None,
    ask: float | None,
    size: float | None,
    volume_24h: float | None,
    sequence_id: int | None,
    ts_ms: int,
    received_ms: int,
    source: str,
) -> NormalizedTick:
    """Construct a normalized tick with canonical symbol and ingest latency."""
    symbol, base, quote = normalize_symbol(exchange, raw_symbol)
    return NormalizedTick(
        exchange=exchange,
        symbol=symbol,
        raw_symbol=raw_symbol,
        base_currency=base,
        quote_currency=quote,
        price=price,
        bid=bid,
        ask=ask,
        size=size,
        volume_24h=volume_24h,
        sequence_id=sequence_id,
        ts_ms=ts_ms,
        ts_utc=epoch_ms_to_utc_iso(ts_ms),
        received_ms=received_ms,
        latency_ms=float(received_ms - ts_ms),
        source=source,
    )


class ExchangeAdapter(ABC):
    """Abstract public market-data adapter supporting WebSocket streaming + REST backfill."""

    exchange: str
    canonical_symbol: str
    raw_symbol: str
    ws_url: str

    def __init__(self, rate_limiter: AsyncRateLimiter | None = None) -> None:
        self.rate_limiter = (
            rate_limiter
            if rate_limiter is not None
            else AsyncRateLimiter(min_interval_seconds=0.35)
        )

    @abstractmethod
    def ws_subscribe_payload(self) -> dict[str, Any]:
        """Return JSON subscription frame sent immediately after WebSocket connect."""

    def ws_ping_payload(self) -> str | dict[str, Any] | None:
        """Return optional application-level ping message for keepalive."""
        return None

    def is_heartbeat_or_control(self, text_or_payload: str | dict[str, Any]) -> bool:
        """Return True for non-tick keepalive/ack frames that still prove WS liveness."""
        if isinstance(text_or_payload, str):
            return text_or_payload.strip().lower() == "pong"
        return False

    @abstractmethod
    def parse_ws_payload(self, payload: dict[str, Any], received_ms: int) -> list[NormalizedTick]:
        """Normalize a decoded JSON WebSocket frame into zero or more ticks."""

    @abstractmethod
    async def fetch_rest_ticker(
        self,
        session: aiohttp.ClientSession,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> NormalizedTick | None:
        """Fetch latest L1 ticker snapshot via official REST endpoint."""

    @abstractmethod
    async def fetch_rest_backfill(
        self,
        session: aiohttp.ClientSession,
        start_ts_ms: int,
        end_ts_ms: int,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> list[NormalizedTick]:
        """Fetch recent public trades via REST to backfill a detected WS gap."""

    async def _get_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> Any:
        await self.rate_limiter.acquire()
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        headers = {"User-Agent": "exchange-feed-collector/0.1"}
        async with session.get(url, timeout=timeout, proxy=proxy_url, headers=headers) as resp:
            if resp.status == 429:
                retry_raw = resp.headers.get("Retry-After", "5")
                retry_after = float(retry_raw) if retry_raw.isdigit() else 5.0
                raise ExchangeRateLimitError(self.exchange, retry_after_seconds=retry_after)
            resp.raise_for_status()
            return await resp.json()


class CoinbaseAdapter(ExchangeAdapter):
    """Coinbase Exchange public `BTC-USD` WebSocket ticker + REST fallback/backfill."""

    exchange = "coinbase"
    canonical_symbol = "BTC-USD"
    raw_symbol = "BTC-USD"
    ws_url = "wss://ws-feed.exchange.coinbase.com"
    rest_ticker_url = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    rest_trades_url = "https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=100"

    def ws_subscribe_payload(self) -> dict[str, Any]:
        return {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"],
        }

    def is_heartbeat_or_control(self, text_or_payload: str | dict[str, Any]) -> bool:
        if isinstance(text_or_payload, dict):
            return text_or_payload.get("type") in {"heartbeat", "subscriptions"}
        return super().is_heartbeat_or_control(text_or_payload)

    def parse_ws_payload(self, payload: dict[str, Any], received_ms: int) -> list[NormalizedTick]:
        if payload.get("type") != "ticker":
            return []
        raw_time = payload.get("time")
        ts_ms = parse_iso_to_ms(str(raw_time)) if raw_time else received_ms
        seq = _to_optional_int(payload.get("trade_id")) or _to_optional_int(payload.get("sequence"))
        return [
            build_tick(
                exchange=self.exchange,
                raw_symbol=str(payload.get("product_id", self.raw_symbol)),
                price=float(payload["price"]),
                bid=_to_optional_float(payload.get("best_bid")),
                ask=_to_optional_float(payload.get("best_ask")),
                size=_to_optional_float(payload.get("last_size")),
                volume_24h=_to_optional_float(payload.get("volume_24h")),
                sequence_id=seq,
                ts_ms=ts_ms,
                received_ms=received_ms,
                source="ws",
            )
        ]

    async def fetch_rest_ticker(
        self,
        session: aiohttp.ClientSession,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> NormalizedTick | None:
        data = await self._get_json(session, self.rest_ticker_url, timeout_seconds, proxy_url)
        received_ms = utc_now_ms()
        if not isinstance(data, dict) or "price" not in data:
            return None
        ts_ms = parse_iso_to_ms(str(data["time"])) if data.get("time") else received_ms
        return build_tick(
            exchange=self.exchange,
            raw_symbol=self.raw_symbol,
            price=float(data["price"]),
            bid=_to_optional_float(data.get("bid")),
            ask=_to_optional_float(data.get("ask")),
            size=_to_optional_float(data.get("size")),
            volume_24h=_to_optional_float(data.get("volume")),
            sequence_id=_to_optional_int(data.get("trade_id")),
            ts_ms=ts_ms,
            received_ms=received_ms,
            source="rest_poll",
        )

    async def fetch_rest_backfill(
        self,
        session: aiohttp.ClientSession,
        start_ts_ms: int,
        end_ts_ms: int,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> list[NormalizedTick]:
        data = await self._get_json(session, self.rest_trades_url, timeout_seconds, proxy_url)
        received_ms = utc_now_ms()
        if not isinstance(data, list):
            return []
        ticks: list[NormalizedTick] = []
        for item in data:
            if not isinstance(item, dict) or "price" not in item or "time" not in item:
                continue
            ts_ms = parse_iso_to_ms(str(item["time"]))
            if ts_ms < start_ts_ms or ts_ms > end_ts_ms:
                continue
            ticks.append(
                build_tick(
                    exchange=self.exchange,
                    raw_symbol=self.raw_symbol,
                    price=float(item["price"]),
                    bid=None,
                    ask=None,
                    size=_to_optional_float(item.get("size")),
                    volume_24h=None,
                    sequence_id=_to_optional_int(item.get("trade_id")),
                    ts_ms=ts_ms,
                    received_ms=received_ms,
                    source="rest_backfill",
                )
            )
        ticks.sort(key=lambda t: t.ts_ms)
        return ticks


class KrakenAdapter(ExchangeAdapter):
    """Kraken Spot v2 public `XBT/USD` -> normalized `BTC-USD` WS + REST adapter."""

    exchange = "kraken"
    canonical_symbol = "BTC-USD"
    raw_symbol = "XBT/USD"
    ws_url = "wss://ws.kraken.com/v2"
    rest_ticker_url = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
    rest_trades_url = "https://api.kraken.com/0/public/Trades?pair=XBTUSD&count=100"

    def ws_subscribe_payload(self) -> dict[str, Any]:
        return {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": ["BTC/USD"],
                "event_trigger": "bbo",
            },
        }

    def is_heartbeat_or_control(self, text_or_payload: str | dict[str, Any]) -> bool:
        if isinstance(text_or_payload, dict):
            if text_or_payload.get("channel") in {"heartbeat", "status"}:
                return True
            if text_or_payload.get("method") == "subscribe":
                return True
        return super().is_heartbeat_or_control(text_or_payload)

    def parse_ws_payload(self, payload: dict[str, Any], received_ms: int) -> list[NormalizedTick]:
        if payload.get("channel") != "ticker":
            return []
        items = payload.get("data")
        if not isinstance(items, list):
            return []
        ticks: list[NormalizedTick] = []
        for item in items:
            if not isinstance(item, dict) or "last" not in item:
                continue
            raw_ts = item.get("timestamp")
            ts_ms = parse_iso_to_ms(str(raw_ts)) if raw_ts else received_ms
            ticks.append(
                build_tick(
                    exchange=self.exchange,
                    raw_symbol=str(item.get("symbol", self.raw_symbol)),
                    price=float(item["last"]),
                    bid=_to_optional_float(item.get("bid")),
                    ask=_to_optional_float(item.get("ask")),
                    size=None,
                    volume_24h=_to_optional_float(item.get("volume")),
                    sequence_id=None,
                    ts_ms=ts_ms,
                    received_ms=received_ms,
                    source="ws",
                )
            )
        return ticks

    async def fetch_rest_ticker(
        self,
        session: aiohttp.ClientSession,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> NormalizedTick | None:
        data = await self._get_json(session, self.rest_ticker_url, timeout_seconds, proxy_url)
        received_ms = utc_now_ms()
        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, dict) or not result:
            return None
        raw_pair, ticker = next(iter(result.items()))
        if not isinstance(ticker, dict):
            return None
        ask_list = ticker.get("a") or []
        bid_list = ticker.get("b") or []
        close_list = ticker.get("c") or []
        vol_list = ticker.get("v") or []
        if not close_list:
            return None
        return build_tick(
            exchange=self.exchange,
            raw_symbol=str(raw_pair),
            price=float(close_list[0]),
            bid=_to_optional_float(bid_list[0]) if bid_list else None,
            ask=_to_optional_float(ask_list[0]) if ask_list else None,
            size=_to_optional_float(close_list[1]) if len(close_list) > 1 else None,
            volume_24h=_to_optional_float(vol_list[1]) if len(vol_list) > 1 else None,
            sequence_id=None,
            ts_ms=received_ms,
            received_ms=received_ms,
            source="rest_poll",
        )

    async def fetch_rest_backfill(
        self,
        session: aiohttp.ClientSession,
        start_ts_ms: int,
        end_ts_ms: int,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> list[NormalizedTick]:
        data = await self._get_json(session, self.rest_trades_url, timeout_seconds, proxy_url)
        received_ms = utc_now_ms()
        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, dict):
            return []
        ticks: list[NormalizedTick] = []
        for raw_pair, entries in result.items():
            if raw_pair == "last" or not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, list) or len(entry) < 3:
                    continue
                ts_ms = int(float(entry[2]) * 1000)
                if ts_ms < start_ts_ms or ts_ms > end_ts_ms:
                    continue
                seq = _to_optional_int(entry[6]) if len(entry) > 6 else None
                ticks.append(
                    build_tick(
                        exchange=self.exchange,
                        raw_symbol=str(raw_pair),
                        price=float(entry[0]),
                        bid=None,
                        ask=None,
                        size=_to_optional_float(entry[1]),
                        volume_24h=None,
                        sequence_id=seq,
                        ts_ms=ts_ms,
                        received_ms=received_ms,
                        source="rest_backfill",
                    )
                )
        ticks.sort(key=lambda t: t.ts_ms)
        return ticks


class OKXAdapter(ExchangeAdapter):
    """OKX v5 public `BTC-USDT` ticker adapter (explicit `USDT` quote currency)."""

    exchange = "okx"
    canonical_symbol = "BTC-USDT"
    raw_symbol = "BTC-USDT"
    ws_url = "wss://ws.okx.com/ws/v5/public"
    rest_ticker_url = "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"
    rest_trades_url = "https://www.okx.com/api/v5/market/trades?instId=BTC-USDT&limit=100"

    def ws_subscribe_payload(self) -> dict[str, Any]:
        return {
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": "BTC-USDT"}],
        }

    def ws_ping_payload(self) -> str:
        return "ping"

    def is_heartbeat_or_control(self, text_or_payload: str | dict[str, Any]) -> bool:
        if isinstance(text_or_payload, dict):
            return text_or_payload.get("event") in {"subscribe", "notice", "error"}
        return super().is_heartbeat_or_control(text_or_payload)

    def parse_ws_payload(self, payload: dict[str, Any], received_ms: int) -> list[NormalizedTick]:
        arg = payload.get("arg")
        if not isinstance(arg, dict) or arg.get("channel") != "tickers":
            return []
        items = payload.get("data")
        if not isinstance(items, list):
            return []
        ticks: list[NormalizedTick] = []
        for item in items:
            if not isinstance(item, dict) or "last" not in item:
                continue
            ts_ms = int(item["ts"]) if item.get("ts") else received_ms
            ticks.append(
                build_tick(
                    exchange=self.exchange,
                    raw_symbol=str(item.get("instId", self.raw_symbol)),
                    price=float(item["last"]),
                    bid=_to_optional_float(item.get("bidPx")),
                    ask=_to_optional_float(item.get("askPx")),
                    size=_to_optional_float(item.get("lastSz")),
                    volume_24h=_to_optional_float(item.get("vol24h")),
                    sequence_id=None,
                    ts_ms=ts_ms,
                    received_ms=received_ms,
                    source="ws",
                )
            )
        return ticks

    async def fetch_rest_ticker(
        self,
        session: aiohttp.ClientSession,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> NormalizedTick | None:
        data = await self._get_json(session, self.rest_ticker_url, timeout_seconds, proxy_url)
        received_ms = utc_now_ms()
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            return None
        item = items[0]
        if not isinstance(item, dict) or "last" not in item:
            return None
        ts_ms = int(item["ts"]) if item.get("ts") else received_ms
        return build_tick(
            exchange=self.exchange,
            raw_symbol=str(item.get("instId", self.raw_symbol)),
            price=float(item["last"]),
            bid=_to_optional_float(item.get("bidPx")),
            ask=_to_optional_float(item.get("askPx")),
            size=_to_optional_float(item.get("lastSz")),
            volume_24h=_to_optional_float(item.get("vol24h")),
            sequence_id=None,
            ts_ms=ts_ms,
            received_ms=received_ms,
            source="rest_poll",
        )

    async def fetch_rest_backfill(
        self,
        session: aiohttp.ClientSession,
        start_ts_ms: int,
        end_ts_ms: int,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> list[NormalizedTick]:
        data = await self._get_json(session, self.rest_trades_url, timeout_seconds, proxy_url)
        received_ms = utc_now_ms()
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        ticks: list[NormalizedTick] = []
        for item in items:
            if not isinstance(item, dict) or "px" not in item or "ts" not in item:
                continue
            ts_ms = int(item["ts"])
            if ts_ms < start_ts_ms or ts_ms > end_ts_ms:
                continue
            ticks.append(
                build_tick(
                    exchange=self.exchange,
                    raw_symbol=str(item.get("instId", self.raw_symbol)),
                    price=float(item["px"]),
                    bid=None,
                    ask=None,
                    size=_to_optional_float(item.get("sz")),
                    volume_24h=None,
                    sequence_id=_to_optional_int(item.get("tradeId")),
                    ts_ms=ts_ms,
                    received_ms=received_ms,
                    source="rest_backfill",
                )
            )
        ticks.sort(key=lambda t: t.ts_ms)
        return ticks


def default_adapters() -> list[ExchangeAdapter]:
    """Instantiate the default trio of public exchange adapters."""
    return [CoinbaseAdapter(), KrakenAdapter(), OKXAdapter()]
