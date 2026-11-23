"""Unit tests for Coinbase, Kraken, and OKX normalization and adapter parsing."""

from __future__ import annotations

from typing import Any

import pytest

from exchange_collector.adapters import (
    CoinbaseAdapter,
    ExchangeRateLimitError,
    KrakenAdapter,
    OKXAdapter,
    normalize_symbol,
    parse_iso_to_ms,
)


def test_normalize_symbol_explicit_quotes() -> None:
    assert normalize_symbol("coinbase", "BTC-USD") == ("BTC-USD", "BTC", "USD")
    assert normalize_symbol("kraken", "XBT/USD") == ("BTC-USD", "BTC", "USD")
    assert normalize_symbol("kraken", "XXBTZUSD") == ("BTC-USD", "BTC", "USD")
    assert normalize_symbol("kraken", "BTC/USD") == ("BTC-USD", "BTC", "USD")
    assert normalize_symbol("okx", "BTC-USDT") == ("BTC-USDT", "BTC", "USDT")


def test_parse_iso_to_ms_handles_nanoseconds() -> None:
    ts = parse_iso_to_ms("2026-11-23T02:12:05.698010167Z")
    assert ts == parse_iso_to_ms("2026-11-23T02:12:05.698010Z")
    assert ts % 1000 == 698


def test_coinbase_ws_parsing_and_controls() -> None:
    adapter = CoinbaseAdapter()
    assert adapter.is_heartbeat_or_control({"type": "subscriptions", "channels": []})
    payload = {
        "type": "ticker",
        "sequence": 140056015569,
        "product_id": "BTC-USD",
        "price": "60729.6",
        "volume_24h": "9052.82",
        "best_bid": "60729.60",
        "best_ask": "60729.61",
        "time": "2026-11-23T02:14:21.398919Z",
        "trade_id": 1143218396,
        "last_size": "0.02",
    }
    ticks = adapter.parse_ws_payload(payload, received_ms=1795400061450)
    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.exchange == "coinbase"
    assert tick.symbol == "BTC-USD"
    assert tick.quote_currency == "USD"
    assert tick.sequence_id == 1143218396
    assert tick.bid == 60729.60
    assert tick.ask == 60729.61
    assert tick.latency_ms == pytest.approx(52.0)


def test_kraken_ws_parsing_and_heartbeat() -> None:
    adapter = KrakenAdapter()
    assert adapter.is_heartbeat_or_control({"channel": "heartbeat"})
    assert adapter.is_heartbeat_or_control({"channel": "status", "type": "update"})
    payload = {
        "channel": "ticker",
        "type": "snapshot",
        "data": [
            {
                "symbol": "BTC/USD",
                "bid": 60736.1,
                "ask": 60736.2,
                "last": 60732.7,
                "volume": 3048.79,
                "timestamp": "2026-11-23T02:14:21.503067Z",
            }
        ],
    }
    ticks = adapter.parse_ws_payload(payload, received_ms=1795400061590)
    assert len(ticks) == 1
    assert ticks[0].symbol == "BTC-USD"
    assert ticks[0].quote_currency == "USD"
    assert ticks[0].price == 60732.7


def test_okx_ws_parsing_and_pong() -> None:
    adapter = OKXAdapter()
    assert adapter.is_heartbeat_or_control("pong")
    assert adapter.is_heartbeat_or_control({"event": "subscribe"})
    payload = {
        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
        "data": [
            {
                "instId": "BTC-USDT",
                "last": "60831.5",
                "lastSz": "0.001",
                "askPx": "60831.5",
                "bidPx": "60831.4",
                "vol24h": "8445.13",
                "ts": "1795400061262",
            }
        ],
    }
    ticks = adapter.parse_ws_payload(payload, received_ms=1795400061350)
    assert len(ticks) == 1
    assert ticks[0].symbol == "BTC-USDT"
    assert ticks[0].quote_currency == "USDT"
    assert ticks[0].latency_ms == pytest.approx(88.0)


@pytest.mark.asyncio
async def test_rest_backfill_filters_window_and_handles_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = CoinbaseAdapter()

    async def fake_get_json(*_: Any, **__: Any) -> list[dict[str, Any]]:
        return [
            {
                "trade_id": 10,
                "price": "60700.0",
                "size": "0.01",
                "time": "2026-11-23T01:00:01.000Z",
            },
            {
                "trade_id": 11,
                "price": "60705.0",
                "size": "0.02",
                "time": "2026-11-23T01:00:05.000Z",
            },
            {
                "trade_id": 12,
                "price": "60710.0",
                "size": "0.03",
                "time": "2026-11-23T01:00:20.000Z",
            },
        ]

    monkeypatch.setattr(adapter, "_get_json", fake_get_json)
    start_ms = parse_iso_to_ms("2026-11-23T01:00:02.000Z")
    end_ms = parse_iso_to_ms("2026-11-23T01:00:10.000Z")
    recovered = await adapter.fetch_rest_backfill(None, start_ms, end_ms, 5.0)  # type: ignore[arg-type]
    assert len(recovered) == 1
    assert recovered[0].sequence_id == 11
    assert recovered[0].source == "rest_backfill"

    async def raise_429(*_: Any, **__: Any) -> Any:
        raise ExchangeRateLimitError("coinbase", retry_after_seconds=3.0)

    monkeypatch.setattr(adapter, "_get_json", raise_429)
    with pytest.raises(ExchangeRateLimitError):
        await adapter.fetch_rest_ticker(None, 5.0)  # type: ignore[arg-type]
