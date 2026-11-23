"""Unit tests for Coinbase, Kraken, and OKX normalization and adapter parsing."""

from __future__ import annotations

from typing import Any

import pytest

from exchange_collector.adapters import (
    CoinbaseAdapter,
    ExchangeRateLimitError,
    KrakenAdapter,
    OKXAdapter,
    build_tick,
    normalize_symbol,
    parse_iso_to_ms,
)
from exchange_collector.models import NormalizedTick, epoch_ms_to_utc_iso


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


def test_same_ms_dedup_keys_across_coinbase_kraken_okx() -> None:
    cb = CoinbaseAdapter()
    cb_quote_only = {
        "type": "ticker",
        "sequence": 140056015570,
        "product_id": "BTC-USD",
        "price": "60729.6",
        "best_bid": "60729.60",
        "best_ask": "60729.65",
        "time": "2026-11-23T02:14:21.398919Z",
    }
    cb_quote_next = {**cb_quote_only, "sequence": 140056015571, "best_ask": "60729.70"}
    t_cb1 = cb.parse_ws_payload(cb_quote_only, received_ms=1795400061450)[0]
    t_cb2 = cb.parse_ws_payload(cb_quote_next, received_ms=1795400061451)[0]
    assert t_cb1.sequence_id is None  # does not mix global orderbook sequence into trade_id stream
    assert t_cb1.dedup_seq != t_cb2.dedup_seq

    kr = KrakenAdapter()
    kr_item: dict[str, Any] = {
        "symbol": "BTC/USD",
        "bid": 60736.1,
        "ask": 60736.2,
        "last": 60732.7,
        "volume": 3048.79,
        "timestamp": "2026-11-23T02:14:21.503067Z",
    }
    kr_p1: dict[str, Any] = {"channel": "ticker", "data": [kr_item]}
    kr_p2: dict[str, Any] = {"channel": "ticker", "data": [{**kr_item, "bid": 60736.3}]}
    t_kr1 = kr.parse_ws_payload(kr_p1, received_ms=1795400061590)[0]
    t_kr1_dup = kr.parse_ws_payload(kr_p1, received_ms=1795400061595)[0]
    t_kr2 = kr.parse_ws_payload(kr_p2, received_ms=1795400061596)[0]
    assert t_kr1.dedup_seq == t_kr1_dup.dedup_seq
    assert t_kr1.dedup_seq != t_kr2.dedup_seq

    okx = OKXAdapter()
    okx_item: dict[str, Any] = {
        "instId": "BTC-USDT",
        "last": "60831.5",
        "lastSz": "0.001",
        "askPx": "60831.5",
        "bidPx": "60831.4",
        "vol24h": "8445.13",
        "ts": "1795400061262",
        "seqId": "9001",
    }
    okx_p1: dict[str, Any] = {
        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
        "data": [okx_item],
    }
    okx_p2: dict[str, Any] = {
        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
        "data": [{**okx_item, "seqId": "9002"}],
    }
    t_okx1 = okx.parse_ws_payload(okx_p1, received_ms=1795400061350)[0]
    t_okx2 = okx.parse_ws_payload(okx_p2, received_ms=1795400061351)[0]
    assert t_okx1.sequence_id == 9001
    assert t_okx2.sequence_id == 9002
    assert t_okx1.dedup_seq != t_okx2.dedup_seq


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


@pytest.mark.parametrize(
    "bad_price",
    [0.0, -0.01, -60000.0, float("nan"), float("inf"), float("-inf")],
)
def test_normalized_tick_and_build_tick_reject_non_positive_or_non_finite_price(
    bad_price: float,
) -> None:
    ts_ms = 1_795_400_061_350
    with pytest.raises(ValueError, match="price"):
        NormalizedTick(
            exchange="coinbase",
            symbol="BTC-USD",
            raw_symbol="BTC-USD",
            base_currency="BTC",
            quote_currency="USD",
            price=bad_price,
            bid=None,
            ask=None,
            size=None,
            volume_24h=None,
            sequence_id=1,
            ts_ms=ts_ms,
            ts_utc=epoch_ms_to_utc_iso(ts_ms),
            received_ms=ts_ms + 10,
            latency_ms=10.0,
            source="ws",
        )
    with pytest.raises(ValueError, match="price"):
        build_tick(
            exchange="coinbase",
            raw_symbol="BTC-USD",
            price=bad_price,
            bid=None,
            ask=None,
            size=None,
            volume_24h=None,
            sequence_id=1,
            ts_ms=ts_ms,
            received_ms=ts_ms + 10,
            source="ws",
        )


@pytest.mark.parametrize("bad_price", ["0", "0.0", "-10.5", "NaN", "inf", "-inf", "", None])
def test_ws_adapters_reject_non_positive_and_non_finite_prices(bad_price: Any) -> None:
    cb = CoinbaseAdapter()
    assert (
        cb.parse_ws_payload(
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": bad_price,
                "time": "2026-11-23T02:14:21.398919Z",
                "trade_id": 100,
            },
            received_ms=1_795_400_061_450,
        )
        == []
    )

    kr = KrakenAdapter()
    assert (
        kr.parse_ws_payload(
            {
                "channel": "ticker",
                "data": [
                    {
                        "symbol": "BTC/USD",
                        "last": bad_price,
                        "timestamp": "2026-11-23T02:14:21.503067Z",
                    }
                ],
            },
            received_ms=1_795_400_061_590,
        )
        == []
    )

    okx = OKXAdapter()
    assert (
        okx.parse_ws_payload(
            {
                "arg": {"channel": "tickers", "instId": "BTC-USDT"},
                "data": [
                    {
                        "instId": "BTC-USDT",
                        "last": bad_price,
                        "ts": "1795400061262",
                    }
                ],
            },
            received_ms=1_795_400_061_350,
        )
        == []
    )


@pytest.mark.asyncio
async def test_rest_ticker_and_backfill_reject_non_positive_prices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cb = CoinbaseAdapter()

    async def fake_cb_ticker(*_: Any, **__: Any) -> dict[str, Any]:
        return {"price": "0", "time": "2026-11-23T01:00:05.000Z", "trade_id": 1}

    monkeypatch.setattr(cb, "_get_json", fake_cb_ticker)
    assert await cb.fetch_rest_ticker(None, 5.0) is None  # type: ignore[arg-type]

    async def fake_cb_trades(*_: Any, **__: Any) -> list[dict[str, Any]]:
        return [
            {"trade_id": 10, "price": "0", "size": "0.01", "time": "2026-11-23T01:00:05.000Z"},
            {"trade_id": 11, "price": "-5.0", "size": "0.01", "time": "2026-11-23T01:00:06.000Z"},
            {
                "trade_id": 12,
                "price": "60705.0",
                "size": "0.02",
                "time": "2026-11-23T01:00:07.000Z",
            },
        ]

    monkeypatch.setattr(cb, "_get_json", fake_cb_trades)
    start_ms = parse_iso_to_ms("2026-11-23T01:00:00.000Z")
    end_ms = parse_iso_to_ms("2026-11-23T01:00:10.000Z")
    cb_bf = await cb.fetch_rest_backfill(None, start_ms, end_ms, 5.0)  # type: ignore[arg-type]
    assert [t.sequence_id for t in cb_bf] == [12]

    kr = KrakenAdapter()

    async def fake_kr_ticker(*_: Any, **__: Any) -> dict[str, Any]:
        return {"result": {"XXBTZUSD": {"c": ["0", "0.1"], "b": ["0"], "a": ["0"]}}}

    monkeypatch.setattr(kr, "_get_json", fake_kr_ticker)
    assert await kr.fetch_rest_ticker(None, 5.0) is None  # type: ignore[arg-type]

    okx = OKXAdapter()

    async def fake_okx_ticker(*_: Any, **__: Any) -> dict[str, Any]:
        return {"data": [{"instId": "BTC-USDT", "last": "-1.0", "ts": "1795400061262"}]}

    monkeypatch.setattr(okx, "_get_json", fake_okx_ticker)
    assert await okx.fetch_rest_ticker(None, 5.0) is None  # type: ignore[arg-type]
