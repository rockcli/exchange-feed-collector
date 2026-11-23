"""Tests for SQLite WAL deduplication, concurrency, spread/basis analytics, and anomalies."""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from exchange_collector.analytics import (
    analyze_run,
    compute_spread_buckets,
    evaluate_spread_anomalies,
    summarize_spreads,
)
from exchange_collector.models import CaptureRun, NormalizedTick, epoch_ms_to_utc_iso
from exchange_collector.storage import MarketStorage


def _make_tick(
    *,
    exchange: str,
    price: float,
    ts_ms: int,
    received_ms: int,
    source: str = "ws",
) -> NormalizedTick:
    if exchange == "okx":
        symbol, raw_symbol, quote = "BTC-USDT", "BTC-USDT", "USDT"
    elif exchange == "kraken":
        symbol, raw_symbol, quote = "BTC-USD", "XBT/USD", "USD"
    else:
        symbol, raw_symbol, quote = "BTC-USD", "BTC-USD", "USD"

    return NormalizedTick(
        exchange=exchange,
        symbol=symbol,
        raw_symbol=raw_symbol,
        base_currency="BTC",
        quote_currency=quote,
        price=price,
        bid=price - 0.1,
        ask=price + 0.1,
        size=0.01,
        volume_24h=1000.0,
        sequence_id=None,
        ts_ms=ts_ms,
        ts_utc=epoch_ms_to_utc_iso(ts_ms),
        received_ms=received_ms,
        latency_ms=float(received_ms - ts_ms),
        source=source,
    )


def test_storage_wal_idempotent_upsert_and_latency_filtering(tmp_path: Path) -> None:
    db = tmp_path / "wal_test.db"
    base_ts = 1_795_395_600_000

    with MarketStorage(db) as storage:
        conn = sqlite3.connect(str(db))
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        conn.close()
        assert str(mode).lower() == "wal"

        storage.start_run(
            CaptureRun(
                run_id="r1",
                mode="demo",
                started_ms=base_ts,
                ended_ms=base_ts + 5000,
                started_utc=epoch_ms_to_utc_iso(base_ts),
                ended_utc=epoch_ms_to_utc_iso(base_ts + 5000),
                exchanges="coinbase,kraken,okx",
                notes="test",
            )
        )

        # Same (exchange, symbol, ts_ms, source) inserted twice must deduplicate cleanly
        t1 = _make_tick(exchange="coinbase", price=60700.0, ts_ms=base_ts, received_ms=base_ts + 40)
        t1_dup = _make_tick(
            exchange="coinbase", price=60701.0, ts_ms=base_ts, received_ms=base_ts + 80
        )
        # Skewed tick (clock ahead by 50ms) + historical backfill tick (large lag)
        t2 = _make_tick(
            exchange="coinbase", price=60702.0, ts_ms=base_ts + 1000, received_ms=base_ts + 950
        )
        t_bf = _make_tick(
            exchange="coinbase",
            price=60703.0,
            ts_ms=base_ts + 2000,
            received_ms=base_ts + 12000,
            source="rest_backfill",
        )
        storage.upsert_ticks("r1", [t1, t1_dup, t2, t_bf])

        all_ticks = storage.get_ticks("r1")
        assert len(all_ticks) == 3

        lat = storage.get_latency_percentiles("r1")["coinbase"]
        assert lat["count"] == 2.0  # rest_backfill excluded from live-stream percentiles
        assert lat["skewed_count"] == 1.0
        assert lat["max"] == 40.0  # kept earliest received_ms on duplicate upsert


def test_storage_concurrent_writes_no_lock_errors(tmp_path: Path) -> None:
    db = tmp_path / "concurrent.db"
    base_ts = 1_795_395_600_000

    with MarketStorage(db) as storage:
        storage.start_run(
            CaptureRun(
                run_id="r_conc",
                mode="live-sample",
                started_ms=base_ts,
                ended_ms=None,
                started_utc=epoch_ms_to_utc_iso(base_ts),
                ended_utc=None,
                exchanges="coinbase,kraken,okx",
                notes="",
            )
        )

        def write_batch(worker_idx: int) -> None:
            ex = ("coinbase", "kraken", "okx")[worker_idx % 3]
            batch = [
                _make_tick(
                    exchange=ex,
                    price=60700.0 + i,
                    ts_ms=base_ts + worker_idx * 1000 + i,
                    received_ms=base_ts + worker_idx * 1000 + i + 30,
                )
                for i in range(40)
            ]
            storage.upsert_ticks("r_conc", batch)

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(write_batch, range(12)))

        assert len(storage.get_ticks("r_conc")) == 480


def test_spread_buckets_separate_usd_and_usdt_basis(tmp_path: Path) -> None:
    db = tmp_path / "analytics.db"
    base_ts = 1_795_395_600_000
    ticks = [
        _make_tick(
            exchange="coinbase", price=60000.0, ts_ms=base_ts + 100, received_ms=base_ts + 140
        ),
        _make_tick(
            exchange="kraken", price=60006.0, ts_ms=base_ts + 200, received_ms=base_ts + 250
        ),
        _make_tick(exchange="okx", price=60090.0, ts_ms=base_ts + 300, received_ms=base_ts + 360),
        # Second bucket triggers > 15 bps USD spread anomaly
        _make_tick(
            exchange="coinbase", price=60000.0, ts_ms=base_ts + 1100, received_ms=base_ts + 1140
        ),
        _make_tick(
            exchange="kraken", price=60120.0, ts_ms=base_ts + 1200, received_ms=base_ts + 1250
        ),
        _make_tick(exchange="okx", price=60100.0, ts_ms=base_ts + 1300, received_ms=base_ts + 1360),
    ]

    buckets = compute_spread_buckets(ticks, bucket_ms=1000)
    assert len(buckets) == 2
    assert buckets[0].usd_spread_delta == 6.0
    assert buckets[0].usdt_basis_delta == 87.0  # 60090 - mean(60000, 60006)

    usd_sum = summarize_spreads(buckets, kind="usd")
    usdt_sum = summarize_spreads(buckets, kind="usdt_basis")
    assert usd_sum is not None and usdt_sum is not None
    assert usd_sum.max_abs_bps > 19.0
    assert usdt_sum.mean_delta > 60.0

    anomalies = evaluate_spread_anomalies("r_an", buckets, usd_spread_alert_bps=15.0)
    assert len(anomalies) == 1
    assert anomalies[0].alert_type == "spread_anomaly"

    with MarketStorage(db) as storage:
        storage.start_run(
            CaptureRun(
                run_id="r_an",
                mode="demo",
                started_ms=base_ts,
                ended_ms=base_ts + 2000,
                started_utc=epoch_ms_to_utc_iso(base_ts),
                ended_utc=epoch_ms_to_utc_iso(base_ts + 2000),
                exchanges="coinbase,kraken,okx",
                notes="",
            )
        )
        storage.upsert_ticks("r_an", ticks)
        result = analyze_run(storage, "r_an", persist_spread_alerts=True)
        assert len(result.heatmap_slices) == len(ticks)
        assert len(result.alerts) == 1
