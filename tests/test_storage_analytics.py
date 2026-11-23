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
    sequence_id: int | None = None,
    dedup_seq: str | None = None,
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
        sequence_id=sequence_id,
        ts_ms=ts_ms,
        ts_utc=epoch_ms_to_utc_iso(ts_ms),
        received_ms=received_ms,
        latency_ms=float(received_ms - ts_ms),
        source=source,
        dedup_seq=dedup_seq,
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
        assert lat["raw_count"] == 2.0
        assert lat["non_skewed_count"] == 1.0
        assert lat["skewed_count"] == 1.0
        assert lat["p50"] == 40.0  # computed over non-skewed samples only, not clamped 0.0
        assert lat["max"] == 40.0  # kept earliest received_ms on duplicate upsert
        assert lat["raw_min"] == -50.0


def test_storage_same_ms_distinct_seq_multi_run_isolation_and_dup_counts(tmp_path: Path) -> None:
    db = tmp_path / "dedup_isolation.db"
    base_ts = 1_795_395_600_000

    with MarketStorage(db) as storage:
        for rid in ("run_a", "run_b"):
            storage.start_run(
                CaptureRun(
                    run_id=rid,
                    mode="demo",
                    started_ms=base_ts,
                    ended_ms=base_ts + 5000,
                    started_utc=epoch_ms_to_utc_iso(base_ts),
                    ended_utc=epoch_ms_to_utc_iso(base_ts + 5000),
                    exchanges="coinbase",
                    notes="",
                )
            )

        # (a) Same (exchange, symbol, ts_ms, source) with different sequence_ids must BOTH survive
        t_seq1 = _make_tick(
            exchange="coinbase",
            price=60700.0,
            ts_ms=base_ts,
            received_ms=base_ts + 15,
            sequence_id=1001,
        )
        t_seq2 = _make_tick(
            exchange="coinbase",
            price=60705.0,
            ts_ms=base_ts,
            received_ms=base_ts + 18,
            sequence_id=1002,
        )
        inserted_a, dups_a = storage.insert_ticks("run_a", [t_seq1, t_seq2])
        assert (inserted_a, dups_a) == (2, 0)
        assert [t.sequence_id for t in storage.get_ticks("run_a")] == [1001, 1002]

        # (c) Exact duplicate increments duplicate_count and preserves original row
        t_seq1_dup = _make_tick(
            exchange="coinbase",
            price=60999.0,
            ts_ms=base_ts,
            received_ms=base_ts + 99,
            sequence_id=1001,
        )
        inserted_dup, dups_dup = storage.insert_ticks("run_a", [t_seq1_dup, t_seq2])
        assert (inserted_dup, dups_dup) == (0, 2)
        run_a_ticks = storage.get_ticks("run_a")
        assert len(run_a_ticks) == 2
        assert run_a_ticks[0].price == 60700.0

        # (b) Second run inserting identical ticks must NOT steal rows from run_a
        inserted_b, dups_b = storage.insert_ticks("run_b", [t_seq1, t_seq2])
        assert (inserted_b, dups_b) == (2, 0)
        assert len(storage.get_ticks("run_a")) == 2
        assert len(storage.get_ticks("run_b")) == 2


def test_storage_non_skewed_percentiles_and_v1_schema_migration(tmp_path: Path) -> None:
    db = tmp_path / "legacy_v1.db"
    base_ts = 1_795_395_600_000

    # Create legacy v1 schema without dedup_seq or run_id in UNIQUE
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE capture_runs (
            run_id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            started_ms INTEGER NOT NULL,
            ended_ms INTEGER,
            started_utc TEXT NOT NULL,
            ended_utc TEXT,
            exchanges TEXT NOT NULL,
            notes TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            raw_symbol TEXT NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            price REAL NOT NULL,
            bid REAL,
            ask REAL,
            size REAL,
            volume_24h REAL,
            sequence_id INTEGER,
            ts_ms INTEGER NOT NULL,
            ts_utc TEXT NOT NULL,
            received_ms INTEGER NOT NULL,
            latency_ms REAL NOT NULL,
            source TEXT NOT NULL,
            UNIQUE(exchange, symbol, ts_ms, source)
        );
        """
    )
    conn.execute(
        "INSERT INTO capture_runs VALUES ('r_old', 'demo', ?, ?, '2026-01-01T00:00:00Z', "
        "'2026-01-01T00:00:10Z', 'coinbase', '')",
        (base_ts, base_ts + 10000),
    )
    conn.commit()
    conn.close()

    with MarketStorage(db) as storage:
        ticks = [
            _make_tick(
                exchange="coinbase",
                price=60700.0,
                ts_ms=base_ts,
                received_ms=base_ts - 15,
                sequence_id=1,
            ),
            _make_tick(
                exchange="coinbase",
                price=60701.0,
                ts_ms=base_ts,
                received_ms=base_ts - 5,
                sequence_id=2,
            ),
            _make_tick(
                exchange="coinbase",
                price=60702.0,
                ts_ms=base_ts + 1000,
                received_ms=base_ts + 1010,
                sequence_id=3,
            ),
            _make_tick(
                exchange="coinbase",
                price=60703.0,
                ts_ms=base_ts + 2000,
                received_ms=base_ts + 2030,
                sequence_id=4,
            ),
        ]
        ins, dup = storage.insert_ticks("r_old", ticks)
        assert (ins, dup) == (4, 0)

        lat = storage.get_latency_percentiles("r_old")["coinbase"]
        assert lat["raw_count"] == 4.0
        assert lat["non_skewed_count"] == 2.0
        assert lat["skewed_count"] == 2.0
        assert lat["p50"] == 20.0
        assert lat["max"] == 30.0
        assert lat["raw_min"] == -15.0


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
