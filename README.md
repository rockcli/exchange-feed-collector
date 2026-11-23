# Exchange Feed Collector

Collects real-time public market ticks from Coinbase, Kraken, and OKX into an idempotent SQLite WAL store, auto-recovers stream drops via REST backfill, and computes cross-exchange spreads without conflating `USD` and `USDT` quotes.

**Reference implementation · live public exchange sample + deterministic offline replay**

![Live Capture & Spread Overview](assets/demo_overview.png)

## Quick start

```bash
uv sync && make demo      # deterministic 60s offline replay -> SQLite WAL + HTML/PNG report
make live-sample          # capture 50s of live Coinbase, Kraken, and OKX public feeds
make test                 # run unit, concurrency, and end-to-end tests
```

## Reproducible results

Both runs below were executed locally against the repository code after final edits (`make soak` is included for future 72-hour runs; no 72-hour soak results are claimed here):

| Metric | Deterministic Replay (`make demo`) | Live Capture (`make live-sample`) |
| --- | --- | --- |
| Run ID | `demo_fixture_v1` | `live_20261123T022205Z` |
| UTC Window | `2026-11-23T01:00:00.000Z` → `01:00:59.000Z` | `2026-11-23T02:22:05.492Z` → `02:22:55.495Z` |
| Total Stored Ticks | `177` (deduplicated from `179` raw inputs) | `966` (`0` duplicates) |
| Venue Breakdown | Coinbase `60` ws · Kraken `54` ws + `3` backfill · OKX `60` ws | Coinbase `259` ws · Kraken `405` ws · OKX `302` ws |
| Live Ingest Latency (`p50` / `p95` / `max`) | CB `48.5`/`69.05`/`72.0` · KR `89.0`/`131.7`/`136.0` · OKX `103.5`/`135.1`/`139.0` ms | CB `0.0`/`15.1`/`722.0` · KR `42.0`/`133.0`/`568.0` · OKX `100.0`/`125.0`/`198.0` ms |
| Clock-Skew Rows Flagged | `2` (Coinbase ahead of local wall clock) | `100` (Coinbase ahead of local wall clock) |
| Same-Quote USD Spread (`BTC-USD`) | Mean `+$7.93` · `1.31 bps` mean · `18.84 bps` max (`57` buckets) | Mean `+$3.38` · `0.72 bps` mean · `1.21 bps` max (`44` buckets) |
| Cross-Quote Basis (`BTC-USDT` vs `BTC-USD`) | Mean `+$89.27` · `14.70 bps` mean (`60` buckets) | Mean `+$99.37` · `16.39 bps` mean · `17.53 bps` max (`49` buckets) |
| Gaps & Alerts | `1` gap (`7,000 ms`, recovered via REST) · `4` alerts | `0` gaps · `0` alerts |

## What breaks in production & how this handles it

1. **WebSocket silent half-open drops**: TCP sockets can hang without FIN/RST frames. `FeedCollector._consume_ws` ([collector.py](src/exchange_collector/collector.py)) bounds every read with `ws_receive_timeout_seconds`, `StaleFeedMonitor.check_stale` ([resilience.py](src/exchange_collector/resilience.py)) checks silence independently, and `CircuitBreaker` + `BackoffPolicy.delay_for_attempt` shift the venue to `FeedCollector._poll_rest_fallback` with capped jittered retries (`base * 2^attempt + jitter`) until WS recovers.
2. **Sequence and timestamp gaps**: Packet loss or reconnect windows leave missing price history. `GapDetector.observe` ([resilience.py](src/exchange_collector/resilience.py)) detects both `timestamp_gap` (`> gap_threshold_ms`) and `sequence_gap` without letting out-of-order rows regress watermarks, triggering `FeedCollector._schedule_backfill` → `ExchangeAdapter.fetch_rest_backfill` ([adapters.py](src/exchange_collector/adapters.py)) and recording recovery status in `gaps` via `MarketStorage.insert_gap` ([storage.py](src/exchange_collector/storage.py)).
3. **Cross-exchange quote currency mismatch (`USD` vs `USDT`)**: Treating `BTC-USDT` as `1:1` with `BTC-USD` creates phantom ~$90–$100 arbitrage signals. `normalize_symbol` ([adapters.py](src/exchange_collector/adapters.py)) maps `XBT/USD` → `BTC-USD` (`USD`) while preserving `BTC-USDT` (`USDT`), and `compute_spread_buckets` / `summarize_spreads` ([analytics.py](src/exchange_collector/analytics.py)) keep same-quote `usd_spread_*` strictly separate from `usdt_basis_*`.
4. **SQLite lock contention under burst ticks**: Concurrent async streams hitting SQLite per message cause `database is locked` errors. `MarketStorage._configure_pragmas` ([storage.py](src/exchange_collector/storage.py)) enables `WAL` mode, `synchronous=NORMAL`, and `busy_timeout=5000`, serializes writes under an `RLock` with idempotent `ON CONFLICT(exchange, symbol, ts_ms, source) DO UPDATE`, and `FeedCollector._flush_pending_ticks` batches ticks before committing.
5. **Clock skew & stale-feed false positives**: Comparing exchange event timestamps against local wall clock triggers bogus stale alerts when NTP drifts or an upstream server runs slightly ahead (observed on `100` live Coinbase ticks). `StaleFeedMonitor.record_activity` ([resilience.py](src/exchange_collector/resilience.py)) tracks feed liveness exclusively via `time.monotonic()`, flags `SkewObservation` separately, and `MarketStorage.get_latency_percentiles` excludes historical `rest_backfill` rows while clamping negative skewed deltas so `p50`/`p95` stay meaningful.

## Limitations

- Collects **public L1 ticker and recent trade feeds only** — does not reconstruct L2/L3 order-book depth, private fills, or execution routing.
- **Single-node SQLite WAL persistence** targets single-host monitoring and historical spread analysis (hundreds to low-thousands of ticks/sec), not distributed multi-region HFT matching.
- **REST backfill granularity** is bounded by each exchange's public recent-trades endpoint (latest ~100 trades without historical bid/ask quotes during outage windows).

## Project layout

- [src/exchange_collector/adapters.py](src/exchange_collector/adapters.py) — Coinbase, Kraken, and OKX public WS + REST adapters & quote normalization
- [src/exchange_collector/resilience.py](src/exchange_collector/resilience.py) — Backoff, circuit breaker, monotonic stale-feed monitor, gap detector, REST rate limiter
- [src/exchange_collector/storage.py](src/exchange_collector/storage.py) — SQLite WAL schema, idempotent upsert/dedup, latency percentiles
- [src/exchange_collector/analytics.py](src/exchange_collector/analytics.py) — Aligned spread & basis buckets, anomaly alerts, heatmap slice aggregation
- [src/exchange_collector/report.py](src/exchange_collector/report.py) — Self-contained HTML report and PNG chart rendering
- [src/exchange_collector/cli.py](src/exchange_collector/cli.py) — `demo`, `live-sample`, `soak`, and `report` subcommands
- [DESIGN_NOTES.md](DESIGN_NOTES.md) — Architectural tradeoffs and engineering walkthrough

Built with AI-assisted tooling; architecture, review, and tests by me.
