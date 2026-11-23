# Design Notes & Architecture Walkthrough

## 1. Why SQLite WAL Instead of DuckDB or TimescaleDB?

For a single-node market-data collector ingesting 10–500 ticks/sec across 3–10 pairs, operational simplicity and crash resilience matter more than distributed query parallelism:

- **Zero external daemon**: `sqlite3` ships in Python's stdlib (`Dockerfile` image remains minimal with only `aiohttp`, `matplotlib`, and `pillow`).
- **Concurrent reads during writes**: With `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`, report generation (`exchange-collector report`) can read consistent snapshots while the async collector appends batches without blocking.
- **Idempotency as a schema invariant**: `UNIQUE(exchange, symbol, ts_ms, source)` paired with `ON CONFLICT DO UPDATE` makes REST backfill overlap safe—re-fetching the trailing 100 trades after a reconnect never produces duplicate rows.

**When to upgrade**: Move to TimescaleDB/ClickHouse when ingest exceeds ~5,000–10,000 ticks/sec sustained, multiple collector nodes must share one store over the network, or L2/L3 order-book snapshots require multi-terabyte retention.

## 2. Reconnect, Circuit Breaker & REST Fallback State Machine

```text
   [WS Connected] --(recv timeout / socket error)--> [Increment failure_count + Backoff + REST Poll]
         ^                                                           |
         |                                           failure_count >= threshold
   (2x success in HALF_OPEN)                                         v
   [HALF_OPEN Probe] <---(recovery_timeout elapsed)--- [Circuit OPEN: REST Poll Fallback Only]
```

Key design choices:

- **Decoupled backfill tasks**: When `GapDetector.observe()` detects a timestamp or sequence jump, `_schedule_backfill()` spawns a tracked background task so the WebSocket consumer loop never stalls waiting on HTTP REST I/O.
- **Monotonic watermarks**: Out-of-order `rest_backfill` rows bypass advancing `_last_ts_ms` / `_last_seq`, preventing historical recovery ticks from triggering spurious reverse gaps.
- **Application heartbeats count as liveness**: Kraken v2 sends `{"channel": "heartbeat"}` and OKX replies `"pong"`; both refresh `StaleFeedMonitor` without injecting synthetic prices.

## 3. Spread Bucket Alignment & Explicit Quote Separation

- **Strict bucket overlap vs forward-fill**: `compute_spread_buckets()` assigns ticks to fixed UTC buckets (`bucket_ms = 1000` by default) and computes spreads only where both venues updated inside the bucket (`44/50` buckets in the 50s live sample). Forward-filling across idle buckets would fabricate tight spreads during an undetected feed pause.
- **Separating `USD` vs `USDT`**: In the `2026-11-23T02:22Z` live sample, `BTC-USD` across Coinbase and Kraken averaged `$3.38` (`0.72 bps`), whereas `OKX BTC-USDT` traded `$99.37` (`16.39 bps`) above the `USD` midpoint. Keeping `quote_currency` explicit prevents false arbitrage alerts driven by stablecoin peg basis.

## 4. 5-Minute Walkthrough Talking Points

1. **Problem & boundary**: Normalize heterogeneous exchange feeds (`BTC-USD`, `XBT/USD`, `BTC-USDT`) into one idempotent SQLite schema without losing venue quote semantics.
2. **Resilience proof**: Demonstrate `tests/test_resilience.py` (backoff, circuit breaker, monotonic skew immunity) + `tests/test_cli_e2e.py` (WS failure falling back to REST polling + deterministic gap backfill in `make demo`).
3. **Real-world telemetry**: Point to `live_20261123T022205Z` (`966` real ticks across 50s, `100` clock-skewed Coinbase frames handled cleanly without false stale alarms, `0` gaps, `p95` latencies `15.1–133.0 ms`).
4. **Honest scaling path**: Swap `MarketStorage` behind a repository interface for Postgres/TimescaleDB batch `COPY`, and add L2 delta checksum validation when moving from L1 spreads to order-book depth.
