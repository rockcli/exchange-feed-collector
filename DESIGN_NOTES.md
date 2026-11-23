# Design Notes & Architecture Walkthrough

**Reference implementation · live public exchange sample + deterministic offline replay**

## 1. Why SQLite WAL Instead of DuckDB or TimescaleDB?

For a single-node market-data collector monitoring public L1 pairs on one host (observed ~34 ticks/sec across 3 pairs in the live sample; high-throughput sustained ceiling is not benchmarked in this repo), operational simplicity and crash resilience matter more than distributed query parallelism:

- **Zero external daemon**: `sqlite3` ships in Python's stdlib (`Dockerfile` image remains minimal with only `aiohttp`, `matplotlib`, and `pillow`).
- **Concurrent reads during writes**: With `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`, report generation (`exchange-collector report`) can read consistent snapshots while the async collector appends batches without blocking.
- **Idempotency & multi-run isolation as a schema invariant**: `UNIQUE(run_id, exchange, symbol, ts_ms, source, dedup_seq)` paired with `INSERT OR IGNORE` prevents same-millisecond fills from colliding, stops separate `run_id` captures from overwriting each other's rows, and makes REST backfill overlap safe with exact `(inserted, duplicate)` accounting via SQLite `total_changes`.

**When to upgrade**: Move to TimescaleDB/ClickHouse when multi-host writers must share one store over the network, sustained L2/L3 order-book firehoses exceed single-node SQLite write capacity, or retention grows to multi-terabyte analytical queries.

## 2. Reconnect, Circuit Breaker & REST Fallback State Machine

```text
   [WS Connected] --(recv timeout / stale silence / socket error)--> [Increment failure_count + Backoff + REST Poll]
         ^                                                                            |
         |                                                            failure_count >= threshold
   (valid parsed & persisted ticks in HALF_OPEN)                                      v
   [HALF_OPEN Probe] <---(recovery_timeout elapsed)------------------ [Circuit OPEN: REST Poll Fallback Only]
```

Key design choices:

- **Health verified by persisted ticks, not handshake**: `CircuitBreaker.record_success()` and backoff reset run only after at least one valid parsed market tick has been received and flushed to SQLite, preventing premature `HALF_OPEN -> CLOSED` transitions when a socket accepts subscribe frames and immediately hangs.
- **Active stale-silence preemption**: `StaleFeedMonitor.check_stale()` feeds an async wake-up event that tears down silent WebSockets and immediately triggers REST fallback + reconnect instead of merely logging alerts.
- **Strict backfill verification**: `_schedule_backfill()` spawns a tracked background task, filters REST trades strictly to `[gap_start_ms, gap_end_ms]`, and marks a gap `recovered` only when `>= 1` genuinely new row is inserted (otherwise `no_overlap` or `unrecovered`).
- **Monotonic watermarks & non-mixed sequences**: Out-of-order `rest_backfill` rows bypass advancing `_last_ts_ms` / `_last_seq`, and Coinbase `trade_id` is kept separate from global book `sequence` in `GapDetector`.
- **Application heartbeats count as liveness**: Kraken v2 sends `{"channel": "heartbeat"}` and OKX replies `"pong"`; both refresh `StaleFeedMonitor` without injecting synthetic prices.

## 3. Spread Bucket Alignment & Explicit Quote Separation

- **Strict bucket overlap vs forward-fill**: `compute_spread_buckets()` assigns ticks to fixed UTC buckets (`bucket_ms = 1000` by default) and computes spreads only where both venues updated inside the bucket (`44/45` seconds in the 45s live sample). Forward-filling across idle buckets would fabricate tight spreads during an undetected feed pause.
- **Separating `USD` vs `USDT`**: In the `2026-11-23T02:47Z` live sample, `BTC-USD` across Coinbase and Kraken averaged `+$4.56` (`0.83 bps` abs / `+0.75 bps` signed), whereas `OKX BTC-USDT` traded `+$95.99` (`15.88 bps`) above the `USD` midpoint. Keeping `quote_currency` explicit prevents false arbitrage alerts driven by stablecoin peg basis.

## 4. 5-Minute Walkthrough Talking Points

1. **Problem & boundary**: Normalize heterogeneous exchange feeds (`BTC-USD`, `XBT/USD`, `BTC-USDT`) into one idempotent SQLite schema without losing venue quote semantics or collapsing same-millisecond updates.
2. **Resilience proof**: Demonstrate `tests/test_resilience.py` + `tests/test_cli_e2e.py` (connect/read-timeout breaker tripping, `HALF_OPEN` requiring valid persisted ticks, stale silence triggering REST fallback, strict backfill window/new-row check, and deterministic gap backfill in `make demo`).
3. **Real-world telemetry**: Point to `live_20261123T024759Z` (`1,543` deduplicated ticks from `1,614` raw frames across 45s, `0` gaps, `p95` non-skewed latencies `159.0–241.0 ms`).
4. **Honest scaling path**: Swap `MarketStorage` behind a repository interface for Postgres/TimescaleDB batch `COPY`, and add L2 delta checksum validation when moving from L1 spreads to order-book depth.

Built with AI-assisted tooling; architecture, review, and tests by me.
