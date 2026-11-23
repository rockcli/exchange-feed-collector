"""Cross-exchange spread, quote-basis, latency heatmap, and anomaly analytics."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from exchange_collector.models import (
    AlertRecord,
    CaptureRun,
    GapRecord,
    NormalizedTick,
    epoch_ms_to_utc_iso,
)
from exchange_collector.storage import MarketStorage, compute_percentile


@dataclass(frozen=True, slots=True)
class SpreadBucket:
    """Aligned time bucket comparing USD venues and explicit USDT cross-quote basis."""

    bucket_ts_ms: int
    bucket_utc: str
    coinbase_mid: float | None
    kraken_mid: float | None
    okx_usdt_mid: float | None
    usd_spread_delta: float | None
    usd_spread_bps: float | None
    usdt_basis_delta: float | None
    usdt_basis_bps: float | None


@dataclass(frozen=True, slots=True)
class HeatmapSlice:
    """Aggregated slice metrics for p95 latency and spread intensity visualization."""

    label: str
    start_ts_ms: int
    end_ts_ms: int
    p95_latency_by_exchange: dict[str, float | None]
    mean_usd_spread_bps: float | None
    mean_usdt_basis_bps: float | None


@dataclass(frozen=True, slots=True)
class SpreadSummary:
    """Summary statistics for a spread or cross-quote basis series."""

    overlapping_buckets: int
    mean_delta: float
    mean_abs_bps: float
    p95_abs_bps: float
    max_abs_bps: float
    min_delta: float
    max_delta: float


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    """Complete analytical snapshot for a captured or replayed run."""

    run: CaptureRun
    total_ticks: int
    exchange_counts: dict[str, dict[str, int]]
    latency_by_exchange: dict[str, dict[str, float]]
    spread_buckets: list[SpreadBucket]
    usd_spread_summary: SpreadSummary | None
    usdt_basis_summary: SpreadSummary | None
    heatmap_slices: list[HeatmapSlice]
    gaps: list[GapRecord]
    alerts: list[AlertRecord]


def compute_spread_buckets(
    ticks: list[NormalizedTick],
    bucket_ms: int = 1000,
) -> list[SpreadBucket]:
    """Align ticks into fixed UTC buckets and compute same-quote vs cross-quote spreads.

    - Same-quote USD spread compares `coinbase` (`BTC-USD`) and `kraken` (`BTC-USD`).
    - Cross-quote basis compares `okx` (`BTC-USDT`) against the available USD reference
      midpoint (`mean(coinbase, kraken)` or whichever USD venue appeared in the bucket),
      keeping `USD` vs `USDT` strictly separated.
    """
    if not ticks or bucket_ms <= 0:
        return []

    by_bucket: dict[int, dict[str, NormalizedTick]] = defaultdict(dict)
    for tick in ticks:
        bucket_ts = (tick.ts_ms // bucket_ms) * bucket_ms
        prev = by_bucket[bucket_ts].get(tick.exchange)
        if prev is None or tick.ts_ms >= prev.ts_ms:
            by_bucket[bucket_ts][tick.exchange] = tick

    buckets: list[SpreadBucket] = []
    for bucket_ts in sorted(by_bucket):
        venues = by_bucket[bucket_ts]
        cb = venues.get("coinbase")
        kr = venues.get("kraken")
        okx = venues.get("okx")

        cb_mid = cb.mid_price if cb is not None else None
        kr_mid = kr.mid_price if kr is not None else None
        okx_mid = okx.mid_price if okx is not None else None

        usd_delta: float | None = None
        usd_bps: float | None = None
        if cb_mid is not None and kr_mid is not None:
            usd_delta = kr_mid - cb_mid
            ref = (cb_mid + kr_mid) / 2.0
            if ref > 0:
                usd_bps = (usd_delta / ref) * 10_000.0

        usd_refs = [v for v in (cb_mid, kr_mid) if v is not None]
        usdt_delta: float | None = None
        usdt_bps: float | None = None
        if okx_mid is not None and usd_refs:
            usd_ref = sum(usd_refs) / len(usd_refs)
            usdt_delta = okx_mid - usd_ref
            if usd_ref > 0:
                usdt_bps = (usdt_delta / usd_ref) * 10_000.0

        buckets.append(
            SpreadBucket(
                bucket_ts_ms=bucket_ts,
                bucket_utc=epoch_ms_to_utc_iso(bucket_ts),
                coinbase_mid=cb_mid,
                kraken_mid=kr_mid,
                okx_usdt_mid=okx_mid,
                usd_spread_delta=usd_delta,
                usd_spread_bps=usd_bps,
                usdt_basis_delta=usdt_delta,
                usdt_basis_bps=usdt_bps,
            )
        )
    return buckets


def summarize_spreads(
    buckets: list[SpreadBucket],
    *,
    kind: str,
) -> SpreadSummary | None:
    """Compute descriptive summary metrics for `usd` spread or `usdt_basis`."""
    deltas: list[float] = []
    abs_bps_vals: list[float] = []
    for b in buckets:
        if kind == "usd":
            delta, bps = b.usd_spread_delta, b.usd_spread_bps
        elif kind == "usdt_basis":
            delta, bps = b.usdt_basis_delta, b.usdt_basis_bps
        else:
            raise ValueError(f"Unknown spread kind {kind!r}")
        if delta is not None and bps is not None:
            deltas.append(delta)
            abs_bps_vals.append(abs(bps))

    if not deltas:
        return None
    sorted_bps = sorted(abs_bps_vals)
    return SpreadSummary(
        overlapping_buckets=len(deltas),
        mean_delta=round(sum(deltas) / len(deltas), 2),
        mean_abs_bps=round(sum(abs_bps_vals) / len(abs_bps_vals), 2),
        p95_abs_bps=round(compute_percentile(sorted_bps, 95.0), 2),
        max_abs_bps=round(sorted_bps[-1], 2),
        min_delta=round(min(deltas), 2),
        max_delta=round(max(deltas), 2),
    )


def evaluate_spread_anomalies(
    run_id: str,
    buckets: list[SpreadBucket],
    *,
    usd_spread_alert_bps: float = 15.0,
    usdt_basis_alert_bps: float = 50.0,
) -> list[AlertRecord]:
    """Emit structured anomaly alerts when spread or basis exceeds configured thresholds."""
    alerts: list[AlertRecord] = []
    for bucket in buckets:
        if (
            bucket.usd_spread_bps is not None
            and abs(bucket.usd_spread_bps) > usd_spread_alert_bps
        ):
            alerts.append(
                AlertRecord(
                    run_id=run_id,
                    exchange="coinbase/kraken",
                    symbol="BTC-USD",
                    severity="warning",
                    alert_type="spread_anomaly",
                    message=(
                        f"Same-quote USD spread reached {bucket.usd_spread_bps:+.2f} bps "
                        f"(${bucket.usd_spread_delta:+.2f})"
                    ),
                    metric_value=round(abs(bucket.usd_spread_bps), 2),
                    threshold=usd_spread_alert_bps,
                    ts_ms=bucket.bucket_ts_ms,
                    ts_utc=bucket.bucket_utc,
                )
            )
        if (
            bucket.usdt_basis_bps is not None
            and abs(bucket.usdt_basis_bps) > usdt_basis_alert_bps
        ):
            alerts.append(
                AlertRecord(
                    run_id=run_id,
                    exchange="okx/usd-ref",
                    symbol="BTC-USDT vs BTC-USD",
                    severity="warning",
                    alert_type="basis_anomaly",
                    message=(
                        f"Cross-quote USDT/USD basis reached {bucket.usdt_basis_bps:+.2f} bps "
                        f"(${bucket.usdt_basis_delta:+.2f})"
                    ),
                    metric_value=round(abs(bucket.usdt_basis_bps), 2),
                    threshold=usdt_basis_alert_bps,
                    ts_ms=bucket.bucket_ts_ms,
                    ts_utc=bucket.bucket_utc,
                )
            )
    return alerts


def build_heatmap_slices(
    ticks: list[NormalizedTick],
    buckets: list[SpreadBucket],
    exchanges: list[str],
    num_slices: int = 10,
) -> list[HeatmapSlice]:
    """Aggregate ticks and spreads into uniform chronological slices (hours on 72h runs)."""
    if not ticks:
        return []
    min_ts = min(t.ts_ms for t in ticks)
    max_ts = max(t.ts_ms for t in ticks)
    span_ms = max(1, max_ts - min_ts)
    effective_slices = max(1, min(num_slices, len(ticks)))
    step_ms = max(1, (span_ms + effective_slices - 1) // effective_slices)

    slice_latencies: list[dict[str, list[float]]] = [
        defaultdict(list) for _ in range(effective_slices)
    ]
    for tick in ticks:
        if tick.source == "rest_backfill":
            continue
        idx = min(effective_slices - 1, max(0, (tick.ts_ms - min_ts) // step_ms))
        slice_latencies[idx][tick.exchange].append(max(0.0, tick.latency_ms))

    slice_usd_bps: list[list[float]] = [[] for _ in range(effective_slices)]
    slice_usdt_bps: list[list[float]] = [[] for _ in range(effective_slices)]
    for b in buckets:
        idx = min(effective_slices - 1, max(0, (b.bucket_ts_ms - min_ts) // step_ms))
        if b.usd_spread_bps is not None:
            slice_usd_bps[idx].append(abs(b.usd_spread_bps))
        if b.usdt_basis_bps is not None:
            slice_usdt_bps[idx].append(abs(b.usdt_basis_bps))

    slices: list[HeatmapSlice] = []
    for i in range(effective_slices):
        start_ms = min_ts + i * step_ms
        end_ms = min(max_ts, start_ms + step_ms)
        label = epoch_ms_to_utc_iso(start_ms)[11:19]
        p95_map: dict[str, float | None] = {}
        for ex in exchanges:
            vals = sorted(slice_latencies[i].get(ex, []))
            p95_map[ex] = round(compute_percentile(vals, 95.0), 1) if vals else None
        usd_vals = slice_usd_bps[i]
        usdt_vals = slice_usdt_bps[i]
        slices.append(
            HeatmapSlice(
                label=label,
                start_ts_ms=start_ms,
                end_ts_ms=end_ms,
                p95_latency_by_exchange=p95_map,
                mean_usd_spread_bps=(
                    round(sum(usd_vals) / len(usd_vals), 2) if usd_vals else None
                ),
                mean_usdt_basis_bps=(
                    round(sum(usdt_vals) / len(usdt_vals), 2) if usdt_vals else None
                ),
            )
        )
    return slices


def analyze_run(
    storage: MarketStorage,
    run_id: str,
    *,
    bucket_ms: int = 1000,
    usd_spread_alert_bps: float = 15.0,
    usdt_basis_alert_bps: float = 50.0,
    persist_spread_alerts: bool = False,
) -> AnalysisResult:
    """Compute spreads, latency percentiles, heatmap slices, and anomaly alerts for `run_id`."""
    run = storage.get_run(run_id)
    if run is None:
        raise ValueError(f"Capture run {run_id!r} not found in {storage.db_path}")

    ticks = storage.get_ticks(run_id)
    gaps = storage.get_gaps(run_id)
    stored_alerts = storage.get_alerts(run_id)
    exchange_counts = storage.get_exchange_counts(run_id)
    latency_by_exchange = storage.get_latency_percentiles(run_id)

    buckets = compute_spread_buckets(ticks, bucket_ms=bucket_ms)
    usd_summary = summarize_spreads(buckets, kind="usd")
    usdt_summary = summarize_spreads(buckets, kind="usdt_basis")

    spread_alerts = evaluate_spread_anomalies(
        run_id,
        buckets,
        usd_spread_alert_bps=usd_spread_alert_bps,
        usdt_basis_alert_bps=usdt_basis_alert_bps,
    )
    if persist_spread_alerts and spread_alerts:
        existing_keys = {(a.alert_type, a.ts_ms, a.exchange) for a in stored_alerts}
        new_alerts = [
            a for a in spread_alerts if (a.alert_type, a.ts_ms, a.exchange) not in existing_keys
        ]
        if new_alerts:
            storage.insert_alerts(new_alerts)
            stored_alerts.extend(new_alerts)
    else:
        existing_keys = {(a.alert_type, a.ts_ms, a.exchange) for a in stored_alerts}
        for a in spread_alerts:
            if (a.alert_type, a.ts_ms, a.exchange) not in existing_keys:
                stored_alerts.append(a)

    stored_alerts.sort(key=lambda a: a.ts_ms)
    exchanges = [e.strip() for e in run.exchanges.split(",") if e.strip()] or [
        "coinbase",
        "kraken",
        "okx",
    ]
    heatmap_slices = build_heatmap_slices(ticks, buckets, exchanges=exchanges, num_slices=10)

    return AnalysisResult(
        run=run,
        total_ticks=len(ticks),
        exchange_counts=exchange_counts,
        latency_by_exchange=latency_by_exchange,
        spread_buckets=buckets,
        usd_spread_summary=usd_summary,
        usdt_basis_summary=usdt_summary,
        heatmap_slices=heatmap_slices,
        gaps=gaps,
        alerts=stored_alerts,
    )
