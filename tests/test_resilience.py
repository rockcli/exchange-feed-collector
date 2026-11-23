"""Unit tests for exponential backoff, circuit breaker, stale detector, and gap detector."""

from __future__ import annotations

import random

from exchange_collector.models import NormalizedTick, epoch_ms_to_utc_iso
from exchange_collector.resilience import (
    BackoffPolicy,
    CircuitBreaker,
    CircuitState,
    GapDetector,
    StaleFeedMonitor,
)


def _tick(
    *,
    exchange: str = "coinbase",
    symbol: str = "BTC-USD",
    ts_ms: int,
    received_ms: int | None = None,
    seq: int | None = None,
    source: str = "ws",
) -> NormalizedTick:
    recv = ts_ms + 25 if received_ms is None else received_ms
    return NormalizedTick(
        exchange=exchange,
        symbol=symbol,
        raw_symbol=symbol,
        base_currency="BTC",
        quote_currency="USD",
        price=60700.0,
        bid=60699.9,
        ask=60700.1,
        size=0.01,
        volume_24h=1000.0,
        sequence_id=seq,
        ts_ms=ts_ms,
        ts_utc=epoch_ms_to_utc_iso(ts_ms),
        received_ms=recv,
        latency_ms=float(recv - ts_ms),
        source=source,
    )


def test_backoff_policy_exponential_and_capped() -> None:
    policy = BackoffPolicy(base_seconds=1.0, max_seconds=10.0, jitter_ratio=0.25)
    rng = random.Random(42)

    d0 = policy.delay_for_attempt(0, rng=rng)
    d1 = policy.delay_for_attempt(1, rng=rng)
    d2 = policy.delay_for_attempt(2, rng=rng)
    d6 = policy.delay_for_attempt(6, rng=rng)

    assert 1.0 <= d0 <= 1.25
    assert 2.0 <= d1 <= 2.50
    assert 4.0 <= d2 <= 5.00
    assert d6 == 10.0


def test_circuit_breaker_lifecycle() -> None:
    cb = CircuitBreaker(
        name="kraken",
        failure_threshold=2,
        recovery_timeout_seconds=10.0,
        half_open_success_threshold=2,
    )
    assert cb.current_state(0.0) == CircuitState.CLOSED
    assert cb.allow_request(0.0)

    assert cb.record_failure(1.0) == CircuitState.CLOSED
    assert cb.record_failure(2.0) == CircuitState.OPEN
    assert not cb.allow_request(5.0)

    # Cooldown elapses -> HALF_OPEN
    assert cb.current_state(12.5) == CircuitState.HALF_OPEN
    assert cb.allow_request(12.5)

    # One failure during HALF_OPEN trips back to OPEN
    assert cb.record_failure(13.0) == CircuitState.OPEN
    assert cb.current_state(24.0) == CircuitState.HALF_OPEN

    # Two consecutive successes close the circuit
    assert cb.record_success() == CircuitState.HALF_OPEN
    assert cb.record_success() == CircuitState.CLOSED


def test_stale_feed_monitor_monotonic_and_clock_skew() -> None:
    monitor = StaleFeedMonitor(stale_seconds=5.0, skew_tolerance_ms=100)
    monitor.register_stream("coinbase", "BTC-USD", now_monotonic=100.0)

    # Exchange clock ahead by 500ms triggers skew notice but does NOT trip stale feed
    skew = monitor.record_activity(
        "coinbase",
        "BTC-USD",
        event_ts_ms=1_700_000_000_500,
        received_ms=1_700_000_000_000,
        now_monotonic=102.0,
    )
    assert skew.is_skewed
    assert skew.skew_ms == 500
    assert monitor.check_stale("coinbase", "BTC-USD", now_monotonic=106.0) is None

    # Crossing monotonic threshold emits stale duration once until activity resumes
    stale_elapsed = monitor.check_stale("coinbase", "BTC-USD", now_monotonic=107.5)
    assert stale_elapsed is not None and stale_elapsed >= 5.0
    assert monitor.check_stale("coinbase", "BTC-USD", now_monotonic=108.5) is None

    monitor.record_activity(
        "coinbase",
        "BTC-USD",
        event_ts_ms=1_700_000_009_000,
        received_ms=1_700_000_009_010,
        now_monotonic=109.0,
    )
    assert monitor.check_stale("coinbase", "BTC-USD", now_monotonic=110.0) is None


def test_gap_detector_timestamp_sequence_and_out_of_order_watermarks() -> None:
    detector = GapDetector(gap_threshold_ms=5000, sequence_jump_threshold=10)
    base = 1_795_395_600_000

    assert detector.observe(_tick(ts_ms=base, seq=100)) is None

    # Timestamp gap (> 5000ms)
    ts_gap = detector.observe(_tick(ts_ms=base + 7000, seq=102))
    assert ts_gap is not None
    assert ts_gap.gap_type == "timestamp_gap"
    assert ts_gap.duration_ms == 7000

    # Out-of-order backfill tick inside the gap must not regress watermarks
    assert detector.observe(_tick(ts_ms=base + 3000, seq=101, source="rest_backfill")) is None
    assert detector.observe(_tick(ts_ms=base + 8000, seq=103)) is None

    # Sequence jump (> 10) within normal time delta
    seq_gap = detector.observe(_tick(ts_ms=base + 8500, seq=130))
    assert seq_gap is not None
    assert seq_gap.gap_type == "sequence_gap"
    assert seq_gap.prev_seq == 103
    assert seq_gap.curr_seq == 130
