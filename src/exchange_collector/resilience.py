"""Resilience primitives: exponential backoff, circuit breaker, stale detector, gap detection."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import StrEnum

from exchange_collector.models import NormalizedTick


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Capped exponential backoff configuration with bounded positive jitter."""

    base_seconds: float = 0.5
    max_seconds: float = 30.0
    jitter_ratio: float = 0.25

    def delay_for_attempt(self, attempt: int, rng: random.Random | None = None) -> float:
        """Compute `min(max, base * 2^attempt + jitter)` for zero-indexed retry `attempt`."""
        generator = rng if rng is not None else random.Random()
        exp = min(self.max_seconds, self.base_seconds * (2 ** max(0, attempt)))
        jitter = generator.uniform(0.0, exp * self.jitter_ratio)
        return float(min(self.max_seconds, exp + jitter))


class CircuitState(StrEnum):
    """Lifecycle states of a per-exchange connection or REST fallback circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(slots=True)
class CircuitBreaker:
    """Failure-threshold circuit breaker protecting upstream exchange connections."""

    name: str
    failure_threshold: int = 4
    recovery_timeout_seconds: float = 15.0
    half_open_success_threshold: int = 2
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _half_open_successes: int = field(default=0, init=False)
    _opened_at_monotonic: float = field(default=0.0, init=False)

    def current_state(self, now_monotonic: float | None = None) -> CircuitState:
        """Return active circuit state, promoting OPEN -> HALF_OPEN once cooldown elapses."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        if (
            self._state == CircuitState.OPEN
            and (now - self._opened_at_monotonic) >= self.recovery_timeout_seconds
        ):
            self._state = CircuitState.HALF_OPEN
            self._half_open_successes = 0
        return self._state

    @property
    def failure_count(self) -> int:
        """Return consecutive failure count in the current window."""
        return self._failure_count

    def allow_request(self, now_monotonic: float | None = None) -> bool:
        """Return True if traffic may proceed through the breaker."""
        return self.current_state(now_monotonic) != CircuitState.OPEN

    def record_success(self) -> CircuitState:
        """Reset failure tracking on healthy completion; close half-open circuit after quorum."""
        if self._state == CircuitState.HALF_OPEN:
            self._half_open_successes += 1
            if self._half_open_successes >= self.half_open_success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._half_open_successes = 0
        else:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
        return self._state

    def record_failure(self, now_monotonic: float | None = None) -> CircuitState:
        """Increment failure count and trip circuit to OPEN if threshold is reached."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at_monotonic = now
            self._half_open_successes = 0
            self._failure_count = self.failure_threshold
            return self._state

        self._failure_count += 1
        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at_monotonic = now
        return self._state


@dataclass(frozen=True, slots=True)
class DetectedGap:
    """In-flight gap notification triggering asynchronous REST backfill."""

    exchange: str
    symbol: str
    gap_type: str
    start_ts_ms: int
    end_ts_ms: int
    prev_seq: int | None
    curr_seq: int | None

    @property
    def duration_ms(self) -> int:
        """Elapsed gap duration in milliseconds."""
        return max(0, self.end_ts_ms - self.start_ts_ms)


@dataclass(slots=True)
class GapDetector:
    """Detect timestamp stalls and sequence jumps across consecutive feed ticks."""

    gap_threshold_ms: int = 5000
    sequence_jump_threshold: int = 500
    _last_ts_ms: dict[tuple[str, str], int] = field(default_factory=dict, init=False)
    _last_seq: dict[tuple[str, str], int] = field(default_factory=dict, init=False)

    def observe(self, tick: NormalizedTick) -> DetectedGap | None:
        """Inspect incoming tick against prior state; return DetectedGap when threshold trips."""
        if tick.source == "rest_backfill":
            return None

        key = (tick.exchange, tick.symbol)
        prev_ts = self._last_ts_ms.get(key)
        prev_seq = self._last_seq.get(key)

        detected: DetectedGap | None = None
        # Only update watermarks monotonically so out-of-order rows never regress state.
        if prev_ts is not None and tick.ts_ms - prev_ts > self.gap_threshold_ms:
            detected = DetectedGap(
                exchange=tick.exchange,
                symbol=tick.symbol,
                gap_type="timestamp_gap",
                start_ts_ms=prev_ts,
                end_ts_ms=tick.ts_ms,
                prev_seq=prev_seq,
                curr_seq=tick.sequence_id,
            )
        elif (
            self.sequence_jump_threshold > 0
            and prev_seq is not None
            and tick.sequence_id is not None
            and tick.sequence_id - prev_seq > self.sequence_jump_threshold
            and prev_ts is not None
        ):
            detected = DetectedGap(
                exchange=tick.exchange,
                symbol=tick.symbol,
                gap_type="sequence_gap",
                start_ts_ms=prev_ts,
                end_ts_ms=tick.ts_ms,
                prev_seq=prev_seq,
                curr_seq=tick.sequence_id,
            )

        if prev_ts is None or tick.ts_ms > prev_ts:
            self._last_ts_ms[key] = tick.ts_ms
        if tick.sequence_id is not None and (prev_seq is None or tick.sequence_id > prev_seq):
            self._last_seq[key] = tick.sequence_id
        return detected

    def seed(self, exchange: str, symbol: str, ts_ms: int, seq: int | None = None) -> None:
        """Initialize gap watermark explicitly (used after reconnect or REST warmup)."""
        key = (exchange, symbol)
        self._last_ts_ms[key] = ts_ms
        if seq is not None:
            self._last_seq[key] = seq


@dataclass(frozen=True, slots=True)
class SkewObservation:
    """Clock-skew assessment derived without tripping false stale-feed alarms."""

    is_skewed: bool
    skew_ms: int


@dataclass(slots=True)
class StaleFeedMonitor:
    """Monotonic-clock feed freshness monitor immune to NTP/exchange wall-clock skew."""

    stale_seconds: float = 8.0
    skew_tolerance_ms: int = 1500
    _last_monotonic: dict[tuple[str, str], float] = field(default_factory=dict, init=False)
    _stale_active: dict[tuple[str, str], bool] = field(default_factory=dict, init=False)

    def register_stream(
        self, exchange: str, symbol: str, now_monotonic: float | None = None
    ) -> None:
        """Start tracking freshness for a feed from subscription time."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        key = (exchange, symbol)
        self._last_monotonic.setdefault(key, now)
        self._stale_active.setdefault(key, False)

    def refresh_stream(
        self, exchange: str, symbol: str, now_monotonic: float | None = None
    ) -> None:
        """Reset monotonic freshness timer for a freshly connected stream."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        key = (exchange, symbol)
        self._last_monotonic[key] = now
        self._stale_active[key] = False

    def record_activity(
        self,
        exchange: str,
        symbol: str,
        event_ts_ms: int,
        received_ms: int,
        now_monotonic: float | None = None,
    ) -> SkewObservation:
        """Mark feed alive using monotonic clock and evaluate wall-clock skew."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        key = (exchange, symbol)
        self._last_monotonic[key] = now
        self._stale_active[key] = False

        # Negative ingest latency (exchange clock ahead of local wall clock) indicates skew.
        ahead_ms = event_ts_ms - received_ms
        if ahead_ms > self.skew_tolerance_ms:
            return SkewObservation(is_skewed=True, skew_ms=ahead_ms)
        return SkewObservation(is_skewed=False, skew_ms=ahead_ms)

    def check_stale(
        self, exchange: str, symbol: str, now_monotonic: float | None = None
    ) -> float | None:
        """Return elapsed stale duration when feed exceeds threshold for the first time."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        key = (exchange, symbol)
        last = self._last_monotonic.get(key)
        if last is None:
            return None
        elapsed = now - last
        if elapsed >= self.stale_seconds:
            if not self._stale_active.get(key, False):
                self._stale_active[key] = True
                return elapsed
            return None
        self._stale_active[key] = False
        return None


@dataclass(slots=True)
class AsyncRateLimiter:
    """Minimum-interval asynchronous limiter protecting REST fallback/backfill endpoints."""

    min_interval_seconds: float = 0.25
    _last_call_monotonic: float = field(default=0.0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def acquire(self) -> None:
        """Wait if needed so successive REST calls respect `min_interval_seconds`."""
        async with self._lock:
            now = time.monotonic()
            wait_for = self.min_interval_seconds - (now - self._last_call_monotonic)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last_call_monotonic = time.monotonic()
