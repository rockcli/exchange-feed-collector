"""Domain data models for normalized market ticks, gaps, alerts, and capture runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def epoch_ms_to_utc_iso(ts_ms: int) -> str:
    """Convert epoch milliseconds to an ISO-8601 UTC string with millisecond precision."""
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now_ms() -> int:
    """Return current UTC epoch time in milliseconds."""
    return int(datetime.now(tz=UTC).timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class NormalizedTick:
    """Canonical L1 market tick across exchanges with explicit quote currency."""

    exchange: str
    symbol: str
    raw_symbol: str
    base_currency: str
    quote_currency: str
    price: float
    bid: float | None
    ask: float | None
    size: float | None
    volume_24h: float | None
    sequence_id: int | None
    ts_ms: int
    ts_utc: str
    received_ms: int
    latency_ms: float
    source: str

    @property
    def mid_price(self) -> float:
        """Return midpoint when bid and ask are both available, else last price."""
        if self.bid is not None and self.ask is not None and self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        return self.price

    @property
    def internal_spread_bps(self) -> float | None:
        """Return top-of-book bid-ask spread in basis points for this single venue."""
        if self.bid is None or self.ask is None or self.bid <= 0 or self.ask <= 0:
            return None
        mid = (self.bid + self.ask) / 2.0
        return ((self.ask - self.bid) / mid) * 10_000.0


@dataclass(frozen=True, slots=True)
class GapRecord:
    """Detected time, sequence, or disconnect gap and its REST backfill outcome."""

    run_id: str
    exchange: str
    symbol: str
    gap_type: str
    start_ts_ms: int
    end_ts_ms: int
    duration_ms: int
    prev_seq: int | None
    curr_seq: int | None
    backfill_status: str
    backfilled_rows: int
    detected_at_utc: str


@dataclass(frozen=True, slots=True)
class AlertRecord:
    """Operational or market-structure alert emitted during capture or analysis."""

    run_id: str
    exchange: str
    symbol: str
    severity: str
    alert_type: str
    message: str
    metric_value: float | None
    threshold: float | None
    ts_ms: int
    ts_utc: str


@dataclass(frozen=True, slots=True)
class CaptureRun:
    """Metadata describing a single demo, live-sample, or soak capture execution."""

    run_id: str
    mode: str
    started_ms: int
    ended_ms: int | None
    started_utc: str
    ended_utc: str | None
    exchanges: str
    notes: str
