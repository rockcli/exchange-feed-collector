"""SQLite WAL persistence layer with idempotent tick upserts and latency/spread queries."""

from __future__ import annotations

import contextlib
import math
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path

from exchange_collector.models import AlertRecord, CaptureRun, GapRecord, NormalizedTick


def compute_percentile(sorted_values: Sequence[float], pct: float) -> float:
    """Compute linear-interpolated percentile in `[0, 100]` over already-sorted values."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(sorted_values[low])
    weight = rank - low
    return float(sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    started_ms INTEGER NOT NULL,
    ended_ms INTEGER,
    started_utc TEXT NOT NULL,
    ended_utc TEXT,
    exchanges TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ticks (
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
    FOREIGN KEY (run_id) REFERENCES capture_runs(run_id) ON DELETE CASCADE,
    UNIQUE(exchange, symbol, ts_ms, source)
);

CREATE INDEX IF NOT EXISTS idx_ticks_run_ts ON ticks(run_id, ts_ms);
CREATE INDEX IF NOT EXISTS idx_ticks_exchange_ts ON ticks(exchange, symbol, ts_ms);

CREATE TABLE IF NOT EXISTS gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    gap_type TEXT NOT NULL,
    start_ts_ms INTEGER NOT NULL,
    end_ts_ms INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    prev_seq INTEGER,
    curr_seq INTEGER,
    backfill_status TEXT NOT NULL,
    backfilled_rows INTEGER NOT NULL DEFAULT 0,
    detected_at_utc TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES capture_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_gaps_run ON gaps(run_id, exchange);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    severity TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL,
    metric_value REAL,
    threshold REAL,
    ts_ms INTEGER NOT NULL,
    ts_utc TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES capture_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alerts_run ON alerts(run_id, ts_ms);
"""


class MarketStorage:
    """Thread-safe SQLite WAL storage engine for ticks, gaps, alerts, and run summaries."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=15.0)
        self._conn.row_factory = sqlite3.Row
        self._configure_pragmas()
        self._init_schema()

    def _configure_pragmas(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA busy_timeout=5000;")
            self._conn.execute("PRAGMA foreign_keys=ON;")

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def close(self) -> None:
        """Flush WAL checkpoint best-effort and close the SQLite connection."""
        with self._lock:
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
            self._conn.close()

    def __enter__(self) -> MarketStorage:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def start_run(self, run: CaptureRun) -> None:
        """Insert or replace metadata for a capture run."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO capture_runs (
                    run_id, mode, started_ms, ended_ms, started_utc, ended_utc, exchanges, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    mode=excluded.mode,
                    started_ms=excluded.started_ms,
                    ended_ms=excluded.ended_ms,
                    started_utc=excluded.started_utc,
                    ended_utc=excluded.ended_utc,
                    exchanges=excluded.exchanges,
                    notes=excluded.notes
                """,
                (
                    run.run_id,
                    run.mode,
                    run.started_ms,
                    run.ended_ms,
                    run.started_utc,
                    run.ended_utc,
                    run.exchanges,
                    run.notes,
                ),
            )
            self._conn.commit()

    def finish_run(self, run_id: str, ended_ms: int, ended_utc: str, notes: str = "") -> None:
        """Record completion timestamp and optional summary notes for a capture run."""
        with self._lock:
            self._conn.execute(
                """
                UPDATE capture_runs
                SET ended_ms = ?,
                    ended_utc = ?,
                    notes = CASE WHEN ? != '' THEN ? ELSE notes END
                WHERE run_id = ?
                """,
                (ended_ms, ended_utc, notes, notes, run_id),
            )
            self._conn.commit()

    def upsert_ticks(self, run_id: str, ticks: Iterable[NormalizedTick]) -> int:
        """Idempotently upsert ticks keyed by `(exchange, symbol, ts_ms, source)`."""
        rows = [
            (
                run_id,
                t.exchange,
                t.symbol,
                t.raw_symbol,
                t.base_currency,
                t.quote_currency,
                t.price,
                t.bid,
                t.ask,
                t.size,
                t.volume_24h,
                t.sequence_id,
                t.ts_ms,
                t.ts_utc,
                t.received_ms,
                t.latency_ms,
                t.source,
            )
            for t in ticks
        ]
        if not rows:
            return 0
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO ticks (
                    run_id, exchange, symbol, raw_symbol, base_currency, quote_currency,
                    price, bid, ask, size, volume_24h, sequence_id,
                    ts_ms, ts_utc, received_ms, latency_ms, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, symbol, ts_ms, source) DO UPDATE SET
                    run_id=excluded.run_id,
                    price=excluded.price,
                    bid=COALESCE(excluded.bid, ticks.bid),
                    ask=COALESCE(excluded.ask, ticks.ask),
                    size=COALESCE(excluded.size, ticks.size),
                    volume_24h=COALESCE(excluded.volume_24h, ticks.volume_24h),
                    sequence_id=COALESCE(excluded.sequence_id, ticks.sequence_id),
                    received_ms=MIN(ticks.received_ms, excluded.received_ms),
                    latency_ms=MIN(ticks.latency_ms, excluded.latency_ms)
                """,
                rows,
            )
            self._conn.commit()
        return len(rows)

    def insert_gap(self, gap: GapRecord) -> None:
        """Persist a detected gap and its backfill status."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO gaps (
                    run_id, exchange, symbol, gap_type, start_ts_ms, end_ts_ms,
                    duration_ms, prev_seq, curr_seq, backfill_status,
                    backfilled_rows, detected_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    gap.run_id,
                    gap.exchange,
                    gap.symbol,
                    gap.gap_type,
                    gap.start_ts_ms,
                    gap.end_ts_ms,
                    gap.duration_ms,
                    gap.prev_seq,
                    gap.curr_seq,
                    gap.backfill_status,
                    gap.backfilled_rows,
                    gap.detected_at_utc,
                ),
            )
            self._conn.commit()

    def insert_alerts(self, alerts: Iterable[AlertRecord]) -> int:
        """Persist one or more operational/anomaly alerts."""
        rows = [
            (
                a.run_id,
                a.exchange,
                a.symbol,
                a.severity,
                a.alert_type,
                a.message,
                a.metric_value,
                a.threshold,
                a.ts_ms,
                a.ts_utc,
            )
            for a in alerts
        ]
        if not rows:
            return 0
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO alerts (
                    run_id, exchange, symbol, severity, alert_type,
                    message, metric_value, threshold, ts_ms, ts_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._conn.commit()
        return len(rows)

    def latest_run_id(self, mode: str | None = None) -> str | None:
        """Return the most recent `run_id`, optionally filtered by capture mode."""
        with self._lock:
            if mode is None:
                row = self._conn.execute(
                    "SELECT run_id FROM capture_runs ORDER BY started_ms DESC LIMIT 1"
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT run_id FROM capture_runs WHERE mode = ? "
                    "ORDER BY started_ms DESC LIMIT 1",
                    (mode,),
                ).fetchone()
        return str(row["run_id"]) if row is not None else None

    def get_run(self, run_id: str) -> CaptureRun | None:
        """Load metadata for `run_id`."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM capture_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            return None
        return CaptureRun(
            run_id=str(row["run_id"]),
            mode=str(row["mode"]),
            started_ms=int(row["started_ms"]),
            ended_ms=int(row["ended_ms"]) if row["ended_ms"] is not None else None,
            started_utc=str(row["started_utc"]),
            ended_utc=str(row["ended_utc"]) if row["ended_utc"] is not None else None,
            exchanges=str(row["exchanges"]),
            notes=str(row["notes"]),
        )

    def get_ticks(self, run_id: str) -> list[NormalizedTick]:
        """Fetch all ticks for `run_id` ordered chronologically by `ts_ms`."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT exchange, symbol, raw_symbol, base_currency, quote_currency,
                       price, bid, ask, size, volume_24h, sequence_id,
                       ts_ms, ts_utc, received_ms, latency_ms, source
                FROM ticks
                WHERE run_id = ?
                ORDER BY ts_ms ASC, exchange ASC
                """,
                (run_id,),
            ).fetchall()
        return [
            NormalizedTick(
                exchange=str(r["exchange"]),
                symbol=str(r["symbol"]),
                raw_symbol=str(r["raw_symbol"]),
                base_currency=str(r["base_currency"]),
                quote_currency=str(r["quote_currency"]),
                price=float(r["price"]),
                bid=float(r["bid"]) if r["bid"] is not None else None,
                ask=float(r["ask"]) if r["ask"] is not None else None,
                size=float(r["size"]) if r["size"] is not None else None,
                volume_24h=float(r["volume_24h"]) if r["volume_24h"] is not None else None,
                sequence_id=int(r["sequence_id"]) if r["sequence_id"] is not None else None,
                ts_ms=int(r["ts_ms"]),
                ts_utc=str(r["ts_utc"]),
                received_ms=int(r["received_ms"]),
                latency_ms=float(r["latency_ms"]),
                source=str(r["source"]),
            )
            for r in rows
        ]

    def get_gaps(self, run_id: str) -> list[GapRecord]:
        """Fetch all gap records for `run_id`."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT run_id, exchange, symbol, gap_type, start_ts_ms, end_ts_ms,
                       duration_ms, prev_seq, curr_seq, backfill_status,
                       backfilled_rows, detected_at_utc
                FROM gaps
                WHERE run_id = ?
                ORDER BY start_ts_ms ASC
                """,
                (run_id,),
            ).fetchall()
        return [
            GapRecord(
                run_id=str(r["run_id"]),
                exchange=str(r["exchange"]),
                symbol=str(r["symbol"]),
                gap_type=str(r["gap_type"]),
                start_ts_ms=int(r["start_ts_ms"]),
                end_ts_ms=int(r["end_ts_ms"]),
                duration_ms=int(r["duration_ms"]),
                prev_seq=int(r["prev_seq"]) if r["prev_seq"] is not None else None,
                curr_seq=int(r["curr_seq"]) if r["curr_seq"] is not None else None,
                backfill_status=str(r["backfill_status"]),
                backfilled_rows=int(r["backfilled_rows"]),
                detected_at_utc=str(r["detected_at_utc"]),
            )
            for r in rows
        ]

    def get_alerts(self, run_id: str) -> list[AlertRecord]:
        """Fetch all alert records for `run_id`."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT run_id, exchange, symbol, severity, alert_type,
                       message, metric_value, threshold, ts_ms, ts_utc
                FROM alerts
                WHERE run_id = ?
                ORDER BY ts_ms ASC
                """,
                (run_id,),
            ).fetchall()
        return [
            AlertRecord(
                run_id=str(r["run_id"]),
                exchange=str(r["exchange"]),
                symbol=str(r["symbol"]),
                severity=str(r["severity"]),
                alert_type=str(r["alert_type"]),
                message=str(r["message"]),
                metric_value=float(r["metric_value"]) if r["metric_value"] is not None else None,
                threshold=float(r["threshold"]) if r["threshold"] is not None else None,
                ts_ms=int(r["ts_ms"]),
                ts_utc=str(r["ts_utc"]),
            )
            for r in rows
        ]

    def get_latency_percentiles(
        self,
        run_id: str,
        include_sources: tuple[str, ...] = ("ws", "rest_poll", "fixture"),
    ) -> dict[str, dict[str, float]]:
        """Return `count`, `p50`, `p95`, `max`, and `skewed_count` per exchange for live ticks."""
        placeholders = ",".join("?" for _ in include_sources)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT exchange, latency_ms
                FROM ticks
                WHERE run_id = ? AND source IN ({placeholders})
                ORDER BY exchange ASC, latency_ms ASC
                """,
                (run_id, *include_sources),
            ).fetchall()

        grouped: dict[str, list[float]] = defaultdict(list)
        skewed: dict[str, int] = defaultdict(int)
        for row in rows:
            ex = str(row["exchange"])
            raw = float(row["latency_ms"])
            if raw < 0:
                skewed[ex] += 1
                grouped[ex].append(0.0)
            else:
                grouped[ex].append(raw)

        result: dict[str, dict[str, float]] = {}
        for ex, vals in grouped.items():
            vals.sort()
            result[ex] = {
                "count": float(len(vals)),
                "p50": round(compute_percentile(vals, 50.0), 2),
                "p95": round(compute_percentile(vals, 95.0), 2),
                "max": round(vals[-1], 2),
                "skewed_count": float(skewed[ex]),
            }
        return result

    def get_exchange_counts(self, run_id: str) -> dict[str, dict[str, int]]:
        """Return total tick counts per exchange broken down by ingestion source."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT exchange, source, COUNT(*) AS cnt
                FROM ticks
                WHERE run_id = ?
                GROUP BY exchange, source
                """,
                (run_id,),
            ).fetchall()
        counts: dict[str, dict[str, int]] = defaultdict(dict)
        for row in rows:
            counts[str(row["exchange"])][str(row["source"])] = int(row["cnt"])
        return dict(counts)
