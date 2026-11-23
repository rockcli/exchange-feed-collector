"""Command-line entrypoint for deterministic demo, live-sample capture, soak, and reports."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from exchange_collector.adapters import normalize_symbol
from exchange_collector.analytics import AnalysisResult, analyze_run
from exchange_collector.collector import CollectorConfig, FeedCollector
from exchange_collector.models import (
    AlertRecord,
    CaptureRun,
    GapRecord,
    NormalizedTick,
    epoch_ms_to_utc_iso,
)
from exchange_collector.report import (
    render_cover_1600x1200,
    render_html_report,
    render_overview_chart,
)
from exchange_collector.resilience import GapDetector, StaleFeedMonitor
from exchange_collector.storage import MarketStorage


def _default_fixture_path() -> Path:
    pkg_root = Path(__file__).resolve().parents[2] / "fixtures" / "sample_capture.json"
    if pkg_root.exists():
        return pkg_root
    return Path("fixtures/sample_capture.json")


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _load_fixture_ticks(
    fixture_path: Path,
) -> tuple[str, list[NormalizedTick], list[NormalizedTick]]:
    """Load deterministic offline fixture ticks and simulated backfill batches."""
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    run_id = str(payload["run_id"])

    def _decode(raw: dict[str, Any]) -> NormalizedTick:
        exchange = str(raw["exchange"])
        raw_symbol = str(raw["raw_symbol"])
        symbol, base, quote = normalize_symbol(exchange, raw_symbol)
        ts_ms = int(raw["ts_ms"])
        received_ms = int(raw["received_ms"])
        return NormalizedTick(
            exchange=exchange,
            symbol=symbol,
            raw_symbol=raw_symbol,
            base_currency=base,
            quote_currency=quote,
            price=float(raw["price"]),
            bid=float(raw["bid"]) if raw.get("bid") is not None else None,
            ask=float(raw["ask"]) if raw.get("ask") is not None else None,
            size=float(raw["size"]) if raw.get("size") is not None else None,
            volume_24h=float(raw["volume_24h"]) if raw.get("volume_24h") is not None else None,
            sequence_id=int(raw["sequence_id"]) if raw.get("sequence_id") is not None else None,
            ts_ms=ts_ms,
            ts_utc=epoch_ms_to_utc_iso(ts_ms),
            received_ms=received_ms,
            latency_ms=float(received_ms - ts_ms),
            source=str(raw["source"]),
        )

    stream_ticks = [_decode(item) for item in payload["stream_ticks"]]
    backfill_ticks = [_decode(item) for item in payload["backfill_ticks"]]
    return run_id, stream_ticks, backfill_ticks


def run_demo(
    *,
    fixture_path: Path,
    db_path: Path,
    report_dir: Path,
    write_assets: bool,
    assets_dir: Path,
) -> dict[str, Any]:
    """Replay deterministic offline fixture through resilience, WAL storage, and reporting."""
    if db_path.exists():
        db_path.unlink()

    run_id, stream_ticks, backfill_ticks = _load_fixture_ticks(fixture_path)
    started_ms = min(t.ts_ms for t in stream_ticks)
    ended_ms = max(t.ts_ms for t in stream_ticks)

    gap_detector = GapDetector(gap_threshold_ms=5000, sequence_jump_threshold=50)
    stale_monitor = StaleFeedMonitor(stale_seconds=8.0, skew_tolerance_ms=100)

    with MarketStorage(db_path) as storage:
        storage.start_run(
            CaptureRun(
                run_id=run_id,
                mode="demo",
                started_ms=started_ms,
                ended_ms=ended_ms,
                started_utc=epoch_ms_to_utc_iso(started_ms),
                ended_utc=epoch_ms_to_utc_iso(ended_ms),
                exchanges="coinbase,kraken,okx",
                notes="deterministic offline fixture replay",
            )
        )

        # Replay sequentially so gap detection, clock-skew alerts, and deduplication all execute.
        raw_ingested = 0
        mono_base = 1000.0
        for tick in stream_ticks:
            raw_ingested += 1
            mono_now = mono_base + (tick.ts_ms - started_ms) / 1000.0
            skew = stale_monitor.record_activity(
                tick.exchange, tick.symbol, tick.ts_ms, tick.received_ms, now_monotonic=mono_now
            )
            if skew.is_skewed:
                storage.insert_alerts(
                    [
                        AlertRecord(
                            run_id=run_id,
                            exchange=tick.exchange,
                            symbol=tick.symbol,
                            severity="info",
                            alert_type="clock_skew",
                            message=(
                                f"Exchange event clock ahead of local wall clock by "
                                f"{skew.skew_ms} ms; monotonic freshness preserved"
                            ),
                            metric_value=float(skew.skew_ms),
                            threshold=float(stale_monitor.skew_tolerance_ms),
                            ts_ms=tick.ts_ms,
                            ts_utc=tick.ts_utc,
                        )
                    ]
                )

            gap = gap_detector.observe(tick)
            if gap is not None:
                matched = [
                    b
                    for b in backfill_ticks
                    if b.exchange == gap.exchange
                    and gap.start_ts_ms <= b.ts_ms <= gap.end_ts_ms
                ]
                inserted_bf = storage.upsert_ticks(run_id, matched)
                storage.insert_gap(
                    GapRecord(
                        run_id=run_id,
                        exchange=gap.exchange,
                        symbol=gap.symbol,
                        gap_type=gap.gap_type,
                        start_ts_ms=gap.start_ts_ms,
                        end_ts_ms=gap.end_ts_ms,
                        duration_ms=gap.duration_ms,
                        prev_seq=gap.prev_seq,
                        curr_seq=gap.curr_seq,
                        backfill_status="recovered" if inserted_bf > 0 else "partial",
                        backfilled_rows=inserted_bf,
                        detected_at_utc=tick.ts_utc,
                    )
                )

            storage.upsert_ticks(run_id, [tick])

        analysis = analyze_run(storage, run_id, bucket_ms=1000, persist_spread_alerts=True)

    overview_path = report_dir / f"{run_id}_overview.png"
    html_path = report_dir / f"{run_id}_report.html"
    render_overview_chart(analysis, overview_path)
    render_html_report(analysis, overview_path, html_path)

    if write_assets:
        render_overview_chart(analysis, assets_dir / "demo_overview.png")
        render_cover_1600x1200(analysis, assets_dir / "cover_1600x1200.png")

    summary = _analysis_to_dict(analysis, html_path=html_path, overview_path=overview_path)
    summary["raw_input_rows"] = raw_ingested + len(backfill_ticks)
    summary["deduplicated_rows"] = summary["raw_input_rows"] - analysis.total_ticks
    return summary


async def _run_live_async(
    *,
    mode: str,
    duration_seconds: float,
    db_path: Path,
    report_dir: Path,
    write_assets: bool,
    assets_dir: Path,
) -> dict[str, Any]:
    run_stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    prefix = "live" if mode == "live-sample" else "soak"
    run_id = f"{prefix}_{run_stamp}"

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    cfg = CollectorConfig(
        stale_seconds=float(os.environ.get("EFC_STALE_SECONDS", "8.0")),
        gap_threshold_ms=int(os.environ.get("EFC_GAP_THRESHOLD_MS", "5000")),
        http_timeout_seconds=float(os.environ.get("EFC_HTTP_TIMEOUT_SECONDS", "8.0")),
    )
    bucket_ms = int(os.environ.get("EFC_BUCKET_MS", "1000"))

    with MarketStorage(db_path) as storage:
        collector = FeedCollector(storage, config=cfg)
        await collector.run(
            run_id=run_id,
            mode=mode,
            duration_seconds=duration_seconds,
            stop_event=stop_event,
        )
        analysis = analyze_run(
            storage, run_id, bucket_ms=bucket_ms, persist_spread_alerts=True
        )

    overview_path = report_dir / f"{run_id}_overview.png"
    html_path = report_dir / f"{run_id}_report.html"
    render_overview_chart(analysis, overview_path)
    render_html_report(analysis, overview_path, html_path)

    if write_assets:
        render_overview_chart(analysis, assets_dir / "demo_overview.png")
        render_cover_1600x1200(analysis, assets_dir / "cover_1600x1200.png")

    return _analysis_to_dict(analysis, html_path=html_path, overview_path=overview_path)


def run_report_command(
    *,
    db_path: Path,
    run_id: str | None,
    report_dir: Path,
    bucket_ms: int,
    write_assets: bool,
    assets_dir: Path,
) -> dict[str, Any]:
    """Generate HTML and PNG reports from an existing SQLite capture run."""
    with MarketStorage(db_path) as storage:
        target_id = run_id if run_id is not None else storage.latest_run_id()
        if target_id is None:
            raise SystemExit(f"No capture runs found in {db_path}")
        analysis = analyze_run(storage, target_id, bucket_ms=bucket_ms, persist_spread_alerts=False)

    overview_path = report_dir / f"{target_id}_overview.png"
    html_path = report_dir / f"{target_id}_report.html"
    render_overview_chart(analysis, overview_path)
    render_html_report(analysis, overview_path, html_path)
    if write_assets:
        render_overview_chart(analysis, assets_dir / "demo_overview.png")
        render_cover_1600x1200(analysis, assets_dir / "cover_1600x1200.png")
    return _analysis_to_dict(analysis, html_path=html_path, overview_path=overview_path)


def _analysis_to_dict(
    analysis: AnalysisResult,
    *,
    html_path: Path,
    overview_path: Path,
) -> dict[str, Any]:
    usd = analysis.usd_spread_summary
    usdt = analysis.usdt_basis_summary
    return {
        "run_id": analysis.run.run_id,
        "mode": analysis.run.mode,
        "started_utc": analysis.run.started_utc,
        "ended_utc": analysis.run.ended_utc,
        "total_ticks": analysis.total_ticks,
        "exchange_counts": analysis.exchange_counts,
        "latency_ms": analysis.latency_by_exchange,
        "usd_spread": (
            {
                "overlapping_buckets": usd.overlapping_buckets,
                "mean_delta_usd": usd.mean_delta,
                "mean_abs_bps": usd.mean_abs_bps,
                "p95_abs_bps": usd.p95_abs_bps,
                "max_abs_bps": usd.max_abs_bps,
                "min_delta_usd": usd.min_delta,
                "max_delta_usd": usd.max_delta,
            }
            if usd
            else None
        ),
        "usdt_basis": (
            {
                "overlapping_buckets": usdt.overlapping_buckets,
                "mean_delta": usdt.mean_delta,
                "mean_abs_bps": usdt.mean_abs_bps,
                "p95_abs_bps": usdt.p95_abs_bps,
                "max_abs_bps": usdt.max_abs_bps,
                "min_delta": usdt.min_delta,
                "max_delta": usdt.max_delta,
            }
            if usdt
            else None
        ),
        "gaps_detected": len(analysis.gaps),
        "alerts_emitted": len(analysis.alerts),
        "report_html": str(html_path),
        "overview_png": str(overview_path),
    }


def build_parser() -> argparse.ArgumentParser:
    """Construct CLI parser for `demo`, `live-sample`, `soak`, and `report`."""
    parser = argparse.ArgumentParser(
        prog="exchange-collector",
        description="Multi-exchange market data collector and analytics CLI",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("EFC_LOG_LEVEL", "INFO"),
        help="Python logging level (default: INFO)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_demo = sub.add_parser("demo", help="Run deterministic offline fixture replay")
    p_demo.add_argument("--fixture", type=Path, default=_default_fixture_path())
    p_demo.add_argument("--db", type=Path, default=Path("data/demo.db"))
    p_demo.add_argument("--report-dir", type=Path, default=Path("reports"))
    p_demo.add_argument("--write-assets", action="store_true")
    p_demo.add_argument("--assets-dir", type=Path, default=Path("assets"))

    p_live = sub.add_parser("live-sample", help="Capture a short live public exchange sample")
    p_live.add_argument("--seconds", type=float, default=50.0)
    p_live.add_argument("--db", type=Path, default=Path("data/live_sample.db"))
    p_live.add_argument("--report-dir", type=Path, default=Path("reports"))
    p_live.add_argument("--write-assets", action="store_true")
    p_live.add_argument("--assets-dir", type=Path, default=Path("assets"))

    p_soak = sub.add_parser("soak", help="Run continuous multi-hour capture with signal flush")
    p_soak.add_argument("--hours", type=float, default=72.0)
    p_soak.add_argument("--db", type=Path, default=Path("data/soak.db"))
    p_soak.add_argument("--report-dir", type=Path, default=Path("reports"))

    p_rep = sub.add_parser("report", help="Render HTML/PNG report from existing SQLite DB")
    p_rep.add_argument("--db", type=Path, default=Path("data/live_sample.db"))
    p_rep.add_argument("--run-id", type=str, default=None)
    p_rep.add_argument("--bucket-ms", type=int, default=1000)
    p_rep.add_argument("--report-dir", type=Path, default=Path("reports"))
    p_rep.add_argument("--write-assets", action="store_true")
    p_rep.add_argument("--assets-dir", type=Path, default=Path("assets"))

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint returning process exit status."""
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(str(args.log_level))

    if args.command == "demo":
        summary = run_demo(
            fixture_path=args.fixture,
            db_path=args.db,
            report_dir=args.report_dir,
            write_assets=bool(args.write_assets),
            assets_dir=args.assets_dir,
        )
    elif args.command == "live-sample":
        summary = asyncio.run(
            _run_live_async(
                mode="live-sample",
                duration_seconds=float(args.seconds),
                db_path=args.db,
                report_dir=args.report_dir,
                write_assets=bool(args.write_assets),
                assets_dir=args.assets_dir,
            )
        )
    elif args.command == "soak":
        summary = asyncio.run(
            _run_live_async(
                mode="soak",
                duration_seconds=float(args.hours) * 3600.0,
                db_path=args.db,
                report_dir=args.report_dir,
                write_assets=False,
                assets_dir=Path("assets"),
            )
        )
    elif args.command == "report":
        summary = run_report_command(
            db_path=args.db,
            run_id=args.run_id,
            report_dir=args.report_dir,
            bucket_ms=int(args.bucket_ms),
            write_assets=bool(args.write_assets),
            assets_dir=args.assets_dir,
        )
    else:
        parser.error(f"Unknown command: {args.command}")
        return 2

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
