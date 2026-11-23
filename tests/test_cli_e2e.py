"""End-to-end tests for deterministic demo replay, collector fallback, and chart assets."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from PIL import Image

from exchange_collector.adapters import ExchangeAdapter
from exchange_collector.cli import main, run_demo
from exchange_collector.collector import CollectorConfig, FeedCollector
from exchange_collector.models import NormalizedTick, epoch_ms_to_utc_iso
from exchange_collector.storage import MarketStorage


class _FailingWSRestAdapter(ExchangeAdapter):
    exchange = "coinbase"
    canonical_symbol = "BTC-USD"
    raw_symbol = "BTC-USD"
    ws_url = "ws://127.0.0.1:9/unreachable"

    def ws_subscribe_payload(self) -> dict[str, Any]:
        return {}

    def parse_ws_payload(self, payload: dict[str, Any], received_ms: int) -> list[NormalizedTick]:
        return []

    async def fetch_rest_ticker(
        self,
        session: aiohttp.ClientSession,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> NormalizedTick | None:
        now = 1_795_395_600_000
        return NormalizedTick(
            exchange=self.exchange,
            symbol=self.canonical_symbol,
            raw_symbol=self.raw_symbol,
            base_currency="BTC",
            quote_currency="USD",
            price=60750.0,
            bid=60749.9,
            ask=60750.1,
            size=0.01,
            volume_24h=1000.0,
            sequence_id=1,
            ts_ms=now,
            ts_utc=epoch_ms_to_utc_iso(now),
            received_ms=now + 20,
            latency_ms=20.0,
            source="rest_poll",
        )

    async def fetch_rest_backfill(
        self,
        session: aiohttp.ClientSession,
        start_ts_ms: int,
        end_ts_ms: int,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> list[NormalizedTick]:
        return []


def test_demo_e2e_reproducible_and_cover_dimensions(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "sample_capture.json"
    db_path = tmp_path / "demo.db"
    report_dir = tmp_path / "reports"
    assets_dir = tmp_path / "assets"

    summary = run_demo(
        fixture_path=fixture,
        db_path=db_path,
        report_dir=report_dir,
        write_assets=True,
        assets_dir=assets_dir,
    )

    assert summary["run_id"] == "demo_fixture_v1"
    assert summary["total_ticks"] == 177
    assert summary["raw_input_rows"] == 179
    assert summary["deduplicated_rows"] == 2
    assert summary["gaps_detected"] == 1
    assert summary["alerts_emitted"] == 4
    assert Path(summary["report_html"]).exists()
    assert Path(summary["overview_png"]).exists()

    cover_path = assets_dir / "cover_1600x1200.png"
    assert cover_path.exists()
    with Image.open(cover_path) as im:
        assert im.size == (1600, 1200)

    # Verify CLI `report` regenerates cleanly from the persisted SQLite DB
    rc = main(
        [
            "report",
            "--db",
            str(db_path),
            "--run-id",
            "demo_fixture_v1",
            "--report-dir",
            str(tmp_path / "regen"),
        ]
    )
    assert rc == 0
    assert (tmp_path / "regen" / "demo_fixture_v1_report.html").exists()


@pytest.mark.asyncio
async def test_collector_ws_failure_triggers_rest_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback.db"
    with MarketStorage(db_path) as storage:
        collector = FeedCollector(
            storage,
            adapters=[_FailingWSRestAdapter()],
            config=CollectorConfig(
                stale_seconds=0.4,
                http_timeout_seconds=0.5,
                ws_receive_timeout_seconds=0.5,
                rest_poll_interval_seconds=0.2,
                flush_interval_seconds=0.1,
            ),
        )
        run_id = await collector.run(
            run_id="fallback_test",
            mode="live-sample",
            duration_seconds=1.2,
            stop_event=asyncio.Event(),
        )
        counts = storage.get_exchange_counts(run_id)
        alerts = storage.get_alerts(run_id)
        assert counts["coinbase"]["rest_poll"] >= 1
        assert any(a.alert_type == "rest_fallback" for a in alerts)
