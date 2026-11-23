"""End-to-end tests for deterministic demo replay, collector fallback, and chart assets."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from PIL import Image

from exchange_collector.adapters import CoinbaseAdapter, ExchangeAdapter
from exchange_collector.cli import build_parser, main, run_demo
from exchange_collector.collector import CollectorConfig, FeedCollector
from exchange_collector.models import NormalizedTick, epoch_ms_to_utc_iso
from exchange_collector.resilience import BackoffPolicy, CircuitBreaker, CircuitState, DetectedGap
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
    assert summary["clock_skew_rows"] == 2
    assert summary["usd_spread"] is not None
    assert "mean_signed_bps" in summary["usd_spread"]
    assert "mean_abs_bps" in summary["usd_spread"]
    assert "mean_delta_usd" in summary["usd_spread"]
    assert summary["usdt_basis"] is not None
    assert summary["latency_ms"]["coinbase"]["non_skewed_count"] == 58.0
    assert summary["latency_ms"]["coinbase"]["raw_count"] == 60.0
    assert summary["gaps_detected"] == 1
    assert summary["alerts_emitted"] == 4
    assert Path(summary["report_html"]).exists()
    assert Path(summary["overview_png"]).exists()
    summary_json_path = Path(summary["summary_json"])
    assert summary_json_path.exists()
    loaded_json = json.loads(summary_json_path.read_text(encoding="utf-8"))
    assert loaded_json["run_id"] == "demo_fixture_v1"
    assert loaded_json["raw_input_rows"] == 179
    assert loaded_json["deduplicated_rows"] == 2

    cover_path = assets_dir / "cover_1600x1200.png"
    assert cover_path.exists()
    with Image.open(cover_path) as im:
        assert im.size == (1600, 1200)

    # Verify CLI `report` regenerates HTML, PNG, and JSON cleanly from the persisted SQLite DB
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
    regen_json = json.loads(
        (tmp_path / "regen" / "demo_fixture_v1_summary.json").read_text(encoding="utf-8")
    )
    assert regen_json["raw_input_rows"] == 179
    assert regen_json["deduplicated_rows"] == 2


def test_cli_env_defaults_wired(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EFC_DB_PATH", "/tmp/custom_env.db")
    monkeypatch.setenv("EFC_REPORT_DIR", "/tmp/custom_reports")
    parser = build_parser()
    args_demo = parser.parse_args(["demo"])
    args_live = parser.parse_args(["live-sample"])
    args_rep = parser.parse_args(["report"])
    assert args_demo.db == Path("/tmp/custom_env.db")
    assert args_live.db == Path("/tmp/custom_env.db")
    assert args_rep.db == Path("/tmp/custom_env.db")
    assert args_demo.report_dir == Path("/tmp/custom_reports")
    assert args_rep.report_dir == Path("/tmp/custom_reports")


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


class _LocalWSAdapter(ExchangeAdapter):
    exchange = "coinbase"
    canonical_symbol = "BTC-USD"
    raw_symbol = "BTC-USD"

    def __init__(
        self,
        ws_url: str,
        *,
        backfill_rows: list[NormalizedTick] | None = None,
    ) -> None:
        super().__init__()
        self.ws_url = ws_url
        self._backfill_rows = backfill_rows or []

    def ws_subscribe_payload(self) -> dict[str, Any]:
        return {"type": "subscribe"}

    def is_heartbeat_or_control(self, text_or_payload: str | dict[str, Any]) -> bool:
        if isinstance(text_or_payload, dict):
            return text_or_payload.get("type") == "heartbeat"
        return False

    def parse_ws_payload(self, payload: dict[str, Any], received_ms: int) -> list[NormalizedTick]:
        if payload.get("type") != "tick":
            return []
        ts_ms = int(payload["ts_ms"])
        return [
            NormalizedTick(
                exchange=self.exchange,
                symbol=self.canonical_symbol,
                raw_symbol=self.raw_symbol,
                base_currency="BTC",
                quote_currency="USD",
                price=float(payload["price"]),
                bid=float(payload["price"]) - 0.5,
                ask=float(payload["price"]) + 0.5,
                size=0.01,
                volume_24h=100.0,
                sequence_id=int(payload["seq"]),
                ts_ms=ts_ms,
                ts_utc=epoch_ms_to_utc_iso(ts_ms),
                received_ms=received_ms,
                latency_ms=float(received_ms - ts_ms),
                source="ws",
            )
        ]

    async def fetch_rest_ticker(
        self,
        session: aiohttp.ClientSession,
        timeout_seconds: float,
        proxy_url: str | None = None,
    ) -> NormalizedTick | None:
        now = 1_795_395_605_000
        return NormalizedTick(
            exchange=self.exchange,
            symbol=self.canonical_symbol,
            raw_symbol=self.raw_symbol,
            base_currency="BTC",
            quote_currency="USD",
            price=60800.0,
            bid=60799.5,
            ask=60800.5,
            size=0.02,
            volume_24h=100.0,
            sequence_id=999,
            ts_ms=now,
            ts_utc=epoch_ms_to_utc_iso(now),
            received_ms=now + 10,
            latency_ms=10.0,
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
        return list(self._backfill_rows)


@pytest.mark.asyncio
async def test_ws_timeout_trips_breaker_and_half_open_closes_only_on_valid_ticks(
    tmp_path: Path,
) -> None:
    state: dict[str, Any] = {"mode": "timeout", "conn_count": 0}

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        state["conn_count"] += 1
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            if state["mode"] == "timeout":
                await asyncio.sleep(0.5)
                break
            if state["mode"] == "control_only":
                await ws.send_json({"type": "heartbeat"})
                await asyncio.sleep(0.5)
                break
            if state["mode"] == "valid_ticks":
                base = 1_795_395_610_000
                await ws.send_json({"type": "tick", "price": 60710.0, "seq": 10, "ts_ms": base})
                await ws.send_json(
                    {"type": "tick", "price": 60711.0, "seq": 11, "ts_ms": base + 50}
                )
                await asyncio.sleep(0.5)
                break
        return ws

    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert runner.addresses
    port = int(runner.addresses[0][1])

    try:
        db_path = tmp_path / "breaker.db"
        with MarketStorage(db_path) as storage:
            adapter = _LocalWSAdapter(f"http://127.0.0.1:{port}/ws")
            collector = FeedCollector(
                storage,
                adapters=[adapter],
                config=CollectorConfig(
                    stale_seconds=5.0,
                    http_timeout_seconds=0.3,
                    ws_receive_timeout_seconds=0.08,
                    rest_poll_interval_seconds=0.05,
                    flush_interval_seconds=0.2,
                ),
                backoff=BackoffPolicy(base_seconds=0.01, max_seconds=0.02, jitter_ratio=0.0),
            )
            collector.breakers["coinbase"] = CircuitBreaker(
                name="coinbase",
                failure_threshold=2,
                recovery_timeout_seconds=0.15,
                half_open_success_threshold=2,
            )

            stop_event = asyncio.Event()
            task = asyncio.create_task(
                collector.run(
                    run_id="cb_probe",
                    mode="live-sample",
                    duration_seconds=5.0,
                    stop_event=stop_event,
                )
            )

            # Wait for 2 connect+read timeouts to trip breaker to OPEN
            for _ in range(40):
                if collector.breakers["coinbase"].current_state() == CircuitState.OPEN:
                    break
                await asyncio.sleep(0.03)
            assert collector.breakers["coinbase"].current_state() == CircuitState.OPEN
            assert state["conn_count"] >= 2

            # In HALF_OPEN with control_only, breaker must NOT close; it must re-trip to OPEN
            state["mode"] = "control_only"
            await asyncio.sleep(0.22)
            for _ in range(30):
                alerts = storage.get_alerts("cb_probe")
                if any(a.alert_type == "circuit_open" for a in alerts):
                    break
                await asyncio.sleep(0.03)
            assert collector.breakers["coinbase"].current_state() != CircuitState.CLOSED

            # Switch to valid ticks: cooldown -> HALF_OPEN -> valid ticks persisted -> CLOSED
            state["mode"] = "valid_ticks"
            for _ in range(50):
                if collector.breakers["coinbase"].current_state() == CircuitState.CLOSED:
                    break
                await asyncio.sleep(0.03)
            assert collector.breakers["coinbase"].current_state() == CircuitState.CLOSED
            assert collector.breakers["coinbase"].failure_count == 0
            stop_event.set()
            await task

            alerts = storage.get_alerts("cb_probe")
            assert any(a.alert_type == "circuit_open" for a in alerts)
            assert any(a.alert_type == "circuit_closed" for a in alerts)
            ws_ticks = [t for t in storage.get_ticks("cb_probe") if t.source == "ws"]
            assert len(ws_ticks) >= 2
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_stale_silence_triggers_reconnect_and_rest_fallback(tmp_path: Path) -> None:
    async def silent_ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for _ in ws:
            await asyncio.sleep(1.5)
            break
        return ws

    app = web.Application()
    app.router.add_get("/ws", silent_ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert runner.addresses
    port = int(runner.addresses[0][1])

    try:
        db_path = tmp_path / "stale.db"
        with MarketStorage(db_path) as storage:
            adapter = _LocalWSAdapter(f"http://127.0.0.1:{port}/ws")
            collector = FeedCollector(
                storage,
                adapters=[adapter],
                config=CollectorConfig(
                    stale_seconds=0.15,
                    http_timeout_seconds=0.3,
                    ws_receive_timeout_seconds=5.0,
                    rest_poll_interval_seconds=0.1,
                    flush_interval_seconds=0.05,
                ),
                backoff=BackoffPolicy(base_seconds=0.02, max_seconds=0.05, jitter_ratio=0.0),
            )
            await collector.run(
                run_id="stale_test",
                mode="live-sample",
                duration_seconds=0.55,
                stop_event=asyncio.Event(),
            )
            alerts = storage.get_alerts("stale_test")
            counts = storage.get_exchange_counts("stale_test")
            assert any(a.alert_type == "stale_feed" for a in alerts)
            assert any(a.alert_type == "rest_fallback" for a in alerts)
            assert counts["coinbase"]["rest_poll"] >= 1
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_rest_backfill_strict_window_and_new_row_verification(tmp_path: Path) -> None:
    db_path = tmp_path / "backfill_strict.db"
    gap_start = 1_795_395_610_000
    gap_end = 1_795_395_618_000

    with MarketStorage(db_path) as storage:
        from exchange_collector.models import CaptureRun

        storage.start_run(
            CaptureRun(
                run_id="bf_strict",
                mode="live-sample",
                started_ms=gap_start,
                ended_ms=None,
                started_utc=epoch_ms_to_utc_iso(gap_start),
                ended_utc=None,
                exchanges="coinbase",
                notes="",
            )
        )

        # (1) Rows outside [gap_start, gap_end] must yield no_overlap and 0 rows
        outside_tick = NormalizedTick(
            exchange="coinbase",
            symbol="BTC-USD",
            raw_symbol="BTC-USD",
            base_currency="BTC",
            quote_currency="USD",
            price=60700.0,
            bid=None,
            ask=None,
            size=0.01,
            volume_24h=None,
            sequence_id=50,
            ts_ms=gap_end + 5000,
            ts_utc=epoch_ms_to_utc_iso(gap_end + 5000),
            received_ms=gap_end + 5100,
            latency_ms=100.0,
            source="rest_backfill",
        )
        adapter_outside = _LocalWSAdapter("ws://127.0.0.1:9", backfill_rows=[outside_tick])
        collector = FeedCollector(storage, adapters=[adapter_outside])
        gap = DetectedGap(
            exchange="coinbase",
            symbol="BTC-USD",
            gap_type="timestamp_gap",
            start_ts_ms=gap_start,
            end_ts_ms=gap_end,
            prev_seq=None,
            curr_seq=None,
        )
        async with aiohttp.ClientSession() as session:
            collector._schedule_backfill("bf_strict", adapter_outside, session, None, gap)
            await asyncio.gather(*collector._backfill_tasks)

            # (2) Row inside window that is ALREADY stored (0 newly inserted rows) -> unrecovered
            inside_tick = NormalizedTick(
                exchange="coinbase",
                symbol="BTC-USD",
                raw_symbol="BTC-USD",
                base_currency="BTC",
                quote_currency="USD",
                price=60715.0,
                bid=None,
                ask=None,
                size=0.02,
                volume_24h=None,
                sequence_id=51,
                ts_ms=gap_start + 3000,
                ts_utc=epoch_ms_to_utc_iso(gap_start + 3000),
                received_ms=gap_end + 100,
                latency_ms=5100.0,
                source="rest_backfill",
            )
            storage.insert_ticks("bf_strict", [inside_tick])
            adapter_dup = _LocalWSAdapter("ws://127.0.0.1:9", backfill_rows=[inside_tick])
            collector._schedule_backfill("bf_strict", adapter_dup, session, None, gap)
            await asyncio.gather(*collector._backfill_tasks)

            # (3) Genuinely new row inside [gap_start, gap_end] -> recovered
            fresh_tick = NormalizedTick(
                exchange="coinbase",
                symbol="BTC-USD",
                raw_symbol="BTC-USD",
                base_currency="BTC",
                quote_currency="USD",
                price=60720.0,
                bid=None,
                ask=None,
                size=0.03,
                volume_24h=None,
                sequence_id=52,
                ts_ms=gap_start + 5000,
                ts_utc=epoch_ms_to_utc_iso(gap_start + 5000),
                received_ms=gap_end + 100,
                latency_ms=3100.0,
                source="rest_backfill",
            )
            adapter_fresh = _LocalWSAdapter("ws://127.0.0.1:9", backfill_rows=[fresh_tick])
            collector._schedule_backfill("bf_strict", adapter_fresh, session, None, gap)
            await asyncio.gather(*collector._backfill_tasks)

        gaps = storage.get_gaps("bf_strict")
        assert [g.backfill_status for g in gaps] == ["no_overlap", "unrecovered", "recovered"]
        assert [g.backfilled_rows for g in gaps] == [0, 0, 1]


class _LocalCoinbaseAdapter(CoinbaseAdapter):
    def __init__(self, ws_url: str, rest_ticker_url: str) -> None:
        super().__init__()
        self.ws_url = ws_url
        self.rest_ticker_url = rest_ticker_url


@pytest.mark.asyncio
async def test_zero_and_negative_price_ticks_rejected_without_persisting_or_resetting_breaker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, int] = {"ws_connections": 0, "rest_calls": 0}

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        state["ws_connections"] += 1
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            await ws.send_json(
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "0",
                    "time": "2026-11-23T02:14:21.398919Z",
                    "trade_id": 2001,
                }
            )
            await ws.send_json(
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "-25.5",
                    "time": "2026-11-23T02:14:21.450000Z",
                    "trade_id": 2002,
                }
            )
            await ws.send_json(
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "NaN",
                    "time": "2026-11-23T02:14:21.500000Z",
                    "trade_id": 2003,
                }
            )
            await ws.close()
            break
        return ws

    async def rest_ticker_handler(request: web.Request) -> web.Response:
        state["rest_calls"] += 1
        return web.json_response(
            {
                "price": "0",
                "bid": "0",
                "ask": "0",
                "size": "0.01",
                "volume": "100.0",
                "time": "2026-11-23T02:14:21.600000Z",
                "trade_id": 2004,
            }
        )

    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/ticker", rest_ticker_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert runner.addresses
    port = int(runner.addresses[0][1])

    observed_attempts: list[int] = []
    orig_delay = BackoffPolicy.delay_for_attempt

    def spy_delay(self: BackoffPolicy, attempt: int, rng: Any = None) -> float:
        observed_attempts.append(attempt)
        return orig_delay(self, attempt, rng=rng)

    monkeypatch.setattr(BackoffPolicy, "delay_for_attempt", spy_delay)

    try:
        db_path = tmp_path / "zero_price.db"
        with MarketStorage(db_path) as storage:
            adapter = _LocalCoinbaseAdapter(
                f"http://127.0.0.1:{port}/ws",
                f"http://127.0.0.1:{port}/ticker",
            )
            collector = FeedCollector(
                storage,
                adapters=[adapter],
                config=CollectorConfig(
                    stale_seconds=5.0,
                    http_timeout_seconds=0.5,
                    ws_receive_timeout_seconds=0.3,
                    rest_poll_interval_seconds=0.05,
                    flush_interval_seconds=0.05,
                ),
                backoff=BackoffPolicy(base_seconds=0.01, max_seconds=0.02, jitter_ratio=0.0),
            )
            breaker = CircuitBreaker(
                name="coinbase",
                failure_threshold=3,
                recovery_timeout_seconds=0.12,
                half_open_success_threshold=1,
            )
            breaker.record_failure()
            collector.breakers["coinbase"] = breaker

            stop_event = asyncio.Event()
            task = asyncio.create_task(
                collector.run(
                    run_id="zero_price_probe",
                    mode="live-sample",
                    duration_seconds=3.0,
                    stop_event=stop_event,
                )
            )
            for _ in range(60):
                if (
                    state["ws_connections"] >= 3
                    and breaker.current_state() == CircuitState.OPEN
                    and state["rest_calls"] >= 1
                ):
                    break
                await asyncio.sleep(0.02)
            stop_event.set()
            await task

            assert state["ws_connections"] >= 2
            assert state["rest_calls"] >= 1
            assert storage.get_ticks("zero_price_probe") == []
            assert breaker.failure_count >= 3
            assert breaker.current_state() != CircuitState.CLOSED
            assert observed_attempts[:2] == [0, 1]
            alerts = storage.get_alerts("zero_price_probe")
            assert any(a.alert_type == "circuit_open" for a in alerts)
            assert not any(a.alert_type == "circuit_closed" for a in alerts)
            assert not any(a.alert_type == "rest_fallback" for a in alerts)
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_heartbeat_only_ws_triggers_stale_event_connection_error_rest_fallback_and_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, int] = {"conn_count": 0, "heartbeats_sent": 0}

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        state["conn_count"] += 1
        conn_idx = state["conn_count"]
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            if conn_idx == 1:
                for _ in range(40):
                    if ws.closed:
                        break
                    try:
                        await ws.send_json({"type": "heartbeat"})
                    except Exception:
                        break
                    state["heartbeats_sent"] += 1
                    await asyncio.sleep(0.03)
                break
            await ws.send_json(
                {
                    "type": "tick",
                    "price": 60755.0,
                    "seq": 101,
                    "ts_ms": 1_795_395_610_000,
                }
            )
            await asyncio.sleep(0.5)
            break
        return ws

    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert runner.addresses
    port = int(runner.addresses[0][1])

    try:
        db_path = tmp_path / "heartbeat_stale.db"
        with MarketStorage(db_path) as storage:
            adapter = _LocalWSAdapter(f"http://127.0.0.1:{port}/ws")
            collector = FeedCollector(
                storage,
                adapters=[adapter],
                config=CollectorConfig(
                    stale_seconds=0.15,
                    http_timeout_seconds=0.3,
                    ws_receive_timeout_seconds=5.0,
                    rest_poll_interval_seconds=0.1,
                    flush_interval_seconds=0.05,
                ),
                backoff=BackoffPolicy(base_seconds=0.02, max_seconds=0.04, jitter_ratio=0.0),
            )

            stale_errors: list[BaseException] = []
            orig_receive = collector._receive_ws_msg

            async def spy_receive(*args: Any, **kwargs: Any) -> aiohttp.WSMessage | None:
                try:
                    return await orig_receive(*args, **kwargs)
                except ConnectionError as exc:
                    stale_errors.append(exc)
                    raise

            monkeypatch.setattr(collector, "_receive_ws_msg", spy_receive)

            stop_event = asyncio.Event()
            task = asyncio.create_task(
                collector.run(
                    run_id="hb_stale_test",
                    mode="live-sample",
                    duration_seconds=3.0,
                    stop_event=stop_event,
                )
            )
            for _ in range(80):
                counts = storage.get_exchange_counts("hb_stale_test")
                cb_counts = counts.get("coinbase", {})
                if cb_counts.get("rest_poll", 0) >= 1 and cb_counts.get("ws", 0) >= 1:
                    break
                await asyncio.sleep(0.03)
            stop_event.set()
            await task

            alerts = storage.get_alerts("hb_stale_test")
            counts = storage.get_exchange_counts("hb_stale_test")
            assert state["heartbeats_sent"] >= 3
            assert any(
                isinstance(err, ConnectionError) and "stale threshold" in str(err)
                for err in stale_errors
            )
            assert any(a.alert_type == "stale_feed" for a in alerts)
            assert any(a.alert_type == "rest_fallback" for a in alerts)
            assert counts["coinbase"]["rest_poll"] >= 1
            assert state["conn_count"] >= 2
            assert counts["coinbase"]["ws"] >= 1
    finally:
        await runner.cleanup()
