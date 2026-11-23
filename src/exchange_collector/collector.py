"""Asynchronous multi-exchange collector with reconnects, REST fallback, and WAL batching."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass

import aiohttp

from exchange_collector.adapters import (
    ExchangeAdapter,
    ExchangeRateLimitError,
    default_adapters,
    resolve_proxy_url,
)
from exchange_collector.models import (
    AlertRecord,
    CaptureRun,
    GapRecord,
    NormalizedTick,
    epoch_ms_to_utc_iso,
    utc_now_ms,
)
from exchange_collector.resilience import (
    BackoffPolicy,
    CircuitBreaker,
    CircuitState,
    DetectedGap,
    GapDetector,
    StaleFeedMonitor,
)
from exchange_collector.storage import MarketStorage

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Runtime tuning knobs for live streaming, gap detection, and WAL flushing."""

    stale_seconds: float = 8.0
    gap_threshold_ms: int = 5000
    sequence_jump_threshold: int = 50
    http_timeout_seconds: float = 8.0
    ws_receive_timeout_seconds: float = 12.0
    rest_poll_interval_seconds: float = 2.0
    flush_interval_seconds: float = 0.5
    flush_batch_size: int = 40
    skew_alert_cooldown_seconds: float = 30.0


class FeedCollector:
    """Orchestrates concurrent WS adapters, REST gap backfill, stale detection, and WAL writes."""

    def __init__(
        self,
        storage: MarketStorage,
        adapters: Sequence[ExchangeAdapter] | None = None,
        config: CollectorConfig | None = None,
        backoff: BackoffPolicy | None = None,
    ) -> None:
        self.storage = storage
        self.adapters = list(adapters) if adapters is not None else default_adapters()
        self.config = config if config is not None else CollectorConfig()
        self.backoff = backoff if backoff is not None else BackoffPolicy()
        self.gap_detector = GapDetector(
            gap_threshold_ms=self.config.gap_threshold_ms,
            sequence_jump_threshold=self.config.sequence_jump_threshold,
        )
        self.stale_monitor = StaleFeedMonitor(stale_seconds=self.config.stale_seconds)
        self.breakers: dict[str, CircuitBreaker] = {
            adapter.exchange: CircuitBreaker(name=adapter.exchange) for adapter in self.adapters
        }
        self._tick_buffer: list[NormalizedTick] = []
        self._buffer_lock = asyncio.Lock()
        self._last_skew_alert_mono: dict[str, float] = {}
        self._backfill_tasks: set[asyncio.Task[None]] = set()

    async def run(
        self,
        *,
        run_id: str,
        mode: str,
        duration_seconds: float | None,
        stop_event: asyncio.Event | None = None,
    ) -> str:
        """Execute market-data collection until `duration_seconds` elapses or `stop_event` fires."""
        external_stop = stop_event if stop_event is not None else asyncio.Event()
        started_ms = utc_now_ms()
        exchanges_str = ",".join(a.exchange for a in self.adapters)
        self.storage.start_run(
            CaptureRun(
                run_id=run_id,
                mode=mode,
                started_ms=started_ms,
                ended_ms=None,
                started_utc=epoch_ms_to_utc_iso(started_ms),
                ended_utc=None,
                exchanges=exchanges_str,
                notes=f"stale={self.config.stale_seconds}s,gap={self.config.gap_threshold_ms}ms",
            )
        )

        proxy_url = resolve_proxy_url()
        internal_stop = asyncio.Event()

        async def _timer() -> None:
            if duration_seconds is not None:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(external_stop.wait(), timeout=duration_seconds)
            else:
                await external_stop.wait()
            internal_stop.set()

        connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            tasks: list[asyncio.Task[None]] = [
                asyncio.create_task(_timer(), name="collector-timer"),
                asyncio.create_task(
                    self._flush_loop(run_id, internal_stop), name="collector-flush"
                ),
                asyncio.create_task(
                    self._watchdog_loop(run_id, internal_stop), name="collector-watchdog"
                ),
            ]
            for adapter in self.adapters:
                self.stale_monitor.register_stream(adapter.exchange, adapter.canonical_symbol)
                tasks.append(
                    asyncio.create_task(
                        self._run_adapter_loop(
                            run_id=run_id,
                            adapter=adapter,
                            session=session,
                            proxy_url=proxy_url,
                            stop=internal_stop,
                        ),
                        name=f"stream-{adapter.exchange}",
                    )
                )

            await internal_stop.wait()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            if self._backfill_tasks:
                await asyncio.gather(*self._backfill_tasks, return_exceptions=True)

        await self._flush_pending_ticks(run_id)
        ended_ms = utc_now_ms()
        self.storage.finish_run(run_id, ended_ms, epoch_ms_to_utc_iso(ended_ms))
        return run_id

    async def _run_adapter_loop(
        self,
        *,
        run_id: str,
        adapter: ExchangeAdapter,
        session: aiohttp.ClientSession,
        proxy_url: str | None,
        stop: asyncio.Event,
    ) -> None:
        breaker = self.breakers[adapter.exchange]
        attempt = 0
        last_disconnect_ms: int | None = None

        while not stop.is_set():
            if not breaker.allow_request():
                await self._poll_rest_fallback(run_id, adapter, session, proxy_url, stop)
                continue

            try:
                async with session.ws_connect(
                    adapter.ws_url,
                    proxy=proxy_url,
                    heartbeat=15.0,
                    timeout=aiohttp.ClientWSTimeout(
                        ws_receive=self.config.ws_receive_timeout_seconds,
                        ws_close=5.0,
                    ),
                ) as ws:
                    await ws.send_json(adapter.ws_subscribe_payload())
                    prev_state = breaker.current_state()
                    new_state = breaker.record_success()
                    attempt = 0

                    if prev_state == CircuitState.HALF_OPEN and new_state == CircuitState.CLOSED:
                        self._emit_alert(
                            run_id=run_id,
                            exchange=adapter.exchange,
                            symbol=adapter.canonical_symbol,
                            severity="info",
                            alert_type="circuit_closed",
                            message="WebSocket circuit breaker recovered to CLOSED",
                            metric_value=None,
                            threshold=None,
                        )

                    if last_disconnect_ms is not None:
                        reconnect_ms = utc_now_ms()
                        gap = DetectedGap(
                            exchange=adapter.exchange,
                            symbol=adapter.canonical_symbol,
                            gap_type="disconnect",
                            start_ts_ms=last_disconnect_ms,
                            end_ts_ms=reconnect_ms,
                            prev_seq=None,
                            curr_seq=None,
                        )
                        self._schedule_backfill(run_id, adapter, session, proxy_url, gap)
                        last_disconnect_ms = None

                    await self._consume_ws(run_id, adapter, ws, session, proxy_url, stop)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("WS stream error for %s: %s", adapter.exchange, exc)
                if last_disconnect_ms is None:
                    last_disconnect_ms = utc_now_ms()
                prev_state = breaker.current_state()
                new_state = breaker.record_failure()
                if prev_state != CircuitState.OPEN and new_state == CircuitState.OPEN:
                    self._emit_alert(
                        run_id=run_id,
                        exchange=adapter.exchange,
                        symbol=adapter.canonical_symbol,
                        severity="warning",
                        alert_type="circuit_open",
                        message=f"Circuit breaker OPEN after consecutive failures: {exc}",
                        metric_value=float(breaker.failure_count),
                        threshold=float(breaker.failure_threshold),
                    )
                delay = self.backoff.delay_for_attempt(attempt)
                attempt += 1
                await self._rest_fallback_once(run_id, adapter, session, proxy_url)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)

    async def _consume_ws(
        self,
        run_id: str,
        adapter: ExchangeAdapter,
        ws: aiohttp.ClientWebSocketResponse,
        session: aiohttp.ClientSession,
        proxy_url: str | None,
        stop: asyncio.Event,
    ) -> None:
        last_ping = time.monotonic()
        ping_payload = adapter.ws_ping_payload()

        while not stop.is_set():
            if ping_payload is not None and time.monotonic() - last_ping >= 15.0:
                if isinstance(ping_payload, str):
                    await ws.send_str(ping_payload)
                else:
                    await ws.send_json(ping_payload)
                last_ping = time.monotonic()

            msg = await asyncio.wait_for(
                ws.receive(), timeout=self.config.ws_receive_timeout_seconds
            )
            received_ms = utc_now_ms()
            if msg.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.ERROR,
            }:
                raise ConnectionError(f"{adapter.exchange} WS closed with frame {msg.type}")
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue

            text = msg.data
            if adapter.is_heartbeat_or_control(text):
                self.stale_monitor.record_activity(
                    adapter.exchange, adapter.canonical_symbol, received_ms, received_ms
                )
                continue

            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue

            if not isinstance(payload, dict):
                continue
            if adapter.is_heartbeat_or_control(payload):
                self.stale_monitor.record_activity(
                    adapter.exchange, adapter.canonical_symbol, received_ms, received_ms
                )
                continue

            ticks = adapter.parse_ws_payload(payload, received_ms)
            for tick in ticks:
                await self._ingest_tick(run_id, adapter, tick, session, proxy_url)

    async def _ingest_tick(
        self,
        run_id: str,
        adapter: ExchangeAdapter,
        tick: NormalizedTick,
        session: aiohttp.ClientSession,
        proxy_url: str | None,
    ) -> None:
        skew = self.stale_monitor.record_activity(
            tick.exchange, tick.symbol, tick.ts_ms, tick.received_ms
        )
        if skew.is_skewed:
            now_mono = time.monotonic()
            last_alert = self._last_skew_alert_mono.get(tick.exchange, 0.0)
            if now_mono - last_alert >= self.config.skew_alert_cooldown_seconds:
                self._last_skew_alert_mono[tick.exchange] = now_mono
                self._emit_alert(
                    run_id=run_id,
                    exchange=tick.exchange,
                    symbol=tick.symbol,
                    severity="info",
                    alert_type="clock_skew",
                    message=(
                        f"Exchange event clock ahead of local wall clock by {skew.skew_ms} ms; "
                        "monotonic freshness preserved"
                    ),
                    metric_value=float(skew.skew_ms),
                    threshold=float(self.stale_monitor.skew_tolerance_ms),
                )

        gap = self.gap_detector.observe(tick)
        if gap is not None:
            self._schedule_backfill(run_id, adapter, session, proxy_url, gap)

        should_flush = False
        async with self._buffer_lock:
            self._tick_buffer.append(tick)
            if len(self._tick_buffer) >= self.config.flush_batch_size:
                should_flush = True
        if should_flush:
            await self._flush_pending_ticks(run_id)

    def _schedule_backfill(
        self,
        run_id: str,
        adapter: ExchangeAdapter,
        session: aiohttp.ClientSession,
        proxy_url: str | None,
        gap: DetectedGap,
    ) -> None:
        async def _run_backfill() -> None:
            backfilled_rows = 0
            status = "failed"
            try:
                ticks = await adapter.fetch_rest_backfill(
                    session,
                    gap.start_ts_ms,
                    gap.end_ts_ms,
                    self.config.http_timeout_seconds,
                    proxy_url,
                )
                if ticks:
                    backfilled_rows = self.storage.upsert_ticks(run_id, ticks)
                    status = "recovered"
                else:
                    status = "partial"
            except Exception as exc:
                logger.warning("REST backfill failed for %s: %s", adapter.exchange, exc)
                status = "failed"
            self.storage.insert_gap(
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
                    backfill_status=status,
                    backfilled_rows=backfilled_rows,
                    detected_at_utc=epoch_ms_to_utc_iso(utc_now_ms()),
                )
            )

        task = asyncio.create_task(_run_backfill(), name=f"backfill-{adapter.exchange}")
        self._backfill_tasks.add(task)
        task.add_done_callback(self._backfill_tasks.discard)

    async def _poll_rest_fallback(
        self,
        run_id: str,
        adapter: ExchangeAdapter,
        session: aiohttp.ClientSession,
        proxy_url: str | None,
        stop: asyncio.Event,
    ) -> None:
        await self._rest_fallback_once(run_id, adapter, session, proxy_url)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=self.config.rest_poll_interval_seconds)

    async def _rest_fallback_once(
        self,
        run_id: str,
        adapter: ExchangeAdapter,
        session: aiohttp.ClientSession,
        proxy_url: str | None,
    ) -> None:
        try:
            tick = await adapter.fetch_rest_ticker(
                session, self.config.http_timeout_seconds, proxy_url
            )
            if tick is not None:
                async with self._buffer_lock:
                    self._tick_buffer.append(tick)
                self.stale_monitor.record_activity(
                    tick.exchange, tick.symbol, tick.ts_ms, tick.received_ms
                )
                self._emit_alert(
                    run_id=run_id,
                    exchange=tick.exchange,
                    symbol=tick.symbol,
                    severity="info",
                    alert_type="rest_fallback",
                    message="Ingested REST fallback snapshot while WS stream reconnecting",
                    metric_value=tick.price,
                    threshold=None,
                )
        except ExchangeRateLimitError as exc:
            await asyncio.sleep(min(exc.retry_after_seconds, 5.0))
        except Exception as exc:
            logger.debug("REST fallback error for %s: %s", adapter.exchange, exc)

    async def _watchdog_loop(self, run_id: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            if stop.is_set():
                break
            for adapter in self.adapters:
                stale_elapsed = self.stale_monitor.check_stale(
                    adapter.exchange, adapter.canonical_symbol
                )
                if stale_elapsed is not None:
                    self._emit_alert(
                        run_id=run_id,
                        exchange=adapter.exchange,
                        symbol=adapter.canonical_symbol,
                        severity="warning",
                        alert_type="stale_feed",
                        message=(
                            f"Feed silent for {stale_elapsed:.2f}s "
                            f"(monotonic threshold {self.config.stale_seconds:.1f}s)"
                        ),
                        metric_value=round(stale_elapsed, 2),
                        threshold=self.config.stale_seconds,
                    )

    async def _flush_loop(self, run_id: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.config.flush_interval_seconds)
            await self._flush_pending_ticks(run_id)

    async def _flush_pending_ticks(self, run_id: str) -> None:
        async with self._buffer_lock:
            if not self._tick_buffer:
                return
            batch = self._tick_buffer[:]
            self._tick_buffer.clear()
        self.storage.upsert_ticks(run_id, batch)

    def _emit_alert(
        self,
        *,
        run_id: str,
        exchange: str,
        symbol: str,
        severity: str,
        alert_type: str,
        message: str,
        metric_value: float | None,
        threshold: float | None,
    ) -> None:
        now_ms = utc_now_ms()
        self.storage.insert_alerts(
            [
                AlertRecord(
                    run_id=run_id,
                    exchange=exchange,
                    symbol=symbol,
                    severity=severity,
                    alert_type=alert_type,
                    message=message,
                    metric_value=metric_value,
                    threshold=threshold,
                    ts_ms=now_ms,
                    ts_utc=epoch_ms_to_utc_iso(now_ms),
                )
            ]
        )
