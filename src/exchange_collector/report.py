"""Standalone HTML report and publication-grade PNG chart generator."""

from __future__ import annotations

import base64
import html
from datetime import UTC, datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from PIL import Image

from exchange_collector.analytics import AnalysisResult

BG_COLOR = "#0B1220"
PANEL_COLOR = "#111A2E"
BORDER_COLOR = "#233252"
TEXT_PRIMARY = "#F1F5F9"
TEXT_MUTED = "#94A3B8"
COINBASE_COLOR = "#38BDF8"
KRAKEN_COLOR = "#34D399"
OKX_COLOR = "#F59E0B"
SPREAD_COLOR = "#60A5FA"
BASIS_COLOR = "#FB923C"
ALERT_COLOR = "#F43F5E"

VENUE_LABELS = {
    "coinbase": "Coinbase",
    "kraken": "Kraken",
    "okx": "OKX",
}


def _ts_ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)


def _style_axis(ax: Axes) -> None:
    ax.set_facecolor(PANEL_COLOR)
    ax.tick_params(colors=TEXT_MUTED, labelsize=9)
    for spine in ax.spines.values():
        spine.set_color(BORDER_COLOR)
    ax.grid(True, color=BORDER_COLOR, linestyle="--", linewidth=0.6, alpha=0.55)


def _plot_price_series(ax: Axes, analysis: AnalysisResult) -> None:
    _style_axis(ax)
    buckets = analysis.spread_buckets
    if not buckets:
        ax.text(0.5, 0.5, "No overlapping ticks", color=TEXT_MUTED, ha="center", va="center")
        return

    cb_x = [_ts_ms_to_dt(b.bucket_ts_ms) for b in buckets if b.coinbase_mid is not None]
    cb_y = [b.coinbase_mid for b in buckets if b.coinbase_mid is not None]
    kr_x = [_ts_ms_to_dt(b.bucket_ts_ms) for b in buckets if b.kraken_mid is not None]
    kr_y = [b.kraken_mid for b in buckets if b.kraken_mid is not None]
    okx_x = [_ts_ms_to_dt(b.bucket_ts_ms) for b in buckets if b.okx_usdt_mid is not None]
    okx_y = [b.okx_usdt_mid for b in buckets if b.okx_usdt_mid is not None]

    if cb_x:
        ax.plot(cb_x, cb_y, label="Coinbase (BTC-USD)", color=COINBASE_COLOR, linewidth=1.9)
    if kr_x:
        ax.plot(kr_x, kr_y, label="Kraken (XBT/USD -> BTC-USD)", color=KRAKEN_COLOR, linewidth=1.9)
    if okx_x:
        ax.plot(
            okx_x,
            okx_y,
            label="OKX (BTC-USDT · USDT quote)",
            color=OKX_COLOR,
            linewidth=1.8,
            linestyle="--",
        )

    ax.set_title(
        "Normalized Spot Midpoints (Explicit USD vs USDT Quote)",
        color=TEXT_PRIMARY,
        fontsize=11.5,
        fontweight="bold",
        loc="left",
        pad=10,
    )
    ax.set_ylabel("Midpoint Price", color=TEXT_MUTED, fontsize=9.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))  # type: ignore[no-untyped-call]
    ax.ticklabel_format(axis="y", useOffset=False, style="plain")
    all_y = [float(v) for v in (*cb_y, *kr_y, *okx_y) if v is not None]
    if all_y:
        ymin, ymax = min(all_y), max(all_y)
        pad = max(1.0, ymax - ymin)
        ax.set_ylim(ymin - 0.10 * pad, ymax + 0.34 * pad)
    ax.legend(
        loc="upper left",
        facecolor=BG_COLOR,
        edgecolor=BORDER_COLOR,
        labelcolor=TEXT_PRIMARY,
        fontsize=8.5,
        framealpha=0.92,
    )


def _plot_spread_series(ax: Axes, analysis: AnalysisResult) -> None:
    _style_axis(ax)
    buckets = analysis.spread_buckets
    usd_pts = [
        (_ts_ms_to_dt(b.bucket_ts_ms), b.usd_spread_bps)
        for b in buckets
        if b.usd_spread_bps is not None
    ]
    usdt_pts = [
        (_ts_ms_to_dt(b.bucket_ts_ms), b.usdt_basis_bps)
        for b in buckets
        if b.usdt_basis_bps is not None
    ]

    if usd_pts:
        xs, ys = zip(*usd_pts, strict=True)
        ax.plot(xs, ys, label="USD-USD Spread (Kraken - Coinbase, bps)", color=SPREAD_COLOR, lw=1.8)
        ax.fill_between(xs, ys, 0.0, color=SPREAD_COLOR, alpha=0.16)
    if usdt_pts:
        xs_b, ys_b = zip(*usdt_pts, strict=True)
        ax.plot(
            xs_b,
            ys_b,
            label="USDT-USD Basis (OKX USDT - USD Ref, bps)",
            color=BASIS_COLOR,
            lw=1.7,
            linestyle="-.",
        )

    ax.axhline(0.0, color=TEXT_MUTED, linewidth=0.9, alpha=0.7)
    all_bps = [
        float(v)
        for v in (
            *(p[1] for p in usd_pts),
            *(p[1] for p in usdt_pts),
        )
        if v is not None
    ]
    if all_bps:
        bmin, bmax = min(0.0, min(all_bps)), max(0.0, max(all_bps))
        bpad = max(0.5, bmax - bmin)
        ax.set_ylim(bmin - 0.10 * bpad, bmax + 0.32 * bpad)
    ax.set_title(
        "Cross-Exchange Spread & USDT/USD Basis (bps)",
        color=TEXT_PRIMARY,
        fontsize=11.5,
        fontweight="bold",
        loc="left",
        pad=10,
    )
    ax.set_ylabel("Basis Points (bps)", color=TEXT_MUTED, fontsize=9.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))  # type: ignore[no-untyped-call]
    ax.legend(
        loc="upper left",
        facecolor=BG_COLOR,
        edgecolor=BORDER_COLOR,
        labelcolor=TEXT_PRIMARY,
        fontsize=8.5,
        framealpha=0.92,
    )


def _plot_latency_bars(ax: Axes, analysis: AnalysisResult) -> None:
    _style_axis(ax)
    exchanges = ["coinbase", "kraken", "okx"]
    present = [e for e in exchanges if e in analysis.latency_by_exchange]
    if not present:
        ax.text(0.5, 0.5, "No latency samples", color=TEXT_MUTED, ha="center", va="center")
        return

    x = np.arange(len(present))
    width = 0.25
    p50_vals = [analysis.latency_by_exchange[e]["p50"] for e in present]
    p95_vals = [analysis.latency_by_exchange[e]["p95"] for e in present]
    max_vals = [analysis.latency_by_exchange[e]["max"] for e in present]

    bars_p50 = ax.bar(x - width, p50_vals, width, label="p50 (ms)", color=COINBASE_COLOR)
    bars_p95 = ax.bar(x, p95_vals, width, label="p95 (ms)", color=KRAKEN_COLOR)
    bars_max = ax.bar(x + width, max_vals, width, label="max (ms)", color=OKX_COLOR)

    for bars in (bars_p50, bars_p95, bars_max):
        for bar in bars:
            h = float(bar.get_height())
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                h,
                f"{h:.0f}",
                ha="center",
                va="bottom",
                color=TEXT_PRIMARY,
                fontsize=8,
            )

    labels: list[str] = []
    for e in present:
        m = analysis.latency_by_exchange[e]
        non_skewed = int(m.get("non_skewed_count", m["count"]))
        raw_cnt = int(m.get("raw_count", m["count"]))
        skewed = int(m.get("skewed_count", 0.0))
        venue = VENUE_LABELS.get(e, e.title())
        if skewed > 0:
            labels.append(f"{venue}\n(n={non_skewed}/{raw_cnt}, skew={skewed})")
        else:
            labels.append(f"{venue}\n(n={non_skewed})")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, color=TEXT_PRIMARY, fontsize=8.5)
    max_y = max(max_vals) if max_vals else 10.0
    ax.set_ylim(0, max(10.0, max_y * 1.18))
    ax.set_title(
        "Ingest Latency Percentiles (Non-Skewed Samples >= 0 ms)",
        color=TEXT_PRIMARY,
        fontsize=11.5,
        fontweight="bold",
        loc="left",
        pad=10,
    )
    ax.set_ylabel("Milliseconds (ms)", color=TEXT_MUTED, fontsize=9.5)
    ax.legend(
        loc="upper left",
        facecolor=BG_COLOR,
        edgecolor=BORDER_COLOR,
        labelcolor=TEXT_PRIMARY,
        fontsize=8.5,
    )


def _plot_heatmap(ax: Axes, analysis: AnalysisResult) -> None:
    ax.set_facecolor(PANEL_COLOR)
    for spine in ax.spines.values():
        spine.set_color(BORDER_COLOR)

    slices = analysis.heatmap_slices
    if not slices:
        ax.text(0.5, 0.5, "No slice telemetry", color=TEXT_MUTED, ha="center", va="center")
        return

    exchanges = ["coinbase", "kraken", "okx"]
    row_labels = ["Coinbase p95 ms", "Kraken p95 ms", "OKX p95 ms", "USD Spread |bps|"]
    raw_matrix = np.zeros((4, len(slices)), dtype=float)
    text_grid: list[list[str]] = [["-" for _ in slices] for _ in range(4)]

    for col, sl in enumerate(slices):
        for r_idx, ex in enumerate(exchanges):
            val = sl.p95_latency_by_exchange.get(ex)
            if val is not None:
                raw_matrix[r_idx, col] = val
                text_grid[r_idx][col] = f"{val:.0f}"
        if sl.mean_usd_spread_bps is not None:
            raw_matrix[3, col] = sl.mean_usd_spread_bps
            text_grid[3][col] = f"{sl.mean_usd_spread_bps:.2f}"

    # Normalize latency rows jointly and spread row independently so both remain readable.
    norm_matrix = np.zeros_like(raw_matrix)
    lat_max = float(np.max(raw_matrix[:3, :])) if np.max(raw_matrix[:3, :]) > 0 else 1.0
    spread_max = float(np.max(raw_matrix[3, :])) if np.max(raw_matrix[3, :]) > 0 else 1.0
    norm_matrix[:3, :] = raw_matrix[:3, :] / lat_max
    norm_matrix[3, :] = raw_matrix[3, :] / spread_max

    cmap = plt.get_cmap("viridis")
    ax.imshow(norm_matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=1.0)

    for r_idx in range(4):
        for c_idx in range(len(slices)):
            rgba = cmap(float(norm_matrix[r_idx, c_idx]))
            luminance = 0.299 * rgba[0] + 0.587 * rgba[1] + 0.114 * rgba[2]
            txt_color = "#0B1220" if luminance > 0.56 else TEXT_PRIMARY
            ax.text(
                c_idx,
                r_idx,
                text_grid[r_idx][c_idx],
                ha="center",
                va="center",
                color=txt_color,
                fontsize=8,
                fontweight="bold",
            )

    ax.set_yticks(np.arange(4))
    ax.set_yticklabels(row_labels, color=TEXT_PRIMARY, fontsize=8.5)
    ax.set_xticks(np.arange(len(slices)))
    ax.set_xticklabels(
        [sl.label for sl in slices], color=TEXT_MUTED, fontsize=7.5, rotation=28, ha="right"
    )
    ax.set_title(
        "Time-Slice Latency & Spread Intensity Heatmap (UTC)",
        color=TEXT_PRIMARY,
        fontsize=11.5,
        fontweight="bold",
        loc="left",
        pad=10,
    )


def render_overview_chart(analysis: AnalysisResult, out_path: Path) -> Path:
    """Render the 4-panel technical overview PNG chart."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig: Figure = plt.figure(figsize=(15.0, 9.5), dpi=140, facecolor=BG_COLOR)
    gs = fig.add_gridspec(
        2, 2, hspace=0.34, wspace=0.22, left=0.06, right=0.97, top=0.87, bottom=0.09
    )

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1, 0])
    ax4 = fig.add_subplot(gs[1, 1])

    _plot_price_series(ax1, analysis)
    _plot_spread_series(ax2, analysis)
    _plot_latency_bars(ax3, analysis)
    _plot_heatmap(ax4, analysis)

    end_utc = analysis.run.ended_utc or "active"
    usd_str = (
        f"USD spread mean {analysis.usd_spread_summary.mean_abs_bps:.2f} bps "
        f"(max {analysis.usd_spread_summary.max_abs_bps:.2f})"
        if analysis.usd_spread_summary
        else "USD spread n/a"
    )
    usdt_str = (
        f"USDT basis mean {analysis.usdt_basis_summary.mean_delta:+.2f} "
        f"({analysis.usdt_basis_summary.mean_abs_bps:.2f} bps)"
        if analysis.usdt_basis_summary
        else "USDT basis n/a"
    )
    fig.suptitle(
        f"Exchange Feed Collector · {analysis.run.mode} ({analysis.run.run_id})",
        color=TEXT_PRIMARY,
        fontsize=15.5,
        fontweight="bold",
        x=0.06,
        y=0.96,
        ha="left",
    )
    fig.text(
        0.06,
        0.922,
        (
            f"UTC: {analysis.run.started_utc} -> {end_utc}   |   "
            f"Ticks: {analysis.total_ticks}   |   {usd_str}   |   {usdt_str}   |   "
            f"Gaps: {len(analysis.gaps)}   Alerts: {len(analysis.alerts)}"
        ),
        color=TEXT_MUTED,
        fontsize=9.5,
        ha="left",
    )

    fig.savefig(out_path, facecolor=BG_COLOR, dpi=140)
    plt.close(fig)
    return out_path


def render_cover_1600x1200(analysis: AnalysisResult, out_path: Path) -> Path:
    """Render an exact `1600x1200` pixel portfolio cover PNG verified with Pillow."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig: Figure = plt.figure(figsize=(16.0, 12.0), dpi=100, facecolor=BG_COLOR)

    # Top banner & KPI card layout using figure coordinates so nothing collides.
    fig.text(
        0.045,
        0.945,
        "Multi-Exchange Market Data Collector",
        color=TEXT_PRIMARY,
        fontsize=24,
        fontweight="bold",
        ha="left",
        va="top",
    )
    end_utc = analysis.run.ended_utc or "active"
    fig.text(
        0.045,
        0.903,
        (
            f"Reference implementation · live public exchange sample + deterministic offline replay"
            f"   |   Window: {analysis.run.started_utc} -> {end_utc}"
        ),
        color=TEXT_MUTED,
        fontsize=11.5,
        ha="left",
        va="top",
    )

    usd_sum = analysis.usd_spread_summary
    usdt_sum = analysis.usdt_basis_summary
    peak_p95 = max(
        (m["p95"] for m in analysis.latency_by_exchange.values()),
        default=0.0,
    )

    cards = [
        (
            "TOTAL INGESTED TICKS",
            f"{analysis.total_ticks:,}",
            "Coinbase · Kraken · OKX (SQLite WAL)",
            COINBASE_COLOR,
        ),
        (
            "SAME-QUOTE USD SPREAD",
            f"{usd_sum.mean_abs_bps:.2f} bps" if usd_sum else "n/a",
            f"Max {usd_sum.max_abs_bps:.2f} bps ({usd_sum.overlapping_buckets} buckets)"
            if usd_sum
            else "No overlap",
            KRAKEN_COLOR,
        ),
        (
            "CROSS-QUOTE USDT BASIS",
            f"{usdt_sum.mean_delta:+.2f}" if usdt_sum else "n/a",
            f"{usdt_sum.mean_abs_bps:.2f} bps (OKX USDT vs USD ref)"
            if usdt_sum
            else "No overlap",
            OKX_COLOR,
        ),
        (
            "PEAK P95 INGEST LATENCY",
            f"{peak_p95:.1f} ms",
            f"Gaps: {len(analysis.gaps)}  |  Alerts: {len(analysis.alerts)}",
            SPREAD_COLOR,
        ),
    ]

    card_left = 0.045
    card_w = 0.215
    card_gap = 0.016
    card_bottom = 0.755
    card_h = 0.115
    for idx, (label, value, sub, accent) in enumerate(cards):
        x0 = card_left + idx * (card_w + card_gap)
        ax_card = fig.add_axes((x0, card_bottom, card_w, card_h), facecolor=PANEL_COLOR)
        ax_card.set_xticks([])
        ax_card.set_yticks([])
        for side, spine in ax_card.spines.items():
            spine.set_color(accent if side == "top" else BORDER_COLOR)
            spine.set_linewidth(3.0 if side == "top" else 1.2)
        ax_card.text(
            0.07,
            0.80,
            label,
            color=TEXT_MUTED,
            fontsize=9.5,
            fontweight="bold",
            transform=ax_card.transAxes,
        )
        ax_card.text(
            0.07,
            0.40,
            value,
            color=TEXT_PRIMARY,
            fontsize=20,
            fontweight="bold",
            transform=ax_card.transAxes,
        )
        ax_card.text(
            0.07,
            0.13,
            sub,
            color=TEXT_MUTED,
            fontsize=8.8,
            transform=ax_card.transAxes,
        )

    gs = fig.add_gridspec(
        2, 2, hspace=0.33, wspace=0.20, left=0.055, right=0.965, top=0.71, bottom=0.095
    )
    _plot_price_series(fig.add_subplot(gs[0, 0]), analysis)
    _plot_spread_series(fig.add_subplot(gs[0, 1]), analysis)
    _plot_latency_bars(fig.add_subplot(gs[1, 0]), analysis)
    _plot_heatmap(fig.add_subplot(gs[1, 1]), analysis)

    fig.text(
        0.045,
        0.028,
        (
            "Stack: Python asyncio + aiohttp  ·  SQLite WAL INSERT OR IGNORE dedup  ·  "
            "Exponential backoff + circuit breaker  ·  REST gap backfill  ·  "
            "Explicit USD vs USDT quote normalization"
        ),
        color=TEXT_MUTED,
        fontsize=10.0,
        ha="left",
    )

    fig.savefig(out_path, facecolor=BG_COLOR, dpi=100)
    plt.close(fig)

    with Image.open(out_path) as img:
        if img.size != (1600, 1200):
            resized = img.resize((1600, 1200), Image.Resampling.LANCZOS)
            resized.save(out_path, format="PNG")
    return out_path


def render_html_report(
    analysis: AnalysisResult,
    overview_png_path: Path,
    html_out_path: Path,
) -> Path:
    """Write a self-contained HTML report with embedded PNG chart and exact tables."""
    html_out_path.parent.mkdir(parents=True, exist_ok=True)
    b64_chart = base64.b64encode(overview_png_path.read_bytes()).decode("ascii")

    venue_rows = []
    for ex in sorted(analysis.exchange_counts):
        sources = analysis.exchange_counts[ex]
        total_ex = sum(sources.values())
        source_desc = ", ".join(f"{k}: {v}" for k, v in sorted(sources.items()))
        lat = analysis.latency_by_exchange.get(ex, {})
        non_skewed = int(lat.get("non_skewed_count", lat.get("count", 0.0)))
        raw_cnt = int(lat.get("raw_count", lat.get("count", 0.0)))
        skewed = int(lat.get("skewed_count", 0.0))
        skew_cell = f"{skewed} (min {lat.get('raw_min', 0.0):.1f} ms)" if skewed > 0 else "0"
        venue_rows.append(
            "<tr>"
            f"<td>{html.escape(ex)}</td>"
            f"<td>{total_ex}</td>"
            f"<td>{html.escape(source_desc)}</td>"
            f"<td>{non_skewed} / {raw_cnt}</td>"
            f"<td>{lat.get('p50', 0.0):.2f} ms</td>"
            f"<td>{lat.get('p95', 0.0):.2f} ms</td>"
            f"<td>{lat.get('max', 0.0):.2f} ms</td>"
            f"<td>{html.escape(skew_cell)}</td>"
            "</tr>"
        )

    gap_rows = []
    for g in analysis.gaps:
        gap_rows.append(
            "<tr>"
            f"<td>{html.escape(g.detected_at_utc)}</td>"
            f"<td>{html.escape(g.exchange)}</td>"
            f"<td>{html.escape(g.gap_type)}</td>"
            f"<td>{g.duration_ms} ms</td>"
            f"<td>{html.escape(g.backfill_status)}</td>"
            f"<td>{g.backfilled_rows}</td>"
            "</tr>"
        )
    if not gap_rows:
        gap_rows.append('<tr><td colspan="6">No gaps detected in this run.</td></tr>')

    alert_rows = []
    for a in analysis.alerts[:50]:
        alert_rows.append(
            "<tr>"
            f"<td>{html.escape(a.ts_utc)}</td>"
            f"<td>{html.escape(a.severity)}</td>"
            f"<td>{html.escape(a.exchange)}</td>"
            f"<td>{html.escape(a.alert_type)}</td>"
            f"<td>{html.escape(a.message)}</td>"
            "</tr>"
        )
    if not alert_rows:
        alert_rows.append('<tr><td colspan="5">No operational or spread alerts emitted.</td></tr>')

    usd_s = analysis.usd_spread_summary
    usdt_s = analysis.usdt_basis_summary
    usd_summary_html = (
        f"{usd_s.mean_abs_bps:.2f} bps abs mean ({usd_s.mean_signed_bps:+.2f} signed) | "
        f"{usd_s.p95_abs_bps:.2f} bps p95 | "
        f"{usd_s.max_abs_bps:.2f} bps max ({usd_s.overlapping_buckets} buckets)"
        if usd_s
        else "No overlapping USD buckets"
    )
    usdt_summary_html = (
        f"{usdt_s.mean_delta:+.2f} mean (${usdt_s.min_delta:+.2f} to ${usdt_s.max_delta:+.2f}, "
        f"{usdt_s.mean_abs_bps:.2f} bps abs / {usdt_s.mean_signed_bps:+.2f} signed)"
        if usdt_s
        else "No overlapping USDT/USD buckets"
    )

    document = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Exchange Feed Collector Report — {html.escape(analysis.run.run_id)}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0b1220; color: #f1f5f9; margin: 0; padding: 32px; line-height: 1.5;
  }}
  .wrap {{ max-width: 1220px; margin: 0 auto; }}
  h1 {{ margin: 0 0 6px; font-size: 26px; }}
  .sub {{ color: #94a3b8; margin-bottom: 24px; font-size: 14px; }}
  .grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 14px; margin-bottom: 24px;
  }}
  .card {{
    background: #111a2e; border: 1px solid #233252;
    border-radius: 10px; padding: 14px 16px;
  }}
  .card .k {{
    color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em;
  }}
  .card .v {{ font-size: 20px; font-weight: 700; margin-top: 4px; }}
  img {{ width: 100%; border-radius: 10px; border: 1px solid #233252; margin-bottom: 24px; }}
  h2 {{ font-size: 18px; margin: 24px 0 10px; }}
  table {{
    width: 100%; border-collapse: collapse; background: #111a2e;
    border: 1px solid #233252; margin-bottom: 20px;
  }}
  th, td {{
    padding: 10px 12px; border-bottom: 1px solid #233252; text-align: left; font-size: 13px;
  }}
  th {{ color: #94a3b8; background: #0f172a; font-weight: 600; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>Exchange Feed Collector Report</h1>
  <div class="sub">
    Run: <strong>{html.escape(analysis.run.run_id)}</strong> |
    Mode: <strong>{html.escape(analysis.run.mode)}</strong> |
    Window (UTC): <strong>{html.escape(analysis.run.started_utc)}</strong> &rarr;
    <strong>{html.escape(analysis.run.ended_utc or 'active')}</strong>
  </div>
  <div class="grid">
    <div class="card">
      <div class="k">Total Normalized Ticks</div>
      <div class="v">{analysis.total_ticks}</div>
    </div>
    <div class="card">
      <div class="k">Same-Quote USD Spread</div>
      <div class="v" style="font-size:15px">{html.escape(usd_summary_html)}</div>
    </div>
    <div class="card">
      <div class="k">Cross-Quote USDT Basis</div>
      <div class="v" style="font-size:15px">{html.escape(usdt_summary_html)}</div>
    </div>
    <div class="card">
      <div class="k">Gaps / Alerts</div>
      <div class="v">{len(analysis.gaps)} gaps / {len(analysis.alerts)} alerts</div>
    </div>
  </div>
  <img src="data:image/png;base64,{b64_chart}" alt="Exchange overview chart" />
  <h2>Venue Ingestion &amp; Latency Summary</h2>
  <table>
    <thead>
      <tr>
        <th>Exchange</th><th>Ticks</th><th>Sources</th><th>Non-Skewed Samples</th>
        <th>p50 (&ge;0 ms)</th><th>p95 (&ge;0 ms)</th><th>Max (&ge;0 ms)</th>
        <th>Clock Skew (&lt;0 ms)</th>
      </tr>
    </thead>
    <tbody>{''.join(venue_rows)}</tbody>
  </table>
  <h2>Gap Detection &amp; REST Backfill</h2>
  <table>
    <thead>
      <tr>
        <th>Detected UTC</th><th>Exchange</th><th>Type</th>
        <th>Duration</th><th>Status</th><th>Backfilled Rows</th>
      </tr>
    </thead>
    <tbody>{''.join(gap_rows)}</tbody>
  </table>
  <h2>Alerts &amp; Anomaly Log</h2>
  <table>
    <thead>
      <tr>
        <th>Timestamp UTC</th><th>Severity</th><th>Venue</th><th>Type</th><th>Message</th>
      </tr>
    </thead>
    <tbody>{''.join(alert_rows)}</tbody>
  </table>
</div>
</body>
</html>
"""
    html_out_path.write_text(document, encoding="utf-8")
    return html_out_path
