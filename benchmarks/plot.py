"""Publication-grade plotting suite for Distributed Object Store benchmarks.

Reads latest (or specified) CSVs from benchmarks/results/ and saves 300-DPI PNGs
to benchmarks/plots/ with finding-based titles, error bars, event markers, and
hardware/DFS provenance footers.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "#fbfcfd",
    "axes.edgecolor": "#cbd5e1",
    "axes.grid": True,
    "grid.color": "#e2e8f0",
    "grid.linestyle": "--",
    "grid.linewidth": 0.7,
    "font.size": 10,
    "axes.titlesize": 11.5,
    "axes.titleweight": "bold",
    "figure.titlesize": 13,
    "figure.titleweight": "bold",
    "legend.framealpha": 0.92,
})

BLUE = "#1d4ed8"
TEAL = "#0f766e"
ORANGE = "#d97706"
RED = "#dc2626"
SLATE = "#475569"
PURPLE = "#7c3aed"


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _add_footer(fig: plt.Figure, rows: List[Dict[str, str]], extra: str = "") -> None:
    if not rows:
        return
    r0 = rows[0]
    env = r0.get("env_summary", "")
    commit = r0.get("git_commit", "")
    cfg = r0.get("config", "")
    line1 = f"Env: {env} | git={commit}"
    if extra:
        line1 = f"{extra} | {line1}"
    line2 = f"DFS Config: {cfg}" if cfg else ""
    text = f"{line1}\n{line2}".strip()
    fig.text(0.5, 0.01, text, ha="center", va="bottom", fontsize=7.5, color="#475569")


def _save(fig: plt.Figure, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {out_path}", flush=True)
    return out_path


# ------------------------------------------------------------------ Plot 1
def plot_exp1(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    rows = _read_csv(results_dir / "exp1_throughput_latest.csv")
    if not rows:
        return None

    up = sorted([r for r in rows if r["op"] == "upload"], key=lambda r: int(r["size_bytes"]))
    dl = sorted([r for r in rows if r["op"] == "download"], key=lambda r: int(r["size_bytes"]))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))
    fig.subplots_adjust(bottom=0.20, wspace=0.25, top=0.84)

    for label, sub, color, marker in (("Upload (POST, R=2)", up, BLUE, "o"), ("Download (GET)", dl, TEAL, "s")):
        if not sub:
            continue
        xs = np.array([float(r["size_bytes"]) for r in sub])
        ys = np.array([float(r["throughput_mb_s_median"]) for r in sub])
        yerr = np.array([float(r["throughput_mb_s_stdev"]) for r in sub])
        # Clamp lower error bar above 0 for readability
        lo = np.minimum(yerr, ys * 0.95)
        ax1.errorbar(xs, ys, yerr=[lo, yerr], label=label, color=color, marker=marker,
                     linewidth=2, capsize=4, markersize=6)

        lats = np.array([float(r["latency_s_median"]) * 1000.0 for r in sub])
        p95s = np.array([float(r["latency_s_p95"]) * 1000.0 for r in sub])
        ax2.plot(xs, lats, label=f"{label} median", color=color, marker=marker, linewidth=2)
        ax2.plot(xs, p95s, label=f"{label} p95", color=color, linestyle="--", alpha=0.65, linewidth=1.4)

    tick_rows = up or dl
    ticks = [float(r["size_bytes"]) for r in tick_rows]
    labels = [r["size_label"] for r in tick_rows]
    for ax in (ax1, ax2):
        ax.set_xscale("log")
        ax.set_xticks(ticks)
        ax.set_xticklabels(labels, rotation=25)
        ax.set_xlabel("Object Size (log scale; 1 MB = 1,048,576 B)")
        ax.legend(fontsize=8.5)

    ax1.set_ylabel("Throughput (MB/s, median ± stdev)")
    ax1.set_title("Throughput Saturation Curve")
    ax2.set_yscale("log")
    ax2.set_ylabel("Latency (ms, log scale)")
    ax2.set_title("Median vs p95 Latency Across Object Sizes")

    peak_up = max((float(r["throughput_mb_s_median"]) for r in up), default=0.0)
    peak_dl = max((float(r["throughput_mb_s_median"]) for r in dl), default=0.0)
    total_err = sum(int(r["error_count"]) + int(r.get("checksum_failures", 0)) for r in rows)
    fig.suptitle(
        f"Exp 1: Streaming Throughput Peaks at {peak_up:.1f} MB/s Upload & {peak_dl:.1f} MB/s Download (Errors={total_err})"
    )
    _add_footer(fig, rows)
    return _save(fig, plots_dir / "exp1_throughput_vs_size.png")


# ------------------------------------------------------------------ Plot 2
def plot_exp2(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    sum_rows = _read_csv(results_dir / "exp2_latency_latest.csv")
    raw_rows = _read_csv(results_dir / "exp2_latency_raw_latest.csv")
    if not sum_rows:
        return None

    sizes = []
    for r in sum_rows:
        if r["size_label"] not in sizes:
            sizes.append(r["size_label"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5.2))
    fig.subplots_adjust(bottom=0.20, wspace=0.25, top=0.83)

    # Left: grouped p50 / p95 / p99 bars for upload & download
    cats = []
    p50s, p95s, p99s = [], [], []
    for sz in sizes:
        for op in ("upload", "download"):
            r = next((x for x in sum_rows if x["size_label"] == sz and x["op"] == op), None)
            if r:
                cats.append(f"{sz}\n{op}")
                p50s.append(float(r["median_ms"]))
                p95s.append(float(r["p95_ms"]))
                p99s.append(float(r["p99_ms"]))

    x = np.arange(len(cats))
    w = 0.26
    ax1.bar(x - w, p50s, width=w, label="p50 (median)", color=TEAL)
    ax1.bar(x, p95s, width=w, label="p95", color=ORANGE)
    ax1.bar(x + w, p99s, width=w, label="p99", color=RED)
    for i, v in enumerate(p99s):
        ax1.text(x[i] + w, v + max(p99s) * 0.015, f"{v:.1f}", ha="center", va="bottom", fontsize=7.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels(cats)
    ax1.set_ylim(0, max(p99s or [1.0]) * 1.25)
    ax1.set_ylabel("Latency (ms)")
    ax1.set_title("Latency Percentiles (500 Sequential Requests Each)")
    ax1.legend(loc="upper right", ncol=3, fontsize=8.5)

    # Right: empirical latency distributions (histograms) for raw samples
    if raw_rows:
        palette = [(BLUE, "-"), (TEAL, "--"), (ORANGE, "-.")]
        all_ok = [float(r["latency_ms"]) for r in raw_rows if int(r["status_code"]) in (200, 201)]
        hi_clip = np.percentile(all_ok, 99.5) if all_ok else 100.0
        bins = np.linspace(0, max(1.0, hi_clip * 1.05), 40)
        for idx, sz in enumerate(sizes):
            col, ls = palette[idx % len(palette)]
            up_vals = [float(r["latency_ms"]) for r in raw_rows
                       if r["size_label"] == sz and r["op"] == "upload" and int(r["status_code"]) == 201]
            dl_vals = [float(r["latency_ms"]) for r in raw_rows
                       if r["size_label"] == sz and r["op"] == "download" and int(r["status_code"]) == 200]
            if up_vals:
                ax2.hist(np.clip(up_vals, 0, bins[-1]), bins=bins, histtype="step",
                         linewidth=1.7, color=col, linestyle="-", label=f"{sz} upload")
            if dl_vals:
                ax2.hist(np.clip(dl_vals, 0, bins[-1]), bins=bins, histtype="step",
                         linewidth=1.5, color=col, linestyle="--", label=f"{sz} download")
        ax2.set_xlabel("Request Latency (ms, clipped at p99.5 for tail visibility)")
        ax2.set_ylabel("Request Count")
        ax2.set_title("Empirical Latency Histogram")
        ax2.legend(fontsize=8)

    up_1kb = next((r for r in sum_rows if r["op"] == "upload"), sum_rows[0])
    dl_1kb = next((r for r in sum_rows if r["op"] == "download"), sum_rows[0])
    total_err = sum(int(r["error_count"]) for r in sum_rows)
    fig.suptitle(
        f"Exp 2: Small-Object Baseline Latency — {dl_1kb['size_label']} GET p50={float(dl_1kb['median_ms']):.1f}ms, "
        f"{up_1kb['size_label']} PUT p50={float(up_1kb['median_ms']):.1f}ms (Errors={total_err})"
    )
    _add_footer(fig, sum_rows)
    return _save(fig, plots_dir / "exp2_small_object_latency.png")


# ------------------------------------------------------------------ Plot 3
def plot_exp3(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    rows = _read_csv(results_dir / "exp3_concurrency_latest.csv")
    if not rows:
        return None

    up = sorted([r for r in rows if r["op"] == "upload"], key=lambda r: int(r["concurrency"]))
    dl = sorted([r for r in rows if r["op"] == "download"], key=lambda r: int(r["concurrency"]))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))
    fig.subplots_adjust(bottom=0.20, wspace=0.28, top=0.83)

    concs = [int(r["concurrency"]) for r in (up or dl)]
    if up:
        ax1.plot([int(r["concurrency"]) for r in up], [float(r["agg_throughput_mb_s"]) for r in up],
                 marker="o", color=BLUE, linewidth=2, label="Upload Agg. MB/s")
    if dl:
        ax1.plot([int(r["concurrency"]) for r in dl], [float(r["agg_throughput_mb_s"]) for r in dl],
                 marker="s", color=TEAL, linewidth=2, label="Download Agg. MB/s")
    ax1.set_xticks(concs)
    ax1.set_xlabel("Concurrent Client Threads")
    ax1.set_ylabel("Aggregate Throughput (MB/s)")
    ax1.set_title("Aggregate Throughput Scaling (16 MB Objects)")
    ax1.legend(loc="upper left")

    ax1_r = ax1.twinx()
    if up:
        ax1_r.plot([int(r["concurrency"]) for r in up], [float(r["req_latency_p95_s"]) for r in up],
                   marker="^", linestyle="--", color=ORANGE, alpha=0.85, label="Upload p95 (s)")
    if dl:
        ax1_r.plot([int(r["concurrency"]) for r in dl], [float(r["req_latency_p95_s"]) for r in dl],
                   marker="v", linestyle=":", color=RED, alpha=0.85, label="Download p95 (s)")
    ax1_r.set_ylabel("Per-Request p95 Latency (s)")
    ax1_r.legend(loc="lower right", fontsize=8)
    ax1_r.grid(False)

    # Right: Docker CPU utilization during upload & download
    base_rows = up or dl
    x = np.arange(len(base_rows))
    w = 0.2
    if up:
        ax2.bar(x - 1.5 * w, [float(r["controller_cpu_pct"]) for r in up], width=w,
                color=BLUE, label="Upload: Controller CPU%")
        ax2.bar(x - 0.5 * w, [float(r["workers_sum_cpu_pct"]) for r in up], width=w,
                color="#60a5fa", label="Upload: Workers Sum CPU%")
    if dl:
        ax2.bar(x + 0.5 * w, [float(r["controller_cpu_pct"]) for r in dl], width=w,
                color=TEAL, label="Download: Controller CPU%")
        ax2.bar(x + 1.5 * w, [float(r["workers_sum_cpu_pct"]) for r in dl], width=w,
                color="#5eead4", label="Download: Workers Sum CPU%")
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(r["concurrency"]) for r in base_rows])
    ax2.set_xlabel("Concurrent Client Threads")
    ax2.set_ylabel("Container CPU Utilization (%)")
    ax2.set_title("Docker Container CPU Saturation")
    ax2.legend(fontsize=8)

    best_up = max(up, key=lambda r: float(r["agg_throughput_mb_s"]), default={"agg_throughput_mb_s": 0, "concurrency": 1})
    best_dl = max(dl, key=lambda r: float(r["agg_throughput_mb_s"]), default={"agg_throughput_mb_s": 0, "concurrency": 1})
    total_err = sum(int(r["error_count"]) for r in rows)
    fig.suptitle(
        f"Exp 3: Concurrency Peaks at {float(best_dl['agg_throughput_mb_s']):.1f} MB/s Download (c={best_dl['concurrency']}) "
        f"& {float(best_up['agg_throughput_mb_s']):.1f} MB/s Upload (c={best_up['concurrency']}; Errors={total_err})"
    )
    _add_footer(fig, rows)
    return _save(fig, plots_dir / "exp3_concurrency_scaling.png")


# ------------------------------------------------------------------ Plot 4
def plot_exp4(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    rows = _read_csv(results_dir / "exp4_overhead_latest.csv")
    if not rows:
        return None

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5.0))
    fig.subplots_adjust(bottom=0.21, wspace=0.28, top=0.82)

    paths = ["direct_1x", "direct_2x", "controller"]
    labels_x = ["Direct Worker\n(1x Chunk)", "Direct Worker\n(2x for R=2)", "Controller\n(Full Stack)"]
    x = np.arange(len(paths))
    w = 0.34

    up_vals = [float(next((r["median_ms"] for r in rows if r["op"] == "upload" and r["path"] == p), 0)) for p in paths]
    up_err = [float(next((r["stdev_ms"] for r in rows if r["op"] == "upload" and r["path"] == p), 0)) for p in paths]
    dl_vals = [float(next((r["median_ms"] for r in rows if r["op"] == "download" and r["path"] == p), 0)) for p in paths]
    dl_err = [float(next((r["stdev_ms"] for r in rows if r["op"] == "download" and r["path"] == p), 0)) for p in paths]

    ax1.bar(x - w / 2, up_vals, width=w, yerr=up_err, capsize=4, color=BLUE, label="Upload (1 MB)")
    ax1.bar(x + w / 2, dl_vals, width=w, yerr=dl_err, capsize=4, color=TEAL, label="Download (1 MB)")
    for i in range(len(x)):
        ax1.text(x[i] - w / 2, up_vals[i] + up_err[i] + 0.5, f"{up_vals[i]:.1f}", ha="center", va="bottom", fontsize=8)
        ax1.text(x[i] + w / 2, dl_vals[i] + dl_err[i] + 0.5, f"{dl_vals[i]:.1f}", ha="center", va="bottom", fontsize=8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels_x)
    ax1.set_ylim(0, max([u + e for u, e in zip(up_vals, up_err)] + [1.0]) * 1.20)
    ax1.set_ylabel("Median Latency ± Stdev (ms)")
    ax1.set_title("1 MB Request Latency: Controller vs Direct Worker")
    ax1.legend(loc="upper left")

    ctrl_up = next((r for r in rows if r["op"] == "upload" and r["path"] == "controller"), None)
    ctrl_dl = next((r for r in rows if r["op"] == "download" and r["path"] == "controller"), None)
    ov_cats = ["Upload vs 1x", "Upload vs 2x\n(parallel R=2)", "Download vs 1x"]
    ov_vals = [
        float(ctrl_up["overhead_vs_1x_ms"]) if ctrl_up else 0.0,
        float(ctrl_up["overhead_vs_2x_ms"]) if ctrl_up else 0.0,
        float(ctrl_dl["overhead_vs_1x_ms"]) if ctrl_dl else 0.0,
    ]
    colors = [ORANGE if v >= 0 else TEAL for v in ov_vals]
    bars = ax2.bar(ov_cats, ov_vals, color=colors, width=0.5)
    ax2.axhline(0, color=SLATE, linewidth=1)
    lo_ov = min(0.0, min(ov_vals)) * 1.2
    hi_ov = max(1.0, max(ov_vals)) * 1.22
    ax2.set_ylim(lo_ov, hi_ov)
    for b, v in zip(bars, ov_vals):
        va = "bottom" if v >= 0 else "top"
        off = (hi_ov - lo_ov) * 0.025 if v >= 0 else -(hi_ov - lo_ov) * 0.025
        ax2.text(b.get_x() + b.get_width() / 2, v + off, f"{v:+.2f} ms", ha="center", va=va, fontsize=8.5)
    ax2.set_ylabel("Controller Delta Latency (ms)")
    ax2.set_title("Control-Plane Coordination Cost & Parallel Fan-Out")

    ov1_up = float(ctrl_up["overhead_vs_1x_ms"]) if ctrl_up else 0.0
    ov1_dl = float(ctrl_dl["overhead_vs_1x_ms"]) if ctrl_dl else 0.0
    fig.suptitle(
        f"Exp 4: Controller Overhead vs Direct Worker (1 MB) — +{ov1_up:.1f}ms Upload, +{ov1_dl:.1f}ms Download"
    )
    _add_footer(fig, rows)
    return _save(fig, plots_dir / "exp4_controller_overhead.png")


# ------------------------------------------------------------------ Plot 5
def plot_exp5(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    ts_rows = _read_csv(results_dir / "exp5_failure_timeseries_latest.csv")
    ev_rows = _read_csv(results_dir / "exp5_failure_events_latest.csv")
    if not ts_rows:
        return None

    secs = np.array([int(r["sec"]) for r in ts_rows])
    tp = np.array([float(r["throughput_mb_s"]) for r in ts_rows])
    p95 = np.array([float(r["latency_p95_ms"]) for r in ts_rows])
    errs = np.array([int(r["error_count"]) for r in ts_rows])
    under = np.array([int(r["under_replicated"]) for r in ts_rows])

    events = {r["event"]: (float(r["rel_sec"]) if r["rel_sec"] != "" else None) for r in ev_rows}
    markers = [
        ("t0_stop_worker2", "Stop worker2", RED, "--"),
        ("dead_detected", "DEAD detected", ORANGE, "-."),
        ("repair_complete", "Repair complete (R=2)", TEAL, "-."),
        ("t_restart_worker2", "Start worker2", BLUE, "--"),
        ("worker_rejoined", "Worker rejoined & trimmed", PURPLE, ":"),
    ]

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(13.5, 8.6), sharex=True)
    fig.subplots_adjust(bottom=0.10, hspace=0.26, top=0.87)

    ax1.plot(secs, tp, color=BLUE, linewidth=1.4, label="Total Throughput (MB/s)")
    ax1.fill_between(secs, tp, alpha=0.15, color=BLUE)
    ax1.set_ylim(0, max(10.0, float(tp.max())) * 1.18)
    ax1.set_ylabel("Throughput (MB/s)")
    ax1.set_title("Per-Second Cluster Throughput During Failure & Self-Healing")

    ax2.plot(secs, p95, color=ORANGE, linewidth=1.4, label="Per-Second p95 Latency (ms)")
    ax2.set_ylim(0, max(10.0, float(p95.max())) * 1.18)
    ax2.set_ylabel("p95 Latency (ms)")
    ax2.set_title("Client Request p95 Latency")

    ax3.bar(secs, errs, color=RED, alpha=0.85, width=0.8, label="Failed Requests / s")
    ax3.set_ylim(0, max(1.0, float(errs.max()) * 1.25))
    ax3_r = ax3.twinx()
    ax3_r.plot(secs, under, color=PURPLE, linewidth=1.8, label="Under-Replicated Chunks")
    ax3_r.set_ylim(-0.5, max(2.0, float(under.max()) * 1.22))
    ax3.set_ylabel("Errors / sec")
    ax3_r.set_ylabel("Under-Replicated Chunks")
    ax3_r.grid(False)
    ax3.set_xlabel("Elapsed Time (s)")
    ax3.set_title("Client Errors vs Under-Replicated Chunk Backlog")

    for ax in (ax1, ax2, ax3):
        for key, lbl, col, ls in markers:
            t_ev = events.get(key)
            if t_ev is not None:
                ax.axvline(t_ev, color=col, linestyle=ls, linewidth=1.4,
                           label=f"{lbl} ({t_ev:.1f}s)" if ax is ax1 else None)

    ax1.legend(loc="lower left", fontsize=8, ncol=3)
    ax2.legend(loc="upper right", fontsize=8.5)
    h1, l1 = ax3.get_legend_handles_labels()
    h2, l2 = ax3_r.get_legend_handles_labels()
    ax3.legend(h1 + h2, l1 + l2, loc="upper right", fontsize=8.5)

    total_errs = int(errs.sum())
    total_reqs = sum(int(r["req_count"]) for r in ts_rows)
    mean_tp = float(tp.mean())
    fig.suptitle(
        f"Exp 5: 180s Live Worker Failure & Recovery — Mean {mean_tp:.1f} MB/s, {total_errs}/{total_reqs} Client Errors"
    )
    _add_footer(fig, ts_rows)
    return _save(fig, plots_dir / "exp5_failure_timeline.png")


# ------------------------------------------------------------------ Plot 6
def plot_exp6_8(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    rows = _read_csv(results_dir / "exp6_8_repair_latest.csv")
    if not rows:
        return None

    rows = sorted(rows, key=lambda r: int(r["target_bytes"]))
    labels = [r["volume_label"] for r in rows]
    x = np.arange(len(labels))

    grace_done_s = [float(r["repair_duration_grace_to_done_s"]) for r in rows]
    repair_mb_s = [float(r["repair_mb_s"]) for r in rows]
    detect_s = [float(r["detection_latency_s"]) for r in rows]
    wa_before = [float(r["write_amplification"]) for r in rows]
    wa_after = [float(r["write_amplification_after_rejoin"]) for r in rows]
    skew = [float(r["chunk_skew_max_min"]) for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5.2))
    fig.subplots_adjust(bottom=0.20, wspace=0.28, top=0.83)

    bars = ax1.bar(x, grace_done_s, width=0.5, color=BLUE, label="Repair Duration (grace_end -> 0 under-repl)")
    ax1.set_ylim(0, max(grace_done_s or [1.0]) * 1.24)
    for b, dur, det in zip(bars, grace_done_s, detect_s):
        ax1.text(b.get_x() + b.get_width() / 2, dur + max(grace_done_s) * 0.02,
                 f"{dur:.1f}s\n(det {det:.1f}s)", ha="center", va="bottom", fontsize=8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.set_xlabel("Logical Dataset Volume")
    ax1.set_ylabel("Repair Duration (s)")
    ax1.set_title("Repair Time & Effective Rate vs Stored Volume")

    ax1_r = ax1.twinx()
    ax1_r.plot(x, repair_mb_s, color=ORANGE, marker="o", linewidth=2, label="Repair MB/s (grace->done)")
    ax1_r.set_ylim(min(repair_mb_s or [0.0]) * 0.90, max(repair_mb_s or [1.0]) * 1.14)
    ax1_r.set_ylabel("Effective Repair Throughput (MB/s)")
    ax1_r.grid(False)
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax1_r.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper center", fontsize=8)

    w = 0.25
    ax2.bar(x - w, wa_before, width=w, color=TEAL, label="Write Amp (pre-failure du/logical)")
    ax2.bar(x, wa_after, width=w, color=SLATE, label="Write Amp (post-rejoin du/logical)")
    ax2.bar(x + w, skew, width=w, color=PURPLE, label="Chunk Balance Skew (max/min)")
    ax2.axhline(2.0, color=TEAL, linestyle="--", linewidth=1.0, alpha=0.6, label="Ideal R=2 (2.0x)")
    ax2.axhline(1.0, color=PURPLE, linestyle=":", linewidth=1.0, alpha=0.6, label="Ideal Balance (1.0x)")
    ax2.set_ylim(0, max(wa_before + wa_after + [2.0]) * 1.18)
    for i in range(len(x)):
        ax2.text(x[i] - w, wa_before[i] + 0.04, f"{wa_before[i]:.2f}x", ha="center", va="bottom", fontsize=7.5)
        ax2.text(x[i] + w, skew[i] + 0.04, f"{skew[i]:.2f}x", ha="center", va="bottom", fontsize=7.5)
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_xlabel("Logical Dataset Volume")
    ax2.set_ylabel("Ratio (x)")
    ax2.set_title("Write Amplification & Least-Loaded Chunk Balance")
    ax2.legend(fontsize=8)

    avg_rate = sum(repair_mb_s) / len(repair_mb_s)
    max_wa = max(wa_before)
    fig.suptitle(
        f"Exp 6 & 8: Rate-Limited Repair Averages {avg_rate:.2f} MB/s; "
        f"Write Amplification <= {max_wa:.2f}x (Ideal 2.00x)"
    )
    _add_footer(fig, rows)
    return _save(fig, plots_dir / "exp6_8_repair_and_storage.png")


# ------------------------------------------------------------------ Plot 7
def plot_exp7(results_dir: Path, plots_dir: Path) -> Optional[Path]:
    rows = _read_csv(results_dir / "exp7_scrub_latest.csv")
    if not rows:
        return None

    modes = [r["mode"].replace("_", " ").title() for r in rows]
    durations = [float(r["scrub_duration_s"]) for r in rows]
    rates = [float(r["cluster_scrub_mb_s"]) for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.8))
    fig.subplots_adjust(bottom=0.22, wspace=0.28, top=0.82)

    colors = [BLUE, ORANGE][:len(modes)]
    bars1 = ax1.bar(modes, rates, color=colors, width=0.45)
    ax1.set_ylim(0, max(rates or [1.0]) * 1.18)
    for b, v in zip(bars1, rates):
        ax1.text(b.get_x() + b.get_width() / 2, v + max(rates) * 0.025, f"{v:.1f} MB/s",
                 ha="center", va="bottom", fontsize=9)
    ax1.set_ylabel("Cluster Scrub Rate (MB/s)")
    ax1.set_title("Scrub Throughput (Two-Layer SHA-256)")

    bars2 = ax2.bar(modes, durations, color=colors, width=0.45)
    ax2.set_ylim(0, max(durations or [1.0]) * 1.18)
    for b, v in zip(bars2, durations):
        ax2.text(b.get_x() + b.get_width() / 2, v + max(durations) * 0.025, f"{v:.2f} s",
                 ha="center", va="bottom", fontsize=9)
    ax2.set_ylabel("Total Scrub Duration (s)")
    ax2.set_title("Full-Cluster Scrub Duration")

    idle_r = next((r for r in rows if r["mode"] == "idle"), rows[0])
    load_r = next((r for r in rows if r["mode"] == "under_load"), rows[-1])
    dl_err = int(load_r.get("bg_downloads_err", 0))
    fig.suptitle(
        f"Exp 7: Scrub Throughput — {float(idle_r['cluster_scrub_mb_s']):.1f} MB/s Idle vs "
        f"{float(load_r['cluster_scrub_mb_s']):.1f} MB/s Under Load (Client Read Errors={dl_err})"
    )
    _add_footer(fig, rows)
    return _save(fig, plots_dir / "exp7_scrub_throughput.png")


def generate_all(results_dir: Path = h.RESULTS_DIR, plots_dir: Path = h.PLOTS_DIR) -> List[Path]:
    generated = []
    for fn in (plot_exp1, plot_exp2, plot_exp3, plot_exp4, plot_exp5, plot_exp6_8, plot_exp7):
        out = fn(results_dir, plots_dir)
        if out is not None:
            generated.append(out)
    return generated


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate benchmark PNG plots from results CSVs")
    parser.add_argument("--results-dir", type=Path, default=h.RESULTS_DIR)
    parser.add_argument("--plots-dir", type=Path, default=h.PLOTS_DIR)
    args = parser.parse_args()
    paths = generate_all(args.results_dir, args.plots_dir)
    if not paths:
        print("No result CSVs found to plot.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
