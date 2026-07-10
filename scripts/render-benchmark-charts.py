#!/usr/bin/env python3
"""Generate over-time benchmark charts from benchmarks/results/*.json.

For every (suite, scale) pair we have data for, this script produces:
  - <suite>-<scale>-total.png:    total run duration over time (line)
  - <suite>-<scale>-per-query.png: per-query duration over time (heatmap)
  - <suite>-<scale>-pass.png:     pass count over time (line)

Plus one cross-scale chart per suite:
  - <suite>-cross-scale.png: SF0.1 / SF1 / SF10 total durations on the
    same time axis.

Usage: /tmp/sqe-bench-env/bin/python3 scripts/render-benchmark-charts.py

Charts land in docs/evidence/benchmark/charts/ and are referenced from
docs/evidence/benchmark/index.md.
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "benchmarks" / "results"
OUT_DIR = ROOT / "docs" / "evidence" / "benchmark" / "charts"

# Scales we care about for the headline charts. SF0.01 is too tiny to be
# interesting; SF100 has only one TPC-E run.
HEADLINE_SCALES = ["sf0.1", "sf1", "sf10"]

# Suites listed in narrative order (matches the README benchmark table).
SUITES = ["tpch", "tpcds", "ssb", "tpcc", "tpce", "tpcbb", "clickbench"]

# Per-suite display names for axis titles.
SUITE_DISPLAY = {
    "tpch": "TPC-H",
    "tpcds": "TPC-DS",
    "ssb": "Star Schema Benchmark",
    "tpcc": "TPC-C (read-only subset)",
    "tpce": "TPC-E",
    "tpcbb": "TPC-BB",
    "clickbench": "ClickBench",
}

# Filename pattern: <suite>-<scale>-<protocol>-<timestamp>.json
# Protocol may be flight, http, or absent for pre-V8 runs.
NAME_RE = re.compile(
    r"^(?P<suite>[a-z]+)-(?P<scale>sf[0-9._]+)-(flight-|http-)?(?P<ts>[\dT:.-]+)\.json$"
)


def parse_filename(name: str):
    """Extract (suite, scale, timestamp) from a result filename."""
    m = NAME_RE.match(name)
    if not m:
        return None
    suite = m.group("suite")
    scale = m.group("scale").lower()
    ts_raw = m.group("ts")
    try:
        ts = datetime.fromisoformat(ts_raw.replace("T", " ", 1).split(".")[0].split("Z")[0])
    except ValueError:
        return None
    return suite, scale, ts


def load_runs():
    """Return dict[(suite, scale)] -> list of {timestamp, summary, queries}."""
    runs = defaultdict(list)
    for path in sorted(RESULTS_DIR.glob("*.json")):
        if path.name.startswith("compare-"):
            continue
        parsed = parse_filename(path.name)
        if parsed is None:
            continue
        suite, scale, ts = parsed
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        summary = data.get("summary", {})
        queries = data.get("queries", [])
        if not queries or not summary:
            continue
        runs[(suite, scale)].append({"ts": ts, "summary": summary, "queries": queries})
    for key in runs:
        runs[key].sort(key=lambda r: r["ts"])
    return runs


# ── Run quality filter ───────────────────────────────────────────────────────

def _summary_counts(run):
    """Return (total, passed, failed, errored) for a run.

    Prefer the explicit summary fields; fall back to counting queries by
    status when a field is missing, mirroring how the rest of the script
    treats summary vs queries.
    """
    summary = run.get("summary", {})
    queries = run.get("queries", [])
    total = summary.get("total")
    if total is None:
        total = len(queries)
    passed = summary.get("pass")
    if passed is None:
        passed = sum(1 for q in queries if q.get("status") == "pass")
    failed = summary.get("fail")
    if failed is None:
        failed = sum(1 for q in queries if q.get("status") == "fail")
    errored = summary.get("error")
    if errored is None:
        errored = sum(1 for q in queries if q.get("status") == "error")
    return total, passed, failed, errored


def clean_full_runs(runs):
    """Keep only runs that ran the full suite with every query passing.

    Incomplete runs (fewer queries than the suite's max) and skip-heavy runs
    (full count but many SKIPPED queries, so near-zero duration) plot as
    artificially fast downward outliers on the duration charts. We compute the
    full query count as the max total seen across the given runs, then keep a
    run only when its duration is positive, it ran the full count, and every
    query passed with no failures or errors. This mirrors the interactive
    getsqe aggregator filter.
    """
    if not runs:
        return runs
    full_count = max(_summary_counts(r)[0] for r in runs)
    clean = []
    for r in runs:
        total, passed, failed, errored = _summary_counts(r)
        duration = r["summary"].get("total_duration_ms", 0)
        if (
            duration > 0
            and total == full_count
            and passed == total
            and (failed in (0, None))
            and (errored in (0, None))
        ):
            clean.append(r)
    return clean


# ── Plot helpers ────────────────────────────────────────────────────────────

def _setup_time_axis(ax):
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")


def plot_total_duration(suite, scale, runs, out: Path):
    # Duration chart: only full-suite all-pass runs, so partial and skip-heavy
    # runs do not show up as artificially fast downward outliers.
    runs = clean_full_runs(runs)
    if not runs:
        return
    times = [r["ts"] for r in runs]
    secs = [r["summary"].get("total_duration_ms", 0) / 1000.0 for r in runs]

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(times, secs, marker="o", linewidth=1.8, markersize=4, color="#1f77b4")
    ax.set_title(f"{SUITE_DISPLAY.get(suite, suite)} {scale.upper()} - total run duration over time")
    ax.set_ylabel("Total duration (s)")
    ax.set_xlabel("Run date")
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    _setup_time_axis(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def plot_pass_count(suite, scale, runs, out: Path):
    times = [r["ts"] for r in runs]
    passes = [r["summary"].get("pass", 0) for r in runs]
    totals = [r["summary"].get("total", 0) for r in runs]

    fig, ax = plt.subplots(figsize=(10, 3.5))
    ax.plot(times, passes, marker="o", linewidth=1.8, markersize=4, color="#2ca02c", label="pass")
    if totals and max(totals) > 0:
        ax.axhline(y=max(totals), linestyle="--", color="#888", alpha=0.6, label=f"target ({max(totals)})")
    ax.set_title(f"{SUITE_DISPLAY.get(suite, suite)} {scale.upper()} - queries passing over time")
    ax.set_ylabel("Queries passing")
    ax.set_xlabel("Run date")
    ax.set_ylim(bottom=0, top=max(totals or [1]) + 1)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right", framealpha=0.9)
    _setup_time_axis(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def plot_per_query_heatmap(suite, scale, runs, out: Path):
    """Per-query duration over time as a heatmap (queries x runs)."""
    # Collect all unique query ids in encounter order.
    seen_ids = []
    seen_set = set()
    for r in runs:
        for q in r["queries"]:
            qid = q.get("id")
            if qid and qid not in seen_set:
                seen_set.add(qid)
                seen_ids.append(qid)

    if not seen_ids:
        return

    # Build matrix: rows = queries, cols = runs, values = ms (NaN if missing or skipped).
    matrix = np.full((len(seen_ids), len(runs)), np.nan)
    qid_index = {qid: i for i, qid in enumerate(seen_ids)}
    for j, r in enumerate(runs):
        for q in r["queries"]:
            qid = q.get("id")
            if qid is None:
                continue
            status = q.get("status")
            ms = q.get("duration_ms", 0)
            # Only colour passing runs; let everything else stay NaN (white).
            if status == "pass" and ms > 0:
                matrix[qid_index[qid], j] = ms / 1000.0

    if np.isnan(matrix).all():
        return

    n_runs = len(runs)
    n_queries = len(seen_ids)
    fig_height = max(4.0, 0.18 * n_queries + 1.2)
    fig_width = max(8.0, 0.18 * n_runs + 2.0)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    cmap = plt.colormaps["YlOrRd"].copy()
    cmap.set_bad(color="#f0f0f0")
    masked = np.ma.masked_invalid(matrix)
    im = ax.imshow(masked, aspect="auto", cmap=cmap, interpolation="nearest")

    ax.set_yticks(range(n_queries))
    ax.set_yticklabels(seen_ids, fontsize=8)
    # Show every Nth run on the x axis to keep labels readable.
    step = max(1, n_runs // 12)
    tick_positions = list(range(0, n_runs, step))
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(
        [runs[i]["ts"].strftime("%b %d") for i in tick_positions],
        rotation=45,
        ha="right",
        fontsize=8,
    )
    ax.set_title(
        f"{SUITE_DISPLAY.get(suite, suite)} {scale.upper()} - per-query duration over time (s, white = skipped or failed)"
    )
    ax.set_xlabel("Run")
    ax.set_ylabel("Query")
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    cbar.set_label("Duration (s)")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def plot_cross_scale(suite, runs_by_scale, out: Path):
    """One line per scale on the same time axis. Helpful headline for a suite."""
    fig, ax = plt.subplots(figsize=(10, 4.5))
    colors = {"sf0.1": "#1f77b4", "sf1": "#ff7f0e", "sf10": "#2ca02c"}
    plotted_any = False
    for scale in HEADLINE_SCALES:
        # Duration chart: same full-suite all-pass filter as the per-scale total.
        runs = clean_full_runs(runs_by_scale.get(scale, []))
        if not runs:
            continue
        times = [r["ts"] for r in runs]
        secs = [r["summary"].get("total_duration_ms", 0) / 1000.0 for r in runs]
        ax.plot(
            times,
            secs,
            marker="o",
            linewidth=1.8,
            markersize=4,
            color=colors.get(scale, "#444"),
            label=scale.upper(),
        )
        plotted_any = True
    if not plotted_any:
        plt.close(fig)
        return
    ax.set_yscale("log")
    ax.set_title(f"{SUITE_DISPLAY.get(suite, suite)} - total duration across scales over time")
    ax.set_ylabel("Total duration (s, log scale)")
    ax.set_xlabel("Run date")
    ax.grid(True, alpha=0.3, which="both")
    ax.legend(loc="best", framealpha=0.9)
    _setup_time_axis(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    runs = load_runs()
    if not runs:
        print("No benchmark runs found.")
        return

    # Per (suite, scale) charts.
    suites_with_data = set()
    for (suite, scale), suite_runs in sorted(runs.items()):
        if scale not in HEADLINE_SCALES:
            continue
        if len(suite_runs) < 2:
            # A single run is not a trend; skip the time chart but keep the heatmap.
            pass
        suites_with_data.add(suite)

        prefix = f"{suite}-{scale}"
        plot_total_duration(suite, scale, suite_runs, OUT_DIR / f"{prefix}-total.png")
        plot_pass_count(suite, scale, suite_runs, OUT_DIR / f"{prefix}-pass.png")
        plot_per_query_heatmap(suite, scale, suite_runs, OUT_DIR / f"{prefix}-per-query.png")
        print(f"  {prefix}: {len(suite_runs)} runs charted")

    # Cross-scale headline chart per suite.
    for suite in suites_with_data:
        runs_by_scale = {s: runs.get((suite, s), []) for s in HEADLINE_SCALES}
        plot_cross_scale(suite, runs_by_scale, OUT_DIR / f"{suite}-cross-scale.png")

    print(f"\nWrote charts to {OUT_DIR}")


if __name__ == "__main__":
    main()
