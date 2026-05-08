#!/usr/bin/env python3
"""Identify benchmark JSON files in benchmarks/results/ that should be
deleted: data-generation timings (not query benchmarks), one-off
experiments, and runs with error rates above a threshold.

Usage:
    /tmp/sqe-bench-env/bin/python3 scripts/clean-benchmark-results.py
        [--threshold 0.25] [--apply]

Without `--apply`, the script lists candidates without touching any
files. With `--apply`, the candidates are passed to `git rm`.

Default threshold: error_count / total > 0.25 (any run where more
than a quarter of queries errored). Below that we keep the run as a
real-engine-state data point.
"""

import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "benchmarks" / "results"

# Special non-suite files: data-generation timing and one-off experiments.
# These have no `total` query count and do not belong on suite-level charts.
SPECIAL_PREFIXES = ("tpch-generate-", "mor-vs-cow-")


def find_candidates(threshold: float):
    """Return (deletes, reasons): list of paths and a parallel list of why."""
    deletes = []
    reasons = []

    for path in sorted(RESULTS.glob("*.json")):
        # `compare-*.json` files are SQE-vs-Trino comparisons; the chart
        # generator already skips them, and they live or die with their
        # paired suite file. Skip here too.
        if path.name.startswith("compare-"):
            continue

        if any(path.name.startswith(p) for p in SPECIAL_PREFIXES):
            deletes.append(path)
            reasons.append("special non-suite file (data generation / experiment)")
            continue

        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            deletes.append(path)
            reasons.append("malformed JSON")
            continue

        summary = data.get("summary", {})
        total = summary.get("total", 0)
        errors = summary.get("error", 0)

        if total in (None, 0):
            deletes.append(path)
            reasons.append("missing or zero `total`")
            continue

        ratio = errors / total
        if ratio > threshold:
            deletes.append(path)
            reasons.append(
                f"error rate {errors}/{total} ({ratio:.0%}) exceeds {threshold:.0%}"
            )

    return deletes, reasons


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.25,
        help="Error ratio above which a run is considered broken (default 0.25).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually run `git rm` on the candidates. Without this flag, only list them.",
    )
    args = parser.parse_args()

    deletes, reasons = find_candidates(args.threshold)
    if not deletes:
        print("No candidates found.")
        return

    by_reason = {}
    for path, reason in zip(deletes, reasons):
        by_reason.setdefault(reason, []).append(path)

    print(f"Identified {len(deletes)} files for deletion:")
    print()
    for reason, paths in sorted(by_reason.items()):
        print(f"  {reason}: {len(paths)} files")
    print()

    # Split tracked vs untracked: tracked use `git rm`, untracked use plain rm.
    tracked = subprocess.run(
        ["git", "ls-files"],
        check=True,
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    tracked_set = set(tracked)
    rel_paths = [str(p.relative_to(ROOT)) for p in deletes]
    git_rm = [p for p in rel_paths if p in tracked_set]
    plain_rm = [p for p in rel_paths if p not in tracked_set]

    print(f"  {len(git_rm)} tracked (git rm)")
    print(f"  {len(plain_rm)} untracked (rm)")

    if not args.apply:
        print("Dry run. Pass --apply to execute deletions.")
        for path in deletes:
            mark = "git rm" if str(path.relative_to(ROOT)) in tracked_set else "rm    "
            print(f"  would {mark}  {path.relative_to(ROOT)}")
        return

    # Tracked files: stage with `git rm` in chunks.
    if git_rm:
        chunk = 200
        for i in range(0, len(git_rm), chunk):
            subprocess.run(
                ["git", "rm", "--quiet"] + git_rm[i : i + chunk],
                check=True,
                cwd=ROOT,
            )
        print(f"  staged for deletion: {len(git_rm)} tracked files")

    # Untracked files: plain remove.
    for rel in plain_rm:
        (ROOT / rel).unlink()
    if plain_rm:
        print(f"  removed from disk: {len(plain_rm)} untracked files")

    print(f"\nDone. Run `git status` to verify, then commit.")


if __name__ == "__main__":
    main()
