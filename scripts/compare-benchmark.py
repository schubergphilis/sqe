#!/usr/bin/env python3
"""Compare a benchmark run against a committed baseline and gate on regression.

Reads two sqe-bench JSON reports (the ones written to benchmarks/results/)
and exits non-zero when the candidate is slower than the baseline by more
than the allowed threshold, or when the candidate did not pass every query.

The JSON shape produced by `sqe-bench test` is:

    {
      "benchmark": "tpch",
      "summary": {"total": 22, "pass": 22, "fail": 0, ..., "total_duration_ms": 37529},
      "queries": [...]
    }

Usage:
    compare-benchmark.py --baseline BASELINE.json --candidate CANDIDATE.json
                         [--threshold-percent 20]

Exit codes:
    0  candidate within threshold and all queries passed
    1  regression: candidate slower than baseline beyond threshold
    2  candidate did not pass every query (pass != total, or failures/errors)
    3  usage / parse error
"""

import argparse
import json
import sys


def load_summary(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"ERROR: cannot read JSON report '{path}': {exc}", file=sys.stderr)
        sys.exit(3)
    summary = data.get("summary")
    if not isinstance(summary, dict):
        print(f"ERROR: report '{path}' has no 'summary' object", file=sys.stderr)
        sys.exit(3)
    if "total_duration_ms" not in summary:
        print(f"ERROR: report '{path}' summary lacks 'total_duration_ms'", file=sys.stderr)
        sys.exit(3)
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Gate a benchmark run against a committed baseline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Exit: 0 ok, 1 perf regression, 2 correctness regression, 3 usage error.",
    )
    parser.add_argument("--baseline", required=True, help="path to baseline JSON report")
    parser.add_argument("--candidate", required=True, help="path to candidate (new run) JSON report")
    parser.add_argument(
        "--threshold-percent",
        type=float,
        default=20.0,
        help="max allowed slowdown vs baseline before failing (default: 20)",
    )
    args = parser.parse_args()

    baseline = load_summary(args.baseline)
    candidate = load_summary(args.candidate)

    base_ms = float(baseline["total_duration_ms"])
    cand_ms = float(candidate["total_duration_ms"])
    if base_ms <= 0:
        print(f"ERROR: baseline total_duration_ms is {base_ms}, cannot compare", file=sys.stderr)
        sys.exit(3)

    delta_pct = (cand_ms - base_ms) / base_ms * 100.0

    print(f"baseline : {args.baseline}")
    print(f"candidate: {args.candidate}")
    print(f"baseline total : {base_ms / 1000:.1f}s ({base_ms:.0f} ms)")
    print(f"candidate total: {cand_ms / 1000:.1f}s ({cand_ms:.0f} ms)")
    print(f"delta          : {delta_pct:+.1f}%  (threshold: {args.threshold_percent:.1f}%)")

    # Correctness gate first. A run where queries error out finishes fast and
    # would otherwise sail through the runtime check; that is exactly the
    # silent regression the gate exists to catch.
    cand_total = int(candidate.get("total", 0) or 0)
    cand_pass = int(candidate.get("pass", 0) or 0)
    cand_fail = int(candidate.get("fail", 0) or 0)
    cand_error = int(candidate.get("error", 0) or 0)
    if cand_total == 0 or cand_pass != cand_total or cand_fail > 0 or cand_error > 0:
        print(
            f"FAIL: candidate did not pass every query "
            f"(pass={cand_pass} fail={cand_fail} error={cand_error} total={cand_total})",
            file=sys.stderr,
        )
        sys.exit(2)

    if delta_pct > args.threshold_percent:
        print(
            f"FAIL: performance regression {delta_pct:+.1f}% exceeds "
            f"threshold {args.threshold_percent:.1f}%",
            file=sys.stderr,
        )
        sys.exit(1)

    print("PASS: within threshold and all queries passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
