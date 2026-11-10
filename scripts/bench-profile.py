#!/usr/bin/env python3
"""Print the hot operators of each benchmark query from coordinator logs.

The coordinator logs one `query_profile` record per finished query when
`[query] query_profile` is on (tests/sqe-test.toml sets "all"). Each record
holds the executed plan with DataFusion's per-operator metrics. This script
reads those records and prints, per query, where the time and the spill went.

The records carry no SQL text and no benchmark query id. Pass the bench
report with --results and the script lines profiles up with query ids, in
order, by output row count and elapsed time. Queries that timed out never
produce a profile and are listed as missing.

Usage:
  scripts/bench-profile.py /tmp/sqe-bench-coord-<pid>.log
  scripts/bench-profile.py LOG --results benchmarks/results/tpch-sf100-...json
  scripts/bench-profile.py LOG --results REPORT --query q09 --top 10
  scripts/bench-profile.py LOG --results REPORT --min-ms 30000

Operator time is `elapsed_compute` (joins: build_time + join_time), summed
over partitions, so on an 8-core box an operator can show more than the
query's wall time. Read it as CPU time.
"""

import argparse
import json
import re
import sys

DURATION_UNITS = {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}
BYTE_UNITS = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
COUNT_UNITS = {"": 1, "K": 1e3, "M": 1e6, "B": 1e9}
METRIC_RE = re.compile(r"(\w+)=([^,\]]*)")


def parse_duration_ms(text):
    m = re.fullmatch(r"\s*([\d.]+)\s*(ns|µs|us|ms|s)\s*", text)
    return float(m.group(1)) * DURATION_UNITS[m.group(2)] if m else None


def parse_bytes(text):
    m = re.fullmatch(r"\s*([\d.]+)\s*(B|KB|MB|GB|TB)\s*", text)
    return float(m.group(1)) * BYTE_UNITS[m.group(2)] if m else None


def parse_count(text):
    m = re.fullmatch(r"\s*([\d.]+)\s*([KMB]?)\s*", text)
    return float(m.group(1)) * COUNT_UNITS[m.group(2)] if m else None


def parse_operator(line):
    """One plan line -> (depth, name, detail, metrics dict)."""
    stripped = line.lstrip(" ")
    depth = (len(line) - len(stripped)) // 2
    head, sep, tail = stripped.rpartition(", metrics=[")
    if not sep:
        head, tail = stripped, ""
    name, _, detail = head.partition(":")
    metrics = {}
    for key, value in METRIC_RE.findall(tail):
        if key == "elapsed_compute" or key.endswith("_time"):
            metrics[key] = parse_duration_ms(value)
        elif key.endswith("_bytes"):
            metrics[key] = parse_bytes(value)
        else:
            metrics[key] = parse_count(value)
    return depth, name.strip(), detail.strip(), metrics


def op_cpu(metrics):
    """CPU time of one operator. Joins report it as build_time + join_time
    (sort-merge join leaves elapsed_compute at a few ns)."""
    compute = metrics.get("elapsed_compute") or 0
    join = (metrics.get("build_time") or 0) + (metrics.get("join_time") or 0)
    return max(compute, join)


def read_profiles(paths):
    profiles = []
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                if '"query_profile"' not in raw:
                    continue
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if rec.get("target") != "query_profile":
                    continue
                fields = rec.get("fields", {})
                lines = fields.get("message", "").split("\n")
                header = dict(
                    kv.split("=", 1) for kv in lines[1].split(" ") if "=" in kv
                ) if len(lines) > 1 else {}
                unpushed = re.search(r"unpushed_tables=\[([^\]]*)\]", lines[1] if len(lines) > 1 else "")
                ops = [parse_operator(ln) for ln in lines[2:] if ln.strip()]
                span = rec.get("span", {})
                profiles.append({
                    "timestamp": rec.get("timestamp", ""),
                    "elapsed_ms": float(fields.get("elapsed_ms") or header.get("elapsed_ms") or 0),
                    "output_rows": int(header.get("output_rows", -1)),
                    "unpushed": unpushed.group(1) if unpushed else "",
                    "status": span.get("sqe.status", "?"),
                    "span": span,
                    "ops": ops,
                    "id": None,
                })
    return profiles


def match_results(profiles, report_path):
    """Assign bench query ids to profiles, walking both lists in order."""
    with open(report_path, encoding="utf-8") as fh:
        report = json.load(fh)
    missing = []
    pos = 0
    for q in report.get("queries", []):
        dur = float(q.get("duration_ms") or 0)
        rows = q.get("rows")
        tolerance = max(2000.0, dur * 0.2)
        found = None
        for i in range(pos, len(profiles)):
            p = profiles[i]
            if rows is not None and q.get("status") == "pass" and p["output_rows"] != rows:
                continue
            if abs(p["elapsed_ms"] - dur) <= tolerance:
                found = i
                break
        if found is None:
            missing.append((q.get("id"), q.get("status"), dur))
            continue
        profiles[found]["id"] = q.get("id")
        pos = found + 1
    return [p for p in profiles if p["id"]], missing


def fmt_ms(ms):
    if ms is None:
        return "-"
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    return f"{ms:.0f}ms" if ms >= 1 else f"{ms * 1000:.0f}µs"


def fmt_bytes(n):
    if not n:
        return "0"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{n:.0f}B"
        n /= 1024
    return ""


def fmt_count(n):
    if n is None:
        return "-"
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n / div:.1f}{suffix}"
    return f"{n:.0f}"


def print_profile(p, top, detail_width):
    span = p["span"]
    spilled = sum(m.get("spilled_bytes") or 0 for _, _, _, m in p["ops"])
    compute = sum(op_cpu(m) for _, _, _, m in p["ops"])
    label = p["id"] or p["timestamp"]
    print(f"== {label}  wall {fmt_ms(p['elapsed_ms'])}  cpu {fmt_ms(compute)}  "
          f"rows {p['output_rows']}  status {p['status']}  spilled {fmt_bytes(spilled)}")
    files_read = span.get("sqe.scan.files_read")
    if files_read is not None:
        print(f"   scan: {fmt_bytes(span.get('sqe.scan.bytes_read', 0))} read, "
              f"{files_read}/{span.get('sqe.scan.files_total', '?')} files, "
              f"{fmt_count(span.get('sqe.scan.rows_decoded'))} rows decoded, "
              f"pruned minmax={span.get('sqe.scan.files_pruned_minmax', 0)} "
              f"dynamic={span.get('sqe.scan.files_pruned_dynamic', 0)}")
    if p["unpushed"]:
        print(f"   coordinator-local scans: {p['unpushed']}")
    ranked = sorted(p["ops"], key=lambda op: op_cpu(op[3]), reverse=True)
    for depth, name, detail, m in ranked[:top]:
        cpu = op_cpu(m)
        share = cpu / compute * 100 if compute else 0
        extra = ""
        if m.get("spilled_bytes"):
            extra = (f"  spill {fmt_bytes(m['spilled_bytes'])} in "
                     f"{fmt_count(m.get('spill_count'))} files")
        short = detail if len(detail) <= detail_width else detail[: detail_width - 3] + "..."
        print(f"   {fmt_ms(cpu):>8} {share:5.1f}%  d{depth:<2} {name}"
              f"  out {fmt_count(m.get('output_rows'))}{extra}")
        if short:
            print(f"   {'':>8} {'':>6}      {short}")
    print()


def main():
    ap = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    ap.add_argument("logs", nargs="+", help="coordinator log files (JSON lines)")
    ap.add_argument("--results", help="bench report JSON, to label profiles with query ids")
    ap.add_argument("--query", action="append", help="only these query ids (repeatable)")
    ap.add_argument("--top", type=int, default=5, help="operators per query (default 5)")
    ap.add_argument("--min-ms", type=float, default=0, help="skip queries faster than this")
    ap.add_argument("--width", type=int, default=110, help="operator detail width")
    args = ap.parse_args()

    profiles = read_profiles(args.logs)
    if not profiles:
        sys.exit("no query_profile records found; is [query] query_profile on?")
    missing = []
    if args.results:
        profiles, missing = match_results(profiles, args.results)
    if args.query:
        wanted = set(args.query)
        profiles = [p for p in profiles if p["id"] in wanted]

    shown = [p for p in profiles if p["elapsed_ms"] >= args.min_ms]
    for p in shown:
        print_profile(p, args.top, args.width)

    totals = {}
    for p in shown:
        for _, name, _, m in p["ops"]:
            t = totals.setdefault(name, [0.0, 0.0])
            t[0] += op_cpu(m)
            t[1] += m.get("spilled_bytes") or 0
    all_cpu = sum(t[0] for t in totals.values())
    print(f"== operator totals over {len(shown)} queries (cpu {fmt_ms(all_cpu)})")
    for name, (cpu, spill) in sorted(totals.items(), key=lambda kv: kv[1][0], reverse=True)[:10]:
        share = cpu / all_cpu * 100 if all_cpu else 0
        spill_txt = f"  spill {fmt_bytes(spill)}" if spill else ""
        print(f"   {fmt_ms(cpu):>8} {share:5.1f}%  {name}{spill_txt}")
    if missing:
        print("\n== no profile (timed out, or no match in these logs)")
        for qid, status, dur in missing:
            print(f"   {qid}  {status}  {fmt_ms(dur)}")


if __name__ == "__main__":
    main()
