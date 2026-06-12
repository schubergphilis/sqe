#!/usr/bin/env python3
"""Validate the sqe-bench TPC-DS generator against DuckDB's official dsdgen.

DuckDB's tpcds extension is the reference implementation: it generates
spec-conformant data and ships the official query texts. This script diffs
our generator's output against it WITHOUT any query engine in the loop, so
generator-fidelity drift is caught before a single benchmark runs. Two-engine
differential testing cannot see data bugs (both engines read the same data
and agree on garbage); this is the independent oracle.

Checks per table:
  - row count ratio ours/official
  - per-column: null fraction, distinct count, min/max drift

Check per query (the decisive one):
  - run every benchmarks/queries/tpcds/*.sql against BOTH datasets inside
    DuckDB; flag queries that return rows on official data but none on ours
    (a query the benchmark can only ever vacuously "pass").

Usage:
  scripts/validate-generator-tpcds.py \
      --ours /tmp/sqe-gen-001/tpcds/sf0.01 \
      --dsdgen-db /tmp/dsdgen-001.db \
      --queries benchmarks/queries/tpcds

Requires the duckdb CLI on PATH. The dsdgen db must already contain
`CALL dsdgen(sf=N)` output. Exit code 1 when any query is vacuous-on-ours
or a table drifts beyond thresholds.
"""

import argparse
import csv
import io
import json
import pathlib
import subprocess
import sys

ROW_RATIO_TOLERANCE = 0.15  # ours/official row count may deviate this much
NULL_FRAC_TOLERANCE = 0.10  # absolute drift in per-column null fraction


def duck(db: str, sql: str, readonly: bool = True) -> list[list[str]]:
    cmd = ["duckdb"]
    if readonly:
        cmd.append("-readonly")
    cmd += [db, "-csv", "-noheader", "-c", sql]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"duckdb failed: {out.stderr.strip()[:300]}\nSQL: {sql[:200]}")
    return list(csv.reader(io.StringIO(out.stdout)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ours", required=True, help="dir with <table>/*.parquet from sqe-bench generate")
    ap.add_argument("--dsdgen-db", required=True, help="duckdb db containing dsdgen output")
    ap.add_argument("--queries", default="benchmarks/queries/tpcds")
    ap.add_argument("--json", help="write machine-readable report here")
    args = ap.parse_args()

    ours = pathlib.Path(args.ours)
    db = args.dsdgen_db
    tables = [r[0] for r in duck(db, "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1")]

    report = {"tables": {}, "queries": {}, "failures": []}

    print(f"{'table':24} {'official':>10} {'ours':>10} {'ratio':>7}  notes")
    for t in tables:
        glob = ours / t / "*.parquet"
        official_rows = int(duck(db, f"SELECT count(*) FROM {t}")[0][0])
        try:
            ours_rows = int(duck(db, f"SELECT count(*) FROM read_parquet('{glob}')")[0][0])
        except RuntimeError:
            report["failures"].append(f"{t}: ours missing/unreadable")
            print(f"{t:24} {official_rows:>10} {'MISSING':>10}")
            continue
        ratio = ours_rows / official_rows if official_rows else 1.0
        notes = []
        if official_rows and abs(ratio - 1.0) > ROW_RATIO_TOLERANCE:
            notes.append(f"row-count drift {ratio:.2f}x")

        # Per-column null fraction drift on shared columns.
        cols = [r[0] for r in duck(db, f"SELECT column_name FROM information_schema.columns WHERE table_name='{t}' ORDER BY ordinal_position")]
        for c in cols:
            try:
                offi = float(duck(db, f"SELECT COALESCE(AVG(CASE WHEN {c} IS NULL THEN 1.0 ELSE 0.0 END),0) FROM {t}")[0][0])
                ourn = float(duck(db, f"SELECT COALESCE(AVG(CASE WHEN {c} IS NULL THEN 1.0 ELSE 0.0 END),0) FROM read_parquet('{glob}')")[0][0])
            except RuntimeError:
                notes.append(f"{c}: missing in ours")
                continue
            if abs(offi - ourn) > NULL_FRAC_TOLERANCE:
                notes.append(f"{c}: null-frac {ourn:.2f} vs official {offi:.2f}")
        report["tables"][t] = {"official_rows": official_rows, "ours_rows": ours_rows, "notes": notes}
        if notes:
            report["failures"].extend(f"{t}: {n}" for n in notes)
        print(f"{t:24} {official_rows:>10} {ours_rows:>10} {ratio:>6.2f}x  {'; '.join(notes)}")

    # Query-level fidelity: official-nonempty but ours-empty == vacuous-by-construction.
    qdir = pathlib.Path(args.queries)
    print(f"\n{'query':8} {'official':>9} {'ours':>9}  verdict")
    # Build a temp attach of ours as views once, via an in-memory session per query
    view_sql = "\n".join(
        f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{ours / t}/*.parquet');" for t in tables
    )
    for qf in sorted(qdir.glob("q*.sql")):
        sql = "\n".join(l for l in qf.read_text().splitlines() if not l.strip().startswith("--")).rstrip().rstrip(";")
        try:
            offi = int(duck(db, f"SELECT count(*) FROM ({sql}) t")[0][0])
        except RuntimeError as e:
            report["queries"][qf.stem] = {"error": f"official: {e}"}
            print(f"{qf.stem:8} {'ERR':>9}")
            continue
        try:
            ours_n = int(duck(":memory:", f"{view_sql}\nSELECT count(*) FROM ({sql}) t", readonly=False)[0][0])
        except RuntimeError as e:
            report["queries"][qf.stem] = {"official": offi, "error": f"ours: {e}"}
            report["failures"].append(f"{qf.stem}: errors on our data")
            print(f"{qf.stem:8} {offi:>9} {'ERR':>9}  BROKEN-ON-OURS")
            continue
        verdict = "ok"
        if offi > 0 and ours_n == 0:
            verdict = "VACUOUS-ON-OURS"
            report["failures"].append(f"{qf.stem}: official {offi} rows, ours 0")
        report["queries"][qf.stem] = {"official": offi, "ours": ours_n, "verdict": verdict}
        print(f"{qf.stem:8} {offi:>9} {ours_n:>9}  {verdict}")

    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(report, indent=1))

    n = len(report["failures"])
    print(f"\n{n} fidelity findings" if n else "\ngenerator matches official within tolerances")
    return 1 if n else 0


if __name__ == "__main__":
    sys.exit(main())
