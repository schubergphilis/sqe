#!/usr/bin/env python3
"""SQE-vs-Trino result parity, over the Trino HTTP protocol, Flight SQL, or both.

Both engines are pointed at the same Polaris REST catalog (shared Iceberg
tables), so any result difference is an engine behavior difference, not a data
difference. Queries are read from a JSON file (default: queries.json next to
this script). The Trino baseline is always read over its HTTP protocol; SQE is
read over each protocol asked for, and every SQE result is compared with the
baseline on values, column types and (under a top-level ORDER BY) row order.
See parity_client.compare for the verdicts.

Exit code is non-zero when any query is not MATCH (BOTH-ERR is reported apart
and does not fail the run; each one should be an intended negative).

Usage:
  parity_compare.py [--protocol http|flight|both] [--sqe URL] [--sqe-flight URI]
                    [--trino URL] [--schema S] [--catalog C] [--user U]
                    [--sqe-pass P] [--queries FILE] [--only SUBSTR] [--report FILE]
"""
import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parity_client import FlightAdbc, TrinoHttp, compare  # noqa: E402


def lenient_equal(a, b, ndigits=2):
    """The harness's previous verdict (numbers rounded to 2 places, numeric
    strings read as floats, rows sorted, types ignored), kept so a run stays
    comparable with older reports."""
    def norm(rows):
        out = []
        for row in rows:
            r = []
            for v in row:
                if isinstance(v, bool):
                    r.append(v)
                elif isinstance(v, (int, float)):
                    r.append(round(float(v), ndigits))
                elif isinstance(v, str):
                    try:
                        r.append(round(float(v), ndigits))
                    except ValueError:
                        r.append(v)
                else:
                    r.append(v)
            out.append(r)
        return sorted(out, key=lambda x: json.dumps(x, default=str))
    if a.status == "ERR" or b.status == "ERR":
        return a.status == b.status
    return norm(a.rows) == norm(b.rows)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--protocol", choices=("http", "flight", "both"), default="http")
    p.add_argument("--sqe", default="http://localhost:28080/v1/statement")
    p.add_argument("--sqe-flight", default="grpc://localhost:60051")
    p.add_argument("--trino", default="http://localhost:38080/v1/statement")
    p.add_argument("--schema", default="tpch_demo")
    p.add_argument("--catalog", default="iceberg")
    p.add_argument("--user", default="root")
    p.add_argument("--sqe-pass", default="s3cr3t",
                   help="SQE password (the Trino baseline uses none)")
    p.add_argument("--queries", default=f"{here}/queries.json")
    p.add_argument("--only", help="run only queries whose name contains this")
    p.add_argument("--report", help="write per-query verdicts as JSON here")
    args = p.parse_args()

    trino = TrinoHttp(args.trino, args.user, None, args.catalog, args.schema)
    sqe = {}
    if args.protocol in ("http", "both"):
        sqe["http"] = TrinoHttp(args.sqe, args.user, args.sqe_pass, args.catalog, args.schema)
    if args.protocol in ("flight", "both"):
        sqe["flight"] = FlightAdbc(args.sqe_flight, args.user, args.sqe_pass, args.catalog, args.schema)

    with open(args.queries) as f:
        queries = json.load(f)
    if args.only:
        queries = {k: v for k, v in queries.items() if args.only in k}

    print(f"Trino : {args.trino}")
    for proto in sqe:
        print(f"SQE   : {args.sqe if proto == 'http' else args.sqe_flight} ({proto})")
    print(f"Schema: {args.catalog}.{args.schema}   Queries: {len(queries)}\n")

    counts = {proto: Counter() for proto in sqe}
    lenient = Counter()
    report = []
    for name, sql in queries.items():
        t = trino.run(sql)
        entry = {"name": name, "sql": sql, "trino": t.status}
        for proto, client in sqe.items():
            s = client.run(sql)
            verdict, detail = compare(sql, s, t)
            counts[proto][verdict] += 1
            if lenient_equal(s, t):
                lenient[proto] += 1
            entry[proto] = {"verdict": verdict, "detail": detail}
            print(f"  {name:28s} {proto:6s} {verdict:9s} {detail[:180]}")
        report.append(entry)

    total = len(queries)
    print()
    failed = False
    for proto, c in counts.items():
        summary = ", ".join(f"{k} {v}" for k, v in sorted(c.items()))
        print(f"  {proto:6s} {c['MATCH']}/{total} MATCH  ({summary});  lenient {lenient[proto]}/{total}")
        failed |= (c["MATCH"] + c["BOTH-ERR"]) != total
    if args.report:
        with open(args.report, "w") as f:
            json.dump({"total": total, "counts": {k: dict(v) for k, v in counts.items()},
                       "queries": report}, f, indent=1)
    for client in sqe.values():
        client.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
