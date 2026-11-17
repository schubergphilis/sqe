#!/usr/bin/env python3
"""Iceberg SQL parity: SQE vs real Trino on the shared Polaris catalog.

Unlike the read-only parity harness, each scenario runs a *sequence* of Iceberg
DDL/DML on both engines, then compares a verify query. Two scenario kinds:

  parity  -- SQE and Trino each operate on their OWN table ({t}_sqe / {t}_trino)
             running the identical setup, then a verify SELECT is diffed. Proves
             SQE's Iceberg write path produces the same logical result as Trino.
  interop -- one engine writes a SHARED table; BOTH engines read it and the
             reads are diffed. Proves SQE-written Iceberg is Trino-readable and
             vice versa (the real cross-engine compatibility signal).

Metadata-table reads use the portable form  schema."tbl$snapshots"  (works on
both). {snap0} in a verify string is replaced, per engine, with that engine's
oldest snapshot id (for FOR VERSION AS OF time-travel checks).

SQE is driven over the Trino HTTP protocol, Flight SQL, or both (--protocol);
Trino is always driven over HTTP. Verdicts are parity_client.compare's.

Usage:
  iceberg_parity.py [SCENARIOS_JSON] [--protocol http|flight|both] [--only SUBSTR]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parity_client import FlightAdbc, Result, TrinoHttp, compare  # noqa: E402

SCHEMA = "tpch_demo"
CATALOG = "iceberg"


# Table names are fully qualified (schema.table), which isolates these tests
# to the Iceberg operations themselves. `bare` is kept for metadata-table refs,
# which take the portable  schema."bare$meta"  form.
def sub(sql, bare):
    return (sql
            .replace("{t}", f"{SCHEMA}.{bare}")
            .replace("{snaptbl}", f'{SCHEMA}."{bare}$snapshots"')
            .replace("{histtbl}", f'{SCHEMA}."{bare}$history"'))


def oldest_snapshot(client, bare):
    r = client.run(f'SELECT min(snapshot_id) FROM {SCHEMA}."{bare}$snapshots"')
    if r.status == "OK" and r.rows and r.rows[0][0] is not None:
        return str(r.rows[0][0])
    return None


def do_engine(client, bare, setup, verify):
    for stmt in setup:
        r = client.run(sub(stmt, bare))
        if r.status == "ERR":
            return Result("ERR", error=f"setup: {r.error}  [stmt: {sub(stmt, bare)[:70]}]")
    v = sub(verify, bare)
    if "{snap0}" in v:
        s0 = oldest_snapshot(client, bare)
        if s0 is None:
            return Result("ERR", error="verify: no snapshot id")
        v = v.replace("{snap0}", s0)
    return client.run(v)


def cleanup(client, bare):
    client.run(f"DROP TABLE IF EXISTS {SCHEMA}.{bare}")


def scenario(sc, sqe, trino, proto):
    """Returns (verdict, detail)."""
    name, kind = sc["name"], sc["kind"]
    setup, verify = sc["setup"], sc["verify"]
    engines = {"SQE": sqe, "Trino": trino}
    if kind == "parity":
        st, tt = f"zzip_{proto}_{name}_sqe", f"zzip_{proto}_{name}_trino"
        cleanup(sqe, st)
        cleanup(trino, tt)
        s = do_engine(sqe, st, setup, verify)
        t = do_engine(trino, tt, setup, verify)
        cleanup(sqe, st)
        cleanup(trino, tt)
    else:  # interop
        writer_name = sc["writer"]
        writer = engines[writer_name]
        bare = f"zzip_{proto}_{name}"
        cleanup(writer, bare)
        for stmt in setup:
            w = writer.run(sub(stmt, bare))
            if w.status == "ERR":
                cleanup(writer, bare)
                return ("SQE-ERR" if writer_name == "SQE" else "TRINO-ERR",
                        f"writer({writer_name}) setup: {w.error}")
        v = sub(verify, bare)
        s, t = sqe.run(v), trino.run(v)
        cleanup(writer, bare)
    return compare(verify, s, t)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("scenarios", nargs="?", default=f"{here}/iceberg_scenarios.json")
    p.add_argument("--protocol", choices=("http", "flight", "both"), default="http")
    p.add_argument("--sqe", default="http://localhost:28080/v1/statement")
    p.add_argument("--sqe-flight", default="grpc://localhost:60051")
    p.add_argument("--trino", default="http://localhost:38080/v1/statement")
    p.add_argument("--user", default="root")
    p.add_argument("--sqe-pass", default="s3cr3t")
    p.add_argument("--only", help="run only scenarios whose name contains this")
    args = p.parse_args()

    with open(args.scenarios) as f:
        scen = json.load(f)
    if args.only:
        scen = [s for s in scen if args.only in s["name"]]

    trino = TrinoHttp(args.trino, args.user, None, CATALOG, SCHEMA)
    protos = ["http", "flight"] if args.protocol == "both" else [args.protocol]
    failed = []
    for proto in protos:
        if proto == "http":
            sqe = TrinoHttp(args.sqe, args.user, args.sqe_pass, CATALOG, SCHEMA)
        else:
            sqe = FlightAdbc(args.sqe_flight, args.user, args.sqe_pass, CATALOG, SCHEMA)
        passed = 0
        for sc in scen:
            try:
                verdict, detail = scenario(sc, sqe, trino, proto)
            except Exception as e:  # keep going: one broken scenario must not hide the rest
                verdict, detail = "EXC", str(e)[:200]
            if verdict == "MATCH":
                passed += 1
            else:
                failed.append(f"{proto}:{sc['name']}")
            print(f"  {sc['name']:30s} {proto:6s} {verdict:9s} {detail[:180]}")
        print(f"\n  {proto}: {passed}/{len(scen)} MATCH\n")
        sqe.close()
    if failed:
        print("  not MATCH:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
