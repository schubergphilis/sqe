#!/usr/bin/env python3
"""SQE-vs-Trino result parity over the Trino HTTP wire protocol.

Both engines are pointed at the same Polaris REST catalog (shared Iceberg
tables), so any result difference is an engine behavior difference, not a data
difference. Queries are read from a JSON file (default: queries.json next to
this script). Exit code is non-zero if any query DIFFs or ERRORs on exactly one
engine, which makes this usable as a CI gate.

Usage:
  parity_compare.py [--sqe URL] [--trino URL] [--schema S] [--queries FILE]
                    [--catalog C] [--user U] [--sqe-pass P] [--round N]
"""
import argparse
import base64
import json
import sys
import urllib.request


def _request(url, headers, auth_user_pass, data=None):
    req = urllib.request.Request(url, data=data.encode() if data else None)
    for k, v in headers.items():
        req.add_header(k, v)
    if auth_user_pass:
        token = base64.b64encode(auth_user_pass.encode()).decode()
        req.add_header("Authorization", "Basic " + token)
    return json.load(urllib.request.urlopen(req, timeout=120))


def run_query(engine, sql):
    """Run sql, following nextUri pages. Returns ('OK', rows) or ('ERR', msg)."""
    d = _request(engine["url"], engine["headers"], engine["auth"], data=sql)
    rows = []
    while True:
        if d.get("error"):
            return ("ERR", d["error"].get("message", "").splitlines()[0][:160])
        rows.extend(d.get("data") or [])
        nxt = d.get("nextUri")
        if not nxt:
            return ("OK", rows)
        d = _request(nxt, engine["headers"], engine["auth"])


def normalize(rows, ndigits):
    """Round numerics (incl. numeric strings) and sort, so float formatting and
    row order do not produce false differences."""
    out = []
    for row in rows:
        norm_row = []
        for v in row:
            if isinstance(v, bool):
                norm_row.append(v)
            elif isinstance(v, (int, float)):
                norm_row.append(round(float(v), ndigits))
            elif isinstance(v, str):
                try:
                    norm_row.append(round(float(v), ndigits))
                except ValueError:
                    norm_row.append(v)
            else:
                norm_row.append(v)
        out.append(norm_row)
    try:
        out.sort(key=lambda x: json.dumps(x, default=str))
    except Exception:
        pass
    return out


def make_engine(url, schema, catalog, user, password):
    headers = {
        "X-Trino-User": user,
        "X-Trino-Catalog": catalog,
        "X-Trino-Schema": schema,
        "Content-Type": "text/plain",
    }
    auth = f"{user}:{password}" if password is not None else None
    return {"url": url, "headers": headers, "auth": auth}


def main():
    here = __file__.rsplit("/", 1)[0]
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sqe", default="http://localhost:28080/v1/statement")
    p.add_argument("--trino", default="http://localhost:38080/v1/statement")
    p.add_argument("--schema", default="tpch_demo")
    p.add_argument("--catalog", default="iceberg")
    p.add_argument("--user", default="root")
    p.add_argument("--sqe-pass", default="s3cr3t",
                   help="SQE basic-auth password (Trino baseline uses none)")
    p.add_argument("--queries", default=f"{here}/queries.json")
    p.add_argument("--round", type=int, default=2, help="decimal places for numeric compare")
    args = p.parse_args()

    sqe = make_engine(args.sqe, args.schema, args.catalog, args.user, args.sqe_pass)
    trino = make_engine(args.trino, args.schema, args.catalog, args.user, None)

    with open(args.queries) as f:
        queries = json.load(f)

    print(f"SQE   : {args.sqe}")
    print(f"Trino : {args.trino}")
    print(f"Schema: {args.catalog}.{args.schema}   Queries: {len(queries)}\n")

    match = diff = error = 0
    for name, sql in queries.items():
        s_status, s_data = run_query(sqe, sql)
        t_status, t_data = run_query(trino, sql)
        if s_status == "ERR" or t_status == "ERR":
            if s_status == "ERR" and t_status == "ERR":
                verdict, detail = "BOTH-ERR", f"both reject: SQE={s_data}"
                match += 1  # identical rejection = parity
            else:
                verdict = "ERROR"
                bad = "SQE" if s_status == "ERR" else "Trino"
                detail = f"{bad} only: {s_data if s_status=='ERR' else t_data}"
                error += 1
        else:
            sn, tn = normalize(s_data, args.round), normalize(t_data, args.round)
            if sn == tn:
                verdict, detail = "MATCH", f"{len(sn)} rows"
                match += 1
            else:
                verdict = "DIFF"
                detail = f"SQE={len(sn)}r Trino={len(tn)}r | SQE{sn[:2]} vs Trino{tn[:2]}"
                diff += 1
        print(f"  {name:24s} {verdict:9s} {detail}")

    total = len(queries)
    print(f"\n  {match}/{total} parity  ({diff} DIFF, {error} one-sided ERROR)")
    return 1 if (diff or error) else 0


if __name__ == "__main__":
    sys.exit(main())
