#!/usr/bin/env python3
"""Trino function coverage: every function signature Trino lists, called on SQE.

The denominator is Trino's own ``SHOW FUNCTIONS`` (scalar, aggregate and window
functions), not a hand-picked list. Each signature is called on the Trino
baseline and on SQE with sample arguments of its declared types, and the two
results are compared on return type and value (``parity_client.compare``).
When Trino rejects the samples (a domain error, say), the call is retried with
typed NULLs, which still proves resolution and return type.

Signatures are put in one of four scopes:

  iceberg   every argument and the return type are Iceberg-representable
            (boolean, integers, real/double, decimal, varchar, varbinary,
            date, time, timestamp [with time zone], uuid, array, map, row) or
            plain SQL values (interval, json) -- the target is 100% here
  trino     uses a Trino-only type (HyperLogLog, Geometry, IPADDRESS, ...)
  lambda    takes a lambda this generator cannot build
  untested  Trino itself rejects both generated calls (generator gap)

Per signature: MATCH / TYPE / DIFF / ORDER / SQE-ERR (MISSING when the name
does not resolve on SQE) / TRINO-ERR / BOTH-ERR. Nondeterministic functions
compare return types only.

Usage:
  function_coverage.py [--protocol http|flight|both] [--only NAME]
                       [--report FILE] [--markdown FILE]
"""
import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parity_client import FlightAdbc, Result, TrinoHttp, compare  # noqa: E402

# Types whose values Iceberg (or plain SQL) can carry.
SCALAR_SAMPLES = {
    "boolean": "true",
    "tinyint": "TINYINT '7'",
    "smallint": "SMALLINT '7'",
    "integer": "7",
    "bigint": "BIGINT '7'",
    "real": "REAL '2.5'",
    "double": "DOUBLE '2.5'",
    "decimal": "DECIMAL '12.34'",
    "varchar": "'abc'",
    "char": "CHAR 'abc'",
    "varbinary": "X'616263'",
    "date": "DATE '2024-03-15'",
    "time": "TIME '12:34:56.789'",
    "time with time zone": "TIME '12:34:56.789 +01:00'",
    "timestamp": "TIMESTAMP '2024-03-15 12:34:56.789'",
    "timestamp with time zone": "TIMESTAMP '2024-03-15 12:34:56.789 UTC'",
    "interval day to second": "INTERVAL '2' DAY",
    "interval year to month": "INTERVAL '3' MONTH",
    "json": "JSON '{\"a\":[1,2]}'",
    "uuid": "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
    "unknown": "NULL",
}
# Type variables in generic signatures, bound to a concrete type.
TYPE_VARS = {"t": "bigint", "e": "bigint", "k": "varchar", "v": "bigint",
             "t1": "bigint", "t2": "varchar", "t3": "double", "u": "varchar",
             "r": "bigint", "s": "bigint", "x": "bigint", "y": "bigint"}


def split_top(s):
    """Split on commas that sit outside parentheses."""
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def base_type(t):
    """'timestamp(p) with time zone' -> 'timestamp with time zone',
    'decimal(p,s)' -> 'decimal', 'varchar(x)' -> 'varchar'."""
    return re.sub(r"\s+", " ", re.sub(r"\([^()]*\)", "", t)).strip().lower()


class Unsupported(Exception):
    def __init__(self, scope, what):
        super().__init__(what)
        self.scope = scope


def bind(t):
    """Resolve type variables to concrete types (recursively)."""
    t = t.strip()
    low = t.lower()
    if low in TYPE_VARS:
        return TYPE_VARS[low]
    m = re.match(r"^(array|map|function|row)\((.*)\)$", t, re.I)
    if m:
        return f"{m.group(1).lower()}({','.join(bind(x) for x in split_top(m.group(2)))})"
    return t


def sample(t, nulls=False, texts=None):
    """SQL expression of Trino type ``t`` (already bound). ``texts`` overrides
    the varchar sample (see VARCHAR_SETS)."""
    low = t.lower()
    if low.startswith("array("):
        inner = low[len("array("):-1]
        if nulls:
            return f"CAST(NULL AS {render_type(t)})"
        e = sample(inner)
        return f"ARRAY[{e}, {e}]"
    if low.startswith("map("):
        k, v = split_top(low[len("map("):-1])
        if nulls:
            return f"CAST(NULL AS {render_type(t)})"
        return f"MAP(ARRAY[{sample(k)}], ARRAY[{sample(v)}])"
    if low.startswith("row("):
        raise Unsupported("iceberg", f"row argument {t}")
    if low.startswith("function("):
        raise Unsupported("lambda", t)
    b = base_type(low)
    if b not in SCALAR_SAMPLES:
        raise Unsupported("trino", b)
    if nulls:
        return f"CAST(NULL AS {render_type(t)})"
    if texts is not None and b == "varchar":
        text = texts[0] if len(texts) == 1 else texts.pop(0)
        return "'" + text.replace("'", "''") + "'"
    return SCALAR_SAMPLES[b]


# String samples tried in turn, before the typed-NULL fallback: units, JSON
# paths, zones and formats are strings whose value the function checks. A set
# is consumed one string per varchar argument, the last one repeating.
VARCHAR_SETS = [
    ["abc"], ["day"], ["$.a"], ["UTC"], ["%Y-%m-%d"], ["2024-03-15", "%Y-%m-%d"],
    ["yyyy-MM-dd"], ["2024-03-15", "yyyy-MM-dd"], ["2024-03-15T12:34:56Z"],
    ["a"], ["12"], ["abc", "b"], ["abc", "b", "x"],
]


def render_type(t):
    """A castable type: fill length/precision parameters with concrete ones."""
    low = t.lower()
    low = re.sub(r"decimal\([^)]*\)", "decimal(10,2)", low)
    low = re.sub(r"(varchar|char)\([a-z]\w*\)", r"\1", low)
    low = re.sub(r"(timestamp|time)\([a-z]\w*\)", r"\1(3)", low)
    return low


def lambda_for(ftype):
    """Build a lambda for function(A1..An, R): identity on the first argument,
    or ``true`` when R is boolean."""
    parts = split_top(ftype[len("function("):-1])
    params, ret = parts[:-1], parts[-1].lower()
    names = ["x", "y", "z", "w"][: len(params)]
    if len(names) != len(params):
        raise Unsupported("lambda", ftype)
    head = names[0] if len(names) == 1 else f"({', '.join(names)})"
    if ret == "boolean":
        body = "true"
    elif base_type(ret) == base_type(params[0].lower()):
        body = names[0]
    elif len(params) > 1 and base_type(ret) == base_type(params[1].lower()):
        body = names[1]
    else:
        body = sample(ret)  # a constant of the return type
    return f"{head} -> {body}"


def build_call(name, fn_type, arg_types, nulls, texts=None):
    args = []
    texts = list(texts) if texts else None
    for t in arg_types:
        bt = bind(t)
        args.append(lambda_for(bt) if bt.lower().startswith("function(") else sample(bt, nulls, texts))
    call = f"{name}({', '.join(args)})"
    if fn_type == "window":
        return f"SELECT {call} OVER (ORDER BY 1)"
    return f"SELECT {call}"


def scope_of(ret, arg_types):
    for t in [ret, *arg_types]:
        try:
            bt = bind(t)
            if bt.lower().startswith("function("):
                continue
            sample(bt)
        except Unsupported as e:
            if e.scope != "iceberg":
                return e.scope
    return "iceberg"


_MISSING = re.compile(
    r"invalid function|unknown function|function .*(not found|does not exist)|"
    r"no function (named|matches)|there is no function|not a (known|valid) function",
    re.I)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--protocol", choices=("http", "flight", "both"), default="http")
    p.add_argument("--sqe", default="http://localhost:28080/v1/statement")
    p.add_argument("--sqe-flight", default="grpc://localhost:60051")
    p.add_argument("--trino", default="http://localhost:38080/v1/statement")
    p.add_argument("--catalog", default="iceberg")
    p.add_argument("--schema", default="tpch_demo")
    p.add_argument("--user", default="root")
    p.add_argument("--sqe-pass", default="s3cr3t")
    p.add_argument("--only", help="only functions whose name contains this")
    p.add_argument("--report", help="write per-signature results as JSON")
    p.add_argument("--markdown", help="write the per-function summary as Markdown")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    trino = TrinoHttp(args.trino, args.user, None, args.catalog, args.schema)
    sqe = {}
    if args.protocol in ("http", "both"):
        sqe["http"] = TrinoHttp(args.sqe, args.user, args.sqe_pass, args.catalog, args.schema)
    if args.protocol in ("flight", "both"):
        sqe["flight"] = FlightAdbc(args.sqe_flight, args.user, args.sqe_pass, args.catalog, args.schema)

    listing = trino.run("SHOW FUNCTIONS")
    if listing.status != "OK":
        sys.exit(f"SHOW FUNCTIONS on Trino failed: {listing.error}")
    cols = [c for c, _ in listing.columns]
    ix = {c.lower(): i for i, c in enumerate(cols)}
    version = trino.run("SELECT version()")
    trino_version = version.rows[0][0] if version.status == "OK" else "?"

    sigs = []
    for r in listing.rows:
        fn_type = r[ix["function type"]]
        if fn_type not in ("scalar", "aggregate", "window"):
            continue
        name = r[ix["function"]]
        if args.only and args.only not in name:
            continue
        arg_types = split_top(r[ix["argument types"]] or "")
        sigs.append({
            "name": name, "type": fn_type, "returns": r[ix["return type"]],
            "args": arg_types, "deterministic": bool(r[ix["deterministic"]]),
        })

    results = []
    for sig in sigs:
        scope = scope_of(sig["returns"], sig["args"])
        rec = dict(sig, scope=scope)
        results.append(rec)
        if scope in ("trino", "lambda"):
            continue
        sql, t = None, None
        has_text = any(base_type(bind(a)) == "varchar" for a in sig["args"])
        attempts = [(False, s) for s in (VARCHAR_SETS if has_text else [None])] + [(True, None)]
        for nulls, texts in attempts:
            try:
                cand = build_call(sig["name"], sig["type"], sig["args"], nulls, texts)
            except Unsupported as e:
                rec["scope"] = e.scope if e.scope != "iceberg" else "untested"
                break
            tr = trino.run(cand)
            if tr.status == "OK":
                sql, t = cand, tr
                rec["nulls"] = nulls
                break
        if t is None or sql is None:
            if rec["scope"] == "iceberg":
                rec["scope"] = "untested"
            continue
        rec["sql"] = sql
        for proto, client in sqe.items():
            s = client.run(sql)
            if not sig["deterministic"] and s.status == "OK":
                # Compare types only: blank the values on both sides.
                s = Result("OK", s.columns, [[None] * len(r) for r in s.rows])
                t_cmp = Result("OK", t.columns, [[None] * len(r) for r in t.rows])
            else:
                t_cmp = t
            verdict, detail = compare(sql, s, t_cmp)
            if verdict == "SQE-ERR" and _MISSING.search(detail):
                verdict = "MISSING"
            rec[proto] = {"verdict": verdict, "detail": detail[:300]}
            if args.verbose or verdict != "MATCH":
                print(f"  {proto:6s} {verdict:9s} {sql[:90]:90s} {detail[:140]}")

    # Per function name: covered only when every signature MATCHes. An untested
    # signature (the generator found no call Trino accepts) counts against the
    # function rather than leaving the denominator, and a function whose every
    # MATCH came from typed-NULL arguments is reported apart (`null_only`): it
    # proves the name resolves with Trino's return type, not its values.
    order = ["MISSING", "SQE-ERR", "DIFF", "TYPE", "ORDER", "UNTESTED", "TRINO-ERR", "BOTH-ERR", "MATCH"]
    summary = {}
    for proto in sqe:
        by_name = defaultdict(list)
        valued = defaultdict(bool)
        for rec in results:
            if rec["scope"] == "untested":
                by_name[rec["name"]].append("UNTESTED")
            elif rec["scope"] == "iceberg" and proto in rec:
                by_name[rec["name"]].append(rec[proto]["verdict"])
                valued[rec["name"]] |= not rec.get("nulls", False)
        worst = {n: min(v, key=order.index) for n, v in by_name.items()}
        summary[proto] = {
            "functions": len(worst),
            "covered": sum(1 for v in worst.values() if v == "MATCH"),
            "null_only": sorted(n for n, v in worst.items() if v == "MATCH" and not valued[n]),
            "by_worst_verdict": dict(Counter(worst.values())),
            "signatures": dict(Counter(v for vs in by_name.values() for v in vs)),
            "worst": worst,
        }
    scopes = Counter(r["scope"] for r in results)

    print(f"\nTrino {trino_version}: {len(sigs)} signatures, {len({s['name'] for s in sigs})} functions")
    print("Scopes (signatures): " + ", ".join(f"{k} {v}" for k, v in sorted(scopes.items())))
    for proto, s in summary.items():
        print(f"  {proto:6s} functions covered {s['covered']}/{s['functions']} "
              f"({len(s['null_only'])} on typed NULLs only; worst verdict: {s['by_worst_verdict']}); "
              f"signatures {s['signatures']}")

    if args.report:
        with open(args.report, "w") as f:
            json.dump({"trino_version": trino_version, "scopes": dict(scopes),
                       "summary": {k: {kk: vv for kk, vv in v.items() if kk != "worst"} for k, v in summary.items()},
                       "signatures": results}, f, indent=1, default=str)
    if args.markdown:
        write_markdown(args.markdown, trino_version, results, summary, scopes)
    for c in sqe.values():
        c.close()
    return 0 if all(s["covered"] == s["functions"] for s in summary.values()) else 1


def write_markdown(path, version, results, summary, scopes):
    lines = [f"# Trino function coverage (Trino {version})", ""]
    lines.append("Signature scopes: " + ", ".join(f"{k} {v}" for k, v in sorted(scopes.items())) + ".")
    lines.append("")
    for proto, s in summary.items():
        lines.append(f"- **{proto}**: {s['covered']} of {s['functions']} functions covered "
                     f"({len(s['null_only'])} on typed NULLs only: "
                     f"{', '.join(s['null_only']) or 'none'}); signatures {s['signatures']}")
    lines += ["", "| Function | Kind | " + " | ".join(summary) + " | First failing call |",
              "|---|---|" + "---|" * len(summary) + "---|"]
    first_fail = {}
    kind = {}
    for rec in results:
        if rec["scope"] == "untested":
            kind[rec["name"]] = rec["type"]
            first_fail.setdefault(rec["name"], f"UNTESTED: no generated call Trino accepts "
                                               f"({', '.join(rec['args'])})")
            continue
        if rec["scope"] != "iceberg":
            continue
        kind[rec["name"]] = rec["type"]
        for proto in summary:
            v = rec.get(proto, {}).get("verdict")
            if v and v != "MATCH" and rec["name"] not in first_fail:
                first_fail[rec["name"]] = f"`{rec['sql']}` -> {v}: {rec[proto]['detail'][:120]}"
    for name in sorted(kind):
        cells = [summary[p]["worst"].get(name, "-") for p in summary]
        if all(c == "MATCH" for c in cells):
            continue
        detail = first_fail.get(name, "").replace("|", "\\|")
        lines.append(f"| `{name}` | {kind[name]} | " + " | ".join(cells) + f" | {detail} |")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
