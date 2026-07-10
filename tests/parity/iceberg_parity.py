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
"""
import json, sys, urllib.request, base64

SCHEMA = "tpch_demo"
ENG = {"SQE": ("http://localhost:28080/v1/statement", True),
       "Trino": ("http://localhost:38080/v1/statement", False)}
HDR = {"X-Trino-User": "root", "X-Trino-Catalog": "iceberg",
       "X-Trino-Schema": SCHEMA, "Content-Type": "text/plain"}

def _req(url, auth, data=None):
    r = urllib.request.Request(url, data=data.encode() if data else None)
    for k, v in HDR.items():
        r.add_header(k, v)
    if auth:
        r.add_header("Authorization", "Basic " + base64.b64encode(b"root:s3cr3t").decode())
    return json.load(urllib.request.urlopen(r, timeout=120))

def run(engine, sql):
    url, auth = ENG[engine]
    d = _req(url, auth, sql)
    rows = []
    while True:
        if d.get("error"):
            return ("ERR", d["error"].get("message", "")[:160])
        rows.extend(d.get("data") or [])
        nxt = d.get("nextUri")
        if not nxt:
            break
        d = _req(nxt, auth)
    return ("OK", rows)

def norm(rows, nd=2):
    out = []
    for r in rows:
        nr = []
        for v in r:
            if isinstance(v, float):
                nr.append(round(v, nd))
            elif isinstance(v, str):
                try: nr.append(round(float(v), nd))
                except ValueError: nr.append(v)
            else: nr.append(v)
        out.append(nr)
    try: out.sort(key=lambda x: json.dumps(x, default=str))
    except Exception: pass
    return out

# Table names are fully qualified (schema.table). SQE's write path resolves an
# UNQUALIFIED name to the `default` namespace while reads use the session schema
# -- a real bug reported separately -- so qualifying isolates these tests to the
# Iceberg operations themselves. `bare` is kept for metadata-table refs, which
# take the portable  schema."bare$meta"  form.
def sub(sql, bare):
    return (sql
            .replace("{t}", f"{SCHEMA}.{bare}")
            .replace("{snaptbl}", f'{SCHEMA}."{bare}$snapshots"')
            .replace("{histtbl}", f'{SCHEMA}."{bare}$history"'))

def oldest_snapshot(engine, bare):
    st, rows = run(engine, f'SELECT min(snapshot_id) FROM {SCHEMA}."{bare}$snapshots"')
    if st == "OK" and rows and rows[0][0] is not None:
        return str(rows[0][0])
    return None

def do_engine(engine, bare, setup, verify):
    for stmt in setup:
        st, err = run(engine, sub(stmt, bare))
        if st == "ERR":
            return ("SETUP-ERR", f"{err}  [stmt: {sub(stmt, bare)[:70]}]")
    v = sub(verify, bare)
    if "{snap0}" in v:
        s0 = oldest_snapshot(engine, bare)
        if s0 is None:
            return ("VERIFY-ERR", "no snapshot id")
        v = v.replace("{snap0}", s0)
    return run(engine, v)

def cleanup(engine, bare):
    run(engine, f"DROP TABLE IF EXISTS {SCHEMA}.{bare}")

PASS, FAIL = [], []
def result(name, ok, detail):
    (PASS if ok else FAIL).append(name)
    print(f"  {name:34s} {'MATCH' if ok else 'DIFF ':5s} {detail}")

def scenario(sc):
    name, kind = sc["name"], sc["kind"]
    setup, verify = sc["setup"], sc["verify"]
    if kind == "parity":
        st, tt = f"zzip_{name}_sqe", f"zzip_{name}_trino"
        cleanup("SQE", st); cleanup("Trino", tt)
        s = do_engine("SQE", st, setup, verify)
        t = do_engine("Trino", tt, setup, verify)
        cleanup("SQE", st); cleanup("Trino", tt)
    else:  # interop
        writer = sc["writer"]; reader = "Trino" if writer == "SQE" else "SQE"
        bare = f"zzip_{name}"
        cleanup(writer, bare)
        for stmt in setup:
            wst, werr = run(writer, sub(stmt, bare))
            if wst == "ERR":
                cleanup(writer, bare); result(name, False, f"writer({writer}) setup ERR: {werr}"); return
        v = sub(verify, bare)
        s = run(writer, v)       # writer's own read (baseline)
        t = run(reader, v)       # cross-engine read
        cleanup(writer, bare)
    if s[0] != "OK" or t[0] != "OK":
        result(name, False, f"SQE={s[0]}({s[1] if s[0]!='OK' else 'ok'}) Trino={t[0]}({t[1] if t[0]!='OK' else 'ok'})")
        return
    sn, tn = norm(s[1]), norm(t[1])
    result(name, sn == tn, f"{len(sn)} rows" if sn == tn else f"SQE={sn[:3]} vs Trino={tn[:3]}")

if __name__ == "__main__":
    scen = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "iceberg_scenarios.json"))
    for sc in scen:
        try: scenario(sc)
        except Exception as e:
            result(sc["name"], False, f"EXC {e}")
    print(f"\n  {len(PASS)}/{len(PASS)+len(FAIL)} parity   ({len(FAIL)} fail)")
    if FAIL: print("  FAIL:", FAIL); sys.exit(1)
