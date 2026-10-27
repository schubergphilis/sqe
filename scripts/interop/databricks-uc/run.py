#!/usr/bin/env python3
"""SQE <-> Databricks Unity Catalog interop run. See README.md in this directory.

  run.py setup                         phases A-D (Databricks writes, SQE writes, DML both ways)
  run.py phase A|B|C|D                 one write phase
  run.py compare embedded|flight       phase E: every read on both engines, values compared
  run.py probes embedded|flight        phase F: SQE metadata and catalog commands

Environment: DBX_PROFILE, DBX_HOST, DBX_WAREHOUSE_ID, DBX_CATALOG, DBX_SCHEMA,
SQE_CLI (default target/debug/sqe-cli), SQE_PORT (default 50061), OUT (default
/tmp/sqe-dbx-interop). The Databricks user's token is passed to SQE as the
bearer token and never written to the output.
"""

import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENV = os.environ
PROFILE = ENV["DBX_PROFILE"]
HOST = ENV["DBX_HOST"].rstrip("/")
WAREHOUSE_ID = ENV["DBX_WAREHOUSE_ID"]
CATALOG = ENV["DBX_CATALOG"]
SCHEMA = ENV.get("DBX_SCHEMA", "sqe_probe")
SQE_CLI = ENV.get("SQE_CLI", "target/debug/sqe-cli")
SQE_PORT = ENV.get("SQE_PORT", "50061")
OUT = Path(ENV.get("OUT", "/tmp/sqe-dbx-interop"))
OUT.mkdir(parents=True, exist_ok=True)

DBX_S = f"`{CATALOG}`.{SCHEMA}"
SQE_S = f"dbx.{SCHEMA}"


def token():
    out = subprocess.run(["databricks", "auth", "token", "-p", PROFILE],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)["access_token"]


def statements(path):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("--"):
            yield line.rstrip(";")


# --- Databricks -------------------------------------------------------------

def dbx(sql):
    """Run one statement on the SQL warehouse. Returns (rows, error)."""
    body = json.dumps({"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s",
                       "on_wait_timeout": "CANCEL", "format": "JSON_ARRAY", "disposition": "INLINE"})
    res = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "-p", PROFILE,
                          "--json", body], capture_output=True, text=True)
    try:
        doc = json.loads(res.stdout)
    except json.JSONDecodeError:
        return None, (res.stdout + res.stderr).strip()[:500]
    state = doc.get("status", {}).get("state")
    if state != "SUCCEEDED":
        return None, f"{state}: {doc.get('status', {}).get('error', {}).get('message', doc)}"
    return doc.get("result", {}).get("data_array") or [], None


# --- SQE --------------------------------------------------------------------

def sqe(sql_text, mode, fmt, tok):
    """Run a script on SQE. Returns (stdout+stderr with the token redacted, rc)."""
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        if mode == "embedded":
            f.write(f"CREATE SECRET uc (TYPE bearer, TOKEN '{tok}');\n")
            f.write(f"ATTACH '{HOST}/api/2.1/unity-catalog/iceberg-rest' AS dbx "
                    f"(TYPE iceberg_rest, WAREHOUSE '{CATALOG}', SECRET uc);\n")
        f.write(sql_text)
        script = f.name
    os.chmod(script, 0o600)
    if mode == "embedded":
        cmd = [SQE_CLI, "--embedded", "--memory", "--file", script, "--format", fmt]
    else:
        cmd = [SQE_CLI, "-p", SQE_PORT, "--token", tok, "--file", script, "--format", fmt]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    finally:
        os.unlink(script)
    return (res.stdout + res.stderr).replace(tok, "<TOKEN>"), res.returncode


def sqe_rows(sql, mode, tok):
    out, _ = sqe(sql + ";\n", mode, "tsv", tok)
    lines = [l for l in out.splitlines() if not l.startswith("sqe-cli ")]
    err = [l for l in lines if "error" in l.lower()]
    if err:
        return None, "\n".join(err)[:500]
    if not lines:
        return [], None
    return [l.split("\t") for l in lines[1:] if not l.startswith("(")], None


# --- comparison -------------------------------------------------------------

def norm(v):
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "null", "NULL"):
        return None
    if s.lower() in ("true", "false"):
        return s.lower()
    try:
        d = Decimal(s)
        return "0" if d == 0 else format(d.normalize(), "f")
    except InvalidOperation:
        pass
    up = s.upper()
    if up.startswith(("POINT", "POLYGON", "LINESTRING", "MULTI", "GEOMETRYCOLLECTION")):
        return up.replace(", ", ",").replace(" (", "(")
    return s


def compare(mode):
    tok = token()
    lines = [l for l in (HERE / "compare.tsv").read_text().splitlines() if l and not l.startswith("#")]
    report, failed = [], 0
    for line in lines:
        name, sqe_sql, dbx_sql = line.split("\t")
        if dbx_sql == "=":
            dbx_sql = sqe_sql
        sqe_sql = sqe_sql.replace("@S@", SQE_S)
        dbx_sql = dbx_sql.replace("@S@", DBX_S)
        s_rows, s_err = sqe_rows(sqe_sql, mode, tok)
        d_rows, d_err = dbx(dbx_sql)
        if s_err or d_err:
            ok = False
            detail = f"  sqe: {s_err or 'ok'}\n  dbx: {d_err or 'ok'}"
        else:
            sn = [[norm(c) for c in r] for r in s_rows]
            dn = [[norm(c) for c in r] for r in d_rows]
            ok = sn == dn
            detail = "" if ok else f"  sqe: {sn}\n  dbx: {dn}"
        failed += not ok
        verdict = "MATCH" if ok else "DIFF "
        report.append(f"{verdict} {name} ({len(s_rows or [])} rows)" + (f"\n{detail}" if detail else ""))
        print(report[-1], flush=True)
    summary = f"{mode}: {len(lines) - failed}/{len(lines)} match"
    report.append(summary)
    (OUT / f"compare-{mode}.txt").write_text("\n".join(report) + "\n")
    print(summary)
    return failed == 0


def phase(p, mode="flight"):
    files = {"A": ("dbx", "dbx-setup.sql"), "B": ("sqe", "sqe-writes.sql"),
             "C": ("dbx", "dbx-dml.sql"), "D": ("sqe", "sqe-dml.sql")}
    engine, file = files[p]
    log, errors = [], 0
    if engine == "dbx":
        for st in statements(HERE / file):
            sql = st.replace("@S@", DBX_S)
            rows, err = dbx(sql)
            errors += err is not None
            log.append(f"{'ERR ' + err if err else 'OK'}  {sql}")
            print(log[-1][:200], flush=True)
        text = "\n".join(log) + "\n"
    else:
        sql = "".join(st.replace("@S@", SQE_S) + ";\n" for st in statements(HERE / file))
        text, _ = sqe(sql, mode, "table", token())
        errors = sum(1 for l in text.splitlines() if "] error:" in l)
        print(text)
    (OUT / f"phase-{p}.txt").write_text(text)
    print(f"phase {p}: {errors} error(s)")
    return errors == 0


def probes(mode):
    sql = "".join(st.replace("@S@", SQE_S) + ";\n" for st in statements(HERE / "sqe-probes.sql"))
    text, _ = sqe(sql, mode, "table", token())
    (OUT / f"probes-{mode}.txt").write_text(text)
    print(text)


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "setup":
        ok = all([phase(p) for p in "ABCD"])
    elif cmd == "phase":
        ok = phase(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "flight")
    elif cmd == "compare":
        ok = compare(sys.argv[2])
    elif cmd == "probes":
        probes(sys.argv[2])
        ok = True
    else:
        print(__doc__)
        ok = False
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
