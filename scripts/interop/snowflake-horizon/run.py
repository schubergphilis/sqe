#!/usr/bin/env python3
"""SQE <-> Snowflake Horizon Iceberg REST interop run. See README.md in this directory.

  run.py setup                         phases A-D (Snowflake writes, SQE writes, DML both ways)
  run.py phase A|B|C|D                 one write phase
  run.py compare embedded|flight       phase E: every read on both engines, values compared
  run.py probes embedded|flight        phase F: SQE metadata and catalog commands
  run.py token                         print a Horizon access token (for the coordinator run)

Environment: SF_ACCOUNT (orgname-accountname), SF_USER, SF_ROLE, SF_PRIVATE_KEY_FILE,
SF_DATABASE (default SQE_INTEROP), SF_SCHEMA (default SQE_PROBE), SNOW (default snow),
SQE_CLI (default target/debug/sqe-cli), SQE_PORT (default 50061), SQE_CACHE_TTL (default
30, the coordinator's metadata_cache_ttl_secs), OUT (default /tmp/sqe-sf-interop).

The Horizon token is minted from the key pair (snow connection generate-jwt, then the
Horizon OAuth endpoint) and passed to SQE as the bearer token. The token, account, user,
role, key path, bucket names and AWS account ids are redacted from every output file.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENV = os.environ
ACCOUNT = ENV["SF_ACCOUNT"]
USER = ENV["SF_USER"]
ROLE = ENV["SF_ROLE"]
KEY = os.path.expanduser(ENV["SF_PRIVATE_KEY_FILE"])
DATABASE = ENV.get("SF_DATABASE", "SQE_INTEROP")
SCHEMA = ENV.get("SF_SCHEMA", "SQE_PROBE")
SNOW = ENV.get("SNOW", "snow")
SQE_CLI = ENV.get("SQE_CLI", "target/debug/sqe-cli")
SQE_PORT = ENV.get("SQE_PORT", "50061")
CACHE_TTL = int(ENV.get("SQE_CACHE_TTL", "30"))
OUT = Path(ENV.get("OUT", "/tmp/sqe-sf-interop"))
OUT.mkdir(parents=True, exist_ok=True)

HOST = f"https://{ACCOUNT.lower()}.snowflakecomputing.com"
CATALOG_URL = f"{HOST}/polaris/api/catalog"
SF_S = f"{DATABASE}.{SCHEMA}"
SQE_S = f'sf."{SCHEMA}"'
SNOW_CONN = ["-x", "--account", ACCOUNT, "--user", USER, "--authenticator", "SNOWFLAKE_JWT",
             "--private-key-file", KEY, "--role", ROLE]

_token = {"value": None, "at": 0.0}


def redact(text):
    """Strip every identifier that ties the output to one account."""
    secrets = [_token["value"], KEY, ENV["SF_PRIVATE_KEY_FILE"], ACCOUNT, USER, ROLE]
    for s in filter(None, secrets):
        text = re.sub(re.escape(s), "<REDACTED>", text, flags=re.IGNORECASE)
    text = re.sub(r"eyJ[\w-]+\.[\w-]+\.[\w-]+", "<JWT>", text)
    text = re.sub(r"s3a?://[^/\s'\"]+", "s3://<BUCKET>", text)
    text = re.sub(r"sfc-[a-z0-9-]+", "<BUCKET>", text)
    text = re.sub(r"arn:aws:[^\s'\"]+", "<ARN>", text)
    return re.sub(r"(?<!\d)\d{12}(?!\d)", "<AWS-ACCOUNT>", text)


def token():
    """A Horizon access token, minted from the key pair. Lives about an hour; reuse it for 40 minutes."""
    if _token["value"] and time.time() - _token["at"] < 2400:
        return _token["value"]
    jwt = subprocess.run([SNOW, "connection", "generate-jwt", "-x", "--account", ACCOUNT, "--user", USER,
                          "--private-key-file", KEY, "--silent"],
                         check=True, capture_output=True, text=True).stdout.strip()
    body = urllib.parse.urlencode({"grant_type": "client_credentials", "client_secret": jwt,
                                   "scope": f"session:role:{ROLE}"}).encode()
    req = urllib.request.Request(f"{CATALOG_URL}/v1/oauth/tokens", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=30) as res:  # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dynamic-urllib-use-detected -- fixed https Snowflake host
        _token.update(value=json.load(res)["access_token"], at=time.time())
    return _token["value"]


def statements(path):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("--"):
            yield line.rstrip(";")


# --- Snowflake --------------------------------------------------------------

def sf(sql):
    """Run one statement in Snowflake. Returns (rows as lists, error)."""
    q = f"alter session set timezone = 'UTC'; {sql}"
    # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit, opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-tainted-env-args -- operator's own snow CLI (SNOW), argv list, no shell
    res = subprocess.run([SNOW, "sql", *SNOW_CONN, "--format", "json", "-q", q],
                         capture_output=True, text=True, timeout=600)
    if res.returncode != 0:
        return None, redact((res.stdout + res.stderr).strip())[:500]
    try:
        doc = json.loads(res.stdout)
    except json.JSONDecodeError:
        return None, redact((res.stdout + res.stderr).strip())[:500]
    last = doc[-1] if doc and isinstance(doc[0], list) else doc
    return [list(r.values()) for r in last], None


# --- SQE --------------------------------------------------------------------

def sqe(sql_text, mode, fmt):
    """Run a script on SQE. Returns (redacted stdout+stderr, rc)."""
    tok = token()
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        if mode == "embedded":
            f.write(f"CREATE SECRET horizon (TYPE bearer, TOKEN '{tok}');\n")
            f.write(f"ATTACH '{CATALOG_URL}' AS sf (TYPE iceberg_rest, WAREHOUSE '{DATABASE}', "
                    f"SECRET horizon, ACCESS_DELEGATION 'vended-credentials');\n")
        f.write(sql_text)
        script = f.name
    os.chmod(script, 0o600)
    if mode == "embedded":
        cmd = [SQE_CLI, "--embedded", "--memory", "--file", script, "--format", fmt]
    else:
        cmd = [SQE_CLI, "-p", SQE_PORT, "--token", tok, "--file", script, "--format", fmt]
    try:
        # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit, opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-tainted-env-args -- operator's own sqe-cli binary (SQE_CLI), argv list, no shell
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    finally:
        os.unlink(script)
    return redact(res.stdout + res.stderr), res.returncode


def sqe_rows(sql, mode):
    out, _ = sqe(sql + ";\n", mode, "tsv")
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
    if s in ("", "null", "NULL", "None"):
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
    lines = [l for l in (HERE / "compare.tsv").read_text().splitlines() if l and not l.startswith("#")]
    report, failed = [], 0
    for line in lines:
        name, sqe_sql, sf_sql = line.split("\t")
        if sf_sql == "=":
            sf_sql = sqe_sql
        s_rows, s_err = sqe_rows(sqe_sql.replace("@S@", SQE_S), mode)
        f_rows, f_err = sf(sf_sql.replace("@S@", SF_S))
        if s_err or f_err:
            ok = False
            detail = f"  sqe: {s_err or 'ok'}\n  sf:  {f_err or 'ok'}"
        else:
            sn = [[norm(c) for c in r] for r in s_rows]
            fn = [[norm(c) for c in r] for r in f_rows]
            ok = sn == fn
            detail = "" if ok else f"  sqe: {sn}\n  sf:  {fn}"
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
    files = {"A": ("sf", "sf-setup.sql"), "B": ("sqe", "sqe-writes.sql"),
             "C": ("sf", "sf-dml.sql"), "D": ("sqe", "sqe-dml.sql")}
    engine, file = files[p]
    log, errors = [], 0
    if engine == "sf":
        for st in statements(HERE / file):
            sql = st.replace("@S@", SF_S)
            _, err = sf(sql)
            errors += err is not None
            log.append(f"{'ERR ' + err if err else 'OK'}  {redact(sql)}")
            print(log[-1][:300], flush=True)
        text = "\n".join(log) + "\n"
    else:
        sql = "".join(st.replace("@S@", SQE_S) + ";\n" for st in statements(HERE / file))
        text, _ = sqe(sql, mode, "table")
        errors = sum(1 for l in text.splitlines() if "] error:" in l)
        print(text)
    (OUT / f"phase-{p}.txt").write_text(text)
    print(f"phase {p}: {errors} error(s)")
    return errors == 0


def probes(mode):
    sql = "".join(st.replace("@S@", SQE_S) + ";\n" for st in statements(HERE / "sqe-probes.sql"))
    text, _ = sqe(sql, mode, "table")
    (OUT / f"probes-{mode}.txt").write_text(text)
    print(text)


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "setup":
        ok = True
        for p in "ABCD":
            if p == "D":
                # The coordinator caches table metadata; let Snowflake's phase C commits show.
                time.sleep(CACHE_TTL + 5)
            ok = phase(p) and ok
    elif cmd == "phase":
        ok = phase(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "flight")
    elif cmd == "compare":
        ok = compare(sys.argv[2])
    elif cmd == "probes":
        probes(sys.argv[2])
        ok = True
    elif cmd == "token":
        print(token())
        ok = True
    else:
        print(__doc__)
        ok = False
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
