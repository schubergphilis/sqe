"""SQE on the engine-bench workload: fresh-process runs under a memory watchdog.

    python bench.py load  --size 10m --engines sqe-iceberg,sqe-polaris
    python bench.py run   --sizes 10m --reps 3 --engines sqe-parquet,sqe-iceberg,sqe-polaris,duckdb,polars
    python bench.py validate --size 10m
    python bench.py summary  > summary.md

The workload is https://github.com/firfircelik/engine-bench (data generator,
the four queries, and the DuckDB / Polars / Spark runners), cloned at a pinned
commit by run.sh. This file adds three SQE engines next to those and measures
all of them the same way the upstream bench.py does: one fresh process per
run, peak RSS of the whole process tree sampled every 50 ms, and a kill above
--budget-gib. Every engine reads the same data and writes its answer to
<work>/engine-bench/out/<engine>/<size>_<query>.parquet, so `validate`
compares them all with the same checksums.

SQE engines:
  sqe-parquet  sqe-cli --embedded, views over read_parquet() on the raw files.
               The like-for-like comparison with DuckDB and Polars.
  sqe-iceberg  sqe-cli --embedded on a local Iceberg warehouse (SQLite catalog,
               data files on local disk), loaded once with CTAS.
  sqe-polaris  a native sqe-server (fresh process per run) reading Iceberg
               tables from the Polaris REST catalog, data on RustFS (S3) in
               Docker. The client is sqe-cli over Arrow Flight SQL.

Storage (EB_STORAGE, set by run.sh from STORAGE=):
  local  every engine but sqe-polaris reads <work>/engine-bench/data from local
         disk (upstream's setup).
  s3     every engine reads the same Parquet files from RustFS (S3) at
         $EB_S3_PREFIX on $EB_S3_ENDPOINT, the store sqe-polaris reads from.
         DuckDB / Polars / Spark run through run_engine_s3.py, sqe-parquet's
         read_parquet() takes the s3:// prefix. sqe-iceberg has no S3 form
         (its embedded catalog is local), so sqe-polaris is the Iceberg leg.
"""
import argparse
import glob
import json
import math
import os
import re
import shutil
import signal
import statistics as st
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections import defaultdict
from datetime import datetime

import psutil

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("EB_WORK", os.path.join(HERE, ".work"))
UPSTREAM = os.path.join(WORK, "engine-bench")
DATA = os.path.join(UPSTREAM, "data")
OUT = os.path.join(UPSTREAM, "out")
RESULTS = os.path.join(WORK, "results")
LOGS = os.path.join(WORK, "logs")
GIB = 1024 ** 3
THREADS = 8  # the upstream runners pin 8 threads; SQE gets the same

SQE_CLI = os.environ.get("SQE_CLI", "sqe-cli")
SQE_SERVER = os.environ.get("SQE_SERVER", "sqe-server")
SQE_CONFIG = os.path.join(WORK, "sqe.toml")  # rendered by run.sh
FLIGHT_PORT = int(os.environ.get("SQE_FLIGHT_PORT", "61051"))
HEALTH_PORT = int(os.environ.get("SQE_METRICS_PORT", "39090")) + 1
POLARIS_CATALOG = "polaris"  # the [catalogs.polaris] key in sqe.toml
STORAGE = os.environ.get("EB_STORAGE", "local")
S3_PREFIX = os.environ.get("EB_S3_PREFIX", "s3://warehouse/engine-bench/data").rstrip("/")

QUERIES = ["q1_filter_agg", "q2_join_agg", "q3_window_latest", "q4_highcard_agg"]
SQE_ENGINES = ["sqe-parquet", "sqe-iceberg", "sqe-polaris"]
ORDER_E = SQE_ENGINES + ["duckdb", "polars", "spark"]

# Same row count + rounded checksums as upstream validate.py.
CHECK = {
    "q1_filter_agg": "COUNT(*), SUM(orders), ROUND(SUM(revenue), 0)",
    "q2_join_agg": "COUNT(*), SUM(orders), ROUND(SUM(revenue), 0), ROUND(SUM(avg_delta), 3)",
    "q3_window_latest": "COUNT(*), SUM(order_id), ROUND(SUM(amount), 0)",
    "q4_highcard_agg": "COUNT(*), SUM(orders), ROUND(SUM(revenue), 0), "
                       "CAST(MIN(first_ts) AS TIMESTAMP), CAST(MAX(last_ts) AS TIMESTAMP)",
}


def sql_text(query):
    sys.path.insert(0, UPSTREAM)
    from queries import SQL  # type: ignore  # noqa: E402  (upstream, pinned by run.sh)
    return " ".join(SQL[query].split())


def warehouse(size):
    return os.path.join(WORK, f"iceberg-{size}")


def namespace(size):
    return f"bench_{size}"


# ── process plumbing ─────────────────────────────────────────────────────────

def tree_rss(procs):
    total = 0
    for p in procs:
        try:
            members = [p] + p.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        for q in members:
            try:
                total += q.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    return total


def kill_tree(p):
    try:
        members = p.children(recursive=True) + [p]
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return
    for q in members:
        try:
            q.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


def wait_for_room(min_avail_gib, max_busy_pct):
    """Upstream's gate: enough free memory and a quiet CPU over a 3 s window."""
    while True:
        avail = psutil.virtual_memory().available / GIB
        busy = psutil.cpu_percent(interval=3)
        if avail >= min_avail_gib and busy <= max_busy_pct:
            return avail, busy
        print(f"  waiting: avail={avail:.2f} GiB cpu_busy={busy:.0f}%", flush=True)
        time.sleep(2)


def watch(cmd, stdin_text, watched_extra, budget, floor, timeout, env=None):
    """Run cmd to completion under the watchdog. The RSS budget covers cmd's
    process tree plus every process in watched_extra (the sqe-server)."""
    # stdout/stderr go to files, not pipes: an engine that logs more than a
    # pipe buffer (Spark) would block on a full pipe while we only poll.
    out_f, err_f = tempfile.TemporaryFile("w+"), tempfile.TemporaryFile("w+")
    in_f = tempfile.TemporaryFile("w+")
    in_f.write(stdin_text or "")
    in_f.seek(0)
    t0 = time.perf_counter()
    # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit -- argv list from ENGINES (local engine binaries), no shell
    proc = subprocess.Popen(cmd, stdin=in_f, stdout=out_f, stderr=err_f, text=True,
                            cwd=UPSTREAM, env=env or os.environ.copy())
    p = psutil.Process(proc.pid)
    procs = [p] + watched_extra
    peak, min_avail, killed = 0, float("inf"), None
    while proc.poll() is None:
        rss = tree_rss(procs)
        peak = max(peak, rss)
        avail = psutil.virtual_memory().available
        min_avail = min(min_avail, avail)
        if rss > budget * GIB:
            killed = f"watchdog: tree RSS {rss / GIB:.2f} GiB > budget {budget} GiB"
        elif avail < floor * GIB:
            killed = f"watchdog: system available {avail / GIB:.2f} GiB < floor {floor} GiB"
        elif time.perf_counter() - t0 > timeout:
            killed = f"watchdog: timeout {timeout}s"
        if killed:
            kill_tree(p)
            break
        time.sleep(0.05)
    proc.wait()
    wall = time.perf_counter() - t0
    out_f.seek(0)
    err_f.seek(0)
    out, err = out_f.read(), err_f.read()
    for f in (in_f, out_f, err_f):
        f.close()
    return {
        "wall_s": round(wall, 3),
        "peak_rss_gib": round(peak / GIB, 3),
        "min_sys_avail_gib": round(min_avail / GIB, 2) if min_avail != float("inf") else None,
        "exit": proc.returncode,
        "killed": killed,
        "stdout": out,
        "stderr": err,
    }


def sqe_failed(res):
    """sqe-cli reads a script from stdin and keeps going after a failed
    statement, printing `Error: ...` on stderr; treat any of those as a failure."""
    return res["exit"] != 0 or any(l.startswith("Error:") for l in res["stderr"].splitlines())


def stderr_tail(res):
    """The last stderr lines, with the S3 keys masked: an engine error can echo
    the statement that carried them (DuckDB's CREATE SECRET), and the tail ends
    up in the results file and OUTPUT.md."""
    all_lines = [l for l in res["stderr"].splitlines() if l.strip()]
    # A JVM failure ends in stack frames; the line that names the error is
    # further up. Keep the first few such lines next to the tail.
    named = [l for l in all_lines
             if re.search(r"(Exception|Error)\b", l) and not l.lstrip().startswith("at ")][:4]
    lines = named + [l for l in all_lines[-6:] if l not in named]
    for k in ("S3_SECRET_KEY", "S3_ACCESS_KEY"):
        v = os.environ.get(k)
        if v:
            lines = [l.replace(v, "***") for l in lines]
    return lines


def copy_time(stderr):
    """`.timer on` prints `Time: 1.234s` after each statement; the COPY is last."""
    times = re.findall(r"^Time: ([0-9.]+)s", stderr, re.M)
    return float(times[-1]) if times else None


def cli_version():
    try:
        # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit, opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-tainted-env-args -- operator's own sqe-cli binary (SQE_CLI), argv list, no shell
        return subprocess.run([SQE_CLI, "--version"], capture_output=True, text=True).stdout.strip()
    except OSError:
        return None


# ── sqe-server lifecycle (sqe-polaris) ───────────────────────────────────────

def healthy():
    try:
        # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dynamic-urllib-use-detected -- fixed loopback health endpoint of the local sqe-server
        with urllib.request.urlopen(f"http://127.0.0.1:{HEALTH_PORT}/healthz", timeout=1) as r:
            return r.status == 200
    except OSError:
        return False


def start_server(tag):
    if healthy():
        sys.exit(f"something already answers on health port {HEALTH_PORT}; stop it first")
    os.makedirs(LOGS, exist_ok=True)
    log = open(os.path.join(LOGS, f"sqe-server-{tag}.log"), "w")
    t0 = time.perf_counter()
    # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit, opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-tainted-env-args -- operator's own sqe-server binary (SQE_SERVER), argv list, no shell
    proc = subprocess.Popen([SQE_SERVER, "--mode", "coordinator", "--config", SQE_CONFIG],
                            stdout=log, stderr=subprocess.STDOUT, cwd=WORK,
                            env=dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "warn")))
    while not healthy():
        if proc.poll() is not None:
            sys.exit(f"sqe-server exited during start; see {log.name}")
        if time.perf_counter() - t0 > 120:
            proc.kill()
            sys.exit(f"sqe-server not healthy after 120s; see {log.name}")
        time.sleep(0.1)
    return proc, round(time.perf_counter() - t0, 3)


def stop_server(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            kill_tree(psutil.Process(proc.pid))
            proc.wait()


def flight_cmd():
    return [SQE_CLI, "-H", "127.0.0.1", "-p", str(FLIGHT_PORT), "-u", "root", "-f", "table"]


def flight_env():
    # The client_credentials provider mints Polaris's root token itself; the
    # Flight login only names the user.
    return dict(os.environ, SQE_PASSWORD="unused")


# ── engines ──────────────────────────────────────────────────────────────────

def sqe_script(engine, size, query):
    """The sqe-cli script for the two embedded engines."""
    out = os.path.join(OUT, engine, f"{size}_{query}.parquet")
    if engine == "sqe-parquet":
        data = S3_PREFIX if STORAGE == "s3" else DATA
        src = (f"CREATE VIEW sales AS SELECT * FROM read_parquet('{data}/sales_{size}/');\n"
               f"CREATE VIEW products AS SELECT * FROM read_parquet('{data}/products.parquet');\n")
    else:  # sqe-iceberg
        src = ("CREATE VIEW sales AS SELECT * FROM iceberg.bench.sales;\n"
               "CREATE VIEW products AS SELECT * FROM iceberg.bench.products;\n")
    return out, (f"SET datafusion.execution.target_partitions = {THREADS};\n{src}"
                 f".timer on\nCOPY ({sql_text(query)}) TO '{out}';\n")


def polaris_sql(size, query):
    """The upstream query with its two tables qualified to the Polaris namespace."""
    ns = f"{POLARIS_CATALOG}.{namespace(size)}"
    return re.sub(r"\b(FROM|JOIN)\s+(sales|products)\b", rf"\1 {ns}.\2", sql_text(query))


def flight_query(a):
    """Client side of sqe-polaris, run as its own process under the watchdog:
    send the query over Arrow Flight SQL (ADBC, the driver dbt-sqe uses), fetch
    the Arrow result and write it to Parquet, like the other engines' COPY.
    Prints RESULT {json} the way upstream's run_engine.py does."""
    import adbc_driver_flightsql.dbapi as flight_sql
    import pyarrow.parquet as pq
    t0 = time.perf_counter()
    conn = flight_sql.connect(f"grpc://127.0.0.1:{FLIGHT_PORT}",
                              db_kwargs={"username": "root", "password": "unused"})
    cur = conn.cursor()
    t1 = time.perf_counter()
    cur.execute(polaris_sql(a.size, a.query))
    table = cur.fetch_arrow_table()
    pq.write_table(table, a.out)
    t2 = time.perf_counter()
    cur.close()
    conn.close()
    print("RESULT " + json.dumps({"query_s": round(t2 - t1, 3), "connect_s": round(t1 - t0, 3),
                                  "rows_out": table.num_rows}), flush=True)


def sqe_s3_env():
    """The embedded engine's S3 settings (sqe-cli reads SQE_STORAGE__S3_*), or
    None for the local run. Path-style is implied by the endpoint override."""
    if STORAGE != "s3":
        return None
    return dict(os.environ,
                SQE_STORAGE__S3_ENDPOINT=os.environ["EB_S3_ENDPOINT"],
                SQE_STORAGE__S3_ACCESS_KEY=os.environ["S3_ACCESS_KEY"],
                SQE_STORAGE__S3_SECRET_KEY=os.environ["S3_SECRET_KEY"],
                SQE_STORAGE__S3_REGION="us-east-1")


def run_sqe(engine, size, query, mem, budget, floor, timeout):
    out = os.path.join(OUT, engine, f"{size}_{query}.parquet")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    if os.path.exists(out):
        os.remove(out)
    rec: dict = {"version": cli_version()}
    if engine == "sqe-polaris":
        # A fresh coordinator per run, like every other engine's fresh process.
        # Its start-up (catalog + auth wiring) is recorded but not timed: the
        # watchdog clock starts once /healthz answers. RSS covers server + client.
        server, rec["server_start_s"] = start_server(f"{size}-{query}")
        try:
            res = watch([sys.executable, os.path.abspath(__file__), "flight-query",
                         "--size", size, "--query", query, "--out", out],
                        None, [psutil.Process(server.pid)], budget, floor, timeout)
        finally:
            stop_server(server)
        line = next((l for l in res["stdout"].splitlines() if l.startswith("RESULT ")), None)
        q = json.loads(line[7:]) if line else None
    else:
        _, script = sqe_script(engine, size, query)
        cmd = [SQE_CLI, "--embedded", "--memory-limit", mem_arg(mem), "-f", "table"]
        cmd[2:2] = ["--warehouse", warehouse(size)] if engine == "sqe-iceberg" else ["--memory"]
        res = watch(cmd, script, [], budget, floor, timeout, env=sqe_s3_env())
        t = copy_time(res["stderr"]) if not sqe_failed(res) else None
        q = {"query_s": t} if t is not None else None
    rec.update({k: res[k] for k in ("wall_s", "peak_rss_gib", "min_sys_avail_gib", "exit")})
    if res["killed"] or res["exit"] != 0 or q is None or not os.path.exists(out):
        rec["status"] = res["killed"] or "error"
        rec["stderr_tail"] = stderr_tail(res)
        return rec
    import pyarrow.parquet as pq
    rec.update(q)
    rec.update({"status": "ok", "setup_s": round(res["wall_s"] - q["query_s"], 3),
                "rows_out": pq.ParquetFile(out).metadata.num_rows})
    return rec


def run_upstream(engine, size, query, mem, budget, floor, timeout):
    """DuckDB / Polars / Spark through upstream's own run_engine.py, or through
    run_engine_s3.py (the same runner reading from S3) for STORAGE=s3."""
    runner = (os.path.join(HERE, "run_engine_s3.py") if STORAGE == "s3"
              else os.path.join(UPSTREAM, "run_engine.py"))
    res = watch([sys.executable, runner, engine, size, query, mem],
                None, [], budget, floor, timeout)
    rec = {k: res[k] for k in ("wall_s", "peak_rss_gib", "min_sys_avail_gib", "exit")}
    line = next((l for l in res["stdout"].splitlines() if l.startswith("RESULT ")), None)
    if line and not res["killed"] and res["exit"] == 0:
        rec.update({k: v for k, v in json.loads(line[7:]).items() if k not in rec})
        rec["status"] = "ok"
    else:
        rec["status"] = res["killed"] or "error"
        rec["stderr_tail"] = stderr_tail(res)
    return rec


def mem_arg(mem):
    m = mem.upper()
    return m if m.endswith("B") else m + "B"  # 3g -> 3GB


# ── load (Iceberg engines) ───────────────────────────────────────────────────

def load_script(catalog_ns, size, replace):
    drop = (f"DROP TABLE IF EXISTS {catalog_ns}.sales;\n"
            f"DROP TABLE IF EXISTS {catalog_ns}.products;\n") if replace else ""
    return (f"CREATE SCHEMA IF NOT EXISTS {catalog_ns};\n{drop}"
            f"CREATE TABLE {catalog_ns}.products AS SELECT * FROM read_parquet('{DATA}/products.parquet');\n"
            f"CREATE TABLE {catalog_ns}.sales AS SELECT * FROM read_parquet('{DATA}/sales_{size}/');\n"
            f"SELECT COUNT(*) AS n FROM {catalog_ns}.sales;\n")


def expected_rows(size):
    import pyarrow.parquet as pq
    return sum(pq.ParquetFile(f).metadata.num_rows
               for f in glob.glob(os.path.join(DATA, f"sales_{size}", "*.parquet")))


def loaded_rows(stdout):
    nums = re.findall(r"^\|\s*(\d+)\s*\|\s*$", stdout, re.M)
    return int(nums[-1]) if nums else None


def load(a):
    want = expected_rows(a.size)
    for engine in a.engines.split(","):
        t0 = time.perf_counter()
        if engine == "sqe-iceberg":
            if os.path.isdir(warehouse(a.size)) and not a.force:
                res = run_cli_embedded_iceberg(a.size, "SELECT COUNT(*) AS n FROM iceberg.bench.sales;\n")
                if loaded_rows(res.stdout) == want:
                    print(f"{engine}: {a.size} already loaded ({want} rows)")
                    continue
            shutil.rmtree(warehouse(a.size), ignore_errors=True)
            res = run_cli_embedded_iceberg(a.size, load_script("iceberg.bench", a.size, False))
        elif engine == "sqe-polaris":
            server, _ = start_server(f"load-{a.size}")
            try:
                ns = f"{POLARIS_CATALOG}.{namespace(a.size)}"
                # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit -- operator's own sqe-cli binary (flight_cmd), argv list, no shell
                res = subprocess.run(flight_cmd(), input=f"SELECT COUNT(*) AS n FROM {ns}.sales;\n",
                                     capture_output=True, text=True, env=flight_env())
                if loaded_rows(res.stdout) == want and not a.force:
                    print(f"{engine}: {a.size} already loaded ({want} rows)")
                    continue
                # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit -- operator's own sqe-cli binary (flight_cmd), argv list, no shell
                res = subprocess.run(flight_cmd(), input=load_script(ns, a.size, True),
                                     capture_output=True, text=True, env=flight_env())
            finally:
                stop_server(server)
        else:
            continue  # the file engines read data/ directly
        got = loaded_rows(res.stdout)
        if res.returncode != 0 or got != want:
            print(res.stdout[-2000:], res.stderr[-4000:], sep="\n", file=sys.stderr)
            sys.exit(f"{engine}: load of {a.size} failed (got {got} rows, want {want})")
        print(f"{engine}: loaded {want} rows into Iceberg in {time.perf_counter() - t0:.1f}s")


def run_cli_embedded_iceberg(size, script):
    # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit, opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-tainted-env-args -- operator's own sqe-cli binary (SQE_CLI), argv list, no shell
    return subprocess.run([SQE_CLI, "--embedded", "--warehouse", warehouse(size),
                           "--memory-limit", "4GB", "-f", "table"],
                          input=script, capture_output=True, text=True)


# ── run / validate / summary ─────────────────────────────────────────────────

def rustfs_tx_bytes():
    """Bytes RustFS has sent on its container interface so far, or None when the
    Polaris/RustFS stack is not up. Sampled around each run (outside the timed
    window) so every S3-reading run records how much it pulled from the store."""
    try:
        dev = subprocess.run(["docker", "exec", "engine-bench-rustfs-1", "cat", "/proc/net/dev"],
                             capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"^\s*eth0:\s*(.*)$", dev, re.M)
    return int(m.group(1).split()[8]) if m else None


def run(a):
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"{a.tag}.jsonl")
    engines = a.engines.split(",")
    queries = a.queries.split(",")
    # --budget-for polars=8,spark=6: a per-engine watchdog cap instead of --budget-gib.
    budgets = {e: float(g) for e, g in (kv.split("=") for kv in a.budget_for.split(",") if kv)}
    if STORAGE == "s3" and "sqe-iceberg" in engines:
        sys.exit("sqe-iceberg reads a local warehouse; with STORAGE=s3 use sqe-polaris")
    with open(path, "a") as f:
        for size in a.sizes.split(","):
            for rep in range(1, a.reps + 1):
                for qi, query in enumerate(queries):
                    k = (rep + qi) % len(engines)  # rotate so no engine always runs first
                    for engine in engines[k:] + engines[:k]:
                        avail, busy = wait_for_room(a.min_avail_gib, a.max_busy_pct)
                        runner = run_sqe if engine in SQE_ENGINES else run_upstream
                        rec = {"engine": engine, "size": size, "query": query, "mem": a.mem,
                               "storage": "s3" if STORAGE == "s3" or engine == "sqe-polaris" else "local"}
                        tx0 = rustfs_tx_bytes() if rec["storage"] == "s3" else None
                        budget = budgets.get(engine, a.budget_gib)
                        if budget != a.budget_gib:
                            rec["budget_gib"] = budget  # a per-engine exception, kept on the record
                        rec.update(runner(engine, size, query, a.mem, budget, a.floor_gib, a.timeout))
                        tx1 = rustfs_tx_bytes() if tx0 is not None else None
                        if tx0 is not None and tx1 is not None:
                            rec["s3_read_gb"] = round((tx1 - tx0) / 1e9, 3)
                        rec.update({"rep": rep, "pre_avail_gib": round(avail, 2),
                                    "pre_cpu_busy_pct": round(busy, 1),
                                    "started": datetime.now().isoformat(timespec="seconds")})
                        f.write(json.dumps(rec) + "\n")
                        f.flush()
                        print(f"{size} r{rep} {query:17s} {engine:12s} {rec['status'][:40]:>6s} "
                              f"wall={rec['wall_s']:7.2f}s q={rec.get('query_s', '-')} "
                              f"rss={rec['peak_rss_gib']:.2f}GiB rows={rec.get('rows_out', '-')}"
                              + (f" s3={rec['s3_read_gb']}GB" if "s3_read_gb" in rec else ""),
                              flush=True)
    print("wrote", os.path.relpath(path, HERE))


def output_path(engine, size, query):
    if engine == "spark":
        return os.path.join(OUT, engine, f"{size}_{query}", "*.parquet")
    return os.path.join(OUT, engine, f"{size}_{query}.parquet")


def validate(a):
    import duckdb
    con = duckdb.connect()
    # Only the engines this run measured: an output left by an earlier run in
    # the other storage mode (or of an engine not selected now) would be
    # reported as this run's answer.
    wanted = set(a.engines.split(",")) if a.engines else set(ORDER_E)
    engines = [e for e in ORDER_E if e in wanted and glob.glob(output_path(e, a.size, "*"))]
    if not engines:
        sys.exit(f"no outputs for {a.size} under {OUT}")
    ok = True
    print(f"| query | {' | '.join(engines)} |")
    print(f"|---|{'---|' * len(engines)}")
    for q in QUERIES:
        vals = {}
        for e in engines:
            try:
                # nosemgrep: opt.aikido.semgrep-rules.python.sqlalchemy.security.sqlalchemy-execute-raw-query, opt.aikido.semgrep-rules.python.lang.security.audit.formatted-sql-query -- in-process DuckDB; CHECK is a constant and the path is the bench's own output file
                vals[e] = con.execute(f"SELECT {CHECK[q]} FROM read_parquet('{output_path(e, a.size, q)}')").fetchone()
            except Exception:  # noqa: BLE001  (no output: the run failed or was skipped)
                vals[e] = None
        present = {e: v for e, v in vals.items() if v is not None}
        ref = present.get("duckdb") or next(iter(present.values()), None)
        cells = []
        for e in engines:
            if vals[e] is None:
                cells.append("no output")
            elif vals[e] == ref:
                cells.append("match")
            else:
                ok = False
                cells.append(f"DIFF {vals[e]}")
        print(f"| {q} | {' | '.join(cells)} |")
    print()
    print("ALL MATCH" if ok else "MISMATCH")
    sys.exit(0 if ok else 1)


def summary(a):
    recs = []
    for path in sorted(glob.glob(os.path.join(RESULTS, "*.jsonl"))):
        if a.tag and os.path.basename(path) != f"{a.tag}.jsonl":
            continue
        recs += [json.loads(line) for line in open(path)]
    groups = defaultdict(list)
    for r in recs:
        groups[(r["size"], r["query"], r["engine"])].append(r)
    sizes = sorted({k[0] for k in groups}, key=lambda s: int(s[:-1]))
    for size in sizes:
        engines = [e for e in ORDER_E if any(k[0] == size and k[2] == e for k in groups)]

        def med(q, e, field):
            vals = [r[field] for r in groups.get((size, q, e), [])
                    if r["status"] == "ok" and r.get(field) is not None]
            return st.median(vals) if vals else None

        def table(field, best_low, extra_rows=()):
            print(f"| query | {' | '.join(f'`{e}`' for e in engines)} |")
            print(f"|---|{'---:|' * len(engines)}")
            for q in QUERIES:
                vals = {e: med(q, e, field) for e in engines}
                best = min((v for v in vals.values() if v is not None), default=None) if best_low else None
                cells = []
                for e in engines:
                    v = vals[e]
                    if not groups.get((size, q, e)):
                        cells.append("-")
                    elif v is None:
                        cells.append("failed")
                    else:
                        cells.append(f"**{v:.2f}**" if v == best else f"{v:.2f}")
                print(f"| {q} | {' | '.join(cells)} |")
            for label, fn in extra_rows:
                print(f"| {label} | {' | '.join(fn(e) for e in engines)} |")

        # Geometric mean over the queries every run of the engine finished.
        def geomean(e):
            vals = [med(q, e, "query_s") for q in QUERIES]
            if any(v is None or v <= 0 for v in vals):
                return None
            return math.exp(sum(math.log(v) for v in vals) / len(vals))

        gm = {e: geomean(e) for e in engines}
        ref = gm.get("duckdb")
        best_gm = min((v for v in gm.values() if v), default=None)

        print(f"\n### {size} rows: query time\n")
        print("Median seconds to read, compute and write the answer (lower is better; "
              "the fastest per query in bold).\n")
        table("query_s", True, (
            ("geomean", lambda e: "-" if not gm[e] else
             f"**{gm[e]:.2f}**" if gm[e] == best_gm else f"{gm[e]:.2f}"),
            ("× DuckDB", lambda e: f"{gm[e] / ref:.1f}×" if gm[e] and ref else "-"),
        ))

        ranked = sorted(((e, v) for e, v in gm.items() if v), key=lambda ev: ev[1])
        if ranked:
            top = ranked[-1][1]
            width = max(len(e) for e, _ in ranked)
            print("\n```text")
            print(f"geomean query time, {size} rows (lower is better)")
            for e, v in ranked:
                bar = "█" * max(1, round(40 * v / top))
                print(f"{e:<{width}}  {bar:<40}  {v:.2f} s")
            print("```")

        print(f"\n### {size} rows: memory and wall time\n")
        print("<details><summary>Peak RSS of the whole process tree (GiB) and wall "
              "seconds per run (finished runs / runs)</summary>\n")
        print("**Peak RSS, GiB** (median)\n")
        table("peak_rss_gib", False)
        print("\n**Wall seconds** (median; process start to exit, so start-up and "
              "catalog open are in it)\n")
        print(f"| query | {' | '.join(f'`{e}`' for e in engines)} |")
        print(f"|---|{'---:|' * len(engines)}")
        for q in QUERIES:
            cells = []
            for e in engines:
                rs = groups.get((size, q, e), [])
                okr = [r for r in rs if r["status"] == "ok"]
                v = med(q, e, "wall_s")
                cells.append("-" if not rs else
                             f"{v:.2f} ({len(okr)}/{len(rs)})" if v is not None else
                             f"failed ({len(okr)}/{len(rs)})")
            print(f"| {q} | {' | '.join(cells)} |")
        print("\n</details>")
        fails = [r for r in recs if r["size"] == size and r["status"] != "ok"]
        if fails:
            print("\nFailed runs:\n")
            for r in fails:
                print(f"- {r['engine']} {r['query']} rep {r['rep']}: {r['status']}"
                      + (f" ({r['stderr_tail'][-1][:160]})" if r.get("stderr_tail") else ""))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    lp = sub.add_parser("load")
    lp.add_argument("--size", default="10m")
    lp.add_argument("--engines", default="sqe-iceberg,sqe-polaris")
    lp.add_argument("--force", action="store_true", help="reload even when the row count matches")
    rp = sub.add_parser("run")
    rp.add_argument("--sizes", default="10m")
    rp.add_argument("--engines", default="sqe-parquet,sqe-iceberg,sqe-polaris,duckdb,polars")
    rp.add_argument("--queries", default=",".join(QUERIES))
    rp.add_argument("--reps", type=int, default=3)
    rp.add_argument("--mem", default="3g")
    rp.add_argument("--budget-gib", type=float, default=4.5)
    rp.add_argument("--budget-for", default="", help="per-engine caps, e.g. polars=8 (recorded per run)")
    rp.add_argument("--floor-gib", type=float, default=1.0)
    rp.add_argument("--min-avail-gib", type=float, default=5.0)
    rp.add_argument("--max-busy-pct", type=float, default=30.0)
    rp.add_argument("--timeout", type=int, default=900)
    rp.add_argument("--tag", default="run")
    vp = sub.add_parser("validate")
    vp.add_argument("--size", default="10m")
    vp.add_argument("--engines", default="", help="comma list; default: every engine with an output")
    fp = sub.add_parser("flight-query", help="internal: the sqe-polaris client process")
    fp.add_argument("--size", required=True)
    fp.add_argument("--query", required=True)
    fp.add_argument("--out", required=True)
    sp = sub.add_parser("summary")
    sp.add_argument("--tag", default="")
    a = ap.parse_args()
    {"load": load, "run": run, "validate": validate, "summary": summary,
     "flight-query": flight_query}[a.cmd](a)


if __name__ == "__main__":
    main()
