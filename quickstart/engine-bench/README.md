---
slug: engine-bench
title: "engine-bench: SQE embedded and on Polaris vs DuckDB and Polars"
description: "Run the engine-bench workload (10M-row star schema, four laptop-sized queries) against SQE embedded on Parquet, SQE embedded on a local Iceberg warehouse, and a native SQE coordinator on Polaris + S3, next to DuckDB and Polars, with every answer checked against DuckDB's."
---

# engine-bench: SQE embedded and on Polaris vs DuckDB and Polars

[engine-bench](https://github.com/firfircelik/engine-bench) is a small public
benchmark for single-machine engines: a seeded star schema (a `sales` fact and
a `products` dimension) and four queries a laptop should handle: a filtered
rollup, a fact-to-dimension join, a latest-row-per-customer window and a
one-row-per-customer aggregate. This quickstart runs that workload, unchanged,
against three ways of running SQE and against the upstream DuckDB and Polars
runners. Every engine's answer is checked against DuckDB's.

| Engine | What runs | Where the data is |
|---|---|---|
| `sqe-parquet` | `sqe-cli --embedded`, a view over `read_parquet()` | The generated Parquet files. The like-for-like comparison with DuckDB and Polars. |
| `sqe-iceberg` | `sqe-cli --embedded --warehouse` | A local Iceberg warehouse (SQLite catalog, data on local disk), loaded once with CTAS. |
| `sqe-polaris` | A native `sqe-server` coordinator; the client is Arrow Flight SQL (ADBC) | Iceberg tables in the Polaris REST catalog, data in S3 (RustFS in Docker). |
| `duckdb`, `polars` | Upstream's `run_engine.py` | The generated Parquet files. |
| `spark` (opt-in) | Upstream's `run_engine.py`, PySpark local mode | The generated Parquet files. |

## What you get

| Piece | Role |
|---|---|
| `run.sh` | Clones engine-bench at a pinned commit, generates the data, loads Iceberg, runs, validates, writes [`OUTPUT.md`](./OUTPUT.md). |
| `bench.py` | The runner. One fresh process per run, peak RSS of the whole process tree sampled every 50 ms, a kill above 4.5 GiB, the same as upstream's `bench.py`. Subcommands `load`, `run`, `validate`, `summary`. |
| `docker-compose.yml` + `bootstrap.sh` | RustFS (S3) and Polaris 1.7.0, and a one-shot that creates the `bench` catalog and grants the root principal `CATALOG_MANAGE_CONTENT`. |
| `sqe.toml.tmpl` | The coordinator config for the Polaris leg, rendered to `.work/sqe.toml`. |

Only the catalog and the object store run in Docker. SQE runs natively in every
mode, so its timings and memory are measured the same way as DuckDB's and
Polars', with no container or VM in between.

## Prerequisites

- Docker with Compose v2 (for the Polaris leg).
- [`uv`](https://docs.astral.sh/uv/) and `git`. `run.sh` makes a Python 3.12
  venv in `.work/venv` with the pinned versions in `requirements.txt`.
- A release build of `sqe-cli` and `sqe-server`. `run.sh` uses
  `target/release/` and runs `make release` when it is missing (about 10
  minutes cold). Point `SQE_CLI` / `SQE_SERVER` elsewhere to use other
  binaries. Do not benchmark debug or `make dev-fast` builds.
- About 1 GB of disk for 10M rows (Parquet, two Iceberg copies, outputs), and
  8 GB of RAM (a run waits until 5 GiB is free). Each engine is capped at 3 GB and killed at 4.5 GiB.
- Java 17+ only for `WITH_SPARK=1`.

## Run it

```bash
cd quickstart/engine-bench
./run.sh                          # 10M rows, 3 reps, sqe-* + duckdb + polars
SIZE=50 REPS=5 ./run.sh           # 50M rows
ENGINES=sqe-polaris,duckdb ./run.sh
WITH_SPARK=1 ./run.sh             # add Spark
MAX_BUSY_PCT=50 ./run.sh          # a laptop that never idles below 30% CPU
./run.sh --check                  # run, then assert every run finished and every answer matches
./run.sh --down                   # stop Polaris + RustFS and drop the volume
./run.sh --clean                  # --down, and delete .work/
```

Everything the run produces lives in `.work/` (gitignored): the upstream
checkout and its `data/` and `out/`, the Iceberg warehouse, the rendered
`sqe.toml`, server logs, and the raw runs as JSON lines in
`.work/results/<size>-<timestamp>.jsonl`.

## How it works

```
gen_data.py (upstream, seeded)   ->  .work/engine-bench/data/sales_10m/*.parquet + products.parquet
bench.py load                    ->  CTAS read_parquet() into iceberg.bench.*  (embedded, local)
                                     CTAS read_parquet() into polaris.bench_10m.*  (sqe-server -> Polaris + RustFS)
bench.py run                     ->  for each rep, query, engine (rotated): wait for a quiet machine,
                                     start a fresh process, run the query, write the answer to
                                     out/<engine>/10m_<query>.parquet, record wall/query/RSS
bench.py validate                ->  row count + rounded checksums of every answer, compared to DuckDB's
bench.py summary                 ->  median tables into OUTPUT.md
```

How each SQE engine runs one query:

- **`sqe-parquet`, `sqe-iceberg`**: one `sqe-cli --embedded --memory-limit 3GB`
  process reads a script from stdin. The script sets 8 target partitions (the
  upstream runners pin 8 threads), defines `sales` and `products` as views (over
  `read_parquet()`, or over `iceberg.bench.*`), so the upstream SQL runs
  unchanged, and ends with `COPY (<query>) TO '<out>.parquet'`. `query_s` is
  the time sqe-cli's `.timer` reports for that COPY; the rest of the wall time
  (process start, catalog open, views) is `setup_s`.
- **`sqe-polaris`**: `bench.py` starts a fresh `sqe-server --mode coordinator`
  and waits for `/healthz`. Then a Python client process connects over Arrow
  Flight SQL with ADBC (the driver the dbt-sqe adapter uses), runs the query
  with `sales` / `products` qualified to `polaris.bench_10m`, fetches the Arrow
  result and writes it to Parquet. `query_s` is execute + fetch + write; the
  RSS figure covers server and client together. The server's own start-up is
  recorded as `server_start_s` and is not in `wall_s`, since the other engines
  have no server to start.

## Configuration explained

### `sqe.toml.tmpl` (the Polaris leg)

```toml
[coordinator]
memory_limit = "3GB"          # the same 3 GB the other engines get (MEM)
spill_to_disk = true

[[auth.providers]]
type = "client_credentials"   # every Flight login becomes Polaris's root principal
token_endpoint = "http://localhost:38181/api/catalog/v1/oauth/tokens"
client_id = "root"

[catalogs.polaris]            # the SQL catalog name: polaris.bench_10m.sales
polaris_url = "http://localhost:38181/api/catalog"
warehouse = "bench"

[storage.tvf]
allow_local_paths = true      # the load reads the generated Parquet off local disk
```

- The `client_credentials` provider gives every user one service token. That
  keeps the bench about the engine, not about identity; never use it where
  users must be told apart (see the `polaris-keycloak-*` quickstarts for that).
- `allow_local_paths` is a security control that is off by default; it is on
  here only because the load stages data on local disk.
- `[storage]` gives SQE the RustFS key directly. The catalog is created with
  `stsUnavailable: true`, so Polaris vends no credentials.

### `bootstrap.sh`

Creates the `bench` catalog on `s3://warehouse/`, with `endpoint` set to the
host-visible `http://localhost:39000` (SQE runs on the host) and
`endpointInternal` set to `http://rustfs:9000` (Polaris runs in the compose
network). It then grants `CATALOG_MANAGE_CONTENT` to the `service_admin` role
the root principal holds. The namespaces (`bench_10m`, ...) are created by the
load through SQE.

### `.env.example`

Ports (offset to 38181 / 39000 / 61051, so this stack runs next to the
integration and Ranger stacks), the local demo secrets, and the optional
`SIZE` / `REPS` / `MEM` / `ENGINES` / `SQE_CLI` / `SQE_SERVER`. Leave the
workload settings commented out if you want to set them inline: `run.sh`
sources `.env` after the shell environment, so a value there wins.

## Output

From a clean `MAX_BUSY_PCT=50 ./run.sh --check` at 10M rows on an Apple M3 Pro
laptop (11 cores, 36 GB), captured in [`OUTPUT.md`](./OUTPUT.md) with the memory
and wall-time tables. Each run starts only when the machine is under 50% busy.
Every answer matches DuckDB's:

| query | `sqe-parquet` | `sqe-iceberg` | `sqe-polaris` | `duckdb` | `polars` |
|---|---:|---:|---:|---:|---:|
| q1_filter_agg | 0.11 | 0.51 | 0.85 | **0.07** | 0.12 |
| q2_join_agg | 0.25 | 0.55 | 0.91 | 0.15 | **0.12** |
| q3_window_latest | 0.79 | 0.97 | 1.78 | **0.34** | 1.19 |
| q4_highcard_agg | 0.23 | 0.37 | 1.28 | 0.25 | **0.19** |
| geomean | 0.27 | 0.56 | 1.15 | **0.17** | 0.24 |
| × DuckDB | 1.5× | 3.2× | 6.6× | 1.0× | 1.4× |

Median query seconds (lower is better; the fastest per query in bold).

```text
geomean query time, 10m rows (lower is better)
duckdb       ██████                                    0.17 s
polars       ████████                                  0.24 s
sqe-parquet  █████████                                 0.27 s
sqe-iceberg  ███████████████████                       0.56 s
sqe-polaris  ████████████████████████████████████████  1.15 s
```

How to read it:

- `sqe-parquet` is SQE's embedded engine on the same Parquet files: 1.5×
  DuckDB on the geomean, close to Polars, and faster than both on q4.
- `sqe-iceberg` reads the same rows through an Iceberg table and takes about
  twice as long as `sqe-parquet`. Embedded mode scans an Iceberg table in one
  partition and loads the table again for each scan. The coordinator's scan
  is split across cores; giving embedded mode that scan is a separate change.
- `sqe-polaris` adds a server: each query loads the table from Polaris, runs
  on the coordinator and streams the answer over Flight SQL to an ADBC
  client. The difference from `sqe-iceberg` is that client/server cost, not
  the engine.

## How it is tested

`./run.sh --check` runs the whole pipeline once and asserts on what it
captured, without running anything twice:

- `bench.py validate` exits 0: every engine's answer to every query has the
  same row count and rounded checksums as DuckDB's (the checks are upstream's
  `validate.py` expressions).
- Every run finished (`status: ok`), so no engine was killed by the watchdog,
  timed out or errored.
- The summary has a column for every engine in `ENGINES`.

The SQE-side behaviour it depends on is covered by unit tests in the repo:
`ctas_widens_millisecond_timestamps_to_micros` (`crates/sqe-cli`) and
`test_timestamps_for_format_version_widens_s_and_ms_to_us`
(`crates/sqe-write`), since the generated `ts` column is
`timestamp[ms]` and Iceberg only stores microseconds or nanoseconds.

## Gotchas

- **A busy laptop waits.** Like upstream, a run starts only when at least
  5 GiB is free and the CPU was at most 30% busy over 3 s. Endpoint security
  and sync agents (Defender, OneDrive) can keep a laptop above that, and the
  runner then prints `waiting:` lines. Raise `MAX_BUSY_PCT` rather than
  benchmarking on top of a build.
- **Partitions.** The embedded runs and the upstream runners use 8 threads.
  The coordinator does not accept `SET datafusion.execution.target_partitions`
  over Flight, so `sqe-polaris` runs with DataFusion's default: one partition
  per core.
- **What `sqe-polaris` measures.** It reads from S3 through Docker's port
  forward and returns its result over gRPC to a client that writes the
  Parquet. That is the cost of the client/server deployment, not of the
  engine; compare `sqe-iceberg` to `sqe-polaris` to see it.
- **Polaris forgets on restart.** Polaris runs with in-memory persistence. If
  its container restarts, the tables are gone while the files stay in RustFS;
  `bench.py load` sees the row count missing and reloads.
- **Bigger sizes.** 10M rows is 213 MB of Parquet, `SIZE=50` 1.1 GB and
  `SIZE=150` 3.2 GB (upstream's figures). Upstream reports Polars 2.0's q3
  window dedup at 150M not fitting under the 4.5 GiB cap; a `watchdog:`
  status in the summary is a result, not a harness bug.
- **Generated outputs** under `.work/engine-bench/out/` are replaced on every
  run; only the last run's answers are validated.
