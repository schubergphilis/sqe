# Benchmarks: TPC-H / TPC-DS / SSB

## Goal

Generate a benchmark dataset, load it into SQE as Iceberg tables, and run the suite's
queries with per-query timings. Everything runs in Docker: a Nessie catalog over RustFS
holds the tables, and `sqe-bench` drives the three phases — generate, load, and test.
The default is TPC-H at scale factor 0.01, which finishes in seconds.

This is a local smoke-check and timing harness, not a correctness gate against committed
baselines. Use it to confirm SQE runs all queries cleanly and to get a rough timing
profile on your machine.

## Components

| Service | Role |
|---|---|
| `rustfs` + `bucket-init` | S3-compatible warehouse storage. |
| `nessie` | Iceberg REST catalog (auth-less). |
| `sqe` | Coordinator: Flight SQL on 50051. Reads generated Parquet via `read_parquet`. |
| `sqe-bench` | One-shot tool: `generate` → `load` → `test`. Built from `Dockerfile.bench`. |

## Configuration

### Backend (sqe.toml)

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "hybrid"

[worker]
memory_limit = "4GB"

[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]

[catalogs.nessie]
polaris_url = "http://nessie:19120/iceberg"
warehouse = "warehouse"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_region = "eu-example-1"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true

# Allows the coordinator to read Parquet from the shared bench-data volume.
# Leave this off in production; stage data in object storage instead.
[storage.tvf]
allow_local_paths = true

[metrics]
prometheus_port = 9090
```

Queries come from the TPC suite generators: `sqe-bench generate` writes
`benchmarks/queries/<suite>/*.sql` (one file per query) to the shared `bench-data`
volume, which `sqe-bench test` then executes over Flight SQL.

## The test

`run.sh` brings the infrastructure stack up (`rustfs`, `nessie`, `sqe`), then runs the
three `sqe-bench` phases via `docker compose run --rm`:

1. **generate** — writes Parquet tables to the `bench-data` Docker volume shared with
   the coordinator.
2. **load** — issues one CTAS per table (`CREATE TABLE … AS SELECT * FROM read_parquet(…)`);
   the coordinator reads the volume directly (`allow_local_paths = true`). Tables land
   in `nessie.<suite>_sf<scale>`.
3. **test** — runs every query in the suite over Flight SQL, reports `pass / fail / diff
   / skip / error` per query plus per-query timings, and emits a machine-parseable
   `BENCH_SUMMARY:` line. Output is captured to `OUTPUT.md`.

Suite and scale factor are configurable: `BENCH=ssb SCALE=0.1 ./run.sh` or
`BENCH=tpcds SCALE=1 ./run.sh`. Tear down with `./run.sh --down`.

## Output

```
TPCH SF0.01 — flight protocol
────────────────────────────────────────────────────────────
v q01          0.04s          6 rows
v q02          0.05s          4 rows
v q03          0.03s         10 rows
...
v q22          0.01s          0 rows

Results: 22 pass, 0 fail, 0 diff, 0 skip, 0 error  (total 0.4s)
BENCH_SUMMARY:tpch:22:0:0:0:0:22:378
```
