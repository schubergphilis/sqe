# engine-bench: SQE embedded and on Polaris vs DuckDB and Polars

## Goal

Run the public [engine-bench](https://github.com/firfircelik/engine-bench)
workload, unchanged, against SQE in three deployments and against the DuckDB
and Polars runners it ships with, measured the same way and with every answer
checked against DuckDB's. The question it answers: on one laptop, under one
memory cap, how does SQE compare as an embedded engine on files, as an
embedded engine on Iceberg, and as a server on a real Iceberg REST catalog?

This is a local comparison harness, not a correctness gate against committed
baselines. The numbers belong to the machine they ran on.

## Components

| Piece | Role |
|---|---|
| engine-bench (pinned commit) | Data generator, the four queries, the DuckDB / Polars / Spark runners, the validation checksums. Cloned into `.work/`. |
| `bench.py` | Fresh process per run, 50 ms process-tree RSS sampling, 4.5 GiB kill, quiet-machine gate; `load`, `run`, `validate`, `summary`. |
| `sqe-cli --embedded` | `sqe-parquet` (views over `read_parquet()`) and `sqe-iceberg` (local SQLite-catalog warehouse). |
| `sqe-server` + ADBC Flight SQL | `sqe-polaris`: a native coordinator per run, the client fetches Arrow and writes Parquet. |
| RustFS + Polaris 1.7.0 (Docker) | S3 store and Iceberg REST catalog for `sqe-polaris`. |

## Flow

1. `run.sh` clones engine-bench at the pinned commit, builds a pinned Python
   venv and generates the seeded data (`SIZE` million rows, default 10).
2. `bench.py load` CTASes the Parquet into `iceberg.bench.*` (embedded) and
   `polaris.bench_<size>.*` (through a coordinator), skipping a load whose row
   count already matches.
3. `bench.py run` loops reps x queries x engines, rotating the engine order,
   and appends one JSON line per run.
4. `bench.py validate` compares every answer's row count and rounded
   checksums to DuckDB's; `bench.py summary` writes the median tables.

## Configuration

- `sqe.toml.tmpl`: 3 GB memory limit with spill, `client_credentials` auth as
  Polaris's root principal, one catalog `polaris` on warehouse `bench`, static
  RustFS key, local paths readable for the load.
- `bootstrap.sh`: catalog `bench` on `s3://warehouse/` with host and internal
  endpoints, `CATALOG_MANAGE_CONTENT` for `service_admin`.
- `.env.example`: ports 38181 / 39000 / 61051 / 39090, demo secrets, workload
  knobs.

## Validation

`./run.sh --check`: validation exits 0 (every answer matches DuckDB's), every
run finished, and the summary has every engine. Unit tests cover the CTAS
timestamp widening the load needs (`timestamp[ms]` source column).
