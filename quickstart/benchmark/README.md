---
slug: benchmark
title: "Benchmarks: TPC-H / TPC-DS / SSB"
description: "Generate a TPC dataset, load it into SQE as Iceberg tables, and run the suite's queries with per-query timings. Uses sqe-bench against a Nessie catalog over RustFS, all in Docker."
---

# Benchmarks: TPC-H / TPC-DS / SSB

Generate a benchmark dataset, load it into SQE as Iceberg tables, and run the
suite's queries with per-query timings. Everything runs in Docker: a Nessie
catalog over RustFS holds the tables, and `sqe-bench` drives generate, load, and
test. The default is TPC-H at scale factor 0.01, which finishes in seconds.

## What you get

| Service | Role |
|---|---|
| `rustfs` + `bucket-init` | S3-compatible warehouse storage. |
| `nessie` | The Iceberg REST catalog (auth-less). |
| `sqe` | The coordinator (Flight SQL on 50051). Reads the generated Parquet to build the tables. |
| `sqe-bench` | One-shot tool: `generate` -> `load` -> `test`. Built from `Dockerfile.bench`. |

## Prerequisites

- Docker (with Compose v2).
- Two SQE images. They build from this repo on first run if absent:
  - `sqe-quickstart:latest` (coordinator, `Dockerfile`)
  - `sqe-bench:latest` (`Dockerfile.bench`)

## Run it

```bash
cd quickstart/benchmark
cp .env.example .env
./run.sh                        # tpch SF0.01
BENCH=ssb   SCALE=0.1 ./run.sh  # other suite / larger scale
BENCH=tpcds SCALE=1   ./run.sh
./run.sh --down                 # tear everything down
```

`run.sh` brings the stack up, then runs the three `sqe-bench` phases and captures
the timing table to [`OUTPUT.md`](./OUTPUT.md).

## How the three phases work

```
sqe-bench generate  ->  Parquet on the shared bench-data volume
sqe-bench load      ->  CREATE TABLE ... AS SELECT * FROM read_parquet(...)   (coordinator reads the volume)
sqe-bench test      ->  runs benchmarks/queries/<suite>/*.sql over Flight SQL, times each
```

- **generate** writes the suite's tables to a Docker volume (`bench-data`) shared
  with the coordinator.
- **load** issues one CTAS per table. The coordinator (not sqe-bench) reads the
  Parquet via the `read_parquet` table-valued function, so the volume is mounted
  into both containers at the same path. Tables land in `nessie.<suite>_sf<scale>`.
- **test** runs every `.sql` in the suite and reports `pass / fail / diff / skip /
  error` plus a `BENCH_SUMMARY:` line. With no expected-results file for the
  chosen scale, a query "passes" by executing cleanly (this is a smoke + timing
  run, not a correctness gate).

## Output

From a clean run of the default (`./run.sh`), captured in [`OUTPUT.md`](./OUTPUT.md):
all 22 TPC-H queries pass, with per-query timings and the summary line:

```
Results: 22 pass, 0 fail, 0 diff, 0 skip, 0 error  (total 0.4s)
BENCH_SUMMARY:tpch:22:0:0:0:0:22:378
```

## How it is tested

`run.sh` runs generate -> load -> test for TPC-H SF0.01 from a clean state and
captures the timing table. Validated 2026-06-07 (22/22 pass).

## Getting a JSON report on the host

`sqe-bench test` also writes a JSON report to `benchmarks/results/` *inside the
container*. This quickstart does not mount that directory (the repo's
`benchmarks/results/` holds the project's committed SF1 baselines, and a small
demo run should not land there). To keep a report, mount a local directory:

```yaml
# in docker-compose.yml, under the sqe-bench service volumes:
- ./reports:/benchmarks/results
```

## Gotchas

- **Scale factor and time.** SF0.01 is tiny (seconds). SF1 is ~1 GB per suite and
  takes minutes; generation and load dominate. Start small.
- **Local-path reads are enabled here.** `sqe.toml` sets
  `[storage.tvf] allow_local_paths = true` so the coordinator can read the
  generated Parquet off the shared volume. That gate is a security control
  (it blocks `read_parquet('/etc/shadow')` and metadata-endpoint pivots); leave
  it off in production and stage data in object storage instead.
- **Anonymous auth.** The Nessie stack is auth-less, so `sqe-bench` connects with
  `--username anonymous --password anonymous`; SQE's anonymous provider accepts
  it and mints a token. For real auth, see the `polaris-keycloak-*` quickstarts.
- **Not a published baseline.** These numbers are for a local sanity check on
  your machine, not comparable to the committed `benchmarks/results/` SF1 runs.
