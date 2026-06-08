---
slug: benchmark
title: "Benchmarks: TPC-H / TPC-DS / SSB"
description: "Generate a TPC dataset, load it into SQE as Iceberg tables, and run the suite's queries with per-query timings. Uses sqe-bench against a Nessie catalog over RustFS, all in Docker."
---

# Benchmarks: TPC-H / TPC-DS / SSB

Generate a benchmark dataset, load it into SQE as Iceberg tables, and run the
suite's queries with per-query timings. Everything runs in Docker: a Nessie
catalog over RustFS holds the tables, and `sqe-bench` drives generate, load, and
test. The default is TPC-H at scale factor 0.01, which completes in seconds.

## How it works

The run has three phases:

1. **Generate** — `sqe-bench generate` writes the suite's tables as Parquet
   files to a shared Docker volume.
2. **Load** — `sqe-bench load` issues one `CREATE TABLE … AS SELECT * FROM read_parquet(…)` per table. The coordinator reads the Parquet from the shared
   volume; tables land in the Nessie catalog.
3. **Test** — `sqe-bench test` runs every `.sql` in the suite over Flight SQL
   and reports pass / fail / error plus a per-query timing table.

`sqe-bench` is a separate image built from `Dockerfile.bench`. Both images build
from this repo on first run if absent.

## What it demonstrates

- All three `sqe-bench` phases — generate, load, and test — running end to end.
- TPC-H (default, SF0.01): all 22 queries pass with per-query timings.
- Configurable suite (`BENCH=ssb`, `BENCH=tpcds`) and scale factor
  (`SCALE=0.1`, `SCALE=1`).
- `sqe-bench test` output: a pass/fail/diff/skip/error summary and a
  `BENCH_SUMMARY:` line for machine parsing.

**Status:** validated (2026-06-07).

## Run it

Full config, `docker compose`, suite SQL, and captured output are in the repo:

**→ [quickstart/benchmark/](https://github.com/schubergphilis/sqe/tree/main/quickstart/benchmark/)**

```bash
cd quickstart/benchmark
cp .env.example .env
./run.sh
```
