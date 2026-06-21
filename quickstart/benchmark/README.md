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
./run.sh --check                # generate+load+test, then assert no failed queries
```

`run.sh` brings the stack up, then runs the three `sqe-bench` phases and captures
the timing table to [`OUTPUT.md`](./OUTPUT.md). `BENCH` and `SCALE` are honored
by `--check` too, so `BENCH=ssb SCALE=0.1 ./run.sh --check` asserts that suite.

## How it works

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

## Configuration explained

### `sqe.toml` (the engine)

A minimal Nessie-over-RustFS SQE (the same catalog config as the
[`nessie`](../nessie/) quickstart), with one line that matters for benchmarks:

```toml
[storage.tvf]
allow_local_paths = true
```

- `[storage.tvf] allow_local_paths = true` lets the coordinator's `read_parquet`
  TVF read the generated Parquet off the shared `bench-data` volume (a local
  path inside the container). That gate is a security control: it blocks
  `read_parquet('/etc/shadow')` and metadata-endpoint pivots. It is on here only
  because the data is staged on a local volume; leave it off in production and
  stage data in object storage.
- `[catalogs.nessie]` points at Nessie's `/iceberg` mount; the loaded tables
  resolve as `nessie.<suite>_sf<scale>.<table>`.
- Auth is the `anonymous` dev provider, so `sqe-bench` connects with
  `--username anonymous --password anonymous` and SQE mints a token.

### `.env.example`

`BENCH` (`tpch` | `ssb` | `tpcds`) and `SCALE` (the scale factor) are read by
`run.sh`, overridable inline. Also sets the RustFS credentials, the offset host
ports, and `SQE_IMAGE`.

### `docker-compose.yml`

Brings up RustFS + Nessie + the coordinator, and a one-shot `sqe-bench` service
(built from `Dockerfile.bench`, behind a compose profile) that runs
generate/load/test. The `bench-data` volume is mounted into both the coordinator
and `sqe-bench` at the same path so the coordinator can read what the generator
wrote.

## Output

From a clean run of the default (`./run.sh`), captured in [`OUTPUT.md`](./OUTPUT.md):
all 22 TPC-H queries pass, with per-query timings and the summary line:

```
Results: 22 pass, 0 fail, 0 diff, 0 skip, 0 error  (total 0.4s)
BENCH_SUMMARY:tpch:22:0:0:0:0:22:378
```

## How it is tested

`./run.sh --check` runs the full generate -> load -> test pipeline and asserts
the invariants in `run.sh`, against the already-captured test output (no re-run):

- the suite prints a results summary line (`... pass, ...`),
- there are no failed, diffing, skipped, or errored queries
  (`0 fail, 0 diff, 0 skip, 0 error`),
- the machine-parseable `BENCH_SUMMARY:` line is emitted.

The check does not hardcode a query count, so it works for any `BENCH` / `SCALE`.
Validated 2026-06-07 (TPC-H SF0.01, 22/22 pass).

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
