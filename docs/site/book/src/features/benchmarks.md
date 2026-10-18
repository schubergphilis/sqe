# Benchmark Suite

SQE ships with `sqe-bench`, a Rust CLI tool that generates benchmark data, loads it into SQE via the `read_parquet()` TVF, and runs query suites to validate SQL correctness and measure query performance.

For the longitudinal view (every benchmark JSON in `benchmarks/results/` plotted across time, per-suite, per-scale, per-query heatmaps), see [getsqe.com/performance](https://getsqe.com/performance). Charts auto-regenerate from the committed JSONs via `make benchmark-charts`.

## Available Benchmarks

| Benchmark | Queries | Tables | Focus |
|-----------|---------|--------|-------|
| `tpch` | 22 | 8 | Star/snowflake schema, pure analytical reads |
| `tpcds` | 99 | 24 | Complex SQL, correlated subqueries, window functions |
| `ssb` | 13 | 5 | Denormalized star schema, fast smoke testing |
| `tpcc` | 17 | 9 | OLTP read + write queries (DELETE/UPDATE via CoW) |
| `tpce` | 11 | 33 | Brokerage OLTP, complex demographics and trade schema |
| `tpcbb` | 10 | ~25 | SQL-only subset over TPC-DS data + web logs |

**Why these benchmarks?** Each covers a different slice of SQL correctness:

- **TPC-H and SSB** validate the analytical core: joins, aggregates, GROUP BY, ORDER BY, date arithmetic. TPC-H is the standard first check for any SQL engine.
- **TPC-DS** is the hardest. Its 99 queries exercise correlated subqueries, CTEs, window functions, GROUPING SETS, and complex multi-table joins. Passing TPC-DS well means the engine handles real analytical workloads.
- **TPC-C and TPC-E** cover OLTP patterns: point lookups, small aggregates, indexed access by key ranges, plus write operations (DELETE, UPDATE) exercised via Copy-on-Write.
- **TPC-BB** exercises semi-structured data alongside the TPC-DS schema, useful for validating string functions and JSON handling.

## Results (vs Trino 481)

The numbers below are the latest run, as of 2026-07-09, against Trino 481 on identical Iceberg tables and S3 storage, both engines capped at 12 GB on a dedicated 8-core host. SQE is faster on seven of eight suites at SF1 and on six of seven at SF10, where SSB is at parity.

| Suite | SF1 SQE | SF1 Trino | SF1 speedup | SF10 SQE | SF10 Trino | SF10 speedup |
|---|---|---|---|---|---|---|
| TPC-H (22) | 14.2s | 40.8s | 2.9x | 66.5s | 111.9s | 1.7x |
| TPC-DS (99) | 49.3s | 114.9s | 2.3x | 216.6s | 320.5s | 1.5x |
| SSB (13) | 5.2s | 7.3s | 1.4x | 20.4s | 21.4s | 1.0x |
| ClickBench (43) | 3.1s | 7.3s | 2.4x | 10.0s | 31.6s | 3.2x |
| TPC-E (11) | 3.8s | 11.6s | 3.0x | test-only | test-only | - |
| TPC-BB (10) | 53.7s | 480.5s | 8.9x | 405.5s | 1429.7s | 3.5x |
| Bank (8) | 10.5s | 19.8s | 1.9x | 370.5s | 85.5s | 0.23x |
| TPC-C (8 read) | 5.6s | 4.3s | 0.77x | 5.0s | 18.0s | 3.6x |

Run-to-run variance is real, so treat each figure as approximate. The full per-query timeline is on [getsqe.com/performance](https://getsqe.com/performance).

TPC-BB is lopsided. On the 2026-08-15 Flight SF1 run the suite is 63.1s and q01 alone is 35.5s (20 rows). The other nine queries share the remaining half. Quote q01 when you quote a TPC-BB total (issue #417). The README compare envelope that day is 56.3s vs Trino 290.3s, still 10/10.

### Where SQE trails

- **TPC-C at SF1 (0.77x)**: the read queries are sub-second, so fixed dispatch overhead dominates. At SF10 SQE is ahead (3.6x).
- **Bank at SF10 (0.23x, 7/8)**: spill stalls under an 8 GB memory pool. A fix is in progress.
- **ClickBench (40/43)**: three queries are pending a LIMIT-drop regression (q17).
- **TPC-E at SF10**: the differential check needs about 14 GB for a 10M-row result, so SF10 is validated test-only.

### How it is validated

Timing data is only as good as the result data behind it. Two layers of validation run before any number is trusted.

The first layer is differential validation against Trino. `sqe-bench compare <suite>` runs every query against SQE (Flight SQL) and Trino (HTTP) on the same Iceberg tables and diffs the result rows. A row-count or value mismatch fails the query. A query that returns zero rows on both engines is reported as vacuous, not a match: agreement on nothing validates nothing.

The second layer is an independent data oracle. DuckDB's official `dsdgen` output loads side by side with the generated parquet, with per-table row counts and per-column null fractions checked, and all queries run against both datasets inside DuckDB. A query that returns rows on official data and none on ours is a generator-fidelity bug, found without either SQL engine in the loop.

The oracle earned its keep. It flagged 16 vacuous TPC-DS queries as generator gaps rather than engine bugs, and it settled the one genuine engine disagreement in SQE's favor: TPC-DS q75 differs by two rows because Trino's `DECIMAL(17,2)` division rounds two ratios up past a `< 0.9` filter and drops them. DuckDB matches SQE exactly. The benchmark that looked like an SQE failure was a Trino rounding bug.

For the longitudinal view across every committed run, see the [benchmark history](https://getsqe.com/performance) on getsqe.com.

## Generating Data

The `generate` command produces Parquet files on local disk or S3. Data is deterministic (seeded) so results are reproducible.

```bash
# Generate TPC-H at scale factor 1 (~1 GB, 8 tables)
cargo run -p sqe-bench -- generate tpch --scale 1 --output ./data

# Scale factor 10 (~10 GB)
cargo run -p sqe-bench -- generate tpch --scale 10 --output ./data

# Write directly to S3
cargo run -p sqe-bench -- generate tpch --scale 1 \
  --output s3://bench-data/ \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --s3-endpoint http://localhost:9000 \
  --s3-region us-east-1

# Generate all benchmarks at once
./scripts/benchmark-generate-all.sh
```

### Scale factors explained

The scale factor controls dataset size. Scale factor 1 produces roughly 1 GB for TPC-H and TPC-DS; SSB is ~600 MB at SF1.

| Scale factor | TPC-H size | TPC-DS size | Use case |
|---|---|---|---|
| 1 | ~1 GB | ~1 GB | Development, CI, correctness checks |
| 10 | ~10 GB | ~10 GB | Performance testing |
| 100 | ~100 GB | ~100 GB | Near-production load |
| 1000 | ~1 TB | ~1 TB | Full-scale benchmarking |

Files are split at 128 MB for parallelism. Output is structured as:

```
./data/
└── tpch/
    └── sf1/
        ├── lineitem/
        │   ├── part-0000.parquet
        │   └── part-0001.parquet
        ├── orders/
        │   └── part-0000.parquet
        └── ... (8 tables total)
```

## Direct-to-Iceberg Sink

`generate --sink iceberg` skips the staging Parquet step entirely. Instead of
writing files to `--output` for a later `load` run, it connects straight to
the Iceberg REST catalog, creates the tables, and commits data files with one
`fast_append` per table.

```bash
cargo run -p sqe-bench -- generate tpce --scale 10 --sink iceberg \
  --catalog-uri http://localhost:8181/api/catalog \
  --warehouse bench --namespace tpce_sf10 \
  --client-id ... --client-secret ...
```

The command creates the namespace if it does not exist, then generates and
commits each table in turn. Data is written in parallel shards for TPC-H and
bank; the other generators write one shard per table.

The sink writes a table only the way `load` would. `load` partitions TPC-H
`lineitem`/`orders` by month and the TPC-C tables by warehouse and district,
and the sink creates those tables with the same partition spec. It splits rows
by partition into one buffer per table, shared by every shard, and writes the
largest partition out as a file whenever the buffer passes `--partition-buffer`
(default `2g`). A larger buffer means fewer, bigger files: TPC-H SF10
`lineitem` lands as 269 files with the default, against 415 from `load`. Peak
memory is the buffer plus the files being written, about 3.5 GB for that run.

`load` also sorts the SSB `lineorder` and TPC-DS fact tables by their date key
on write. The sink never sorts. Instead those generators emit the rows already
in date order: each order or return row is a pure function of its number, and
a counting sort over the dates (4 bytes per order or return row, built before
the first batch) fixes the emission order. TPC-DS inventory is generated week
by week. A table `load` sorts whose generator does not declare that order is
refused up front by name, since a table written differently prunes differently
and its timings would not compare with a loaded baseline; `--ignore-layout`
writes it anyway, with a warning. Every direct write carries the bloom filters
the engine adds on its own (`_sk` integer columns). Bank has its own
partitioned writer and is not affected.

Generators stream batches into the writer, so peak memory follows `--threads`
rather than table size (TPC-C SF10 generates in 167 MB, ClickBench SF10 in
199 MB).

Two flags control repeated runs against the same namespace:

- `--resume` skips a table that already carries a
  `sqe-bench.table.<name>=done` property, so an interrupted or repeated
  `generate` run does not redo work or duplicate rows. The marker is set on
  the table itself, not derived from snapshot history, because some
  catalogs serve a trimmed snapshot list.
- `--clean` drops and recreates every table first, so the run is idempotent
  regardless of what state the namespace was in. `--clean` and `--resume`
  are mutually exclusive.

Without `--resume` or `--clean`, re-running `generate` against a table that
already holds data commits another `fast_append` on top of it, duplicating
every row. The done marker is only consulted when you pass `--resume`.

Catalog and storage settings match the `load` command's `--s3-*` flags, plus
the catalog's OAuth2 client-credentials pair (`--client-id`/`--client-secret`)
or a pre-acquired `--bearer-token`. See `sqe-bench generate --help` for the
full flag list.

`scripts/benchmark-test.sh` uses the sink for every suite, then restarts the
coordinator so it sees the tables committed behind its back. TPC-BB writes its
own two tables into the TPC-DS namespace, after a direct TPC-DS load when the
run has not loaded it already. A suite falls back to generate + `load` under
`BENCH_DATA_SOURCE` other than `generate`, `BENCH_TABLE_FORMAT=hive`,
`BENCH_BLOOM_FILTER` or `BENCH_PUFFIN_STATS`, which are `load` features.
`BENCH_LOAD_MODE=load` forces generate + `load` for every suite, for an A/B
against the direct path.

## Loading Data

The `load` command connects to SQE and creates Iceberg tables using `read_parquet()` + CTAS. No intermediate format conversion is needed. Parquet files are read directly and written as Iceberg.

```bash
# Load TPC-H from local disk
cargo run -p sqe-bench -- load tpch \
  --scale 1 \
  --data ./data \
  --host localhost \
  --port 60051 \
  --username root \
  --password ""

# Load from S3
cargo run -p sqe-bench -- load tpch \
  --scale 1 \
  --data s3://bench-data/ \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --s3-endpoint http://localhost:9000 \
  --s3-region us-east-1 \
  --host localhost \
  --port 60051 \
  --username root \
  --password ""

# Drop and recreate tables before loading
cargo run -p sqe-bench -- load tpch --scale 1 --data ./data --clean \
  --host localhost --port 60051 --username root --password ""

# Use the Trino HTTP protocol instead of Flight SQL
cargo run -p sqe-bench -- load tpch --scale 1 --data ./data \
  --protocol trino \
  --host localhost \
  --port 8080 \
  --username root \
  --password ""
```

The loader creates a namespace named `<benchmark>_sf<N>` (e.g., `tpch_sf1`) and sends one CTAS statement per table:

```sql
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/*.parquet');
```

For S3 sources, inline credentials are injected:

```sql
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet(
  's3://bench-data/tpch/sf1/lineitem/*.parquet',
  access_key => 'AKIA...',
  secret_key => '...',
  endpoint => 'http://localhost:9000',
  region => 'us-east-1'
);
```

See [read_parquet TVF](./read-parquet.md) for full syntax documentation.

## Fast benchmark runs via attached golden tables

For the read-only suites (`tpch`, `ssb`, `tpcds`, `clickbench`), the load step dominates wall-clock time and adds nothing to the query measurement: every run rewrites the same parquet into fresh Iceberg tables before a single query runs. Publish those tables once into a persistent Polaris, then attach them read-only on every subsequent run instead of reloading.

Publish once:

```bash
BENCH_GOLDEN_POLARIS_URL=https://polaris.example.com/api/catalog \
BENCH_GOLDEN_WAREHOUSE=golden_warehouse \
BENCH_GOLDEN_TOKEN=<bearer> \
BENCH_S3_ENDPOINT=https://s3.example.com \
BENCH_GOLDEN_S3_ACCESS_KEY=... BENCH_GOLDEN_S3_SECRET_KEY=... \
BENCH_SCALE=1 ./scripts/benchmark-publish-iceberg.sh
```

Then run any read-only suite with `BENCH_DATA_SOURCE=attach`:

```bash
BENCH_GOLDEN_POLARIS_URL=https://polaris.example.com/api/catalog \
BENCH_GOLDEN_WAREHOUSE=golden_warehouse \
BENCH_GOLDEN_TOKEN=<bearer> \
BENCH_S3_ENDPOINT=https://s3.example.com \
BENCH_GOLDEN_S3_ACCESS_KEY=... BENCH_GOLDEN_S3_SECRET_KEY=... \
BENCH_DATA_SOURCE=attach BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch
```

`benchmark-test.sh` attaches the golden catalog once (`scripts/benchmark-attach-golden.sh`) and queries each table as `golden.<namespace>.<table>` instead of generating and loading data. `tpcc` and `tpce` are write suites (their queries include DELETE/UPDATE via CoW), so they still generate and load normally even in attach mode; a shallow-clone path that would let them run against golden tables too, without mutating the shared copy, is planned but not yet built. `bank` and `tpcbb` also still generate and load normally in attach mode: `bank` is published to golden by `benchmark-publish-iceberg.sh`, but attach mode does not yet query it from there, and `tpcbb`'s own tables (`web_clickstreams`, `product_reviews`) are not published at all, since `tpcbb` shares `tpcds`'s namespace and only needs the two tables it adds. Wiring bank/tpcbb into attach mode is the deferred follow-up alongside the tpcc/tpce shallow-clone path. Attach is loud by design: a missing or unreachable golden catalog fails the run with the ATTACH error rather than silently falling back to a full load, which would mask the exact cost this path removes.

## Unified harness (benchmark.sh + sqe-bench run)

For the read-only suites (tpch, ssb, tpcds, clickbench), the unified harness combines data attachment with query execution in one command:

```bash
BENCH_PROFILE=local BENCH_SCALE=1 scripts/benchmark.sh tpch ssb tpcds clickbench
```

The `scripts/benchmark.sh` orchestrates two steps. First, it attaches the golden Iceberg catalog (published once via `benchmark-publish-iceberg.sh`). Second, it runs the suites through `sqe-bench run`, which executes all queries against the attached tables and emits JSON reports to `benchmarks/results/`.

Configuration profiles live in `benchmarks/profiles/<name>.toml`. Profiles define the coordinator config, catalog credentials, and suite-specific settings. Credentials come from environment variables or AWS profiles, never stored in the TOML file itself. The profile loader redacts secrets on error and checks for committed keys at startup.

Each read-only suite attaches golden (zero load); write suites (tpcc, tpce) are a follow-up plan. The old `benchmark-*.sh` scripts remain: the migration to unified is a later commit after parity is confirmed.

## Running Tests

The `test` command executes all queries in the benchmark suite against the loaded data and reports correctness and timing.

```bash
# Run all TPC-H queries (Flight SQL, default)
cargo run -p sqe-bench -- test tpch \
  --scale 1 \
  --host localhost \
  --port 60051 \
  --username root \
  --password ""

# Run a single query
cargo run -p sqe-bench -- test tpch --scale 1 --query q03 \
  --host localhost --port 60051 --username root --password ""

# Use Trino HTTP protocol
cargo run -p sqe-bench -- test tpch --scale 1 \
  --protocol trino \
  --host localhost \
  --port 8080 \
  --username root \
  --password ""

# Run all benchmarks end-to-end
./scripts/benchmark-test.sh tpch
./scripts/benchmark-test.sh tpcds
./scripts/benchmark-test.sh ssb
```

### Query result statuses

| Status | Meaning |
|--------|---------|
| `PASS` | Result matches expected output exactly (within numeric tolerance) |
| `DIFF` | Result matches in shape but has minor differences (e.g., decimal precision) |
| `FAIL` | Result is wrong: wrong rows, wrong values, wrong schema |
| `SKIP` | Query requires an unimplemented feature; counted but not failed |
| `ERROR` | Query failed to execute (engine error, timeout, crash) |

`DIFF` is not treated as a failure in CI. It is a signal for investigation. Decimal precision differences are expected when comparing float-heavy aggregates across different engines.

Queries can declare their requirements in a header comment:

```sql
-- name: Revenue by nation
-- requires: delete, merge
-- timeout: 30s
SELECT ...
```

Any query with `-- requires:` will be `SKIP`ped if SQE does not support that feature, rather than `FAIL`ing the suite.

## Understanding Results

### Terminal output

```
TPC-H SF1 - Flight SQL (localhost:50051)
─────────────────────────────────────────
q01  PASS   1.23s   6001215 rows
q02  PASS   0.45s       460 rows
q03  PASS   0.89s     11620 rows
...
q17  DIFF   2.10s         1 rows  (decimal precision)
q22  PASS   0.33s         7 rows

Results: 20/22 PASS, 1 DIFF, 1 SKIP
Total time: 28.4s
Report: benchmarks/results/tpch-sf1-flight-2026-03-24T14:30:00.json
```

### JSON report format

Reports are written to `benchmarks/results/<benchmark>-sf<N>-<protocol>-<timestamp>.json`:

```json
{
  "benchmark": "tpch",
  "scale_factor": 1,
  "protocol": "flight",
  "host": "localhost:50051",
  "timestamp": "2026-03-24T14:30:00Z",
  "summary": {
    "total": 22,
    "pass": 20,
    "fail": 0,
    "diff": 1,
    "skip": 1,
    "error": 0,
    "total_duration_ms": 28400
  },
  "queries": [
    {
      "id": "q01",
      "status": "pass",
      "duration_ms": 1230,
      "rows": 6001215
    },
    {
      "id": "q17",
      "status": "diff",
      "duration_ms": 2100,
      "rows": 1,
      "diff_detail": "decimal precision mismatch: expected 1.0000, got 0.9999"
    }
  ]
}
```

JSON reports are machine-readable and suitable for tracking regressions over time in CI.

## Historical Performance Tracking

Benchmark JSON results are committed to `benchmarks/results/` for historical comparison. This enables tracking performance regressions and improvements across releases. The committed JSONs feed the per-suite, per-scale, per-query timeline on [getsqe.com/performance](https://getsqe.com/performance); refer there for the longitudinal view of how each suite moved across the optimization work.

## Comparing against Trino

The benchmark harness can run the same suite against a real Trino on the same
data, so you can compare SQE and Trino directly. The [Results section](#results-vs-trino-481)
above is the output of exactly this run. There are two modes:

- **Correctness parity**: `--compare-trino` diffs SQE's results against
  Trino's row-for-row. This is how SQL correctness is validated at scale, not
  just timing. A row-count or value mismatch fails the query. A query that
  returns zero rows on both engines is reported as vacuous, not a match, because
  agreement on nothing validates nothing. Small decimal differences on
  float-heavy aggregates are flagged for investigation rather than treated as
  failures.
- **Timing**: the same run records per-query wall-clock for both engines, so a
  head-to-head speed comparison falls out of the parity run.

Run a comparison yourself and see the captured numbers in the benchmark
quickstart: [Benchmarks: TPC-H / TPC-DS / SSB](../quickstart/benchmark.md), or
in the repo under
[`benchmarks/`](https://github.com/schubergphilis/sqe/tree/main/benchmarks/).

## CI/CD Integration

All three scripts support automated use without a TTY:

```bash
# Generate data once (idempotent - skip if files exist)
./scripts/benchmark-generate-all.sh

# Load all benchmarks
./scripts/benchmark-load.sh

# Run and report
./scripts/benchmark-test.sh tpch
./scripts/benchmark-test.sh tpcds
./scripts/benchmark-test.sh ssb

# Exit code is 0 if all queries are PASS or SKIP
# Exit code is 1 if any query is FAIL or ERROR
```

A typical CI pipeline runs TPC-H at SF1 as a smoke test on every PR, and the full suite (TPC-H + TPC-DS + SSB) nightly.

## Query Files

Query SQL files are stored under `benchmarks/queries/<benchmark>/`:

```
benchmarks/
├── queries/
│   ├── tpch/     q01.sql ... q22.sql
│   ├── tpcds/    q01.sql ... q99.sql
│   ├── ssb/      q1.1.sql ... q4.3.sql
│   ├── tpcc/     order_status.sql, stock_level.sql, ...
│   ├── tpce/     trade_lookup.sql, customer_position.sql, ...
│   └── tpcbb/    q01.sql ... q10.sql
├── expected/
│   ├── tpch/sf1/    q01.csv ... q22.csv
│   └── ...
└── schemas/
    ├── tpch.sql
    ├── tpcds.sql
    └── ...
```

Expected results under `benchmarks/expected/` are CSV files containing the correct output at the given scale factor. They are committed to the repository and used for regression checking.

## Adding New Benchmarks

Implement the `BenchmarkGenerator` trait in `crates/sqe-bench/src/generate/`:

```rust
pub trait BenchmarkGenerator {
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output: &dyn ObjectStore,
        prefix: &str,
    ) -> Result<GenerateStats>;
}

pub struct TableDef {
    pub name: String,
    pub schema: Arc<Schema>,             // Arrow schema
    pub row_count_fn: fn(f64) -> usize,  // scale factor to row count
}
```

Steps to add a benchmark:

1. Create `crates/sqe-bench/src/generate/<name>.rs` implementing `BenchmarkGenerator`.
2. Add SQL query files under `benchmarks/queries/<name>/`.
3. Add expected result CSVs under `benchmarks/expected/<name>/sf1/`.
4. Register the generator in `crates/sqe-bench/src/generate/mod.rs`.
5. Add the benchmark name to the CLI subcommand list in `crates/sqe-bench/src/cli.rs`.
6. Add a schema DDL file under `benchmarks/schemas/<name>.sql` for reference.
