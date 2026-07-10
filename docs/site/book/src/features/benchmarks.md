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

## Results (SF1, vs Trino 465)

The numbers below are the latest SF1 run, as of 2026-06-12, against Trino 465 on identical Iceberg tables and S3 storage. All 222 queries pass (222/222). SQE wins six of seven suites at SF1.

| Suite | SQE | Trino | Speedup | Pass |
|---|---|---|---|---|
| TPC-E (11) | 9.3s | 172.0s | 18.5x | 11/11 |
| TPC-BB (10) | 28.0s | 255.7s | 9.1x | 10/10 |
| TPC-C (8 read) | 0.41s | 2.65s | 6.5x | 8/8 |
| TPC-DS (99) | 13.4s | 45.6s | 3.4x | 93/99 |
| ClickBench (43) | 1.3s | 4.46s | 3.4x | 43/43 |
| TPC-H (22) | 16.8s | 26.7s | 1.6x | 22/22 |
| SSB (13) | 8.3s | 5.8s | 0.70x | 13/13 |

Run-to-run variance is real, so treat each figure as approximate. The rank order is stable across the last month of runs.

### Where SQE trails

SSB is the one suite SQE loses at SF1: 8.3s against Trino's 5.8s, a 0.70x result. SSB is a denormalized star schema built for fast star-join filtering. Trino ships build-side key sets (bloom filters) into its scans, which prunes the `lineorder` fact table before it is read. SQE's equivalent, shipping build-side key sets to distributed workers, is in progress. The `lineorder` fact has a uniform foreign-key distribution that defeats row-group min/max pruning, so the runtime filter only helps at row level today.

TPC-DS has the most misses at SF1 (93/99). The six gaps are GROUPING SETS edge cases around grand-total row presence; they are the same six since March, not new regressions. TPC-E passes 11/11 but is the suite that historically needed the most work: it joins across 33 tables and uses IN-subquery patterns that DataFusion cannot always decorrelate, so deep-join queries dominate its run time.

The SF1 wins are decisive. At SF10 the picture narrows. On the level rig (Trino 481, totals across runs, single-node / distributed-2-worker / Trino range):

| Suite | SQE single-node | SQE distributed 2w | Trino 481 |
|---|---|---|---|
| TPC-H | 130.5s | 95.5s | 106.4s - 138.6s |
| SSB | 42.0s | 53.6s | 28.0s - 41.1s |
| TPC-DS | 543.9s | 338.3s | 328.4s - 468.0s |

At SF10, TPC-H distributed (95.5s) lands inside Trino's range, roughly par to ahead. TPC-DS distributed (338.3s) sits inside Trino's range, close. SSB still trails at SF10, the same pattern as SF1. These are SF10 figures on a single rig, not the canonical SF1 results above.

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
data, so you can compare SQE and Trino directly. The [Results section](#results-sf1-vs-trino-465)
above is the output of exactly this run at SF1. There are two modes:

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
    pub row_count_fn: fn(f64) -> usize,  // scale factor → row count
}
```

Steps to add a benchmark:

1. Create `crates/sqe-bench/src/generate/<name>.rs` implementing `BenchmarkGenerator`.
2. Add SQL query files under `benchmarks/queries/<name>/`.
3. Add expected result CSVs under `benchmarks/expected/<name>/sf1/`.
4. Register the generator in `crates/sqe-bench/src/generate/mod.rs`.
5. Add the benchmark name to the CLI subcommand list in `crates/sqe-bench/src/cli.rs`.
6. Add a schema DDL file under `benchmarks/schemas/<name>.sql` for reference.
