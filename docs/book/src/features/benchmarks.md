# Benchmark Suite

SQE ships with `sqe-bench`, a Rust CLI tool that generates benchmark data, loads it into SQE via the `read_parquet()` TVF, and runs query suites to validate SQL correctness and measure query performance.

## Available Benchmarks

| Benchmark | Queries | Tables | Focus |
|-----------|---------|--------|-------|
| `tpch` | 22 | 8 | Star/snowflake schema, pure analytical reads |
| `tpcds` | 99 | 24 | Complex SQL, correlated subqueries, window functions |
| `ssb` | 13 | 5 | Denormalized star schema, fast smoke testing |
| `tpcc` | 8 | 9 | OLTP read queries; write queries require DELETE/MERGE |
| `tpce` | 11 | 33 | Brokerage OLTP, complex demographics and trade schema |
| `tpcbb` | 10 | ~25 | SQL-only subset over TPC-DS data + web logs |

**Why these benchmarks?** Each covers a different slice of SQL correctness:

- **TPC-H and SSB** validate the analytical core: joins, aggregates, GROUP BY, ORDER BY, date arithmetic. TPC-H is the standard first check for any SQL engine.
- **TPC-DS** is the hardest. Its 99 queries exercise correlated subqueries, CTEs, window functions, GROUPING SETS, and complex multi-table joins. Passing TPC-DS well means the engine handles real analytical workloads.
- **TPC-C and TPC-E** cover OLTP read patterns: point lookups, small aggregates, indexed access by key ranges. Their write queries are skipped until DELETE/MERGE land.
- **TPC-BB** exercises semi-structured data alongside the TPC-DS schema — useful for validating string functions and JSON handling.

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

The `load` command connects to SQE and creates Iceberg tables using `read_parquet()` + CTAS. No intermediate format conversion is needed — Parquet files are read directly and written as Iceberg.

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
| `FAIL` | Result is wrong — wrong rows, wrong values, wrong schema |
| `SKIP` | Query requires an unimplemented feature (e.g., DELETE/MERGE); counted but not failed |
| `ERROR` | Query failed to execute (engine error, timeout, crash) |

`DIFF` is not treated as a failure in CI — it is a signal for investigation. Decimal precision differences are expected when comparing float-heavy aggregates across different engines.

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
TPC-H SF1 — Flight SQL (localhost:50051)
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

## CI/CD Integration

All three scripts support automated use without a TTY:

```bash
# Generate data once (idempotent — skip if files exist)
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
