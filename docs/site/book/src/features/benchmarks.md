# Benchmark Suite

SQE ships with `sqe-bench`, a Rust CLI tool that generates benchmark data, loads it into SQE via the `read_parquet()` TVF, and runs query suites to validate SQL correctness and measure query performance.

For the longitudinal view (every benchmark JSON in `benchmarks/results/` plotted across time, per-suite, per-scale, per-query heatmaps), see [`docs/benchmark/`](../../../benchmark/index.md). Charts auto-regenerate from the committed JSONs via `make benchmark-charts`.

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

## Historical Performance Tracking

Benchmark JSON results are committed to `benchmarks/results/` for historical comparison. This enables tracking performance regressions and improvements across releases.

### TPC-H SF1: Historical Comparison (Apr 2 baseline vs. Apr 6 streaming execution)

After implementing the streaming execution engine (coordinator spill-to-disk, late materialization, file-level pruning, S3 I/O pipeline, distributed execution), TPC-H SF1 improved 3.1x on a distributed cluster (coordinator + 2 workers) compared to the single-node baseline:

```
Query   Apr 2 (single-node)   Apr 6 (distributed)   Speedup
────────────────────────────────────────────────────────────
q01               3.21s                1.29s     2.5x
q02               0.89s                0.27s     3.3x
q03               2.23s                0.94s     2.4x
q04               1.14s                0.32s     3.6x
q05               1.89s                0.55s     3.4x
q06               1.13s                0.30s     3.7x
q07               2.07s                0.85s     2.4x
q08               1.81s                0.54s     3.4x
q09               1.78s                0.60s     3.0x
q10               2.47s                0.63s     3.9x
q11               0.74s                0.11s     6.8x
q12               1.71s                0.57s     3.0x
q13               1.10s                0.18s     6.1x
q14               1.46s                0.55s     2.7x
q15               2.24s                0.72s     3.1x
q16               0.75s                0.10s     7.4x
q17               1.89s                0.63s     3.0x
q18               3.19s                0.74s     4.3x
q19               1.68s                0.79s     2.1x
q20               1.39s                0.53s     2.6x
q21               2.11s                0.68s     3.1x
q22               0.67s                0.09s     7.7x
────────────────────────────────────────────────────────────
TOTAL             37.5s                12.0s     3.1x
```

Key observations:

- **Metadata-light queries** (q11, q13, q16, q22) see 6-8x speedup: footer cache, file pruning, and scan distribution eliminate I/O overhead
- **Scan-heavy queries** (q01, q03, q07) see 2-2.5x speedup, proportional to worker count (2 workers)
- **q18** (the hardest TPC-H query) improved from 3.19s to 0.74s (4.3x), benefiting from distributed aggregation across workers
- **Single-node with 512MB spill**: 21/22 pass. Only q18 fails due to DataFusion hash aggregate memory limitation (DF#17334). With 1GB+ memory or with workers, all 22 pass.

### Full Benchmark Matrix (Apr 7, 2026, SF1)

| Suite (queries) | single-512mb | single-8gb | distributed-2w |
|---|---|---|---|
| TPC-H (22) | 21/22 (29.6s) | 22/22 (28.6s) | 22/22 (13.5s) |
| TPC-DS (99) | 92/99 (94.1s) | 99/99 (99.4s) | 98/99 (36.1s) |
| SSB (13) | 4/13 (14.4s) | 13/13 (14.3s) | 13/13 (5.3s) |
| TPC-C (17) | 17/17 (21.5s) | 17/17 (22.0s) | 17/17 (8.6s) |
| TPC-E (18) | 12/18 (8.4s) | 13/18 (127.4s) | 10/18 (56.0s) |
| **Total (169)** | **146 (86%)** | **164 (97%)** | **162 (96%)** |

### Spill behavior across configs

| Config | Sort Spills | Bytes Spilled | Analysis |
|---|---|---|---|
| single-512mb | 30 | 1.1 GB | TPC-DS complex sorts spill to disk. 92/99 pass, spill works. |
| single-8gb | 128 | 27.7 GB | Mostly TPC-E (33-table joins). More spills because more queries run to completion. |
| distributed-2w | 3 | 49 MB | Near-zero spill. Workers absorb scan/aggregation work. |

The counterintuitive finding: 8GB spills *more* than 512MB. This is because 8GB successfully runs TPC-E queries that 512MB cannot. Those TPC-E queries involve massive multi-table joins that produce 27GB of intermediate sorted data. With 512MB, the same queries OOM before reaching the spill point.

With distribution (2 workers), spill drops to 49MB. Workers handle scan and partial aggregation; the coordinator only merges small result sets.

### Scheduler observations

At SF1, all distributed queries ran locally on the coordinator (`scheduler_decisions{local}=120+`). This is correct: SF1 tables have 1-2 data files each, below the distribution threshold (default: 4 files). The 2.5x speedup comes from streaming execution improvements (spill, scan planning), not from worker distribution. To observe actual worker distribution, run at SF10+ where tables have 10+ files.

### TPC-E: the outlier

TPC-E has the lowest pass rate (56-72%) across all configs:
- 5 queries blocked by DataFusion's IN(subquery) limitation (cannot decorrelate)
- Deep join chains across 33 tables produce massive intermediate results
- Some queries timeout at 600s after spilling 27GB

### Metrics gaps

Several Phase A/B metrics show 0 because the increment calls are not yet wired into the execution path (the infrastructure exists but `metric.inc()` calls are missing):
- Footer cache hits/misses: `FooterCache` not wired into `IcebergScanExec`
- File pruning counts: `PruningPredicate` built but counter not incremented
- Late materialization bytes: RowFilter wired but byte tracking not connected
- Time to first row: histogram registered but not observed

These are wiring tasks for the next iteration.

## Comparing against Trino

The benchmark harness can run the same suite against a real Trino on the same
data, so you can compare SQE and Trino directly. There are two modes:

- **Correctness parity**: `--compare-trino` diffs SQE's results against
  Trino's row-for-row. This is how SQL correctness is validated at scale, not
  just timing. Small decimal differences on float-heavy aggregates are expected
  and flagged for investigation rather than treated as failures.
- **Timing**: the same run records per-query wall-clock for both engines, so a
  head-to-head speed comparison falls out of the parity run.

SQE's own distributed execution path (coordinator + workers, spill-to-disk,
late materialization, file-level pruning) gives a measured ~3.1× speedup over
single-node on TPC-H SF1, with metadata-light queries seeing more.

Run a comparison yourself and see the captured numbers in the benchmark
quickstart: [Benchmarks: TPC-H / TPC-DS / SSB](../quickstart/benchmark.md), or
in the repo under
[`benchmarks/`](https://github.com/schubergphilis/sqe/tree/main/benchmarks/).

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
