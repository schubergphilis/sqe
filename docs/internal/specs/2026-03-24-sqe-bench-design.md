# sqe-bench — Benchmark Suite Design

## Goal

A Rust CLI tool (`sqe-bench`) that generates, loads, and tests TPC benchmark suites against SQE. Validates SQL correctness and measures query performance across Flight SQL and Trino HTTP protocols.

## Commands

```
sqe-bench generate <benchmark> --scale <N> --output <path|s3://...>
sqe-bench load <benchmark> --scale <N> --data <path|s3://...> --protocol <flight|trino> --host <addr>
sqe-bench test <benchmark> --scale <N> --protocol <flight|trino> --host <addr> [--query <qNN>]
```

### `generate`

Generates Parquet data files for a benchmark at the given scale factor.

- **Input:** benchmark name, scale factor, output path
- **Output:** Parquet files organized as `<output>/<benchmark>/sf<N>/<table>/*.parquet`
- **Targets:** local filesystem or S3 (via `object_store` crate)
- **Idempotent:** overwrites existing files at the same path

### `load`

Creates Iceberg tables from generated Parquet files via `read_parquet()` + CTAS.

- **Input:** benchmark name, scale factor, data path, connection details
- **Process:**
  1. Connect to SQE via Flight SQL or Trino HTTP
  2. Create namespace `<benchmark>_sf<N>` (e.g., `tpch_sf1`)
  3. If `--clean`: drop existing tables first
  4. For each table, send SQL:
     ```sql
     CREATE TABLE <ns>.<table> AS
     SELECT * FROM read_parquet('<data-path>/<benchmark>/sf<N>/<table>/*.parquet',
       access_key => '...', secret_key => '...', endpoint => '...', region => '...')
     ```
  5. For local paths: `read_parquet('/abs/path/to/<table>/*.parquet')`
- **Credentials:** passed via `--s3-access-key`, `--s3-secret-key`, `--s3-endpoint`, `--s3-region` CLI flags (injected into SQL). For local paths, no credentials needed.
- **Depends on:** `read_parquet()` TVF in SQE (see prerequisite section)

### `test`

Runs the query suite and validates results against expected output.

- **Input:** benchmark name, scale factor, connection details
- **Process:**
  1. Connect to SQE via Flight SQL or Trino HTTP
  2. Execute each `.sql` query file in order
  3. Compare results against `expected/<benchmark>/sf<N>/qNN.csv`
  4. Collect timing, row counts, correctness
  5. Output terminal summary + JSON report
- **Skipping:** Queries with `-- requires: delete` or `-- requires: merge` headers are skipped with status `SKIP (not implemented)` rather than `FAIL`
- **Single query:** `--query q03` runs only that query

## Supported Benchmarks

| Benchmark | Tables | Queries | Type | Notes |
|-----------|--------|---------|------|-------|
| `tpch` | 8 | 22 | Analytical | Star/snowflake, pure reads |
| `tpcds` | 24 | 99 | Analytical | Complex schema, advanced SQL |
| `ssb` | 5 | 13 | Analytical | Denormalized star, quick smoke test |
| `tpcc` | 9 | 5 tx types | OLTP | Read queries now, writes when DELETE/MERGE land |
| `tpce` | 33 | 10 tx types | OLTP | Read queries now, writes when DELETE/MERGE land |
| `tpcbb` | ~25 | 10 (SQL-only) | Mixed | Reuses TPC-DS data + web logs, skips ML/UDF queries |

## Prerequisite: `read_parquet` Table-Valued Function

The `load` command depends on a `read_parquet()` TVF in SQE that reads Parquet files from local filesystem or S3 and returns them as a DataFusion table scan. This enables:

```sql
-- Local file
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/*.parquet');

-- S3 with inline credentials
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet(
  's3://bench-data/tpch/sf1/lineitem/*.parquet',
  access_key => 'AKIA...',
  secret_key => '...',
  endpoint => 'http://localhost:9000',
  region => 'us-east-1'
);

-- S3 with default credentials (falls back to sqe.toml storage config)
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet('s3://bench-data/tpch/sf1/lineitem/*.parquet');
```

### Implementation

Register a DataFusion `TableFunction` that:
1. Parses the path argument (detect `s3://` vs local `file://` / absolute path)
2. For S3: builds an `AmazonS3Builder` from inline named args (`access_key`, `secret_key`, `endpoint`, `region`) with fallback to `StorageConfig` defaults
3. For local: uses DataFusion's built-in local filesystem `ObjectStore`
4. Supports glob patterns (`*.parquet`, `**/*.parquet`)
5. Returns a `ListingTable` scan over the matched Parquet files

This lives in `sqe-catalog` or a new `sqe-functions` crate and is registered on every `SessionContext` during query planning.

## Data Generation

Each benchmark implements:

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
    pub schema: Arc<Schema>,       // Arrow schema
    pub row_count_fn: fn(f64) -> usize,  // scale factor → row count
}

pub struct GenerateStats {
    pub table: String,
    pub rows: usize,
    pub bytes: usize,
    pub files: usize,
    pub duration: Duration,
}
```

Data is generated as Arrow RecordBatches and written to Parquet using the `parquet` crate (already in workspace). Files are split at ~128MB for parallelism.

### Generator implementations

**TPC-H:** Port of dbgen logic. 8 tables (nation, region, supplier, customer, part, partsupp, orders, lineitem). Scale factor 1 = ~1GB. Well-documented spec, straightforward generation.

**TPC-DS:** Port of dsdgen logic. 24 tables across 3 schemas (store, catalog, web sales + returns + dimensions). Scale factor 1 = ~1GB. More complex than TPC-H — date dimensions, surrogate keys, nullable columns.

**SSB (Star Schema Benchmark):** Derived from TPC-H with denormalized fact table. 5 tables (lineorder, customer, supplier, part, date). Scale factor 1 = ~600MB. Simple star schema.

**TPC-C:** 9 tables (warehouse, district, customer, history, orders, new_order, order_line, item, stock). Scale factor = number of warehouses. Generation follows the TPC-C spec for data distributions and cardinality constraints.

**TPC-E:** 33 tables modeling a brokerage. Scale factor = number of customers. Most complex schema. Tables cover accounts, trades, market data, customer demographics, company financials.

**TPC-BB (SQL subset):** Reuses TPC-DS data generation + adds web_logs and product_reviews tables. Only the 10 pure-SQL queries are included (skips ML/UDF queries).

## Query Files

Plain SQL files stored at:
```
benchmarks/
├── queries/
│   ├── tpch/
│   │   ├── q01.sql
│   │   ├── q02.sql
│   │   └── ... q22.sql
│   ├── tpcds/
│   │   ├── q01.sql
│   │   └── ... q99.sql
│   ├── ssb/
│   │   ├── q1.1.sql
│   │   └── ... q4.3.sql
│   ├── tpcc/
│   │   ├── new_order_read.sql
│   │   ├── order_status.sql
│   │   ├── stock_level.sql
│   │   ├── new_order_write.sql      # -- requires: delete, merge
│   │   └── payment_write.sql        # -- requires: delete, merge
│   ├── tpce/
│   │   ├── trade_lookup.sql
│   │   ├── customer_position.sql
│   │   ├── market_watch.sql
│   │   ├── trade_order.sql          # -- requires: delete, merge
│   │   └── ...
│   └── tpcbb/
│       ├── q01.sql
│       └── ... q10.sql
├── expected/
│   ├── tpch/sf1/
│   │   ├── q01.csv
│   │   └── ... q22.csv
│   └── ...
└── schemas/
    ├── tpch.sql        # CREATE TABLE DDL for reference
    ├── tpcds.sql
    ├── ssb.sql
    ├── tpcc.sql
    ├── tpce.sql
    └── tpcbb.sql
```

### Query header annotations

```sql
-- name: Revenue by nation
-- requires: delete, merge
-- timeout: 30s
SELECT ...
```

- `requires`: comma-separated list of features. If SQE doesn't support any, the query is skipped.
- `timeout`: per-query timeout override (default: 60s)

## Protocol Clients

### Flight SQL client

Uses `arrow-flight` crate (already in workspace). Connects with bearer token auth (from OIDC handshake or direct token). Streams results as Arrow RecordBatches.

### Trino HTTP client

Uses `reqwest` (already in workspace). Implements the Trino v1 statement protocol:
1. `POST /v1/statement` with SQL body + auth header
2. Poll `nextUri` until state = `FINISHED`
3. Parse JSON row data into Arrow RecordBatches for comparison

## Result Validation

For each query:
1. Execute query, collect result as `Vec<RecordBatch>`
2. Load expected result from CSV
3. Compare:
   - **Schema match**: column names and types
   - **Row count match**
   - **Data match**: sort both by all columns, compare row-by-row
   - **Numeric tolerance**: configurable epsilon for float/decimal comparisons (default: 1e-4)
4. Status: `PASS`, `FAIL` (wrong result), `DIFF` (minor mismatch like decimal precision), `SKIP` (unsupported feature), `ERROR` (query failed)

## Report Output

### Terminal

```
sqe-bench test tpch --scale 1 --protocol flight

TPC-H SF1 — Flight SQL (localhost:50051)
─────────────────────────────────────────
q01  ✅ PASS   1.23s   6001215 rows
q02  ✅ PASS   0.45s       460 rows
q03  ✅ PASS   0.89s     11620 rows
...
q17  ⚠️  DIFF   2.10s         1 rows  (decimal precision)
q22  ✅ PASS   0.33s         7 rows

Results: 20/22 PASS, 1 DIFF, 1 SKIP
Total time: 28.4s
Report: benchmarks/results/tpch-sf1-flight-2026-03-24T14:30:00.json
```

### JSON report

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
    }
  ]
}
```

## Crate Structure

```
crates/sqe-bench/
├── Cargo.toml
├── src/
│   ├── main.rs              # CLI entry point (clap)
│   ├── cli.rs               # Command definitions
│   ├── generate/
│   │   ├── mod.rs            # BenchmarkGenerator trait + registry
│   │   ├── tpch.rs
│   │   ├── tpcds.rs
│   │   ├── ssb.rs
│   │   ├── tpcc.rs
│   │   ├── tpce.rs
│   │   └── tpcbb.rs
│   ├── load.rs               # Table loader (Parquet → CTAS/INSERT)
│   ├── client/
│   │   ├── mod.rs            # BenchmarkClient trait
│   │   ├── flight.rs         # Flight SQL client
│   │   └── trino.rs          # Trino HTTP client
│   ├── test.rs               # Query runner + validation
│   ├── compare.rs            # Result comparison (Arrow vs CSV)
│   └── report.rs             # JSON + terminal output
```

## Dependencies

From workspace (already available):
- `arrow`, `arrow-array`, `arrow-schema`, `arrow-flight` — Arrow ecosystem
- `parquet` — Parquet file writing
- `object_store` — S3 + local filesystem
- `reqwest` — HTTP client (Trino protocol)
- `tokio` — async runtime
- `serde`, `serde_json` — serialization
- `tracing` — logging
- `clap` — CLI parsing (add to workspace)
- `csv` — expected result loading (add to workspace)
- `rand` — data generation with deterministic seeds

## Implementation Priority

1. **Phase 0: `read_parquet()` TVF** — Prerequisite for the `load` command. Inline S3 credentials + local file support.
2. **Phase 1: TPC-H + SSB** — Simplest schemas, well-understood queries, validates the full pipeline (generate → load → test)
3. **Phase 2: TPC-DS** — More complex SQL (correlated subqueries, window functions, CTEs), larger schema
4. **Phase 3: TPC-C + TPC-E** — Schema + data generation, read-only queries now, write queries behind feature flag
5. **Phase 4: TPC-BB** — Extends TPC-DS with semi-structured data, SQL-only subset

## Success Criteria

- `sqe-bench generate tpch --scale 1` produces valid Parquet files in <30s
- `sqe-bench load tpch --scale 1` creates all 8 tables via CTAS
- `sqe-bench test tpch --scale 1 --protocol flight` runs all 22 queries, reports correctness + timing
- Same workflow works for each benchmark
- JSON reports enable automated regression tracking
- Unsupported queries (DELETE/MERGE) are skipped, not failed
