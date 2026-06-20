# sqe-bench Phase 0+1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `read_parquet()` TVF to SQE and build the `sqe-bench` CLI with TPC-H and SSB benchmark support (generate → load → test).

**Architecture:** Phase 0 adds a DataFusion table function `read_parquet(path, ...)` that reads Parquet from local or S3 with inline credentials. Phase 1 builds the `sqe-bench` binary with data generators, a loader using `read_parquet`, and a query runner with correctness validation. TPC-H (8 tables, 22 queries) and SSB (5 tables, 13 queries) are the first benchmarks.

**Tech Stack:** Rust, DataFusion 52, Arrow 57, Parquet 57, object_store 0.12, clap (CLI), arrow-flight (Flight SQL client), reqwest (Trino HTTP client)

---

## File Map

### Phase 0: `read_parquet` TVF

| File | Action | Purpose |
|------|--------|---------|
| `crates/sqe-catalog/src/read_parquet.rs` | Create | `ReadParquetFunction` implementing DataFusion `TableFunctionImpl` |
| `crates/sqe-catalog/src/lib.rs` | Modify | Export `read_parquet` module |
| `crates/sqe-catalog/Cargo.toml` | Modify | Add `datafusion-expr` dependency |
| `crates/sqe-coordinator/src/query_handler.rs:384` | Modify | Register `read_parquet` TVF on SessionContext |
| `crates/sqe-core/src/config.rs` | Modify | Add `s3_allow_http` to StorageConfig if not already present |

### Phase 1: `sqe-bench` CLI + TPC-H + SSB

| File | Action | Purpose |
|------|--------|---------|
| `Cargo.toml` | Modify | Add `sqe-bench` to workspace members, add `clap`, `csv`, `rand` deps |
| `crates/sqe-bench/Cargo.toml` | Create | Crate manifest |
| `crates/sqe-bench/src/main.rs` | Create | CLI entry (clap) |
| `crates/sqe-bench/src/cli.rs` | Create | Command definitions |
| `crates/sqe-bench/src/generate/mod.rs` | Create | `BenchmarkGenerator` trait + registry |
| `crates/sqe-bench/src/generate/tpch.rs` | Create | TPC-H data generation (8 tables) |
| `crates/sqe-bench/src/generate/ssb.rs` | Create | SSB data generation (5 tables) |
| `crates/sqe-bench/src/generate/parquet_writer.rs` | Create | Shared Parquet file writer |
| `crates/sqe-bench/src/load.rs` | Create | Table loader (generates CTAS + read_parquet SQL) |
| `crates/sqe-bench/src/client/mod.rs` | Create | `BenchClient` trait |
| `crates/sqe-bench/src/client/flight.rs` | Create | Flight SQL client |
| `crates/sqe-bench/src/client/trino.rs` | Create | Trino HTTP client |
| `crates/sqe-bench/src/test.rs` | Create | Query runner + validation |
| `crates/sqe-bench/src/compare.rs` | Create | Result comparison (Arrow vs CSV) |
| `crates/sqe-bench/src/report.rs` | Create | JSON + terminal output |
| `benchmarks/queries/tpch/q01.sql..q22.sql` | Create | TPC-H query files |
| `benchmarks/queries/ssb/q1.1.sql..q4.3.sql` | Create | SSB query files |
| `benchmarks/schemas/tpch.sql` | Create | TPC-H DDL reference |
| `benchmarks/schemas/ssb.sql` | Create | SSB DDL reference |

---

### Task 1: `read_parquet` — Parse path and named args from SQL

**Files:**
- Create: `crates/sqe-catalog/src/read_parquet.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`
- Modify: `crates/sqe-catalog/Cargo.toml`

- [ ] **Step 1: Add `datafusion-expr` dependency to sqe-catalog**

In `crates/sqe-catalog/Cargo.toml`, add:
```toml
datafusion-expr = { workspace = true }
```

- [ ] **Step 2: Create the ReadParquetFunction struct**

Create `crates/sqe-catalog/src/read_parquet.rs`:
```rust
use std::sync::Arc;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion_expr::{TableFunctionImpl, Expr};
use tracing::debug;

/// Table-valued function: read_parquet(path, [access_key => '...', ...])
///
/// Reads Parquet files from local filesystem or S3.
/// Supports glob patterns (*.parquet, **/*.parquet).
///
/// Named arguments for S3:
/// - access_key: S3 access key ID
/// - secret_key: S3 secret access key
/// - endpoint: S3 endpoint URL
/// - region: S3 region
pub struct ReadParquetFunction {
    /// Fallback S3 config from sqe.toml
    pub default_storage: sqe_core::config::StorageConfig,
}

impl ReadParquetFunction {
    pub fn new(default_storage: sqe_core::config::StorageConfig) -> Self {
        Self { default_storage }
    }
}
```

- [ ] **Step 3: Add module to lib.rs**

In `crates/sqe-catalog/src/lib.rs`, add:
```rust
pub mod read_parquet;
```

- [ ] **Step 4: Run build to verify**

Run: `cargo build -p sqe-catalog`
Expected: compiles (trait not yet implemented)

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/
git commit -m "feat: scaffold read_parquet TVF struct"
```

---

### Task 2: `read_parquet` — Implement TableFunctionImpl

**Files:**
- Modify: `crates/sqe-catalog/src/read_parquet.rs`

- [ ] **Step 1: Implement TableFunctionImpl trait**

The implementation must:
1. Extract the first positional arg as the path string
2. Extract named args (access_key, secret_key, endpoint, region)
3. Detect S3 (`s3://`) vs local path
4. Build an `ObjectStore` (AmazonS3Builder or LocalFileSystem)
5. Register the store on DataFusion's runtime
6. Create a `ListingTable` over the Parquet files
7. Return it as `Arc<dyn TableProvider>`

```rust
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::context::SessionContext;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use url::Url;

impl TableFunctionImpl for ReadParquetFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        // This is synchronous but we need async for schema inference.
        // Use block_in_place to bridge.
        let args = args.to_vec();
        let default_storage = self.default_storage.clone();

        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                call_async(&args, &default_storage).await
            })
        })
    }
}
```

The `call_async` function does the heavy lifting:
- Parse path from `Expr::Literal(ScalarValue::Utf8(Some(path)))` (first arg)
- Parse named args from `Expr::BinaryExpr { left: Identifier, op: Eq, right: Literal }`
  - Note: DataFusion passes TVF named args as `col("key") = lit("value")` expressions
- Build object store and listing table

- [ ] **Step 2: Add helper to extract named string args from Expr list**

```rust
fn extract_named_args(args: &[Expr]) -> HashMap<String, String> {
    // DataFusion TVF named params come as: Expr::BinaryExpr { col = lit }
    // or as Expr::Literal for positional args
}
```

- [ ] **Step 3: Add S3 store builder**

```rust
fn build_s3_store(
    bucket: &str,
    args: &HashMap<String, String>,
    defaults: &StorageConfig,
) -> DFResult<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket);

    // Use explicit args, fall back to defaults
    if let Some(key) = args.get("access_key").or(Some(&defaults.s3_access_key).filter(|s| !s.is_empty())) {
        builder = builder.with_access_key_id(key);
    }
    // ... secret_key, endpoint, region, allow_http

    Ok(Arc::new(builder.build().map_err(|e|
        datafusion::error::DataFusionError::External(format!("S3 config error: {e}").into())
    )?))
}
```

- [ ] **Step 4: Add tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_named_args_empty() { ... }

    #[test]
    fn test_path_detection_s3() {
        assert!(path.starts_with("s3://"));
    }

    #[test]
    fn test_path_detection_local() {
        assert!(!"/data/file.parquet".starts_with("s3://"));
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-catalog && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/src/read_parquet.rs
git commit -m "feat: implement read_parquet TVF with S3 and local file support"
```

---

### Task 3: Register `read_parquet` on SessionContext

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs:384`

- [ ] **Step 1: Register the TVF after catalog registration**

In `create_session_context`, after line 384 (`ctx.register_catalog(...)`):

```rust
// Register read_parquet() table-valued function
ctx.register_udtf(
    "read_parquet",
    Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
        self.config.storage.clone(),
    )),
);
```

- [ ] **Step 2: Add sqe-catalog import if needed**

The `sqe_catalog` crate is already imported (line 11: `use sqe_catalog::{SessionCatalog, SqeCatalogProvider};`).

- [ ] **Step 3: Run full test suite**

Run: `cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat: register read_parquet TVF on every SessionContext"
```

---

### Task 4: Integration test for `read_parquet` with local files

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`

- [ ] **Step 1: Add integration test that writes Parquet then reads via read_parquet**

```rust
#[tokio::test]
async fn test_read_parquet_local_file() {
    // 1. Create a small Parquet file in a temp dir
    // 2. Connect via Flight SQL
    // 3. Run: CREATE TABLE test_ns.from_parquet AS SELECT * FROM read_parquet('/tmp/.../test.parquet')
    // 4. Verify: SELECT * FROM test_ns.from_parquet returns the expected rows
}
```

- [ ] **Step 2: Run integration test**

Run: `./scripts/integration-test.sh test_read_parquet_local_file`

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/tests/integration_test.rs
git commit -m "test: integration test for read_parquet with local Parquet files"
```

---

### Task 5: `sqe-bench` crate scaffold + CLI

**Files:**
- Create: `crates/sqe-bench/Cargo.toml`
- Create: `crates/sqe-bench/src/main.rs`
- Create: `crates/sqe-bench/src/cli.rs`
- Modify: `Cargo.toml` (workspace)

- [ ] **Step 1: Add sqe-bench to workspace**

In root `Cargo.toml`, add to members:
```toml
"crates/sqe-bench",
```

Add new workspace deps:
```toml
clap = { version = "4", features = ["derive"] }
csv = "1"
rand = "0.8"
```

- [ ] **Step 2: Create Cargo.toml**

```toml
[package]
name = "sqe-bench"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "sqe-bench"
path = "src/main.rs"

[dependencies]
clap = { workspace = true }
arrow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
arrow-flight = { workspace = true }
parquet = { workspace = true }
object_store = { workspace = true }
tokio = { workspace = true }
reqwest = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
csv = { workspace = true }
rand = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
anyhow = { workspace = true }
chrono = { workspace = true }
url = { workspace = true }
tonic = { workspace = true }
futures = { workspace = true }
bytes = { workspace = true }
```

- [ ] **Step 3: Create CLI with clap**

`src/cli.rs`:
```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "sqe-bench", about = "SQE Benchmark Suite")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Generate benchmark data as Parquet files
    Generate {
        /// Benchmark name (tpch, ssb, tpcds, tpcc, tpce, tpcbb)
        benchmark: String,
        /// Scale factor
        #[arg(short, long, default_value = "1")]
        scale: f64,
        /// Output path (local dir or s3://bucket/prefix)
        #[arg(short, long)]
        output: String,
        /// S3 access key (for s3:// output)
        #[arg(long)]
        s3_access_key: Option<String>,
        /// S3 secret key
        #[arg(long)]
        s3_secret_key: Option<String>,
        /// S3 endpoint
        #[arg(long)]
        s3_endpoint: Option<String>,
        /// S3 region
        #[arg(long, default_value = "us-east-1")]
        s3_region: String,
    },
    /// Load generated data into SQE via read_parquet + CTAS
    Load {
        benchmark: String,
        #[arg(short, long, default_value = "1")]
        scale: f64,
        /// Data path (local or s3://)
        #[arg(short, long)]
        data: String,
        /// Protocol: flight or trino
        #[arg(short, long, default_value = "flight")]
        protocol: String,
        /// SQE host:port
        #[arg(long, default_value = "localhost:50051")]
        host: String,
        /// Drop existing tables first
        #[arg(long)]
        clean: bool,
        /// S3 credentials for read_parquet inline args
        #[arg(long)]
        s3_access_key: Option<String>,
        #[arg(long)]
        s3_secret_key: Option<String>,
        #[arg(long)]
        s3_endpoint: Option<String>,
        #[arg(long, default_value = "us-east-1")]
        s3_region: String,
        /// Auth: username for OIDC
        #[arg(long)]
        username: Option<String>,
        /// Auth: password for OIDC
        #[arg(long)]
        password: Option<String>,
    },
    /// Run benchmark queries and validate results
    Test {
        benchmark: String,
        #[arg(short, long, default_value = "1")]
        scale: f64,
        #[arg(short, long, default_value = "flight")]
        protocol: String,
        #[arg(long, default_value = "localhost:50051")]
        host: String,
        /// Run only this query (e.g., q03)
        #[arg(short, long)]
        query: Option<String>,
        /// Auth
        #[arg(long)]
        username: Option<String>,
        #[arg(long)]
        password: Option<String>,
    },
}
```

`src/main.rs`:
```rust
mod cli;
use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = cli::Cli::parse();

    match cli.command {
        cli::Command::Generate { .. } => todo!("generate"),
        cli::Command::Load { .. } => todo!("load"),
        cli::Command::Test { .. } => todo!("test"),
    }
}
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p sqe-bench`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/sqe-bench/
git commit -m "feat: scaffold sqe-bench crate with CLI"
```

---

### Task 6: Benchmark generator trait + Parquet writer

**Files:**
- Create: `crates/sqe-bench/src/generate/mod.rs`
- Create: `crates/sqe-bench/src/generate/parquet_writer.rs`

- [ ] **Step 1: Define BenchmarkGenerator trait**

`src/generate/mod.rs`:
```rust
pub mod parquet_writer;
pub mod tpch;
pub mod ssb;

use arrow_schema::SchemaRef;
use std::time::Duration;

pub struct TableDef {
    pub name: String,
    pub schema: SchemaRef,
    pub row_count: fn(f64) -> usize,
}

pub struct GenerateStats {
    pub table: String,
    pub rows: usize,
    pub bytes: usize,
    pub files: usize,
    pub duration: Duration,
}

pub trait BenchmarkGenerator: Send + Sync {
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
    ) -> anyhow::Result<GenerateStats>;
}

pub fn get_generator(name: &str) -> anyhow::Result<Box<dyn BenchmarkGenerator>> {
    match name {
        "tpch" => Ok(Box::new(tpch::TpchGenerator)),
        "ssb" => Ok(Box::new(ssb::SsbGenerator)),
        _ => anyhow::bail!("Unknown benchmark: {name}. Supported: tpch, ssb"),
    }
}
```

- [ ] **Step 2: Create shared Parquet writer**

`src/generate/parquet_writer.rs`:
```rust
use std::fs;
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Write RecordBatches to Parquet files, splitting at ~128MB.
pub fn write_parquet_files(
    batches: &[RecordBatch],
    schema: SchemaRef,
    output_dir: &str,
    table_name: &str,
    max_file_bytes: usize,
) -> anyhow::Result<(usize, usize)> {
    fs::create_dir_all(format!("{output_dir}/{table_name}"))?;

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut file_idx = 0;
    let mut total_bytes = 0usize;
    let mut current_writer: Option<ArrowWriter<fs::File>> = None;
    let mut current_bytes = 0usize;

    for batch in batches {
        if current_writer.is_none() || current_bytes >= max_file_bytes {
            if let Some(w) = current_writer.take() {
                let meta = w.close()?;
                total_bytes += meta.num_rows as usize; // approximate
            }
            let path = format!("{output_dir}/{table_name}/{file_idx:05}.parquet");
            let file = fs::File::create(&path)?;
            current_writer = Some(ArrowWriter::try_new(file, schema.clone(), Some(props.clone()))?);
            file_idx += 1;
            current_bytes = 0;
        }

        if let Some(ref mut w) = current_writer {
            w.write(batch)?;
            current_bytes += batch.get_array_memory_size();
        }
    }

    if let Some(w) = current_writer.take() {
        w.close()?;
    }

    Ok((file_idx, total_bytes))
}
```

- [ ] **Step 3: Verify build**

Run: `cargo build -p sqe-bench`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/generate/
git commit -m "feat: BenchmarkGenerator trait and Parquet writer"
```

---

### Task 7: TPC-H data generator

**Files:**
- Create: `crates/sqe-bench/src/generate/tpch.rs`

- [ ] **Step 1: Implement TPC-H schemas**

Define Arrow schemas for all 8 tables: region, nation, supplier, customer, part, partsupp, orders, lineitem. Follow the TPC-H spec for column types and names.

- [ ] **Step 2: Implement row generation**

For each table, generate rows using deterministic random data seeded by scale factor. Follow TPC-H cardinality rules:
- SF1: lineitem ~6M rows, orders ~1.5M, customer ~150K, etc.
- Use `rand::StdRng::seed_from_u64(table_seed)` for reproducibility

- [ ] **Step 3: Wire into generate command**

In `main.rs`, implement the `Generate` command:
```rust
cli::Command::Generate { benchmark, scale, output, .. } => {
    let gen = generate::get_generator(&benchmark)?;
    for table_def in gen.tables() {
        let stats = gen.generate_table(&table_def.name, scale, &output)?;
        println!("{}: {} rows, {} files", stats.table, stats.rows, stats.files);
    }
}
```

- [ ] **Step 4: Test generation**

Run: `cargo run -p sqe-bench -- generate tpch --scale 0.01 --output /tmp/tpch-test`
Verify: `/tmp/tpch-test/tpch/sf0.01/lineitem/*.parquet` exists

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/generate/tpch.rs crates/sqe-bench/src/main.rs
git commit -m "feat: TPC-H data generator (8 tables)"
```

---

### Task 8: SSB data generator

**Files:**
- Create: `crates/sqe-bench/src/generate/ssb.rs`

- [ ] **Step 1: Implement SSB schemas**

5 tables: lineorder, customer, supplier, part, date. Denormalized star schema derived from TPC-H.

- [ ] **Step 2: Implement row generation**

SSB cardinality at SF1: lineorder ~6M, customer ~30K, supplier ~2K, part ~200K, date ~2.5K.

- [ ] **Step 3: Test generation**

Run: `cargo run -p sqe-bench -- generate ssb --scale 0.01 --output /tmp/ssb-test`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/generate/ssb.rs
git commit -m "feat: SSB data generator (5 tables)"
```

---

### Task 9: Flight SQL client for sqe-bench

**Files:**
- Create: `crates/sqe-bench/src/client/mod.rs`
- Create: `crates/sqe-bench/src/client/flight.rs`

- [ ] **Step 1: Define BenchClient trait**

```rust
#[async_trait::async_trait]
pub trait BenchClient: Send + Sync {
    /// Execute SQL and return results as RecordBatches
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>>;
    /// Execute SQL that returns no rows (DDL, DML)
    async fn execute_update(&self, sql: &str) -> anyhow::Result<()>;
    /// Protocol name for reporting
    fn protocol_name(&self) -> &str;
}
```

- [ ] **Step 2: Implement Flight SQL client**

Connect via `FlightServiceClient`, do handshake with username/password → bearer token, execute via `execute` + `do_get`.

- [ ] **Step 3: Test against running SQE**

Run: `cargo run -p sqe-bench -- test tpch --scale 0.01 --protocol flight --host localhost:50051`
(Will fail since `test` not yet implemented, but validates connection)

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/client/
git commit -m "feat: Flight SQL client for sqe-bench"
```

---

### Task 10: Trino HTTP client for sqe-bench

**Files:**
- Create: `crates/sqe-bench/src/client/trino.rs`

- [ ] **Step 1: Implement Trino HTTP client**

Uses reqwest. Protocol:
1. `POST /v1/statement` with SQL body, `Authorization: Basic base64(user:pass)` or `Bearer token`
2. Poll `nextUri` until `stats.state == "FINISHED"`
3. Collect `data` arrays into Arrow RecordBatches using column metadata

- [ ] **Step 2: Commit**

```bash
git add crates/sqe-bench/src/client/trino.rs
git commit -m "feat: Trino HTTP client for sqe-bench"
```

---

### Task 11: Table loader (`load` command)

**Files:**
- Create: `crates/sqe-bench/src/load.rs`

- [ ] **Step 1: Implement loader**

For each table in the benchmark:
1. Generate SQL: `CREATE TABLE <ns>.<table> AS SELECT * FROM read_parquet('<path>/<table>/*.parquet', access_key => '...', ...)`
2. If `--clean`: run `DROP TABLE IF EXISTS <ns>.<table>` first
3. Execute via BenchClient
4. Report progress

```rust
pub async fn load_benchmark(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    data_path: &str,
    s3_args: &S3Args,
    clean: bool,
) -> anyhow::Result<()> {
    let namespace = format!("{}_sf{}", benchmark, scale);
    let gen = generate::get_generator(benchmark)?;

    // Create namespace
    client.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {namespace}")).await?;

    for table_def in gen.tables() {
        let table_path = format!("{data_path}/{benchmark}/sf{scale}/{}", table_def.name);

        if clean {
            let _ = client.execute_update(
                &format!("DROP TABLE IF EXISTS {namespace}.{}", table_def.name)
            ).await;
        }

        let mut sql = format!(
            "CREATE TABLE {namespace}.{} AS SELECT * FROM read_parquet('{table_path}/*.parquet'",
            table_def.name
        );
        // Append S3 credentials if provided
        if let Some(ref key) = s3_args.access_key {
            sql.push_str(&format!(", access_key => '{key}'"));
        }
        // ... secret_key, endpoint, region
        sql.push(')');

        client.execute_update(&sql).await?;
        println!("  ✅ {}.{} loaded", namespace, table_def.name);
    }
    Ok(())
}
```

- [ ] **Step 2: Wire into CLI**

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-bench/src/load.rs crates/sqe-bench/src/main.rs
git commit -m "feat: load command — creates Iceberg tables via read_parquet + CTAS"
```

---

### Task 12: TPC-H query files

**Files:**
- Create: `benchmarks/queries/tpch/q01.sql` through `q22.sql`
- Create: `benchmarks/schemas/tpch.sql`

- [ ] **Step 1: Add all 22 TPC-H queries**

Standard TPC-H queries adapted for Iceberg table names. Each file has:
```sql
-- name: Pricing Summary Report
-- timeout: 60s
SELECT
    l_returnflag,
    l_linestatus,
    ...
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

Note: Table references must use the namespace prefix (injected at runtime by the test runner, e.g., `tpch_sf1.lineitem`).

- [ ] **Step 2: Add TPC-H schema DDL**

`benchmarks/schemas/tpch.sql` with all 8 CREATE TABLE statements for reference.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/
git commit -m "feat: TPC-H query files (22 queries) and schema DDL"
```

---

### Task 13: SSB query files

**Files:**
- Create: `benchmarks/queries/ssb/q1.1.sql` through `q4.3.sql`
- Create: `benchmarks/schemas/ssb.sql`

- [ ] **Step 1: Add all 13 SSB queries**

Star Schema Benchmark queries (4 flights × 3-4 queries each). Simple star-join patterns.

- [ ] **Step 2: Add SSB schema DDL**

- [ ] **Step 3: Commit**

```bash
git add benchmarks/
git commit -m "feat: SSB query files (13 queries) and schema DDL"
```

---

### Task 14: Result comparison engine

**Files:**
- Create: `crates/sqe-bench/src/compare.rs`

- [ ] **Step 1: Implement result comparison**

```rust
pub enum CompareResult {
    Pass,
    Diff { message: String },  // minor mismatch (decimal precision)
    Fail { message: String },  // wrong results
}

/// Compare actual Arrow RecordBatches against expected CSV content.
pub fn compare_results(
    actual: &[RecordBatch],
    expected_csv: &str,
    epsilon: f64,
) -> anyhow::Result<CompareResult> {
    // 1. Parse expected CSV into rows
    // 2. Convert actual batches to comparable rows
    // 3. Sort both by all columns
    // 4. Compare row-by-row with numeric tolerance
}
```

- [ ] **Step 2: Add tests**

Unit tests with known good/bad comparisons.

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-bench/src/compare.rs
git commit -m "feat: result comparison engine for benchmark validation"
```

---

### Task 15: Test runner (`test` command) + report

**Files:**
- Create: `crates/sqe-bench/src/test.rs`
- Create: `crates/sqe-bench/src/report.rs`

- [ ] **Step 1: Implement test runner**

```rust
pub async fn run_benchmark_test(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
) -> anyhow::Result<BenchmarkReport> {
    let namespace = format!("{}_sf{}", benchmark, scale);
    let queries = load_query_files(benchmark)?;
    let mut results = Vec::new();

    for query in &queries {
        if let Some(filter) = query_filter {
            if query.id != filter { continue; }
        }

        // Check requires
        if !query.requires.is_empty() {
            // Skip unsupported queries
            results.push(QueryResult { id: query.id.clone(), status: Status::Skip, .. });
            continue;
        }

        let start = std::time::Instant::now();
        match client.execute(&query.sql_with_namespace(&namespace)).await {
            Ok(batches) => {
                let duration = start.elapsed();
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                // Compare if expected file exists
                let status = if let Some(expected) = load_expected(benchmark, scale, &query.id)? {
                    compare_results(&batches, &expected, 1e-4)?
                } else {
                    CompareResult::Pass // No expected file = just check it runs
                };
                results.push(QueryResult { id: query.id.clone(), status, duration, rows });
            }
            Err(e) => {
                results.push(QueryResult { id: query.id.clone(), status: Status::Error(e.to_string()), .. });
            }
        }
    }

    Ok(BenchmarkReport { benchmark, scale, protocol: client.protocol_name(), results })
}
```

- [ ] **Step 2: Implement report output**

`src/report.rs`:
- Terminal summary with colors/icons
- JSON report written to `benchmarks/results/<benchmark>-sf<N>-<protocol>-<timestamp>.json`

- [ ] **Step 3: Wire into CLI**

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/test.rs crates/sqe-bench/src/report.rs crates/sqe-bench/src/main.rs
git commit -m "feat: test command — runs queries, validates results, generates reports"
```

---

### Task 16: Generate TPC-H expected results at SF0.01

**Files:**
- Create: `benchmarks/expected/tpch/sf0.01/q01.csv` through `q22.csv`

- [ ] **Step 1: Generate expected results**

Run all 22 TPC-H queries against a known-good engine (DuckDB or DataFusion directly) at SF0.01 and save results as CSV. This is a one-time data generation step.

Alternative: start without expected files (test runner reports PASS if query executes without error) and add expected results incrementally as queries are validated.

- [ ] **Step 2: Commit**

```bash
git add benchmarks/expected/
git commit -m "feat: TPC-H expected results for SF0.01"
```

---

### Task 17: End-to-end integration test

- [ ] **Step 1: Full pipeline test**

With the test stack running:
```bash
# Generate small dataset
cargo run -p sqe-bench -- generate tpch --scale 0.01 --output /tmp/bench-data

# Load into SQE
cargo run -p sqe-bench -- load tpch --scale 0.01 \
  --data /tmp/bench-data \
  --protocol flight --host localhost:50051 \
  --username root --password s3cr3t --clean

# Run queries
cargo run -p sqe-bench -- test tpch --scale 0.01 \
  --protocol flight --host localhost:50051 \
  --username root --password s3cr3t
```

- [ ] **Step 2: Fix any query failures**

Some TPC-H queries may need syntax adjustments for DataFusion compatibility (e.g., date arithmetic, HAVING clauses).

- [ ] **Step 3: Commit fixes**

```bash
git commit -m "fix: adjust TPC-H queries for DataFusion SQL dialect compatibility"
```

---

### Task 18: Final build + clippy + docs

- [ ] **Step 1: Full build and test**

Run: `cargo build --all && cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 2: Update README/nextsteps**

Add benchmark section to README and mark relevant items in nextsteps.md.

- [ ] **Step 3: Commit**

```bash
git commit -m "docs: update README with benchmark suite documentation"
```
