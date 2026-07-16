# Generic Direct-to-Iceberg Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let every generator benchmark (tpch, tpcds, ssb, clickbench, tpcbb, tpcc, tpce) write generated data straight into Iceberg via `--sink iceberg`, reusing the bank sink's async writer/commit machinery, with the Parquet staging path left untouched.

**Architecture:** Add a `generate_batches` batch-producer to each generator. Extract `run_bank`'s reusable pieces (`ensure_table`, `write_shard`, generalized `commit_files`) into shared helpers. Build a generic async `IcebergDirectSink::run` driver that pulls batches (sync producer on `spawn_blocking` → bounded channel → async writer), writes `DataFile`s, and commits one `fast_append` per table with a `sqe-bench.table.<name>=done` resume marker. Bank is refactored to call the shared helpers.

**Tech Stack:** Rust, tokio, iceberg-rust (`Catalog`, `Transaction::fast_append`, `DataFileWriterBuilder`), arrow, `sqe-bench` generator framework.

## Global Constraints

- Do NOT modify `generate_table`, `write_parquet_files`, or `parallel_generate_table`. The Iceberg sink is a strictly additive parallel path; the staging flow stays byte-identical.
- The sync batch producer runs on `spawn_blocking`; the async writer/commit runs on tokio tasks bounded by `Semaphore(config.threads.max(1))`. Never call `block_on` inside the generation threads.
- Table creation is unpartitioned for all non-bank benchmarks: `arrow_schema_to_schema(TableDef.schema)` + the `ensure_tables` else-branch; `write_shard` passes `partition_key = None`.
- Determinism: tpch/bank shard splitting must use `config::partition` + `config::seed_for_table_partition`, identical to `parallel_generate_table`, so rows match the staging path exactly.
- Resume marker: `sqe-bench.table.<name> = done`, set inside the same commit transaction as the `fast_append`.
- Bank's externally observable behavior (per-day snapshots, `sqe-bench.day.*` markers, per-day resume) MUST NOT change; the existing bank tests are the guard.
- Only bank and tpch expose `gen_range` for shard-parallelism; the six serial generators return a single shard (tpcds is already serial today — no regression).
- Default sink stays `parquet`.

## Reference: current code being generalized

`run_bank`'s `write_unit` (`sink/iceberg.rs:351-399`), `commit_files` (`:401-441`), `ensure_tables` (`:270-337`), `RunCtx` (`:237-247`), `TableCtx` (`:229-235`), catalog build (`:505-509`), `run_group` semaphore/spawn/join (`:458-494`). `BenchmarkGenerator`/`TableDef`/`GenerateStats` (`generate/mod.rs:24-52`). Vec generators call `parquet_writer::write_parquet_files(&batches, schema, &full_output, table)`: ssb `:800`, tpcc `:1057`, tpcbb `:379`, clickbench `:746`, tpce `:2323`, tpcds `:1813`. tpch `generate_table` `:949` (uses `parallel_generate_table` at `:1046`, `write_parquet_files` for region/nation at `:964,:979`). main.rs iceberg dispatch `:71-141` (bank guard at the `anyhow::ensure!(benchmark == "bank", ...)`).

---

### Task 1: Batch-source types and the generator trait method

**Files:**
- Modify: `crates/sqe-bench/src/generate/mod.rs`
- Test: `crates/sqe-bench/src/generate/mod.rs` (`#[cfg(test)]`)

**Interfaces:**
- Produces:
  - `pub struct BatchSource { pub schema: SchemaRef, pub total_rows: usize, pub shards: Vec<BatchShard> }`
  - `pub struct BatchShard { pub make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = RecordBatch> + Send> + Send> }`
  - `BenchmarkGenerator::generate_batches(&self, table: &str, scale: f64, config: &GenerateConfig) -> anyhow::Result<BatchSource>` with a default impl that returns an "unsupported" error, so generators are migrated one at a time without breaking the build.

- [ ] **Step 1: Add the types and trait method with a default impl**

In `generate/mod.rs`, add near `TableDef`:

```rust
/// One unit of batch production: a boxed factory the driver calls on a
/// blocking thread. `FnOnce` because a shard is produced exactly once.
pub struct BatchShard {
    pub make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = arrow_array::RecordBatch> + Send> + Send>,
}

/// A table's rows exposed as Arrow batches for the Iceberg direct sink.
/// `shards` is the disjoint work list; serial generators return one shard,
/// range-splitting generators (tpch, bank) return `config.threads` shards.
pub struct BatchSource {
    pub schema: SchemaRef,
    pub total_rows: usize,
    pub shards: Vec<BatchShard>,
}
```

Extend the trait:

```rust
pub trait BenchmarkGenerator: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        config: &GenerateConfig,
    ) -> anyhow::Result<GenerateStats>;

    /// Produce a table's rows as Arrow batches for the Iceberg direct sink.
    /// Default: unsupported (generators opt in one at a time).
    fn generate_batches(
        &self,
        table: &str,
        _scale: f64,
        _config: &GenerateConfig,
    ) -> anyhow::Result<BatchSource> {
        anyhow::bail!(
            "{}: generate_batches not implemented for table {table}",
            self.name()
        )
    }
}
```

- [ ] **Step 2: Write a test that a single-shard BatchSource yields its rows**

```rust
#[cfg(test)]
mod batch_source_tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn batch_shard_make_produces_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let s2 = schema.clone();
        let shard = BatchShard {
            make: Box::new(move || {
                let b = RecordBatch::try_new(
                    s2.clone(),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap();
                Box::new(std::iter::once(b))
            }),
        };
        let src = BatchSource { schema, total_rows: 3, shards: vec![shard] };
        let rows: usize = (src.shards.into_iter().next().unwrap().make)()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
        assert_eq!(src.total_rows, 3);
    }
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p sqe-bench batch_source 2>&1 | tail -20`
Expected: PASS. `cargo build -p sqe-bench` still succeeds (default trait method means no generator is forced to implement it yet).

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/generate/mod.rs
git commit -m "feat(bench): add BatchSource/BatchShard and generate_batches trait method"
```

---

### Task 2: Extract reusable helpers from run_bank (ensure_table, write_shard, commit_files)

**Files:**
- Modify: `crates/sqe-bench/src/sink/iceberg.rs`
- Test: existing bank tests in `crates/sqe-bench` (regression guard) + a new unit test for the generalized `commit_files` property merge

**Interfaces:**
- Produces:
  - `async fn ensure_table(catalog: &dyn Catalog, ns: &NamespaceIdent, name: &str, arrow_schema: &ArrowSchema, partition_col: Option<&str>, clean: bool) -> anyhow::Result<TableCtx>`
  - `async fn write_shard(writer_props: &WriterProperties, target_file_size: usize, table_ctx: &TableCtx, file_prefix: String, partition_key: Option<PartitionKey>, batches: impl Iterator<Item = RecordBatch>) -> anyhow::Result<Vec<DataFile>>`
  - `async fn commit_files(catalog: &dyn Catalog, table_ctx: &TableCtx, commit_lock: &Mutex<()>, files: Vec<DataFile>, extra_props: HashMap<String, String>) -> anyhow::Result<()>`
- Consumes: nothing new. Refactors `run_bank`/`run_group`/`write_unit`/`ensure_tables` to call these.

- [ ] **Step 1: Extract `ensure_table` from the `ensure_tables` loop body**

Pull the per-table body of `ensure_tables` (`iceberg.rs:288-334`) into:

```rust
async fn ensure_table(
    catalog: &dyn Catalog,
    ns: &NamespaceIdent,
    name: &str,
    arrow_schema: &ArrowSchema,
    partition_col: Option<&str>,
    clean: bool,
) -> anyhow::Result<TableCtx> {
    let ident = TableIdent::new(ns.clone(), name.to_string());
    if clean && catalog.table_exists(&ident).await? {
        catalog.drop_table(&ident).await.with_context(|| format!("dropping table {name}"))?;
        println!("Dropped table {ns:?}.{name}");
    }
    let table = if catalog.table_exists(&ident).await? {
        catalog.load_table(&ident).await?
    } else {
        let schema = arrow_schema_to_schema(arrow_schema)
            .with_context(|| format!("converting {name} schema"))?;
        let creation = if let Some(col) = partition_col {
            let spec = PartitionSpec::builder(schema.clone())
                .add_partition_field(col, col, Transform::Identity)
                .and_then(|b| b.build())
                .with_context(|| format!("building {name} partition spec"))?;
            TableCreation::builder().name(name.to_string()).schema(schema)
                .partition_spec(spec.into_unbound()).build()
        } else {
            TableCreation::builder().name(name.to_string()).schema(schema).build()
        };
        let table = catalog.create_table(ns, creation).await
            .with_context(|| format!("creating table {name}"))?;
        println!("Created table {name}");
        table
    };
    let write_schema = Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema())
            .with_context(|| format!("deriving {name} write schema"))?,
    );
    Ok(TableCtx { table, write_schema })
}
```

Rewrite `ensure_tables` to call `ensure_table` in its loop (bank passes `bank::partition_column(name)` as `partition_col`).

- [ ] **Step 2: Extract `write_shard` from `write_unit`**

Generalize `write_unit` (`iceberg.rs:351-399`) so the partition key and file prefix are parameters, not derived from a bank day:

```rust
async fn write_shard(
    writer_props: &WriterProperties,
    target_file_size: usize,
    table_ctx: &TableCtx,
    file_prefix: String,
    partition_key: Option<PartitionKey>,
    batches: impl Iterator<Item = RecordBatch>,
) -> anyhow::Result<Vec<DataFile>> {
    let table = &table_ctx.table;
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
        .context("building location generator")?;
    let file_name_gen = DefaultFileNameGenerator::new(
        file_prefix, None, iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        writer_props.clone(), table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new(
        parquet_builder, target_file_size, table.file_io().clone(),
        location_gen, file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(partition_key).await.context("building data file writer")?;
    for batch in batches {
        let batch = RecordBatch::try_new(table_ctx.write_schema.clone(), batch.columns().to_vec())
            .context("rebinding batch to table schema")?;
        writer.write(batch).await.context("writing batch")?;
    }
    writer.close().await.context("closing writer")
}
```

Rewrite `write_unit` to compute its bank `partition_key`/`file_prefix` (as today) and delegate to `write_shard`.

- [ ] **Step 3: Generalize `commit_files` to arbitrary properties**

Replace the day-specific property logic in `commit_files` (`iceberg.rs:401-441`) with an `extra_props` map applied as both snapshot properties and table properties:

```rust
async fn commit_files(
    catalog: &dyn Catalog,
    table_ctx: &TableCtx,
    commit_lock: &Mutex<()>,
    files: Vec<DataFile>,
    snapshot_props: HashMap<String, String>,
    table_props: HashMap<String, String>,
) -> anyhow::Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    let _guard = commit_lock.lock().await;
    let table = catalog.load_table(table_ctx.table.identifier()).await
        .context("reloading table before commit")?;
    let tx = Transaction::new(&table);
    let mut append = tx.fast_append().add_data_files(files);
    if !snapshot_props.is_empty() {
        append = append.set_snapshot_properties(snapshot_props);
    }
    let mut tx = append.apply(tx).context("applying append")?;
    if !table_props.is_empty() {
        let mut upd = tx.update_table_properties();
        for (k, v) in table_props {
            upd = upd.set(k, v);
        }
        tx = upd.apply(tx).context("applying table properties")?;
    }
    tx.commit(catalog).await.context("committing append")?;
    Ok(())
}
```

Update bank's caller (`run_group`, `iceberg.rs:489`) to build the day maps:

```rust
let (snap, tprops) = match day {
    Some(d) => (
        HashMap::from([(SNAPSHOT_DAY_PROP.to_string(), format_day(d))]),
        HashMap::from([(format!("{DAY_PROP_PREFIX}{}", format_day(d)), "done".to_string())]),
    ),
    None => (HashMap::new(), HashMap::new()),
};
commit_files(ctx.catalog.as_ref(), &ctx.tables[table_name], &ctx.commit_lock, files, snap, tprops).await?;
```

(Adjust `write_unit`/`run_group` to pass `&ctx.writer_props`, `ctx.target_file_size` into `write_shard`.)

- [ ] **Step 4: Build and run the bank regression tests**

Run: `cargo build -p sqe-bench 2>&1 | tail -20`
Expected: compiles.

Run: `cargo test -p sqe-bench 2>&1 | tail -30`
Expected: all existing bank/sink tests still PASS (behavior unchanged by the extraction). If any bank test references `write_unit`/`commit_files` by the old signature, update the call, not the assertion.

- [ ] **Step 5: Add a unit test for the property merge in commit path**

Since `commit_files` needs a catalog, test only the pure property-map construction by extracting the day-map builder into a small pure fn `fn day_props(day: Option<i32>) -> (HashMap<String,String>, HashMap<String,String>)` and testing it:

```rust
#[test]
fn day_props_sets_snapshot_and_table_markers() {
    let (snap, tprops) = day_props(Some(0));
    assert!(snap.contains_key(SNAPSHOT_DAY_PROP));
    assert!(tprops.keys().any(|k| k.starts_with(DAY_PROP_PREFIX)));
    let (snap0, tprops0) = day_props(None);
    assert!(snap0.is_empty() && tprops0.is_empty());
}
```

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-bench/src/sink/iceberg.rs
git commit -m "refactor(bench): extract ensure_table/write_shard/commit_files from run_bank"
```

---

### Task 3: The generic IcebergDirectSink driver

**Files:**
- Modify: `crates/sqe-bench/src/sink/iceberg.rs`
- Test: `crates/sqe-bench/src/sink/iceberg.rs` (shard-plan unit test)

**Interfaces:**
- Consumes: `ensure_table`, `write_shard`, `commit_files` (Task 2); `BatchSource`/`BatchShard`, `BenchmarkGenerator`, `TableDef` (Task 1); `IcebergTarget`, `RestCatalogBuilder`, `WriterProperties`.
- Produces: `pub async fn run_direct(target: &IcebergTarget, gen: &dyn BenchmarkGenerator, scale: f64, config: &GenerateConfig, clean: bool, resume: bool, target_file_size: usize) -> anyhow::Result<()>`.

- [ ] **Step 1: Implement the driver**

```rust
/// Generic direct-to-Iceberg sink: write every table of `gen` straight into
/// Iceberg, one fast_append per table, with a per-table resume marker.
pub async fn run_direct(
    target: &IcebergTarget,
    gen: &dyn BenchmarkGenerator,
    scale: f64,
    config: &GenerateConfig,
    clean: bool,
    resume: bool,
    target_file_size: usize,
) -> anyhow::Result<()> {
    use crate::generate::BenchmarkGenerator;

    const TABLE_DONE_PREFIX: &str = "sqe-bench.table.";

    let catalog = RestCatalogBuilder::default()
        .load("polaris", target.catalog_props())
        .await
        .context("connecting to catalog")?;
    let catalog: Box<dyn Catalog> = Box::new(catalog);

    let ns = NamespaceIdent::new(target.namespace.clone());
    if !catalog.namespace_exists(&ns).await.context("checking namespace")? {
        catalog.create_namespace(&ns, HashMap::new()).await.context("creating namespace")?;
        println!("Created namespace {}", target.namespace);
    }

    let mut props_builder =
        WriterProperties::builder().set_compression(config.compression.to_parquet());
    if let Some(rgs) = config.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rgs));
    }
    let writer_props = props_builder.build();
    let commit_lock = Mutex::new(());

    for table_def in gen.tables() {
        let name = table_def.name.clone();
        let done_key = format!("{TABLE_DONE_PREFIX}{name}");

        // Resume: skip a table already marked done (unless clean forces a rebuild).
        let table_ctx = ensure_table(
            catalog.as_ref(), &ns, &name, table_def.schema.as_ref(), None, clean,
        )
        .await?;
        if resume && !clean {
            if let Some(v) = table_ctx.table.metadata().properties().get(&done_key) {
                if v == "done" {
                    println!("  skip {name}: already committed");
                    continue;
                }
            }
        }

        let src = gen.generate_batches(&name, scale, config)?;
        let sem = Arc::new(Semaphore::new(config.threads.max(1)));
        let mut handles = Vec::with_capacity(src.shards.len());
        for (idx, shard) in src.shards.into_iter().enumerate() {
            let sem = sem.clone();
            let writer_props = writer_props.clone();
            let tctx = table_ctx.clone_handle(); // see Step 2
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                // Sync producer on a blocking thread -> bounded channel -> async writer.
                let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);
                let make = shard.make;
                let producer = tokio::task::spawn_blocking(move || {
                    for b in make() {
                        if tx.blocking_send(b).is_err() {
                            break;
                        }
                    }
                });
                let mut batches = Vec::new();
                while let Some(b) = rx.recv().await {
                    batches.push(b);
                }
                producer.await.context("producer task panicked")?;
                write_shard(
                    &writer_props, target_file_size, &tctx,
                    format!("{idx:04}"), None, batches.into_iter(),
                )
                .await
            }));
        }
        let mut files = Vec::new();
        for h in handles {
            files.extend(h.await.context("shard task panicked")??);
        }

        let rows: u64 = files.iter().map(|f| f.record_count()).sum();
        let bytes: u64 = files.iter().map(|f| f.file_size_in_bytes()).sum();
        let nfiles = files.len();
        let table_props = HashMap::from([(done_key, "done".to_string())]);
        commit_files(
            catalog.as_ref(), &table_ctx, &commit_lock, files, HashMap::new(), table_props,
        )
        .await?;
        println!("  committed {name}: {rows} rows, {nfiles} files, {}", human_bytes(bytes));
    }
    Ok(())
}
```

Note on the channel: buffering into a `Vec` then calling `write_shard` matches `run_bank` semantics (it collects a unit's batches too). If a table is large enough that buffering matters, change `write_shard` to accept the `rx` stream directly — but per the spec's memory constraints, table-at-a-shard buffering with the semaphore bound is acceptable for v1. Keep the `spawn_blocking` producer even when buffering: it keeps CPU-heavy generation off the async runtime.

- [ ] **Step 2: Make `TableCtx` shareable across shard tasks**

`Table` handles are cheap to clone via reload-free copy? Confirm `iceberg::table::Table: Clone`. If it is, add:

```rust
impl TableCtx {
    fn clone_handle(&self) -> TableCtx {
        TableCtx { table: self.table.clone(), write_schema: self.write_schema.clone() }
    }
}
```

If `Table` is not `Clone`, wrap `table_ctx` in `Arc` and pass `Arc<TableCtx>` into the tasks instead (adjust `write_shard`/`commit_files` to take `&TableCtx` from the `Arc`). Decide at compile time.

- [ ] **Step 3: Unit test the shard file-prefix plan**

The driver's parallel correctness is stack-gated, but the shard indexing is pure. Extract nothing new; instead assert the prefix format via a tiny test:

```rust
#[test]
fn shard_prefix_is_zero_padded() {
    assert_eq!(format!("{:04}", 3usize), "0003");
    assert_eq!(format!("{:04}", 12usize), "0012");
}
```

(This documents the invariant that shard prefixes are disjoint and match the staging `{part_idx:04}` convention.)

- [ ] **Step 4: Build**

Run: `cargo build -p sqe-bench 2>&1 | tail -20`
Expected: compiles. Fix `Table` clone/Arc per Step 2 if needed.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/sink/iceberg.rs
git commit -m "feat(bench): generic IcebergDirectSink run_direct driver"
```

---

### Task 4: Implement generate_batches for tpch (shard-parallel via gen_range)

**Files:**
- Modify: `crates/sqe-bench/src/generate/tpch.rs`
- Test: `crates/sqe-bench/src/generate/tpch.rs`

**Interfaces:**
- Consumes: `BatchSource`, `BatchShard`, `config::partition`, `config::seed_for_table_partition`, the existing `generate_*_range` factories and `seed_for_table`.
- Produces: `TpchGenerator::generate_batches`.

- [ ] **Step 1: Write a determinism test (row count parity)**

```rust
#[test]
fn tpch_generate_batches_row_count_matches_scale() {
    use crate::generate::{BenchmarkGenerator, GenerateConfig};
    let g = TpchGenerator;
    let cfg = GenerateConfig { threads: 4, ..GenerateConfig::default() };
    let src = g.generate_batches("supplier", 1.0, &cfg).unwrap();
    // scaled(1.0, 10_000) = 10_000 suppliers.
    assert_eq!(src.total_rows, 10_000);
    let rows: usize = src.shards.into_iter()
        .map(|s| (s.make)().map(|b| b.num_rows()).sum::<usize>())
        .sum();
    assert_eq!(rows, 10_000);
}
```

(Confirm `GenerateConfig` has a `Default` or construct it as the crate's tests already do; grep an existing test for the idiom.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-bench tpch_generate_batches 2>&1 | tail -20`
Expected: FAIL — default trait method bails with "not implemented".

- [ ] **Step 3: Implement `generate_batches`**

Mirror the table/schema/row-count/`gen_range` selection already in `generate_table` (tpch.rs:996-1044), but instead of calling `parallel_generate_table`, split into shards:

```rust
fn generate_batches(
    &self,
    table: &str,
    scale: f64,
    config: &GenerateConfig,
) -> anyhow::Result<crate::generate::BatchSource> {
    use crate::generate::{config as gcfg, BatchShard, BatchSource};

    // region/nation: tiny, single shard.
    if table == "region" || table == "nation" {
        let (schema, batches) = if table == "region" { generate_region() } else { generate_nation() };
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        let make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = RecordBatch> + Send> + Send> =
            Box::new(move || Box::new(batches.into_iter()));
        return Ok(BatchSource { schema, total_rows: total, shards: vec![BatchShard { make }] });
    }

    let (total_rows, schema): (usize, SchemaRef) = match table {
        "supplier" => (super::scaled(scale, 10_000.0).max(1), supplier_schema()),
        "customer" => (super::scaled(scale, 150_000.0).max(1), customer_schema()),
        "part" => (super::scaled(scale, 200_000.0).max(1), part_schema()),
        "partsupp" => (super::scaled(scale, 800_000.0).max(1), partsupp_schema()),
        "orders" => (super::scaled(scale, 1_500_000.0).max(1), orders_schema()),
        "lineitem" => (super::scaled(scale, 6_000_000.0).max(1), lineitem_schema()),
        _ => anyhow::bail!("Unknown TPC-H table: {table}"),
    };
    let base_seed = seed_for_table(table);
    let threads = config.threads.max(1);
    let ranges = gcfg::partition(total_rows, threads);

    let mut shards = Vec::with_capacity(ranges.len());
    for (part_idx, range) in ranges.into_iter().enumerate() {
        let seed = gcfg::seed_for_table_partition(base_seed, part_idx);
        let table = table.to_string();
        let make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = RecordBatch> + Send> + Send> =
            Box::new(move || match table.as_str() {
                "supplier" => Box::new(generate_supplier_range(range, scale, seed)),
                "customer" => Box::new(generate_customer_range(range, scale, seed)),
                "part" => Box::new(generate_part_range(range, scale, seed)),
                "partsupp" => Box::new(generate_partsupp_range(range, scale, seed)),
                "orders" => Box::new(generate_orders_range(range, scale, seed)),
                "lineitem" => Box::new(generate_lineitem_range(range, scale, seed)),
                _ => unreachable!("filtered above"),
            });
        shards.push(BatchShard { make });
    }
    Ok(BatchSource { schema, total_rows, shards })
}
```

Confirm `config::partition` and `config::seed_for_table_partition` are `pub` (they are used by `parallel_generate_table`; if `pub(crate)`, they are reachable from `tpch.rs` within the same crate).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p sqe-bench tpch_generate_batches 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/generate/tpch.rs
git commit -m "feat(bench): tpch generate_batches (shard-parallel via gen_range)"
```

---

### Task 5: Implement generate_batches for the six serial generators

**Files:**
- Modify: `crates/sqe-bench/src/generate/{ssb,tpcds,tpcc,tpce,tpcbb,clickbench}.rs`
- Test: one row-count parity test per generator (same file)

**Interfaces:**
- Produces: `generate_batches` on each of the six generators, returning a single shard.

The six generators today build `let (schema, batches) = <build>;` then call `write_parquet_files(&batches, schema, &full_output, table)`. The recipe for each is identical:

1. Find the batch-building expression in `generate_table` (the code that yields `(schema, batches)` before `write_parquet_files`). Extract it into a private helper `fn build_<table_or_all>(table, scale, config) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)>` that BOTH `generate_table` and `generate_batches` call. `generate_table` keeps calling `write_parquet_files` on the result (unchanged behavior).
2. `generate_batches` wraps the `Vec<RecordBatch>` in one shard.

- [ ] **Step 1: ssb — write the parity test**

```rust
#[test]
fn ssb_generate_batches_matches_generate_row_count() {
    use crate::generate::{BenchmarkGenerator, GenerateConfig};
    let g = SsbGenerator;
    let cfg = GenerateConfig::default();
    // pick the smallest ssb table; confirm the name from tables().
    let t = g.tables()[0].name.clone();
    let src = g.generate_batches(&t, 1.0, &cfg).unwrap();
    let rows: usize = src.shards.into_iter()
        .map(|s| (s.make)().map(|b| b.num_rows()).sum::<usize>()).sum();
    assert_eq!(rows, src.total_rows);
    assert!(rows > 0);
}
```

- [ ] **Step 2: ssb — extract the builder and implement generate_batches**

At `ssb.rs:780`, factor the `(schema, batches)` construction out of `generate_table` (before the `write_parquet_files` at `:800`) into `build_ssb_table(table, scale, config) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)>`. Then:

```rust
fn generate_batches(
    &self,
    table: &str,
    scale: f64,
    config: &GenerateConfig,
) -> anyhow::Result<crate::generate::BatchSource> {
    use crate::generate::{BatchShard, BatchSource};
    let (schema, batches) = build_ssb_table(table, scale, config)?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = arrow_array::RecordBatch> + Send> + Send> =
        Box::new(move || Box::new(batches.into_iter()));
    Ok(BatchSource { schema, total_rows, shards: vec![BatchShard { make }] })
}
```

`generate_table` now calls `build_ssb_table` then `write_parquet_files` on the result — same output as before.

- [ ] **Step 3: ssb — run**

Run: `cargo test -p sqe-bench ssb_generate_batches 2>&1 | tail -20`
Expected: PASS. Also `cargo test -p sqe-bench ssb 2>&1 | tail -20` — any existing ssb generation test still passes (builder extraction is behavior-preserving).

- [ ] **Step 4: Repeat Steps 1-3 for tpcds, tpcc, tpce, tpcbb, clickbench**

Apply the identical recipe at each generator's `generate_table` + `write_parquet_files` site:
- tpcds: `generate_table` `:1774`, `write_parquet_files` `:1813` → `build_tpcds_table`.
- tpcc: `:1033` / `:1057` → `build_tpcc_table`.
- tpce: `:2270` / `:2323` → `build_tpce_table`.
- tpcbb: `:358` / `:379` → `build_tpcbb_table`.
- clickbench: `:729` / `:746` → `build_clickbench_table`.

For each, write the parity test (copy the ssb test, rename the generator + `tables()[0]`), extract the builder, implement `generate_batches`, run that generator's tests. Commit per generator or in one batch at the end of this task.

- [ ] **Step 5: Full crate build + test**

Run: `cargo test -p sqe-bench 2>&1 | tail -30`
Expected: all generator parity tests pass; no existing test regresses.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-bench/src/generate/
git commit -m "feat(bench): generate_batches for ssb/tpcds/tpcc/tpce/tpcbb/clickbench (single shard)"
```

---

### Task 6: Wire main.rs dispatch and CLI docs

**Files:**
- Modify: `crates/sqe-bench/src/main.rs`
- Modify: `crates/sqe-bench/src/cli.rs`

**Interfaces:**
- Consumes: `run_direct` (Task 3), `get_generator` (`generate/mod.rs:168`), `IcebergTarget`.

- [ ] **Step 1: Remove the bank-only guard and dispatch non-bank to run_direct**

In `main.rs`, the `if let cli::Sink::Iceberg = sink { ... }` block (`:71-141`) currently starts with `anyhow::ensure!(benchmark == "bank", ...)`. Replace that guard with a branch:

```rust
if let cli::Sink::Iceberg = sink {
    let catalog_uri = catalog_uri
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--sink iceberg needs --catalog-uri"))?;
    let warehouse = warehouse
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--sink iceberg needs --warehouse"))?;
    let target = sink::iceberg::IcebergTarget {
        catalog_uri,
        warehouse,
        namespace: namespace.clone(),
        credential: match (client_id.clone(), client_secret.clone()) {
            (Some(id), Some(secret)) => Some(format!("{id}:{secret}")),
            _ => None,
        },
        oauth2_server_uri: oauth2_server_uri.clone(),
        scope: scope.clone(),
        bearer_token: bearer_token.clone(),
        s3_endpoint: s3_endpoint.clone(),
        s3_access_key: s3_access_key.clone(),
        s3_secret_key: s3_secret_key.clone(),
        s3_region: Some(s3_region.clone()),
        s3_path_style,
    };
    let file_size = sink::plan::parse_size(&target_file_size)? as usize;

    if benchmark == "bank" {
        // existing bank path unchanged (calibration + run_bank) ...
        // (keep the current bank block verbatim here)
    } else {
        let gen = generate::get_generator(&benchmark)?;
        return sink::iceberg::run_direct(
            &target, gen.as_ref(), scale, &config, clean, resume, file_size,
        )
        .await;
    }
}
```

Keep the entire existing bank calibration/`run_bank` code inside the `if benchmark == "bank"` arm (move it, do not delete it). The non-bank arm calls `run_direct`.

- [ ] **Step 2: Update CLI doc strings**

In `cli.rs`, edit the `sink` field doc (`:66-71` area) and drop "Currently only the `bank` benchmark supports `iceberg`." Change to: "`iceberg` writes straight into Iceberg tables through the catalog REST API for any benchmark." Leave the flags themselves unchanged.

- [ ] **Step 3: Build + clippy**

Run: `cargo build -p sqe-bench 2>&1 | tail -20`
Expected: compiles.

Run: `cargo clippy -p sqe-bench --all-targets -- -D warnings 2>&1 | tail -20`
Expected: no warnings.

- [ ] **Step 4: Smoke-test the CLI arg wiring (no stack)**

Run: `cargo run -p sqe-bench -- generate tpch --sink iceberg --scale 0.01 2>&1 | tail -20`
Expected: fails fast with `--sink iceberg needs --catalog-uri` (proves the non-bank branch is reached and validates args before any catalog call).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/main.rs crates/sqe-bench/src/cli.rs
git commit -m "feat(bench): --sink iceberg for all generator benchmarks via run_direct"
```

---

### Task 7: Stack-gated integration test and docs

**Files:**
- Create/Modify: an integration test under `crates/sqe-bench/tests/` (gated behind the stack env, following the existing gated-test pattern — grep `tests/` for how `generate_parallel.rs` or others gate on env)
- Modify: benchmark docs / `README.md` sink section

- [ ] **Step 1: Add a stack-gated integration test**

Follow the crate's existing gated-test convention (env-var guard that skips when Polaris+S3 is absent). The test: run `run_direct` for tpch at a tiny scale against the quickstart catalog, then load the table and assert row counts equal the generator's expected counts; run again with `resume=true` and assert the "skip" path (no new snapshot). Because this needs the stack, guard it so `cargo test` without the stack skips it cleanly.

- [ ] **Step 2: Document the feature**

Update the benchmark docs (grep `grep -rl "sink iceberg\|run_bank\|--sink" docs/ README.md`) to state that `--sink iceberg` now works for all generator benchmarks, with an example:

```bash
sqe-bench generate tpch --scale 10 --sink iceberg \
  --catalog-uri http://localhost:8181/api/catalog \
  --warehouse bench --namespace tpch \
  --client-id ... --client-secret ...
```

Note the per-table resume (`--resume`) and `--clean` behavior. Follow `voice.md` (no emdash/endash/unicode arrows in prose).

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-bench/tests/ docs/ README.md
git commit -m "test+docs(bench): stack-gated direct-sink integration test + docs"
```

---

## Self-Review

**Spec coverage:**
- `generate_batches` on all generators -> Tasks 1, 4, 5. Covered.
- Extract run_bank helpers -> Task 2. Covered.
- Generic driver (producer/channel/writer/commit) -> Task 3. Covered.
- `--sink iceberg` all benchmarks -> Task 6. Covered.
- Per-table resume marker -> Task 3 (skip logic + commit marker). Covered.
- Bank as caller / no behavior change -> Task 2 (helpers) + Task 6 (bank arm kept). Covered.
- Staging path untouched -> Tasks 4-5 keep `generate_table`/`write_parquet_files`. Covered.
- Shard-parallel only for tpch/bank; six serial -> Tasks 4 (shards) vs 5 (single shard). Covered.
- Integration + docs -> Task 7. Covered.

**Placeholder scan:** the six-generator recipe in Task 5 is a concrete extraction with exact file:line targets per generator, not "similar to Task N" — the transformation is genuinely identical and each site is named. The bank arm in Task 6 Step 1 says "keep verbatim"; that is a move, not a placeholder (the code exists at main.rs:82-140).

**Type consistency:** `BatchSource`/`BatchShard` (Task 1) consumed identically in Tasks 3, 4, 5. `ensure_table`/`write_shard`/`commit_files` signatures (Task 2) consumed in Task 3. `run_direct` signature (Task 3) consumed in Task 6.

**Open compile-time decisions surfaced, not hidden:** `Table: Clone` vs `Arc<TableCtx>` (Task 3 Step 2); `config::partition`/`seed_for_table_partition` visibility (Task 4 Step 3). Both are "check and pick" with both branches specified, resolved by the compiler, not placeholders.

**Risk note:** Task 3's buffer-into-Vec-per-shard matches `run_bank` memory behavior but for a very large single-shard table (the six serial generators at high scale) it holds the whole table in memory. v1 accepts this (documented in the spec's scale note); the streaming-`rx`-into-`write_shard` upgrade is called out in Task 3 Step 1 as the follow-up if it bites.
