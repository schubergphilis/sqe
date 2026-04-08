# CoW DELETE + UPDATE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement DELETE FROM and UPDATE SQL statements using Copy-on-Write (CoW) via the RisingWave iceberg-rust fork's `rewrite_files()` transaction API.

**Architecture:** Switch iceberg dependency to risingwavelabs/iceberg-rust fork (rev `1978911ec4`) which provides `Transaction::rewrite_files()`. DELETE/UPDATE use the CoW pattern: scan table → read all data files → filter/transform rows → write new files → atomically commit old-files-removed + new-files-added via a single `rewrite_files` transaction.

**Tech Stack:** Rust, iceberg-rust (RisingWave fork), DataFusion (predicate evaluation), Arrow/Parquet

---

### Task 1: Switch iceberg dependency to RisingWave fork

**Files:**
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Update workspace iceberg dependencies**

Replace the crates.io version pins with git dependencies pointing to the RisingWave fork:

```toml
# Iceberg — RisingWave fork for rewrite_files() / overwrite_files() transaction support
# Upstream apache/iceberg-rust 0.9 lacks OverwriteAction/RewriteFilesAction.
# Pin to known-good rev used by nimtable/iceberg-compaction.
iceberg = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "1978911ec4" }
iceberg-catalog-rest = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "1978911ec4" }
iceberg-storage-opendal = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "1978911ec4" }
iceberg-datafusion = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "1978911ec4" }
```

- [ ] **Step 2: Build to verify compatibility**

Run: `cargo build --all 2>&1 | tail -20`

Expected: Build succeeds. The RisingWave fork is API-compatible with upstream 0.9 for the methods we already use (`Transaction::new`, `fast_append`, `ApplyTransactionAction`). If there are breaking changes, fix import paths.

- [ ] **Step 3: Run tests to verify no regressions**

Run: `cargo test --all 2>&1 | grep -E "test result"`

Expected: All 18 unit tests pass, same as before.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -5`

Expected: Zero warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: switch iceberg deps to RisingWave fork for rewrite_files support

Pin to risingwavelabs/iceberg-rust rev 1978911ec4 (dev_rebase_main_20260303).
This fork adds Transaction::rewrite_files() and overwrite_files() which are
needed for CoW DELETE and UPDATE. Upstream 0.9 only has fast_append.
Same rev used by nimtable/iceberg-compaction in production."
```

---

### Task 2: Add UPDATE to SQL classifier

**Files:**
- Modify: `crates/sqe-sql/src/classifier.rs`

- [ ] **Step 1: Add Update variant to StatementKind enum**

In `classifier.rs`, add `Update(Box<Statement>)` to the `StatementKind` enum between `Delete` and `Drop`:

```rust
pub enum StatementKind {
    Query(Box<Statement>),
    CreateTable(Box<Statement>),
    Ctas(Box<Statement>),
    Insert(Box<Statement>),
    Merge(Box<Statement>),
    Delete(Box<Statement>),
    Update(Box<Statement>),  // NEW
    Drop(Box<Statement>),
    // ... rest unchanged
}
```

Add the label in `StatementKind::label()`:

```rust
StatementKind::Update(_) => "update",
```

- [ ] **Step 2: Route Statement::Update to StatementKind::Update**

Change line 217 from:

```rust
Statement::Update { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
```

to:

```rust
Statement::Update { .. } => Ok(StatementKind::Update(Box::new(stmt))),
```

- [ ] **Step 3: Add classifier test**

Add after the existing DELETE test:

```rust
#[test]
fn classify_update() {
    let result = parse_and_classify("UPDATE ns.t SET col1 = 1 WHERE id = 5").unwrap();
    assert!(matches!(result, StatementKind::Update(_)));
}

#[test]
fn classify_update_label() {
    let kind = StatementKind::Update(Box::new(
        Parser::parse_sql(&GenericDialect {}, "UPDATE t SET x = 1").unwrap().remove(0),
    ));
    assert_eq!(kind.label(), "update");
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-sql 2>&1 | tail -5`

Expected: All tests pass including the new ones.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-sql/src/classifier.rs
git commit -m "feat(sql): classify UPDATE as its own StatementKind

Previously UPDATE was routed to Utility (unsupported). Now it gets its own
variant for dispatch to the write handler."
```

---

### Task 3: Implement handle_delete in write_handler.rs

**Files:**
- Modify: `crates/sqe-coordinator/src/write_handler.rs`

This is the core CoW DELETE implementation. The pattern:
1. Parse DELETE statement to extract table name + WHERE predicate
2. Load table from catalog
3. Scan all data files from current snapshot via `table.scan().plan_files()`
4. For each file: read Parquet, filter out rows matching predicate, write surviving rows to new file
5. Commit via `rewrite_files()`: remove old files, add new files

- [ ] **Step 1: Add imports**

At the top of `write_handler.rs`, add:

```rust
use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use datafusion::prelude::SessionContext as DFSessionContext;
use futures::TryStreamExt;
use iceberg::spec::DataFile;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use url::Url;
```

- [ ] **Step 2: Implement handle_delete**

Add this method to the `impl WriteHandler` block:

```rust
/// Handle DELETE FROM ns.table [WHERE ...]
///
/// Uses Copy-on-Write: reads all data files, filters out rows matching
/// the WHERE predicate, writes new files with surviving rows, and
/// atomically swaps via rewrite_files().
///
/// Without a WHERE clause, this is a truncate: commits an empty snapshot
/// that removes all data files.
#[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
pub async fn handle_delete(
    &self,
    session: &Session,
    stmt: &Statement,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let delete = match stmt {
        Statement::Delete(d) => d,
        other => {
            return Err(SqeError::Execution(format!(
                "Expected DELETE statement, got: {other}"
            )));
        }
    };

    let table_ref = &delete.from.relations[0];
    let table_name = match table_ref {
        sqlparser::ast::FromTable::WithFromKeyword(tables) => {
            &tables[0].relation
        }
        sqlparser::ast::FromTable::WithoutKeyword(tables) => {
            &tables[0].relation
        }
    };
    let table_factor_name = match table_name {
        sqlparser::ast::TableFactor::Table { name, .. } => name,
        other => {
            return Err(SqeError::Execution(format!(
                "Expected table name in DELETE, got: {other}"
            )));
        }
    };

    let (namespace, name) = parse_table_ref(table_factor_name)?;
    let table_ident = TableIdent::new(namespace, name);

    let catalog = self.create_catalog_bridge(session).await?;
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

    // Get all data files from current snapshot
    let scan = table.scan().build().map_err(|e| {
        SqeError::Execution(format!("Failed to build table scan: {e}"))
    })?;
    let file_scan_tasks: Vec<_> = scan.plan_files().await
        .map_err(|e| SqeError::Execution(format!("Failed to plan files: {e}")))?
        .try_collect().await
        .map_err(|e| SqeError::Execution(format!("Failed to collect file scan tasks: {e}")))?;

    if file_scan_tasks.is_empty() {
        info!(table = %table_ident, "DELETE: table has no data files, nothing to delete");
        return Ok(vec![]);
    }

    let where_clause = &delete.selection;

    // No WHERE = truncate: remove all files, add none
    if where_clause.is_none() {
        info!(table = %table_ident, file_count = file_scan_tasks.len(), "DELETE: truncating table (no WHERE clause)");
        let old_data_files: Vec<DataFile> = file_scan_tasks
            .iter()
            .map(|t| t.data_file().clone())
            .collect();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files()
            .delete_files(old_data_files);
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply truncate transaction: {e}"))
        })?;
        tx.commit(catalog.as_ref()).await.map_err(|e| {
            SqeError::Execution(format!("Failed to commit truncate: {e}"))
        })?;
        info!(table = %table_ident, "DELETE: table truncated successfully");
        return Ok(vec![]);
    }

    // WHERE clause present: CoW rewrite
    let where_sql = format!("{}", where_clause.as_ref().unwrap());
    info!(
        table = %table_ident,
        file_count = file_scan_tasks.len(),
        where_clause = %where_sql,
        "DELETE: CoW rewrite"
    );

    let old_data_files: Vec<DataFile> = file_scan_tasks
        .iter()
        .map(|t| t.data_file().clone())
        .collect();

    // Build S3 object store for reading existing files
    let store = self.build_object_store()?;

    // For predicate evaluation, use a DataFusion session
    let df_ctx = DFSessionContext::new();

    let mut new_data_files = Vec::new();
    let mut total_deleted = 0usize;

    for task in &file_scan_tasks {
        let file_path = task.data_file_path();
        let batches = self.read_parquet_file(&store, file_path).await?;

        if batches.is_empty() {
            continue;
        }

        // Evaluate WHERE predicate against each batch, keep rows that do NOT match
        let mut surviving_batches = Vec::new();
        for batch in &batches {
            let filtered = self.filter_batch_negate(
                &df_ctx, batch, &where_sql, &table_ident,
            ).await?;
            total_deleted += batch.num_rows() - filtered.num_rows();
            if filtered.num_rows() > 0 {
                surviving_batches.push(filtered);
            }
        }

        // Write surviving rows as new data files (skip if all rows deleted)
        if !surviving_batches.is_empty() {
            let new_files = write_data_files(&table, surviving_batches, "delete").await?;
            new_data_files.extend(new_files);
        }
    }

    info!(
        table = %table_ident,
        deleted_rows = total_deleted,
        old_files = old_data_files.len(),
        new_files = new_data_files.len(),
        "DELETE: committing CoW rewrite"
    );

    // Atomic commit: remove old files, add new files
    let tx = Transaction::new(&table);
    let action = tx.rewrite_files()
        .add_data_files(new_data_files)
        .delete_files(old_data_files);
    let tx = action.apply(tx).map_err(|e| {
        SqeError::Execution(format!("Failed to apply DELETE rewrite: {e}"))
    })?;
    tx.commit(catalog.as_ref()).await.map_err(|e| {
        SqeError::Execution(format!("Failed to commit DELETE: {e}"))
    })?;

    info!(table = %table_ident, deleted_rows = total_deleted, "DELETE committed successfully");
    Ok(vec![])
}
```

- [ ] **Step 3: Add helper methods**

Add these private helper methods to `impl WriteHandler`:

```rust
/// Build an S3 ObjectStore for reading existing Parquet files.
fn build_object_store(&self) -> sqe_core::Result<Arc<dyn ObjectStore>> {
    let storage = &self.config.storage;
    let mut builder = AmazonS3Builder::new();
    if !storage.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(&storage.s3_endpoint);
    }
    if !storage.s3_region.is_empty() {
        builder = builder.with_region(&storage.s3_region);
    }
    if !storage.s3_access_key.is_empty() {
        builder = builder.with_access_key_id(&storage.s3_access_key);
    }
    if !storage.s3_secret_key.is_empty() {
        builder = builder.with_secret_access_key(&storage.s3_secret_key);
    }
    if storage.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    builder = builder.with_allow_http(storage.s3_allow_http);
    // Bucket extracted from first file path during read — use a placeholder
    builder = builder.with_bucket_name("placeholder");
    Ok(Arc::new(builder.build().map_err(|e| {
        SqeError::Execution(format!("Failed to build S3 object store: {e}"))
    })?))
}

/// Read all RecordBatches from a Parquet file at the given S3 path.
async fn read_parquet_file(
    &self,
    store: &Arc<dyn ObjectStore>,
    file_path: &str,
) -> sqe_core::Result<Vec<RecordBatch>> {
    // Rebuild store with correct bucket from the file path
    let parsed = Url::parse(file_path).map_err(|e| {
        SqeError::Execution(format!("Invalid file path URL: {e}"))
    })?;
    let bucket = parsed.host_str().unwrap_or("warehouse");
    let key = parsed.path().trim_start_matches('/');

    let storage = &self.config.storage;
    let mut builder = AmazonS3Builder::new();
    if !storage.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(&storage.s3_endpoint);
    }
    if !storage.s3_region.is_empty() {
        builder = builder.with_region(&storage.s3_region);
    }
    if !storage.s3_access_key.is_empty() {
        builder = builder.with_access_key_id(&storage.s3_access_key);
    }
    if !storage.s3_secret_key.is_empty() {
        builder = builder.with_secret_access_key(&storage.s3_secret_key);
    }
    if storage.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    builder = builder.with_allow_http(storage.s3_allow_http);
    builder = builder.with_bucket_name(bucket);
    let store: Arc<dyn ObjectStore> = Arc::new(builder.build().map_err(|e| {
        SqeError::Execution(format!("Failed to build S3 object store for bucket '{bucket}': {e}"))
    })?);

    let path = object_store::path::Path::from(key);
    let meta = store.head(&path).await.map_err(|e| {
        SqeError::Execution(format!("Failed to read file metadata for '{file_path}': {e}"))
    })?;

    let reader = ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.map_err(|e| {
        SqeError::Execution(format!("Failed to open Parquet reader for '{file_path}': {e}"))
    })?;
    let stream = builder.build().map_err(|e| {
        SqeError::Execution(format!("Failed to build Parquet stream for '{file_path}': {e}"))
    })?;

    let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| {
        SqeError::Execution(format!("Failed to read Parquet file '{file_path}': {e}"))
    })?;
    Ok(batches)
}

/// Evaluate a WHERE clause against a RecordBatch and return rows that do NOT match.
/// Used for DELETE: we keep the rows that don't match the WHERE predicate.
async fn filter_batch_negate(
    &self,
    ctx: &DFSessionContext,
    batch: &RecordBatch,
    where_sql: &str,
    table_ident: &TableIdent,
) -> sqe_core::Result<RecordBatch> {
    use arrow::compute::not;
    use datafusion::arrow::array::BooleanArray;

    // Register the batch as a temporary table so DataFusion can evaluate the predicate
    let table_name = format!("__delete_{}", table_ident.name());
    let mem_table = datafusion::datasource::MemTable::try_new(
        batch.schema(),
        vec![vec![batch.clone()]],
    ).map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
    ctx.register_table(&table_name, Arc::new(mem_table))
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

    // Execute: SELECT <where_clause> AS __match FROM __delete_<table>
    let eval_sql = format!("SELECT CAST(({where_sql}) AS BOOLEAN) AS __match FROM {table_name}");
    let df = ctx.sql(&eval_sql).await.map_err(|e| {
        SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}"))
    })?;
    let result_batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
        SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}"))
    })?;

    // Deregister temp table
    let _ = ctx.deregister_table(&table_name);

    // Build a boolean mask: NOT <predicate> (rows to keep)
    let mask_batch = &result_batches[0];
    let match_col = mask_batch.column(0).as_boolean();
    let negated = not(match_col).map_err(|e| {
        SqeError::Execution(format!("Failed to negate WHERE mask: {e}"))
    })?;

    // Apply the mask to the original batch
    filter_record_batch(batch, &negated).map_err(|e| {
        SqeError::Execution(format!("Failed to filter batch: {e}"))
    })
}
```

- [ ] **Step 4: Add required dependencies to sqe-coordinator/Cargo.toml**

Add `object_store`, `url` if not already present (they should be from existing workspace deps).

- [ ] **Step 5: Build to verify compilation**

Run: `cargo build -p sqe-coordinator 2>&1 | tail -10`

Expected: Compiles successfully.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/write_handler.rs crates/sqe-coordinator/Cargo.toml
git commit -m "feat: implement DELETE FROM via CoW rewrite_files

Copy-on-Write DELETE: reads all data files, filters out matching rows,
writes new files, atomically swaps via rewrite_files() transaction.
DELETE without WHERE = truncate (removes all data files).
Uses DataFusion for predicate evaluation against Arrow RecordBatches."
```

---

### Task 4: Implement handle_update in write_handler.rs

**Files:**
- Modify: `crates/sqe-coordinator/src/write_handler.rs`

UPDATE is similar to DELETE but transforms matching rows instead of removing them.

- [ ] **Step 1: Implement handle_update**

Add this method to `impl WriteHandler`:

```rust
/// Handle UPDATE ns.table SET col = expr [WHERE ...]
///
/// Uses Copy-on-Write: reads all data files, applies SET assignments to
/// rows matching WHERE, writes new files, atomically swaps.
#[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
pub async fn handle_update(
    &self,
    session: &Session,
    stmt: &Statement,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let update = match stmt {
        Statement::Update { table, assignments, selection, .. } => {
            (table, assignments, selection)
        }
        other => {
            return Err(SqeError::Execution(format!(
                "Expected UPDATE statement, got: {other}"
            )));
        }
    };
    let (table_factor, assignments, selection) = update;

    let table_name = match &table_factor.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => name,
        other => {
            return Err(SqeError::Execution(format!(
                "Expected table name in UPDATE, got: {other}"
            )));
        }
    };

    let (namespace, name) = parse_table_ref(table_name)?;
    let table_ident = TableIdent::new(namespace, name);

    let catalog = self.create_catalog_bridge(session).await?;
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

    // Get all data files
    let scan = table.scan().build().map_err(|e| {
        SqeError::Execution(format!("Failed to build table scan: {e}"))
    })?;
    let file_scan_tasks: Vec<_> = scan.plan_files().await
        .map_err(|e| SqeError::Execution(format!("Failed to plan files: {e}")))?
        .try_collect().await
        .map_err(|e| SqeError::Execution(format!("Failed to collect file scan tasks: {e}")))?;

    if file_scan_tasks.is_empty() {
        info!(table = %table_ident, "UPDATE: table has no data files");
        return Ok(vec![]);
    }

    // Build the SET clause as SQL CASE expressions for a SELECT rewrite
    // UPDATE t SET col1 = expr1, col2 = expr2 WHERE cond
    // becomes:
    // SELECT CASE WHEN cond THEN expr1 ELSE col1 END AS col1,
    //        CASE WHEN cond THEN expr2 ELSE col2 END AS col2,
    //        col3, col4, ...  (unchanged columns)
    // FROM t
    let where_sql = selection
        .as_ref()
        .map(|w| format!("{w}"))
        .unwrap_or_else(|| "TRUE".to_string());

    let old_data_files: Vec<DataFile> = file_scan_tasks
        .iter()
        .map(|t| t.data_file().clone())
        .collect();

    info!(
        table = %table_ident,
        file_count = old_data_files.len(),
        assignments = assignments.len(),
        where_clause = %where_sql,
        "UPDATE: CoW rewrite"
    );

    let df_ctx = DFSessionContext::new();
    let mut new_data_files = Vec::new();
    let mut total_updated = 0usize;

    for task in &file_scan_tasks {
        let file_path = task.data_file_path();
        let batches = self.read_parquet_file(
            &self.build_object_store()?,
            file_path,
        ).await?;

        if batches.is_empty() {
            continue;
        }

        let schema = batches[0].schema();
        let mut rewritten_batches = Vec::new();

        for batch in &batches {
            let rewritten = self.apply_update(
                &df_ctx, batch, assignments, &where_sql, &table_ident,
            ).await?;
            rewritten_batches.push(rewritten);
        }

        // Count updated rows by comparing before/after
        // (approximation: count rows matching the WHERE)
        for batch in &batches {
            let count = self.count_matching_rows(&df_ctx, batch, &where_sql, &table_ident).await?;
            total_updated += count;
        }

        let new_files = write_data_files(&table, rewritten_batches, "update").await?;
        new_data_files.extend(new_files);
    }

    info!(
        table = %table_ident,
        updated_rows = total_updated,
        old_files = old_data_files.len(),
        new_files = new_data_files.len(),
        "UPDATE: committing CoW rewrite"
    );

    let tx = Transaction::new(&table);
    let action = tx.rewrite_files()
        .add_data_files(new_data_files)
        .delete_files(old_data_files);
    let tx = action.apply(tx).map_err(|e| {
        SqeError::Execution(format!("Failed to apply UPDATE rewrite: {e}"))
    })?;
    tx.commit(catalog.as_ref()).await.map_err(|e| {
        SqeError::Execution(format!("Failed to commit UPDATE: {e}"))
    })?;

    info!(table = %table_ident, updated_rows = total_updated, "UPDATE committed successfully");
    Ok(vec![])
}
```

- [ ] **Step 2: Add apply_update and count_matching_rows helpers**

```rust
/// Apply UPDATE SET assignments to a RecordBatch using DataFusion SQL evaluation.
///
/// For each column, generates CASE WHEN <where> THEN <new_value> ELSE <old_value> END.
/// Unchanged columns pass through directly.
async fn apply_update(
    &self,
    ctx: &DFSessionContext,
    batch: &RecordBatch,
    assignments: &[sqlparser::ast::Assignment],
    where_sql: &str,
    table_ident: &TableIdent,
) -> sqe_core::Result<RecordBatch> {
    let table_name = format!("__update_{}", table_ident.name());
    let mem_table = datafusion::datasource::MemTable::try_new(
        batch.schema(),
        vec![vec![batch.clone()]],
    ).map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
    ctx.register_table(&table_name, Arc::new(mem_table))
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

    // Build assignment map: column_name -> expression_sql
    let mut assignment_map = std::collections::HashMap::new();
    for a in assignments {
        let col_name = a.target.iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(".");
        let expr_sql = format!("{}", a.value);
        assignment_map.insert(col_name, expr_sql);
    }

    // Build SELECT with CASE expressions for assigned columns
    let columns: Vec<String> = batch.schema().fields().iter().map(|f| {
        let col = f.name().clone();
        if let Some(expr) = assignment_map.get(&col) {
            format!("CASE WHEN ({where_sql}) THEN ({expr}) ELSE \"{col}\" END AS \"{col}\"")
        } else {
            format!("\"{col}\"")
        }
    }).collect();

    let select_sql = format!("SELECT {} FROM {table_name}", columns.join(", "));
    let df = ctx.sql(&select_sql).await.map_err(|e| {
        SqeError::Execution(format!("Failed to evaluate UPDATE: {e}"))
    })?;
    let result_batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
        SqeError::Execution(format!("Failed to collect UPDATE results: {e}"))
    })?;

    let _ = ctx.deregister_table(&table_name);

    // Return the first (and only) result batch
    result_batches.into_iter().next().ok_or_else(|| {
        SqeError::Execution("UPDATE produced no output batches".to_string())
    })
}

/// Count rows matching a WHERE clause in a batch.
async fn count_matching_rows(
    &self,
    ctx: &DFSessionContext,
    batch: &RecordBatch,
    where_sql: &str,
    table_ident: &TableIdent,
) -> sqe_core::Result<usize> {
    let table_name = format!("__count_{}", table_ident.name());
    let mem_table = datafusion::datasource::MemTable::try_new(
        batch.schema(),
        vec![vec![batch.clone()]],
    ).map_err(|e| SqeError::Execution(format!("MemTable error: {e}")))?;
    ctx.register_table(&table_name, Arc::new(mem_table))
        .map_err(|e| SqeError::Execution(format!("Register error: {e}")))?;

    let sql = format!("SELECT COUNT(*) AS cnt FROM {table_name} WHERE {where_sql}");
    let df = ctx.sql(&sql).await.map_err(|e| {
        SqeError::Execution(format!("Count query failed: {e}"))
    })?;
    let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
        SqeError::Execution(format!("Count collect failed: {e}"))
    })?;

    let _ = ctx.deregister_table(&table_name);

    let count = batches.first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<arrow_array::Int64Array>())
        .map(|a| a.value(0) as usize)
        .unwrap_or(0);
    Ok(count)
}
```

- [ ] **Step 3: Build to verify**

Run: `cargo build -p sqe-coordinator 2>&1 | tail -10`

Expected: Compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/write_handler.rs
git commit -m "feat: implement UPDATE via CoW rewrite_files

Copy-on-Write UPDATE: reads all data files, applies SET assignments via
CASE WHEN expressions evaluated by DataFusion, writes new files,
atomically swaps. UPDATE without WHERE applies to all rows."
```

---

### Task 5: Wire DELETE and UPDATE into query_handler.rs

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Replace the NotImplemented stubs with handler calls**

Replace lines 291-296:

```rust
StatementKind::Delete(_) => Err(SqeError::NotImplemented(
    "DELETE FROM requires Iceberg overwrite transaction support (planned for Chunk 3)".to_string(),
)),
StatementKind::Merge(_) => Err(SqeError::NotImplemented(
    "MERGE INTO requires Iceberg overwrite transaction support (planned for Chunk 3)".to_string(),
)),
```

With:

```rust
StatementKind::Delete(stmt) => {
    self.write_handler.handle_delete(session, stmt).await
}

StatementKind::Update(stmt) => {
    self.write_handler.handle_update(session, stmt).await
}

StatementKind::Merge(_) => Err(SqeError::NotImplemented(
    "MERGE INTO is not yet supported".to_string(),
)),
```

- [ ] **Step 2: Build and run unit tests**

Run: `cargo build --all && cargo test --all 2>&1 | grep "test result"`

Expected: All tests pass.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -5`

Expected: Zero warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat: wire DELETE and UPDATE into query handler dispatch

DELETE and UPDATE now route to WriteHandler instead of returning
NotImplemented. MERGE INTO remains unimplemented."
```

---

### Task 6: Integration test — DELETE and UPDATE

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`

- [ ] **Step 1: Add DELETE integration test**

Add to the integration test file:

```rust
#[tokio::test]
#[ignore] // Requires running test stack
async fn test_delete_with_where() {
    let ctx = TestContext::new().await;

    // Create and populate a test table
    ctx.execute("CREATE TABLE test_ns.delete_test AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val)").await.unwrap();

    // Delete one row
    ctx.execute("DELETE FROM test_ns.delete_test WHERE id = 2").await.unwrap();

    // Verify
    let result = ctx.execute("SELECT id, val FROM test_ns.delete_test ORDER BY id").await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);
    // Should have rows (1,'a') and (3,'c') only

    // Cleanup
    let _ = ctx.execute("DROP TABLE test_ns.delete_test").await;
}

#[tokio::test]
#[ignore]
async fn test_delete_truncate() {
    let ctx = TestContext::new().await;
    ctx.execute("CREATE TABLE test_ns.trunc_test AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await.unwrap();
    ctx.execute("DELETE FROM test_ns.trunc_test").await.unwrap();

    let result = ctx.execute("SELECT COUNT(*) AS cnt FROM test_ns.trunc_test").await.unwrap();
    let batch = &result[0];
    let cnt = batch.column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert_eq!(cnt, 0);

    let _ = ctx.execute("DROP TABLE test_ns.trunc_test").await;
}
```

- [ ] **Step 2: Add UPDATE integration test**

```rust
#[tokio::test]
#[ignore]
async fn test_update_with_where() {
    let ctx = TestContext::new().await;
    ctx.execute("CREATE TABLE test_ns.update_test AS SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(id, val)").await.unwrap();

    ctx.execute("UPDATE test_ns.update_test SET val = 99 WHERE id = 2").await.unwrap();

    let result = ctx.execute("SELECT id, val FROM test_ns.update_test ORDER BY id").await.unwrap();
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);
    // Row 2 should now have val=99

    let _ = ctx.execute("DROP TABLE test_ns.update_test").await;
}

#[tokio::test]
#[ignore]
async fn test_update_all_rows() {
    let ctx = TestContext::new().await;
    ctx.execute("CREATE TABLE test_ns.update_all AS SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, val)").await.unwrap();

    ctx.execute("UPDATE test_ns.update_all SET val = val + 100").await.unwrap();

    let result = ctx.execute("SELECT id, val FROM test_ns.update_all ORDER BY id").await.unwrap();
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);
    // Both vals should be +100

    let _ = ctx.execute("DROP TABLE test_ns.update_all").await;
}
```

- [ ] **Step 3: Run integration tests**

Run: `./scripts/integration-test.sh`

Expected: All existing tests still pass, plus the new DELETE/UPDATE tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/integration_test.rs
git commit -m "test: add integration tests for DELETE and UPDATE

Tests DELETE with WHERE, DELETE without WHERE (truncate),
UPDATE with WHERE, and UPDATE without WHERE (all rows)."
```

---

### Task 7: Enable benchmark queries and run full suite

**Files:**
- Modify: Benchmark query files that currently require `update`, `delete`, `insert`

- [ ] **Step 1: Remove `requires` tags from benchmark queries that only need delete/update**

For each file, remove the `-- requires:` line (or remove `delete`/`update` from it, keeping `insert` if present for queries that need standalone INSERT which we already support):

```bash
# Files to edit — remove the requires line entirely:
# benchmarks/queries/tpcc/payment.sql     (requires: update)
# benchmarks/queries/tpcc/delivery.sql    (requires: delete, update)
# benchmarks/queries/tpce/data_maintenance.sql (requires: update, delete)
# benchmarks/queries/tpce/market_feed.sql (requires: update)
# benchmarks/queries/tpce/trade_update.sql (requires: update)
#
# Files where insert is already supported — remove requires:
# benchmarks/queries/tpcc/new_order.sql   (requires: insert, update)
# benchmarks/queries/tpce/trade_order.sql (requires: insert)
# benchmarks/queries/tpce/trade_result.sql (requires: update, insert)
```

- [ ] **Step 2: Run full benchmark suite**

Run: `BENCH_SCALE=0.01 ./scripts/benchmark-test.sh 2>&1 | grep -E '(Benchmark:|Results:)'`

Expected: Previously skipped queries now execute (some may error on SQL syntax details — fix iteratively).

- [ ] **Step 3: Fix any SQL compatibility issues**

Review benchmark query errors and fix as needed. Common issues:
- Column name quoting differences
- Type casting in UPDATE SET expressions
- Multi-table DELETE syntax

- [ ] **Step 4: Commit results**

```bash
git add benchmarks/queries/
git commit -m "feat: enable DELETE/UPDATE benchmark queries

Remove requires: tags from TPC-C and TPC-E queries that needed
delete/update support, now that CoW write path is implemented."
```

---

### Task 8: Update project tracking files

**Files:**
- Modify: `nextsteps.md`
- Modify: `README.md`

- [ ] **Step 1: Update nextsteps.md**

Update the status line and roadmap to reflect DELETE/UPDATE support.

- [ ] **Step 2: Update README.md features**

Add DELETE and UPDATE to the DDL/DML feature list.

- [ ] **Step 3: Commit**

```bash
git add nextsteps.md README.md
git commit -m "docs: update roadmap for DELETE/UPDATE support"
```
