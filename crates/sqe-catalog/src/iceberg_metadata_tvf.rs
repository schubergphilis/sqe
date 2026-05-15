//! Table-valued functions for querying Iceberg table metadata.
//!
//! Registers six TVFs on the [`SessionContext`]:
//!
//! ```sql
//! SELECT * FROM table_snapshots('namespace', 'table_name');
//! SELECT * FROM table_manifests('namespace', 'table_name');
//! SELECT * FROM table_history('namespace', 'table_name');
//! SELECT * FROM table_files('namespace', 'table_name');
//! SELECT * FROM table_partitions('namespace', 'table_name');
//! SELECT * FROM table_refs('namespace', 'table_name');
//! ```
//!
//! Each function is implemented as [`TableFunctionImpl`] — the same pattern
//! as [`crate::read_parquet::ReadParquetFunction`].
//!
//! ## Removing block_in_place (issue #19)
//!
//! Earlier versions bridged DataFusion's sync `TableFunctionImpl::call` into
//! the async `catalog.load_table()` via `tokio::task::block_in_place` +
//! `Handle::current().block_on(...)`. `block_in_place` requires the multi-
//! threaded runtime and forces the worker thread off the scheduler. Under
//! concurrent metadata-heavy workloads (dbt macros probing `table_files`
//! across many models) each TVF call took a scheduler worker out of rotation
//! for the duration of the Polaris load.
//!
//! This version defers the load and the RecordBatch construction to
//! [`TableProvider::scan`], which is already async. The TVF dispatch path is
//! purely sync: `call()` constructs an [`IcebergMetadataProvider`] capturing
//! `(catalog, ident, kind)` and returns immediately. The async work happens
//! later, on the executor's natural async surface.
//!
//! ## Time travel
//!
//! The RisingWave iceberg-rust fork (pinned to DataFusion 52.x) does not
//! expose `TableScanBuilder::snapshot_id()`. Time-travel scanning is therefore
//! not implemented here and is documented as blocked on the upstream fork.
//! Track progress at: <https://github.com/risingwavelabs/iceberg-rust>

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent};
use tracing::warn;

use crate::rest_catalog::SessionCatalog;

// ─────────────────────────────────────────────────────────────────────────────
// Lazy provider — async load deferred to scan() so the TVF dispatch is sync.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum MetadataKind {
    Snapshots,
    Manifests,
    History,
    Files,
    Partitions,
    Refs,
}

impl MetadataKind {
    fn fn_name(self) -> &'static str {
        match self {
            Self::Snapshots => "table_snapshots",
            Self::Manifests => "table_manifests",
            Self::History => "table_history",
            Self::Files => "table_files",
            Self::Partitions => "table_partitions",
            Self::Refs => "table_refs",
        }
    }
}

/// TableProvider that defers the Iceberg catalog load + metadata RecordBatch
/// construction to `scan()` — which is already async — so the TVF
/// `call()` site never has to bridge sync-to-async with `block_in_place`.
/// Issue #19.
#[derive(Debug)]
struct IcebergMetadataProvider {
    schema: SchemaRef,
    catalog: Arc<SessionCatalog>,
    ident: TableIdent,
    kind: MetadataKind,
}

#[async_trait]
impl TableProvider for IcebergMetadataProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let fn_name = self.kind.fn_name();
        let table = self.catalog.load_table(&self.ident).await.map_err(|e| {
            DataFusionError::Plan(format!(
                "{fn_name}: failed to load table '{}.{}': {e}",
                self.ident.namespace().as_ref().join("."),
                self.ident.name()
            ))
        })?;

        let batch = match self.kind {
            MetadataKind::Snapshots => build_snapshots_batch(&table, &self.schema)?,
            MetadataKind::Manifests => build_manifests_batch(&table, &self.schema).await?,
            MetadataKind::History => build_history_batch(&table, &self.schema)?,
            MetadataKind::Files => build_files_batch(&table, &self.schema).await?,
            MetadataKind::Partitions => build_partitions_batch(&table, &self.schema).await?,
            MetadataKind::Refs => build_refs_batch(&table, &self.schema)?,
        };

        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

fn ident_for(namespace: &str, table_name: &str) -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared argument parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Parse `(namespace, table_name)` positional string literals from a TVF call.
fn parse_two_string_args(fn_name: &str, exprs: &[Expr]) -> DFResult<(String, String)> {
    let extract = |pos: usize, label: &str| -> DFResult<String> {
        match exprs.get(pos) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _))
            | Some(Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)) => Ok(s.clone()),
            Some(_) => plan_err!("{fn_name}: argument {pos} ({label}) must be a non-null string literal"),
            None => plan_err!("{fn_name}: requires exactly 2 arguments (namespace, table_name); argument {pos} ({label}) is missing"),
        }
    };
    if exprs.len() != 2 {
        return plan_err!(
            "{fn_name}: requires exactly 2 arguments (namespace, table_name), got {}",
            exprs.len()
        );
    }
    Ok((extract(0, "namespace")?, extract(1, "table_name")?))
}

// ─────────────────────────────────────────────────────────────────────────────
// table_snapshots
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for `table_snapshots()` output.
fn snapshots_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_snapshot_id", DataType::Int64, true),
        Field::new("sequence_number", DataType::Int64, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("manifest_list", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, false),
        Field::new("is_current_snapshot", DataType::Boolean, false),
    ]))
}

/// DataFusion TVF: `table_snapshots('namespace', 'table_name')`
///
/// Returns one row per Iceberg snapshot for the given table.
#[derive(Debug)]
pub struct TableSnapshotsFunction {
    session_catalog: Arc<SessionCatalog>,
}

impl TableSnapshotsFunction {
    pub fn new(session_catalog: Arc<SessionCatalog>) -> Self {
        Self { session_catalog }
    }
}

impl TableFunctionImpl for TableSnapshotsFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (namespace, table_name) = parse_two_string_args("table_snapshots", exprs)?;
        Ok(Arc::new(IcebergMetadataProvider {
            schema: snapshots_schema(),
            catalog: Arc::clone(&self.session_catalog),
            ident: ident_for(&namespace, &table_name),
            kind: MetadataKind::Snapshots,
        }))
    }
}

fn build_snapshots_batch(table: &Table, schema: &SchemaRef) -> DFResult<RecordBatch> {
    let metadata = table.metadata();
    let current_snapshot_id = metadata.current_snapshot_id();

    let mut snapshot_id_b = Int64Builder::new();
    let mut parent_id_b = Int64Builder::new();
    let mut sequence_b = Int64Builder::new();
    let mut timestamp_ms_b = Int64Builder::new();
    let mut operation_b = StringBuilder::new();
    let mut manifest_list_b = StringBuilder::new();
    let mut summary_b = StringBuilder::new();
    let mut is_current_b = arrow_array::builder::BooleanBuilder::new();

    for snap in metadata.snapshots() {
        let sid = snap.snapshot_id();
        snapshot_id_b.append_value(sid);

        match snap.parent_snapshot_id() {
            Some(pid) => parent_id_b.append_value(pid),
            None => parent_id_b.append_null(),
        }

        sequence_b.append_value(snap.sequence_number());
        timestamp_ms_b.append_value(snap.timestamp_ms());
        operation_b.append_value(snap.summary().operation.as_str());
        manifest_list_b.append_value(snap.manifest_list());

        // Serialize additional_properties as a compact JSON object
        let extra = &snap.summary().additional_properties;
        if extra.is_empty() {
            summary_b.append_value("{}");
        } else {
            // Build a simple JSON string without pulling in serde_json as a new dep —
            // the serde_json crate is already transitively available via iceberg-rust.
            let pairs: Vec<String> = extra
                .iter()
                .map(|(k, v)| format!("\"{}\":\"{}\"", k.replace('"', "\\\""), v.replace('"', "\\\"")))
                .collect();
            summary_b.append_value(format!("{{{}}}", pairs.join(",")));
        }

        is_current_b.append_value(current_snapshot_id == Some(sid));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(snapshot_id_b.finish()) as ArrayRef,
            Arc::new(parent_id_b.finish()) as ArrayRef,
            Arc::new(sequence_b.finish()) as ArrayRef,
            Arc::new(timestamp_ms_b.finish()) as ArrayRef,
            Arc::new(operation_b.finish()) as ArrayRef,
            Arc::new(manifest_list_b.finish()) as ArrayRef,
            Arc::new(summary_b.finish()) as ArrayRef,
            Arc::new(is_current_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// table_manifests
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for `table_manifests()` output.
fn manifests_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("manifest_path", DataType::Utf8, false),
        Field::new("manifest_length", DataType::Int64, false),
        Field::new("partition_spec_id", DataType::Int64, false),
        Field::new("added_snapshot_id", DataType::Int64, false),
        Field::new("added_data_files_count", DataType::Int64, true),
        Field::new("existing_data_files_count", DataType::Int64, true),
        Field::new("deleted_data_files_count", DataType::Int64, true),
        Field::new("added_rows_count", DataType::Int64, true),
        Field::new("existing_rows_count", DataType::Int64, true),
        Field::new("deleted_rows_count", DataType::Int64, true),
    ]))
}

/// DataFusion TVF: `table_manifests('namespace', 'table_name')`
///
/// Returns one row per manifest file in the current snapshot's manifest list.
/// If the table has no current snapshot the result is empty.
#[derive(Debug)]
pub struct TableManifestsFunction {
    session_catalog: Arc<SessionCatalog>,
}

impl TableManifestsFunction {
    pub fn new(session_catalog: Arc<SessionCatalog>) -> Self {
        Self { session_catalog }
    }
}

impl TableFunctionImpl for TableManifestsFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (namespace, table_name) = parse_two_string_args("table_manifests", exprs)?;
        Ok(Arc::new(IcebergMetadataProvider {
            schema: manifests_schema(),
            catalog: Arc::clone(&self.session_catalog),
            ident: ident_for(&namespace, &table_name),
            kind: MetadataKind::Manifests,
        }))
    }
}

async fn build_manifests_batch(table: &Table, schema: &SchemaRef) -> DFResult<RecordBatch> {
    let metadata = table.metadata();

    let mut manifest_path_b = StringBuilder::new();
    let mut manifest_length_b = Int64Builder::new();
    let mut partition_spec_id_b = Int64Builder::new();
    let mut added_snapshot_id_b = Int64Builder::new();
    let mut added_files_b = Int64Builder::new();
    let mut existing_files_b = Int64Builder::new();
    let mut deleted_files_b = Int64Builder::new();
    let mut added_rows_b = Int64Builder::new();
    let mut existing_rows_b = Int64Builder::new();
    let mut deleted_rows_b = Int64Builder::new();

    if let Some(snapshot) = metadata.current_snapshot() {
        match snapshot.load_manifest_list(table.file_io(), metadata).await {
            Ok(manifest_list) => {
                for mf in manifest_list.entries() {
                    manifest_path_b.append_value(&mf.manifest_path);
                    manifest_length_b.append_value(mf.manifest_length);
                    partition_spec_id_b.append_value(mf.partition_spec_id as i64);
                    added_snapshot_id_b.append_value(mf.added_snapshot_id);

                    match mf.added_files_count {
                        Some(c) => added_files_b.append_value(c as i64),
                        None => added_files_b.append_null(),
                    }
                    match mf.existing_files_count {
                        Some(c) => existing_files_b.append_value(c as i64),
                        None => existing_files_b.append_null(),
                    }
                    match mf.deleted_files_count {
                        Some(c) => deleted_files_b.append_value(c as i64),
                        None => deleted_files_b.append_null(),
                    }
                    match mf.added_rows_count {
                        Some(c) => added_rows_b.append_value(c as i64),
                        None => added_rows_b.append_null(),
                    }
                    match mf.existing_rows_count {
                        Some(c) => existing_rows_b.append_value(c as i64),
                        None => existing_rows_b.append_null(),
                    }
                    match mf.deleted_rows_count {
                        Some(c) => deleted_rows_b.append_value(c as i64),
                        None => deleted_rows_b.append_null(),
                    }
                }
            }
            Err(e) => {
                warn!(
                    table = %table.identifier(),
                    error = %e,
                    "table_manifests: failed to load manifest list; returning empty result"
                );
                // Return empty table rather than error — the function signature remains valid.
            }
        }
    }
    // If there is no current snapshot the result is empty (no rows appended).

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(manifest_path_b.finish()) as ArrayRef,
            Arc::new(manifest_length_b.finish()) as ArrayRef,
            Arc::new(partition_spec_id_b.finish()) as ArrayRef,
            Arc::new(added_snapshot_id_b.finish()) as ArrayRef,
            Arc::new(added_files_b.finish()) as ArrayRef,
            Arc::new(existing_files_b.finish()) as ArrayRef,
            Arc::new(deleted_files_b.finish()) as ArrayRef,
            Arc::new(added_rows_b.finish()) as ArrayRef,
            Arc::new(existing_rows_b.finish()) as ArrayRef,
            Arc::new(deleted_rows_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// table_history
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for `table_history()` output.
fn history_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("made_current_at", DataType::Int64, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("is_current_ancestor", DataType::Boolean, false),
    ]))
}

/// DataFusion TVF: `table_history('namespace', 'table_name')`
///
/// Returns one row per entry in the Iceberg snapshot log (the history of which
/// snapshot was current at each point in time). Mirrors the output of Trino's
/// `$history` metadata table.
#[derive(Debug)]
pub struct TableHistoryFunction {
    session_catalog: Arc<SessionCatalog>,
}

impl TableHistoryFunction {
    pub fn new(session_catalog: Arc<SessionCatalog>) -> Self {
        Self { session_catalog }
    }
}

impl TableFunctionImpl for TableHistoryFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (namespace, table_name) = parse_two_string_args("table_history", exprs)?;
        Ok(Arc::new(IcebergMetadataProvider {
            schema: history_schema(),
            catalog: Arc::clone(&self.session_catalog),
            ident: ident_for(&namespace, &table_name),
            kind: MetadataKind::History,
        }))
    }
}

fn build_history_batch(table: &Table, schema: &SchemaRef) -> DFResult<RecordBatch> {
    let metadata = table.metadata();

    // Build a set of snapshot IDs that are ancestors of the current snapshot.
    // Walk the parent chain from the current snapshot to the root.
    let ancestor_ids: std::collections::HashSet<i64> = {
        let mut ids = std::collections::HashSet::new();
        let mut cursor = metadata.current_snapshot().map(|s| s.snapshot_id());
        while let Some(sid) = cursor {
            ids.insert(sid);
            cursor = metadata
                .snapshot_by_id(sid)
                .and_then(|s| s.parent_snapshot_id());
        }
        ids
    };

    let mut made_current_at_b = Int64Builder::new();
    let mut snapshot_id_b = Int64Builder::new();
    let mut parent_id_b = Int64Builder::new();
    let mut is_current_ancestor_b = arrow_array::builder::BooleanBuilder::new();

    // The snapshot log records when each snapshot became current.
    for log_entry in metadata.history() {
        let sid = log_entry.snapshot_id;
        made_current_at_b.append_value(log_entry.timestamp_ms);
        snapshot_id_b.append_value(sid);

        let parent_id = metadata
            .snapshot_by_id(sid)
            .and_then(|s| s.parent_snapshot_id());
        match parent_id {
            Some(pid) => parent_id_b.append_value(pid),
            None => parent_id_b.append_null(),
        }

        is_current_ancestor_b.append_value(ancestor_ids.contains(&sid));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(made_current_at_b.finish()) as ArrayRef,
            Arc::new(snapshot_id_b.finish()) as ArrayRef,
            Arc::new(parent_id_b.finish()) as ArrayRef,
            Arc::new(is_current_ancestor_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// table_files
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for `table_files()` output.
fn files_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_format", DataType::Utf8, false),
        Field::new("record_count", DataType::Int64, false),
        Field::new("file_size_in_bytes", DataType::Int64, false),
        Field::new("column_sizes", DataType::Utf8, true),
        Field::new("value_counts", DataType::Utf8, true),
        Field::new("null_value_counts", DataType::Utf8, true),
        Field::new("partition", DataType::Utf8, false),
    ]))
}

/// DataFusion TVF: `table_files('namespace', 'table_name')`
///
/// Returns one row per data file in the current snapshot. Column-level
/// statistics (sizes, value counts) are serialised as JSON strings.
#[derive(Debug)]
pub struct TableFilesFunction {
    session_catalog: Arc<SessionCatalog>,
}

impl TableFilesFunction {
    pub fn new(session_catalog: Arc<SessionCatalog>) -> Self {
        Self { session_catalog }
    }
}

impl TableFunctionImpl for TableFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (namespace, table_name) = parse_two_string_args("table_files", exprs)?;
        Ok(Arc::new(IcebergMetadataProvider {
            schema: files_schema(),
            catalog: Arc::clone(&self.session_catalog),
            ident: ident_for(&namespace, &table_name),
            kind: MetadataKind::Files,
        }))
    }
}

fn u64_to_i64_saturating(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Serialize a `HashMap<i32, u64>` to a compact JSON string like `{"1":100,"2":200}`.
fn int_map_to_json(map: &std::collections::HashMap<i32, u64>) -> String {
    if map.is_empty() {
        return "{}".to_string();
    }
    let pairs: Vec<String> = map.iter().map(|(k, v)| format!("\"{}\":{}", k, v)).collect();
    format!("{{{}}}", pairs.join(","))
}

async fn build_files_batch(table: &Table, schema: &SchemaRef) -> DFResult<RecordBatch> {
    use iceberg::spec::{DataContentType, ManifestStatus};

    let metadata = table.metadata();

    let mut file_path_b = StringBuilder::new();
    let mut file_format_b = StringBuilder::new();
    let mut record_count_b = Int64Builder::new();
    let mut file_size_b = Int64Builder::new();
    let mut column_sizes_b = StringBuilder::new();
    let mut value_counts_b = StringBuilder::new();
    let mut null_value_counts_b = StringBuilder::new();
    let mut partition_b = StringBuilder::new();

    if let Some(snapshot) = metadata.current_snapshot() {
        match snapshot.load_manifest_list(table.file_io(), metadata).await {
            Ok(manifest_list) => {
                for mf in manifest_list.entries() {
                    match mf.load_manifest(table.file_io()).await {
                        Ok(manifest) => {
                            for entry in manifest.entries() {
                                if entry.status() == ManifestStatus::Deleted {
                                    continue;
                                }
                                let df = entry.data_file();
                                if df.content_type() != DataContentType::Data {
                                    continue;
                                }

                                file_path_b.append_value(df.file_path());
                                file_format_b.append_value(format!("{:?}", df.file_format()));
                                record_count_b.append_value(u64_to_i64_saturating(df.record_count()));
                                file_size_b.append_value(u64_to_i64_saturating(df.file_size_in_bytes()));

                                column_sizes_b.append_value(int_map_to_json(df.column_sizes()));
                                value_counts_b.append_value(int_map_to_json(df.value_counts()));
                                null_value_counts_b.append_value(int_map_to_json(df.null_value_counts()));

                                // Represent partition as a simple string of field values
                                let parts: Vec<String> = df
                                    .partition()
                                    .fields()
                                    .iter()
                                    .map(|f| f.as_ref().map_or("null".to_string(), |v| format!("{v:?}")))
                                    .collect();
                                partition_b.append_value(format!("[{}]", parts.join(",")));
                            }
                        }
                        Err(e) => {
                            warn!(
                                table = %table.identifier(),
                                error = %e,
                                "table_files: failed to load manifest; skipping"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    table = %table.identifier(),
                    error = %e,
                    "table_files: failed to load manifest list; returning empty result"
                );
            }
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(file_path_b.finish()) as ArrayRef,
            Arc::new(file_format_b.finish()) as ArrayRef,
            Arc::new(record_count_b.finish()) as ArrayRef,
            Arc::new(file_size_b.finish()) as ArrayRef,
            Arc::new(column_sizes_b.finish()) as ArrayRef,
            Arc::new(value_counts_b.finish()) as ArrayRef,
            Arc::new(null_value_counts_b.finish()) as ArrayRef,
            Arc::new(partition_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// table_partitions
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for `table_partitions()` output.
fn partitions_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::Utf8, false),
        Field::new("record_count", DataType::Int64, false),
        Field::new("file_count", DataType::Int64, false),
        Field::new("total_size", DataType::Int64, false),
    ]))
}

/// DataFusion TVF: `table_partitions('namespace', 'table_name')`
///
/// Returns one row per distinct partition in the current snapshot, with
/// aggregated record count, file count, and total size.
#[derive(Debug)]
pub struct TablePartitionsFunction {
    session_catalog: Arc<SessionCatalog>,
}

impl TablePartitionsFunction {
    pub fn new(session_catalog: Arc<SessionCatalog>) -> Self {
        Self { session_catalog }
    }
}

impl TableFunctionImpl for TablePartitionsFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (namespace, table_name) = parse_two_string_args("table_partitions", exprs)?;
        Ok(Arc::new(IcebergMetadataProvider {
            schema: partitions_schema(),
            catalog: Arc::clone(&self.session_catalog),
            ident: ident_for(&namespace, &table_name),
            kind: MetadataKind::Partitions,
        }))
    }
}

async fn build_partitions_batch(table: &Table, schema: &SchemaRef) -> DFResult<RecordBatch> {
    use iceberg::spec::{DataContentType, ManifestStatus};

    let metadata = table.metadata();

    // Aggregate stats per partition key (serialised as string)
    let mut partition_stats: std::collections::BTreeMap<String, (i64, i64, i64)> =
        std::collections::BTreeMap::new(); // key → (record_count, file_count, total_size)

    if let Some(snapshot) = metadata.current_snapshot() {
        match snapshot.load_manifest_list(table.file_io(), metadata).await {
            Ok(manifest_list) => {
                for mf in manifest_list.entries() {
                    match mf.load_manifest(table.file_io()).await {
                        Ok(manifest) => {
                            for entry in manifest.entries() {
                                if entry.status() == ManifestStatus::Deleted {
                                    continue;
                                }
                                let df = entry.data_file();
                                if df.content_type() != DataContentType::Data {
                                    continue;
                                }

                                let parts: Vec<String> = df
                                    .partition()
                                    .fields()
                                    .iter()
                                    .map(|f| f.as_ref().map_or("null".to_string(), |v| format!("{v:?}")))
                                    .collect();
                                let partition_key = format!("[{}]", parts.join(","));

                                let entry_stats = partition_stats
                                    .entry(partition_key)
                                    .or_insert((0, 0, 0));
                                entry_stats.0 = entry_stats
                                    .0
                                    .saturating_add(u64_to_i64_saturating(df.record_count()));
                                entry_stats.1 += 1;
                                entry_stats.2 = entry_stats
                                    .2
                                    .saturating_add(u64_to_i64_saturating(df.file_size_in_bytes()));
                            }
                        }
                        Err(e) => {
                            warn!(
                                table = %table.identifier(),
                                error = %e,
                                "table_partitions: failed to load manifest; skipping"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    table = %table.identifier(),
                    error = %e,
                    "table_partitions: failed to load manifest list; returning empty result"
                );
            }
        }
    }

    let mut partition_b = StringBuilder::new();
    let mut record_count_b = Int64Builder::new();
    let mut file_count_b = Int64Builder::new();
    let mut total_size_b = Int64Builder::new();

    for (key, (records, files, size)) in &partition_stats {
        partition_b.append_value(key);
        record_count_b.append_value(*records);
        file_count_b.append_value(*files);
        total_size_b.append_value(*size);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(partition_b.finish()) as ArrayRef,
            Arc::new(record_count_b.finish()) as ArrayRef,
            Arc::new(file_count_b.finish()) as ArrayRef,
            Arc::new(total_size_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// table_refs
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for `table_refs()` output.
fn refs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("max_reference_age_in_ms", DataType::Int64, true),
    ]))
}

/// DataFusion TVF: `table_refs('namespace', 'table_name')`
///
/// Returns one row per named reference (branch or tag) on the table.
/// Since `refs` is not publicly iterable in this iceberg-rust fork, we
/// expose the well-known reference names by probing `snapshot_for_ref`.
/// At minimum the `main` branch is always reported when it exists.
#[derive(Debug)]
pub struct TableRefsFunction {
    session_catalog: Arc<SessionCatalog>,
}

impl TableRefsFunction {
    pub fn new(session_catalog: Arc<SessionCatalog>) -> Self {
        Self { session_catalog }
    }
}

impl TableFunctionImpl for TableRefsFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (namespace, table_name) = parse_two_string_args("table_refs", exprs)?;
        Ok(Arc::new(IcebergMetadataProvider {
            schema: refs_schema(),
            catalog: Arc::clone(&self.session_catalog),
            ident: ident_for(&namespace, &table_name),
            kind: MetadataKind::Refs,
        }))
    }
}

fn build_refs_batch(table: &Table, schema: &SchemaRef) -> DFResult<RecordBatch> {
    let metadata = table.metadata();

    let mut name_b = StringBuilder::new();
    let mut type_b = StringBuilder::new();
    let mut snapshot_id_b = Int64Builder::new();
    let mut max_age_b = Int64Builder::new();

    // The iceberg-rust fork exposes `snapshot_for_ref(name)` which returns
    // Arc<Snapshot> but does not expose the retention policy. We probe well-known
    // ref names and report "BRANCH" for everything (the main use case).
    // Iceberg's default branch is always "main".
    let well_known_refs = ["main", "trunk", "master", "branch-0", "v1", "v2", "latest"];
    let mut found = false;
    for ref_name in &well_known_refs {
        if let Some(snap_ref) = metadata.snapshot_for_ref(ref_name) {
            name_b.append_value(ref_name);
            // We can't distinguish branches from tags without the private `refs` map,
            // so report BRANCH for all named refs (branches are the common case).
            type_b.append_value("BRANCH");
            snapshot_id_b.append_value(snap_ref.snapshot_id());
            max_age_b.append_null();
            found = true;
        }
    }

    // If no refs found from probing, fall back to reporting the current snapshot as "main".
    if !found {
        if let Some(current) = metadata.current_snapshot() {
            name_b.append_value("main");
            type_b.append_value("BRANCH");
            snapshot_id_b.append_value(current.snapshot_id());
            max_age_b.append_null();
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(name_b.finish()) as ArrayRef,
            Arc::new(type_b.finish()) as ArrayRef,
            Arc::new(snapshot_id_b.finish()) as ArrayRef,
            Arc::new(max_age_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;

    fn str_literal(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    // ── parse_two_string_args ────────────────────────────────────────────────

    #[test]
    fn test_parse_two_args_ok() {
        let exprs = vec![str_literal("my_schema"), str_literal("my_table")];
        let (ns, tbl) = parse_two_string_args("test_fn", &exprs).unwrap();
        assert_eq!(ns, "my_schema");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_parse_two_args_missing_second() {
        let exprs = vec![str_literal("only_one")];
        let result = parse_two_string_args("test_fn", &exprs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exactly 2 arguments"));
    }

    #[test]
    fn test_parse_two_args_no_args() {
        let result = parse_two_string_args("test_fn", &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exactly 2 arguments"));
    }

    #[test]
    fn test_parse_two_args_non_string_is_error() {
        let exprs = vec![
            Expr::Literal(ScalarValue::Int64(Some(42)), None),
            str_literal("table"),
        ];
        let result = parse_two_string_args("test_fn", &exprs);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_two_args_too_many() {
        let exprs = vec![
            str_literal("ns"),
            str_literal("tbl"),
            str_literal("extra"),
        ];
        let result = parse_two_string_args("test_fn", &exprs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exactly 2 arguments"));
    }

    // ── snapshots_schema ─────────────────────────────────────────────────────

    #[test]
    fn test_snapshots_schema_column_count() {
        let schema = snapshots_schema();
        assert_eq!(schema.fields().len(), 8);
    }

    #[test]
    fn test_snapshots_schema_column_names() {
        let schema = snapshots_schema();
        let expected = [
            "snapshot_id",
            "parent_snapshot_id",
            "sequence_number",
            "timestamp_ms",
            "operation",
            "manifest_list",
            "summary",
            "is_current_snapshot",
        ];
        for (i, name) in expected.iter().enumerate() {
            assert_eq!(schema.field(i).name(), *name, "snapshots column {i}");
        }
    }

    #[test]
    fn test_snapshots_schema_nullability() {
        let schema = snapshots_schema();
        // snapshot_id must be non-null
        assert!(!schema.field(0).is_nullable(), "snapshot_id must be non-null");
        // parent_snapshot_id must be nullable (root snapshots have no parent)
        assert!(schema.field(1).is_nullable(), "parent_snapshot_id must be nullable");
        // is_current_snapshot must be non-null
        assert!(!schema.field(7).is_nullable(), "is_current_snapshot must be non-null");
    }

    // ── manifests_schema ─────────────────────────────────────────────────────

    #[test]
    fn test_manifests_schema_column_count() {
        let schema = manifests_schema();
        assert_eq!(schema.fields().len(), 10);
    }

    #[test]
    fn test_manifests_schema_column_names() {
        let schema = manifests_schema();
        let expected = [
            "manifest_path",
            "manifest_length",
            "partition_spec_id",
            "added_snapshot_id",
            "added_data_files_count",
            "existing_data_files_count",
            "deleted_data_files_count",
            "added_rows_count",
            "existing_rows_count",
            "deleted_rows_count",
        ];
        for (i, name) in expected.iter().enumerate() {
            assert_eq!(schema.field(i).name(), *name, "manifests column {i}");
        }
    }

    #[test]
    fn test_manifests_schema_required_columns_non_null() {
        let schema = manifests_schema();
        // manifest_path, manifest_length, partition_spec_id, added_snapshot_id are always present
        for i in 0..4 {
            assert!(!schema.field(i).is_nullable(), "manifests column {i} must be non-null");
        }
        // Count columns are nullable (may be absent in older Iceberg formats)
        for i in 4..10 {
            assert!(schema.field(i).is_nullable(), "manifests column {i} must be nullable");
        }
    }

    // ── summary JSON serialisation ────────────────────────────────────────────

    #[test]
    fn test_summary_empty_map_produces_empty_object() {
        let extra: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let result = if extra.is_empty() {
            "{}".to_string()
        } else {
            let pairs: Vec<String> = extra
                .iter()
                .map(|(k, v)| format!("\"{}\":\"{}\"", k.replace('"', "\\\""), v.replace('"', "\\\"")))
                .collect();
            format!("{{{}}}", pairs.join(","))
        };
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_summary_escapes_double_quotes_in_key_and_value() {
        let mut extra: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        extra.insert("ke\"y".to_string(), "va\"lue".to_string());
        let pairs: Vec<String> = extra
            .iter()
            .map(|(k, v)| format!("\"{}\":\"{}\"", k.replace('"', "\\\""), v.replace('"', "\\\"")))
            .collect();
        let result = format!("{{{}}}", pairs.join(","));
        assert!(result.contains("ke\\\"y"), "key should have escaped quotes");
        assert!(result.contains("va\\\"lue"), "value should have escaped quotes");
    }
}
