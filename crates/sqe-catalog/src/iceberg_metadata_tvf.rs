//! Table-valued functions for querying Iceberg table metadata.
//!
//! Registers two TVFs on the [`SessionContext`]:
//!
//! ```sql
//! -- List all snapshots for a table
//! SELECT * FROM table_snapshots('namespace', 'table_name');
//!
//! -- List all manifest files from the current snapshot
//! SELECT * FROM table_manifests('namespace', 'table_name');
//! ```
//!
//! Both functions are implemented as [`TableFunctionImpl`] — the same pattern
//! as [`crate::read_parquet::ReadParquetFunction`].
//!
//! ## Time travel
//!
//! The RisingWave iceberg-rust fork (pinned to DataFusion 52.x) does not
//! expose `TableScanBuilder::snapshot_id()`. Time-travel scanning is therefore
//! not implemented here and is documented as blocked on the upstream fork.
//! Track progress at: <https://github.com/risingwavelabs/iceberg-rust>

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{ScalarValue, plan_err};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion_expr::Expr;
use iceberg::{NamespaceIdent, TableIdent};
use tracing::warn;

use crate::rest_catalog::SessionCatalog;

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
        let catalog = Arc::clone(&self.session_catalog);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                build_snapshots_table(&catalog, &namespace, &table_name).await
            })
        })
    }
}

async fn build_snapshots_table(
    catalog: &SessionCatalog,
    namespace: &str,
    table_name: &str,
) -> DFResult<Arc<dyn TableProvider>> {
    let schema = snapshots_schema();

    let ns = NamespaceIdent::new(namespace.to_string());
    let ident = TableIdent::new(ns, table_name.to_string());

    let table = catalog.load_table(&ident).await.map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "table_snapshots: failed to load table '{namespace}.{table_name}': {e}"
        ))
    })?;

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

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
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
        let catalog = Arc::clone(&self.session_catalog);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                build_manifests_table(&catalog, &namespace, &table_name).await
            })
        })
    }
}

async fn build_manifests_table(
    catalog: &SessionCatalog,
    namespace: &str,
    table_name: &str,
) -> DFResult<Arc<dyn TableProvider>> {
    let schema = manifests_schema();

    let ns = NamespaceIdent::new(namespace.to_string());
    let ident = TableIdent::new(ns, table_name.to_string());

    let table = catalog.load_table(&ident).await.map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "table_manifests: failed to load table '{namespace}.{table_name}': {e}"
        ))
    })?;

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
                    namespace = namespace,
                    table = table_name,
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

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
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
