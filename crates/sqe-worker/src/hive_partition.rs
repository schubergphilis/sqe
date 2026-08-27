//! Worker-side materialization of Hive partition columns.
//!
//! Hive partition columns are directory-encoded (`k=v/`) and do not exist in
//! the data file. Coordinator-local execution materializes them in
//! `HiveTableProvider::attach_partition_values`, which wraps the scan in a
//! `ProjectionExec` emitting a column reference for file columns and a typed
//! LITERAL for partition columns.
//!
//! A worker has no plan to wrap: it produces `RecordBatch`es. So the mechanism
//! differs (append constant arrays rather than project), but the RESULT must be
//! identical -- same columns, same order, same Arrow types. The typing rule is
//! therefore imported from the catalog rather than reimplemented:
//! `sqe_catalog::hive::provider::partition_scalar` is the single definition,
//! and cycle 5's whole correctness argument rests on there being only one.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::common::ScalarValue;
use sqe_catalog::hive::manifest::HIVE_DEFAULT_PARTITION;
use sqe_catalog::hive::provider::partition_scalar;
use sqe_planner::HiveScanDescriptor;

/// Split point: file columns are everything before the partition columns.
///
/// Derived rather than carried, so it cannot disagree with the schema it is
/// derived from.
fn file_column_count(full_schema: &Schema, desc: &HiveScanDescriptor) -> Result<usize, String> {
    full_schema
        .fields()
        .len()
        .checked_sub(desc.partition_columns.len())
        .ok_or_else(|| {
            format!(
                "hive descriptor declares {} partition columns but the full schema has only {} \
                 fields",
                desc.partition_columns.len(),
                full_schema.fields().len()
            )
        })
}

/// The schema of the columns actually stored in a data file: the full schema
/// minus the trailing partition columns.
///
/// Derived through [`file_column_count`], the same split
/// [`append_partition_columns`] uses, so a reader and the appender cannot
/// disagree about where the file ends and the partition begins.
///
/// Required by the CSV and JSON readers, which bind fields by POSITION against
/// a declared schema. Letting either infer its own schema is how two files in
/// one table end up with different types, and how a header row gets counted as
/// data -- the failure design.md names.
pub fn file_schema_of(
    desc: &HiveScanDescriptor,
    full_schema: &SchemaRef,
) -> Result<SchemaRef, String> {
    let n_file = file_column_count(full_schema, desc)?;
    Ok(Arc::new(Schema::new(
        full_schema.fields()[..n_file].to_vec(),
    )))
}

/// Parse the descriptor's full schema.
pub fn full_schema_of(desc: &HiveScanDescriptor) -> Result<SchemaRef, String> {
    serde_json::from_str::<Schema>(&desc.full_schema_json)
        .map(Arc::new)
        .map_err(|e| format!("hive descriptor carries an unreadable full schema: {e}"))
}

/// Append the partition columns to a batch read from a data file.
///
/// The batch is expected to carry the FILE columns. Partition columns are
/// appended in the descriptor's declared order, each as a constant array of
/// the batch's row count, typed exactly as the coordinator would type it.
///
/// A partition key absent from `partition_values` becomes
/// `__HIVE_DEFAULT_PARTITION__`, matching `attach_partition_values`' own
/// `unwrap_or(HIVE_DEFAULT_PARTITION)`.
pub fn append_partition_columns(
    batch: &RecordBatch,
    desc: &HiveScanDescriptor,
    full_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    if desc.partition_columns.is_empty() {
        return Ok(batch.clone());
    }
    let n_file = file_column_count(full_schema, desc)?;
    let rows = batch.num_rows();

    let mut columns = batch.columns().to_vec();
    for (i, key) in desc.partition_columns.iter().enumerate() {
        let field = full_schema.field(n_file + i);
        let raw = desc
            .partition_values
            .get(key)
            .map(String::as_str)
            .unwrap_or(HIVE_DEFAULT_PARTITION);
        // The sentinel resolves to NULL, not to the literal string.
        //
        // Coordinator-local execution reaches that in TWO steps:
        // `partition_scalar` yields the literal `__HIVE_DEFAULT_PARTITION__`
        // for a Utf8 column (and a typed NULL for anything else), and then
        // `nullify_default_partition_columns` rewrites the Utf8 case to NULL
        // with a CASE over the column. A worker knows the partition value
        // statically, and a partition column is constant across the whole
        // partition, so the runtime CASE and this static decision agree.
        //
        // Doing only the first step would produce the literal string where the
        // coordinator produces NULL: same type, same row count, different
        // values -- which no schema check would catch.
        let scalar = if raw == HIVE_DEFAULT_PARTITION {
            ScalarValue::try_from(field.data_type())
                .map_err(|e| format!("partition column `{key}`: null of declared type: {e}"))?
        } else {
            partition_scalar(raw, field.data_type())
        };
        let array = scalar
            .to_array_of_size(rows)
            .map_err(|e| format!("partition column `{key}`: {e}"))?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::clone(full_schema), columns)
        .map_err(|e| format!("appending hive partition columns: {e}"))
}
