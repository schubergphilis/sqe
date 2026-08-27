//! Build worker `ScanTask`s for a Hive external table.
//!
//! ## Why the coordinator lists, rather than shipping a prefix
//!
//! `PartitionIndexEntry` carries a partition's `location`, `file_count` and
//! `total_bytes`, but not its file paths. So per-partition tasks need a listing
//! from somewhere, and there were two options:
//!
//! - the coordinator lists the SURVIVING partitions and emits per-file tasks;
//! - the task carries the prefix and the worker lists it.
//!
//! The second distributes the listing, which is appealing, but it also decides
//! placement before anything knows the sizes: a partition holding a thousand
//! files lands wholesale on one worker. Cycle 5 exists to get scan parallelism,
//! and balancing needs sizes, so the coordinator lists.
//!
//! The cost is bounded by cycle 2's pruning: only partitions that survived
//! predicate pruning are listed, not the whole table. That is the property the
//! partition index bought, and this spends it deliberately.
//!
//! The listing itself is NOT done here. This module is a pure function over
//! already-listed files, so the listing strategy stays a separate decision and
//! this stays testable without an object store.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use sqe_planner::{HiveScanDescriptor, HiveScanFormat, ScanTask};

/// One partition's worth of already-listed data files.
pub struct HivePartitionFiles {
    /// Directory-encoded partition values, as recorded in the index.
    pub values: BTreeMap<String, String>,
    /// `(path, size_bytes)` for each data file under the partition location.
    pub files: Vec<(String, u64)>,
}

/// The SerDe settings a worker needs to open the files, plus the format.
pub struct HiveReadFormat {
    pub format: HiveScanFormat,
    pub delimiter: u8,
    pub quote: Option<u8>,
    pub escape: Option<u8>,
    pub has_header: bool,
}

/// S3 access for the emitted tasks. Mirrors the fields `ScanTask` already has.
#[derive(Clone, Default)]
pub struct HiveS3Access {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub path_style: bool,
    pub allow_http: bool,
}

/// What the TABLE contributes to every task: how to read it, its shape, and
/// how to reach its storage. Constant across the whole scan.
pub struct HiveScanTable<'a> {
    pub read: &'a HiveReadFormat,
    pub partition_columns: &'a [String],
    pub full_schema: &'a Schema,
    pub s3: &'a HiveS3Access,
}

/// What the QUERY contributes: projection, pushed-down predicate, limit, and
/// the fragment id prefix. Grouped separately from the table because these
/// change per statement while the table's shape does not.
pub struct HiveScanQuery<'a> {
    pub projected_columns: &'a [String],
    pub predicate_proto: Option<Vec<u8>>,
    pub limit: Option<usize>,
    pub fragment_prefix: &'a str,
}

/// Build one `ScanTask` per partition.
///
/// One task per partition, never spanning: `HiveScanDescriptor` carries a
/// single `partition_values` map, so a task covering two partitions would have
/// to pick one of them and would silently mislabel the other's rows.
///
/// A partition whose listing came back empty is skipped rather than emitted as
/// an empty task, so a worker is never dispatched work it cannot do anything
/// with.
pub fn build_hive_scan_tasks(
    partitions: &[HivePartitionFiles],
    table: &HiveScanTable<'_>,
    query: &HiveScanQuery<'_>,
) -> Result<Vec<ScanTask>, String> {
    let HiveScanTable {
        read,
        partition_columns,
        full_schema,
        s3,
    } = table;
    let HiveScanQuery {
        projected_columns,
        predicate_proto,
        limit,
        fragment_prefix,
    } = query;
    let limit = *limit;
    let full_schema_json = serde_json::to_string(full_schema)
        .map_err(|e| format!("hive scan task: full schema does not serialize: {e}"))?;

    let mut tasks = Vec::new();
    for (i, part) in partitions.iter().enumerate() {
        if part.files.is_empty() {
            continue;
        }
        let (paths, sizes): (Vec<String>, Vec<u64>) = part.files.iter().cloned().unzip();
        tasks.push(ScanTask {
            version: sqe_planner::scan_morsel::SCAN_TASK_VERSION_V3,
            fragment_id: format!("{fragment_prefix}-hive-{i}"),
            morsel_id: None,
            data_file_paths: paths,
            file_sizes_bytes: sizes,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            projected_columns: projected_columns.to_vec(),
            // A Hive table has no Iceberg field IDs. Left empty on purpose:
            // `projected_columns` is already the documented fallback, and
            // sending a bogus id would make the worker project by it.
            projected_field_ids: Vec::new(),
            s3_endpoint: s3.endpoint.clone(),
            s3_region: s3.region.clone(),
            s3_access_key: s3.access_key.clone(),
            s3_secret_key: s3.secret_key.clone(),
            s3_session_token: s3.session_token.clone(),
            s3_path_style: s3.path_style,
            s3_allow_http: s3.allow_http,
            predicate_proto: predicate_proto.clone(),
            limit,
            hive: Some(HiveScanDescriptor {
                format: read.format,
                delimiter: read.delimiter,
                quote: read.quote,
                escape: read.escape,
                has_header: read.has_header,
                partition_values: part.values.clone(),
                partition_columns: partition_columns.to_vec(),
                full_schema_json: full_schema_json.clone(),
            }),
        });
    }
    Ok(tasks)
}

/// Turn a planned Hive scan plus its listing into worker tasks.
///
/// This is the whole task-building half of `try_distribute_hive`, lifted out so
/// something other than a live coordinator can call it. `try_distribute_hive`
/// needs a `Session`, a worker registry and a set of healthy URLs, so it cannot
/// run in a unit test -- and the differential tests would otherwise have to
/// hand-build a `HiveScanQuery`, which is how a test ends up green against a
/// configuration production never produces.
///
/// Two of those production choices are deliberate and load-bearing for the
/// differential tests, so they are stated here rather than left to be
/// rediscovered:
///
/// - `projected_columns` is the FULL schema, every time, including the
///   partition columns that are not in any data file.
///   `resolve_scan_projection` drops the absent names, so the worker projects
///   exactly the file columns IN FULL-SCHEMA ORDER, and
///   `append_partition_columns` then appends the partition columns at the tail.
///   That order is what the `len() - partition_columns.len()` split assumes.
///   Narrowing this to the query's projection would break that split.
/// - `predicate_proto` and `limit` are `None`. Partition pruning already
///   happened on the coordinator (cycle 2 gave `spec.entries` only the
///   survivors), and the `FilterExec` above the scan still applies the
///   predicate, so rows are correct. What is absent is file and row-group
///   pushdown INSIDE the distributed Hive scan. Absent, not broken.
pub fn tasks_for_hive_scan(
    spec: &sqe_catalog::hive::scan_exec::HiveScanSpec,
    partitions: Vec<(BTreeMap<String, String>, Vec<(String, u64)>)>,
    s3: &HiveS3Access,
    fragment_prefix: &str,
) -> Result<Vec<ScanTask>, String> {
    use sqe_catalog::hive::serde_map::HiveFileFormat;

    let files: Vec<HivePartitionFiles> = partitions
        .into_iter()
        .map(|(values, files)| HivePartitionFiles { values, files })
        .collect();

    let read = HiveReadFormat {
        format: match spec.resolved.kind {
            HiveFileFormat::Parquet => HiveScanFormat::Parquet,
            HiveFileFormat::Csv => HiveScanFormat::Csv,
            HiveFileFormat::Json => HiveScanFormat::Json,
        },
        delimiter: spec.resolved.delimiter,
        quote: spec.resolved.quote,
        escape: spec.resolved.escape,
        has_header: spec.resolved.has_header,
    };

    let projected: Vec<String> = spec
        .full_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    build_hive_scan_tasks(
        &files,
        &HiveScanTable {
            read: &read,
            partition_columns: &spec.partition_columns,
            full_schema: spec.full_schema.as_ref(),
            s3,
        },
        &HiveScanQuery {
            projected_columns: &projected,
            predicate_proto: None,
            limit: None,
            fragment_prefix,
        },
    )
}

/// Read S3 access for Hive tasks out of the engine's storage config.
pub fn hive_s3_access(storage: &sqe_core::config::StorageConfig) -> HiveS3Access {
    HiveS3Access {
        endpoint: storage.s3_endpoint.clone(),
        region: storage.s3_region.clone(),
        access_key: storage.s3_access_key.clone(),
        secret_key: storage.s3_secret_key.expose().to_string(),
        session_token: String::new(),
        path_style: storage.s3_path_style,
        allow_http: storage.s3_endpoint.starts_with("http://"),
    }
}

/// Narrow a full-schema distributed Hive scan back to the schema of the node it
/// replaces.
///
/// ## Why this is required rather than tidy
///
/// A Hive task always projects the FULL schema: `resolve_scan_projection` drops
/// the partition names that no data file contains, and
/// `append_partition_columns` appends them at the tail, so the split point
/// `len() - partition_columns.len()` holds. Narrowing the task's projection
/// would break that split.
///
/// But the node being replaced is `HiveScanExec`, whose schema is the QUERY's
/// projection -- `scan_from_entries` applies it. For `SELECT dt FROM t` that is
/// one column while the distributed scan produces three.
///
/// Every parent in the plan indexes its child by POSITION. A `ProjectionExec`
/// holding `Column("dt", 0)` against a replacement whose column 0 is `id` does
/// not fail: it returns `id`'s values under the name `dt`. So the width has to
/// be reconciled at the substitution, or distributed execution silently answers
/// a different query.
///
/// Returns the input untouched when the schemas already agree, which is the
/// `SELECT *` case and the common one.
pub fn align_to_scan_schema(
    wide: Arc<dyn ExecutionPlan>,
    target: &Schema,
) -> Result<Arc<dyn ExecutionPlan>, String> {
    let wide_schema = wide.schema();
    if wide_schema.fields() == target.fields() {
        return Ok(wide);
    }

    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        // By name, not by position: the target is a subset in the full schema's
        // order, and a positional guess is the exact bug this function exists
        // to prevent.
        let idx = wide_schema.index_of(field.name()).map_err(|_| {
            format!(
                "distributed hive scan cannot supply column `{}`; it produces [{}]",
                field.name(),
                wide_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        exprs.push((
            Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }

    ProjectionExec::try_new(exprs, wide)
        .map(|p| Arc::new(p) as Arc<dyn ExecutionPlan>)
        .map_err(|e| format!("narrowing the distributed hive scan: {e}"))
}
