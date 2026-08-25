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

use arrow_schema::Schema;
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
