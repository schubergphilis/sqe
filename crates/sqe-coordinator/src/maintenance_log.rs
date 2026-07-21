//! Best-effort appender for the `sqe_system.maintenance_log` ledger table
//! (Phase 4a advisory auto-compaction, Task 4).
//!
//! The (later) advisory scheduler calls [`append_row`] once per analyzed
//! table to record a row of type `status = "advisory"` (built by
//! [`advisory_row`]). In Phase 4a nothing else writes this table.
//!
//! # Best-effort contract
//!
//! The operator creates `sqe_system.maintenance_log` out-of-band (see the
//! fixed schema below); Phase 4a must not need a `CREATE TABLE` grant. If
//! the state table (or its namespace) does not exist, [`append_row`] logs a
//! `warn!` and returns `Ok(())`. It never fails the caller on a missing
//! table: the scheduler must keep analyzing every other table even when the
//! ledger has not been provisioned yet.
//!
//! Any OTHER failure (auth, network, a commit conflict, malformed
//! `state_table`) is a real operational problem and is returned as `Err` so
//! the caller can decide whether to log it, retry, or surface it.
//!
//! # Fixed row schema
//!
//! [`row_to_record_batch`] builds a single-row Arrow `RecordBatch` whose
//! columns are positionally stamped onto the target table's Iceberg field
//! IDs by `crate::writer::write_data_files` (see `stamp_field_ids`): field
//! *i* of the batch becomes field *i* of the table's current schema,
//! regardless of column name. That means the operator's `CREATE TABLE` MUST
//! declare columns in exactly this order and with Arrow-compatible types:
//!
//! | # | column          | type      | nullable |
//! |---|-----------------|-----------|----------|
//! | 0 | `job_id`        | STRING    | no       |
//! | 1 | `table_name`    | STRING    | no       |
//! | 2 | `trigger`       | STRING    | no       |
//! | 3 | `principal`     | STRING    | no       |
//! | 4 | `started_at_ms` | BIGINT    | no       |
//! | 5 | `finished_at_ms`| BIGINT    | no       |
//! | 6 | `status`        | STRING    | no       |
//! | 7 | `files_in`      | BIGINT    | no       |
//! | 8 | `files_out`     | BIGINT    | no       |
//! | 9 | `bytes_in`      | BIGINT    | no       |
//! |10 | `bytes_out`     | BIGINT    | no       |
//! |11 | `rows_removed`  | BIGINT    | no       |
//! |12 | `snapshot_id`   | BIGINT    | yes      |
//! |13 | `error`         | STRING    | yes      |
//!
//! Example operator DDL:
//!
//! ```sql
//! CREATE TABLE sqe_system.maintenance_log (
//!     job_id         STRING NOT NULL,
//!     table_name     STRING NOT NULL,
//!     trigger        STRING NOT NULL,
//!     principal      STRING NOT NULL,
//!     started_at_ms  BIGINT NOT NULL,
//!     finished_at_ms BIGINT NOT NULL,
//!     status         STRING NOT NULL,
//!     files_in       BIGINT NOT NULL,
//!     files_out      BIGINT NOT NULL,
//!     bytes_in       BIGINT NOT NULL,
//!     bytes_out      BIGINT NOT NULL,
//!     rows_removed   BIGINT NOT NULL,
//!     snapshot_id    BIGINT,
//!     error          STRING
//! )
//! ```
//!
//! # Reused write path
//!
//! [`append_row`] does not hand-roll an Iceberg transaction. It reuses the
//! same append machinery `INSERT INTO` uses in `write_handler.rs`:
//! `crate::writer::write_data_files` turns the one-row `RecordBatch` into a
//! Parquet data file, and `crate::write_handler::commit_with_retry` commits
//! a `Transaction::fast_append` with the same backoff-and-retry-on-conflict
//! behavior every other writer gets. That matters here specifically because
//! `sqe_system.maintenance_log` is also the multi-coordinator lease table
//! (`MaintenanceSchedulerConfig::lease = Catalog`): more than one coordinator
//! can commit to it around the same time, so a single-shot commit would
//! surface routine conflicts as `Err` instead of retrying past them.
//! `crate::writer::WriteCleanupGuard` cleans up the written Parquet file if
//! the commit never lands (e.g. the process is killed between write and
//! commit).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use tracing::warn;
use uuid::Uuid;

use sqe_core::SqeError;

use crate::catalog_ops::is_namespace_not_found;
use crate::table_health::TableHealth;
use crate::write_handler::commit_with_retry;
use crate::writer::{new_upload_tracker, parse_parquet_compression, write_data_files, WriteCleanupGuard};

/// Default `ns.table` for the ledger, matching
/// `sqe_core::config::MaintenanceSchedulerConfig::state_table`'s default.
pub const DEFAULT_MAINTENANCE_LOG_TABLE: &str = "sqe_system.maintenance_log";

/// One row of the `sqe_system.maintenance_log` ledger.
///
/// See the module docs for the fixed Arrow/Iceberg column order this maps
/// to. Count fields are `i64` (not `u64`): Iceberg's `long` type is signed,
/// and there is no unsigned Arrow integer type this table's schema could
/// declare from SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceLogRow {
    /// Unique ID for this job/analysis run.
    pub job_id: String,
    /// Fully-qualified `ns.table` the row is about.
    pub table: String,
    /// What triggered the job (e.g. `"scheduler"`, `"manual"`).
    pub trigger: String,
    /// Principal the job ran as.
    pub principal: String,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    /// Job status, e.g. `"advisory"`, and later (Phase 4b+) `"success"` /
    /// `"failed"`.
    pub status: String,
    pub files_in: i64,
    pub files_out: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
    pub rows_removed: i64,
    /// Snapshot ID the job produced, if it committed one. Always `None` for
    /// an advisory row: nothing is rewritten.
    pub snapshot_id: Option<i64>,
    pub error: Option<String>,
}

/// Build the `status = "advisory"` row the (later) scheduler records for
/// one analyzed table.
///
/// An advisory row reports compaction debt without rewriting anything, so
/// it has no real "job" duration: `started_at_ms` and `finished_at_ms` are
/// both `ts_ms`. `files_in` and `bytes_in` deliberately describe the SAME
/// scope, the table's total live footprint (`health.live_data_files` /
/// `health.avg_file_bytes * health.live_data_files`), so `bytes_in /
/// files_in` in this generic ledger is a meaningful average file size
/// rather than mixing a total file count against a debt-subset byte count.
/// The richer debt signal (`health.eligible_groups` /
/// `health.est_rewrite_bytes`, the bytes a rewrite would actually touch)
/// belongs to `CALL system.table_health`, not this fixed-shape ledger row.
/// `files_out` / `bytes_out` / `rows_removed` are `0` and `snapshot_id` /
/// `error` are `None`: no rewrite happened.
pub fn advisory_row(table: &str, principal: &str, health: &TableHealth, ts_ms: i64) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: Uuid::now_v7().to_string(),
        table: table.to_string(),
        trigger: "scheduler".to_string(),
        principal: principal.to_string(),
        started_at_ms: ts_ms,
        finished_at_ms: ts_ms,
        status: "advisory".to_string(),
        files_in: health.live_data_files as i64,
        files_out: 0,
        bytes_in: (health.avg_file_bytes * health.live_data_files) as i64,
        bytes_out: 0,
        rows_removed: 0,
        snapshot_id: None,
        error: None,
    }
}

/// The fixed Arrow schema documented at the top of this file. Column order
/// is load-bearing: see the module docs.
pub fn maintenance_log_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("job_id", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("trigger", DataType::Utf8, false),
        Field::new("principal", DataType::Utf8, false),
        Field::new("started_at_ms", DataType::Int64, false),
        Field::new("finished_at_ms", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("files_in", DataType::Int64, false),
        Field::new("files_out", DataType::Int64, false),
        Field::new("bytes_in", DataType::Int64, false),
        Field::new("bytes_out", DataType::Int64, false),
        Field::new("rows_removed", DataType::Int64, false),
        Field::new("snapshot_id", DataType::Int64, true),
        Field::new("error", DataType::Utf8, true),
    ]))
}

/// Shape one `MaintenanceLogRow` into the single-row `RecordBatch`
/// [`append_row`] writes.
fn row_to_record_batch(row: &MaintenanceLogRow) -> sqe_core::Result<RecordBatch> {
    RecordBatch::try_new(
        maintenance_log_arrow_schema(),
        vec![
            Arc::new(StringArray::from(vec![row.job_id.as_str()])),
            Arc::new(StringArray::from(vec![row.table.as_str()])),
            Arc::new(StringArray::from(vec![row.trigger.as_str()])),
            Arc::new(StringArray::from(vec![row.principal.as_str()])),
            Arc::new(Int64Array::from(vec![row.started_at_ms])),
            Arc::new(Int64Array::from(vec![row.finished_at_ms])),
            Arc::new(StringArray::from(vec![row.status.as_str()])),
            Arc::new(Int64Array::from(vec![row.files_in])),
            Arc::new(Int64Array::from(vec![row.files_out])),
            Arc::new(Int64Array::from(vec![row.bytes_in])),
            Arc::new(Int64Array::from(vec![row.bytes_out])),
            Arc::new(Int64Array::from(vec![row.rows_removed])),
            Arc::new(Int64Array::from(vec![row.snapshot_id])),
            Arc::new(StringArray::from(vec![row.error.clone()])),
        ],
    )
    .map_err(|e| SqeError::Execution(format!("maintenance_log: failed to build row batch: {e}")))
}

/// Resolve `state_table` (a plain `ns.table` string, e.g. the
/// `[maintenance] state_table` config value) into a `TableIdent`.
///
/// Falls back to [`DEFAULT_MAINTENANCE_LOG_TABLE`] when `state_table` is
/// empty or has no `.` qualifier, rather than guessing at a namespace.
fn resolve_state_table_ident(state_table: &str) -> TableIdent {
    let candidate = if state_table.trim().is_empty() {
        DEFAULT_MAINTENANCE_LOG_TABLE
    } else {
        state_table
    };
    match candidate.split_once('.') {
        Some((ns, name)) if !ns.is_empty() && !name.is_empty() => {
            TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string())
        }
        _ => {
            let (ns, name) = DEFAULT_MAINTENANCE_LOG_TABLE
                .split_once('.')
                .expect("DEFAULT_MAINTENANCE_LOG_TABLE is a fixed ns.table literal");
            TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string())
        }
    }
}

/// Append one row to the `state_table` ledger. Best-effort: see the module
/// docs for exactly which failures are swallowed (table/namespace absent)
/// versus propagated (everything else).
pub async fn append_row(
    catalog: &Arc<dyn Catalog>,
    state_table: &str,
    row: &MaintenanceLogRow,
) -> sqe_core::Result<()> {
    let ident = resolve_state_table_ident(state_table);

    // `table_exists` is preferred over sniffing `load_table`'s error message:
    // it returns a clean `Ok(false)` on every catalog backend this crate
    // supports (REST/Polaris 404 -> false; the SQL/SQLite backend's
    // `no_such_table_err` carries a misleading "already exists" message
    // upstream, which a message-based check would misclassify).
    match catalog.table_exists(&ident).await {
        Ok(true) => {}
        Ok(false) => {
            warn!(
                table = %ident,
                "maintenance_log: state table absent, skipping append (best-effort; \
                 the operator must create it out-of-band)"
            );
            return Ok(());
        }
        Err(e) if is_namespace_not_found(&e) => {
            warn!(
                table = %ident,
                error = %e,
                "maintenance_log: state table's namespace absent, skipping append (best-effort)"
            );
            return Ok(());
        }
        Err(e) => {
            return Err(SqeError::Catalog(format!(
                "maintenance_log: failed to check existence of state table '{ident}': {e}"
            )));
        }
    }

    let table = catalog.load_table(&ident).await.map_err(|e| {
        SqeError::Catalog(format!("maintenance_log: failed to load state table '{ident}': {e}"))
    })?;

    let batch = row_to_record_batch(row)?;
    let tracker = new_upload_tracker();
    let cleanup_guard =
        WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "maintenance-log-append");
    let compression = parse_parquet_compression("zstd");

    let data_files =
        write_data_files(&table, vec![batch], "maintenance-log", compression, tracker).await?;

    if data_files.is_empty() {
        // Defensive: row_to_record_batch always produces exactly one row,
        // so write_data_files always returns at least one file in
        // practice. Nothing to commit either way.
        cleanup_guard.mark_committed();
        return Ok(());
    }

    // commit_with_retry re-loads the table fresh on each attempt and retries
    // past retryable conflicts (see the module docs: this ledger is also the
    // multi-coordinator lease table, so concurrent commits are expected, not
    // exceptional).
    let files_for_retry = data_files;
    let catalog_for_commit = catalog.clone();
    commit_with_retry(catalog.as_ref(), &ident, "maintenance-log-append", move |fresh_table| {
        let files = files_for_retry.clone();
        let cat = catalog_for_commit.clone();
        async move {
            let tx = Transaction::new(&fresh_table);
            let action = tx.fast_append().add_data_files(files);
            let tx = action.apply(tx)?;
            tx.commit(cat.as_ref()).await
        }
    })
    .await
    .map_err(|e| SqeError::Execution(format!("maintenance_log: failed to commit append: {e}")))?;
    cleanup_guard.mark_committed();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    fn sample_health() -> TableHealth {
        TableHealth {
            live_data_files: 42,
            small_files: 10,
            avg_file_bytes: 1_000,
            p50_file_bytes: 900,
            delete_files: 3,
            delete_heavy_files: 1,
            eligible_groups: 2,
            est_rewrite_bytes: 12_345,
            last_compaction_snapshot_ms: None,
            maintenance_enabled: true,
        }
    }

    #[test]
    fn advisory_row_maps_status_to_advisory() {
        let row = advisory_row("ns.t", "svc", &sample_health(), 1_000);
        assert_eq!(row.status, "advisory");
    }

    #[test]
    fn advisory_row_maps_files_in_from_live_data_files() {
        let health = sample_health();
        let row = advisory_row("ns.t", "svc", &health, 1_000);
        assert_eq!(row.files_in, health.live_data_files as i64);
    }

    #[test]
    fn advisory_row_maps_bytes_in_from_total_footprint_not_debt_subset() {
        // bytes_in must be the same scope as files_in (total live footprint),
        // not health.est_rewrite_bytes (only the eligible-group subset) --
        // otherwise bytes_in / files_in in the ledger is not a meaningful
        // average file size.
        let health = sample_health();
        let row = advisory_row("ns.t", "svc", &health, 1_000);
        assert_eq!(row.bytes_in, (health.avg_file_bytes * health.live_data_files) as i64);
        assert_ne!(
            row.bytes_in, health.est_rewrite_bytes as i64,
            "sample_health's est_rewrite_bytes must differ from the total footprint for this test to be meaningful"
        );
    }

    #[test]
    fn advisory_row_zeroes_output_and_removed_fields() {
        let row = advisory_row("ns.t", "svc", &sample_health(), 1_000);
        assert_eq!(row.files_out, 0);
        assert_eq!(row.bytes_out, 0);
        assert_eq!(row.rows_removed, 0);
    }

    #[test]
    fn advisory_row_snapshot_and_error_are_none() {
        let row = advisory_row("ns.t", "svc", &sample_health(), 1_000);
        assert_eq!(row.snapshot_id, None);
        assert_eq!(row.error, None);
    }

    #[test]
    fn advisory_row_uses_given_table_principal_and_timestamp() {
        let row = advisory_row("ns.orders", "maintenance-svc", &sample_health(), 999_000);
        assert_eq!(row.table, "ns.orders");
        assert_eq!(row.principal, "maintenance-svc");
        assert_eq!(row.started_at_ms, 999_000);
        assert_eq!(row.finished_at_ms, 999_000);
    }

    #[test]
    fn advisory_row_generates_nonempty_job_id() {
        let a = advisory_row("ns.t", "svc", &sample_health(), 1_000);
        let b = advisory_row("ns.t", "svc", &sample_health(), 1_000);
        assert!(!a.job_id.is_empty());
        assert_ne!(a.job_id, b.job_id, "each row must get a unique job_id");
    }

    #[test]
    fn maintenance_log_arrow_schema_has_fixed_shape() {
        let schema = maintenance_log_arrow_schema();
        let expected: Vec<(&str, DataType, bool)> = vec![
            ("job_id", DataType::Utf8, false),
            ("table_name", DataType::Utf8, false),
            ("trigger", DataType::Utf8, false),
            ("principal", DataType::Utf8, false),
            ("started_at_ms", DataType::Int64, false),
            ("finished_at_ms", DataType::Int64, false),
            ("status", DataType::Utf8, false),
            ("files_in", DataType::Int64, false),
            ("files_out", DataType::Int64, false),
            ("bytes_in", DataType::Int64, false),
            ("bytes_out", DataType::Int64, false),
            ("rows_removed", DataType::Int64, false),
            ("snapshot_id", DataType::Int64, true),
            ("error", DataType::Utf8, true),
        ];
        assert_eq!(schema.fields().len(), expected.len());
        for (field, (name, ty, nullable)) in schema.fields().iter().zip(expected.iter()) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), ty);
            assert_eq!(field.is_nullable(), *nullable, "column {name}");
        }
    }

    #[test]
    fn row_to_record_batch_round_trips_values() {
        let row = MaintenanceLogRow {
            job_id: "job-1".to_string(),
            table: "ns.orders".to_string(),
            trigger: "scheduler".to_string(),
            principal: "svc".to_string(),
            started_at_ms: 100,
            finished_at_ms: 200,
            status: "advisory".to_string(),
            files_in: 5,
            files_out: 0,
            bytes_in: 500,
            bytes_out: 0,
            rows_removed: 0,
            snapshot_id: None,
            error: None,
        };
        let batch = row_to_record_batch(&row).expect("build batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);

        let job_id = batch
            .column_by_name("job_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(job_id.value(0), "job-1");

        let files_in = batch
            .column_by_name("files_in")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(files_in.value(0), 5);

        let snapshot_id = batch.column_by_name("snapshot_id").unwrap();
        assert!(snapshot_id.is_null(0), "None must round-trip as a null cell");

        let error = batch.column_by_name("error").unwrap();
        assert!(error.is_null(0), "None must round-trip as a null cell");
    }

    #[test]
    fn row_to_record_batch_keeps_some_snapshot_and_error() {
        let mut row = MaintenanceLogRow {
            job_id: "job-1".to_string(),
            table: "ns.orders".to_string(),
            trigger: "scheduler".to_string(),
            principal: "svc".to_string(),
            started_at_ms: 100,
            finished_at_ms: 200,
            status: "success".to_string(),
            files_in: 5,
            files_out: 1,
            bytes_in: 500,
            bytes_out: 100,
            rows_removed: 3,
            snapshot_id: Some(42),
            error: Some("boom".to_string()),
        };
        let batch = row_to_record_batch(&row).expect("build batch");

        let snapshot_id = batch
            .column_by_name("snapshot_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(snapshot_id.value(0), 42);

        let error = batch
            .column_by_name("error")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(error.value(0), "boom");

        // Sanity: mutating the row after building the batch does not affect
        // the already-built batch (no accidental aliasing).
        row.error = Some("changed".to_string());
        assert_eq!(error.value(0), "boom");
    }

    #[test]
    fn resolve_state_table_ident_parses_ns_dot_table() {
        let ident = resolve_state_table_ident("myns.mytable");
        assert_eq!(ident.namespace().as_ref(), &vec!["myns".to_string()]);
        assert_eq!(ident.name(), "mytable");
    }

    #[test]
    fn resolve_state_table_ident_defaults_when_empty() {
        let ident = resolve_state_table_ident("");
        assert_eq!(ident.namespace().as_ref(), &vec!["sqe_system".to_string()]);
        assert_eq!(ident.name(), "maintenance_log");
    }

    #[test]
    fn resolve_state_table_ident_defaults_when_malformed() {
        let ident = resolve_state_table_ident("no_dot_here");
        assert_eq!(ident.namespace().as_ref(), &vec!["sqe_system".to_string()]);
        assert_eq!(ident.name(), "maintenance_log");
    }

    #[test]
    fn resolve_state_table_ident_defaults_when_only_dot() {
        // "." alone: namespace and name both empty -> falls back to default
        // rather than producing a TableIdent with an empty name.
        let ident = resolve_state_table_ident(".");
        assert_eq!(ident.namespace().as_ref(), &vec!["sqe_system".to_string()]);
        assert_eq!(ident.name(), "maintenance_log");
    }
}
