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
//! # Also the lease table (Phase 4d, Task 2)
//!
//! `crate::maintenance_lease` stores the multi-coordinator HA lease as rows
//! in this SAME table (`trigger = "lease"`, `table_name` holding the lease's
//! `job_key`), rather than a dedicated lease table. See that module's docs
//! for the full design; this module only needs to know two things: (1)
//! [`resolve_state_table_ident`] and [`row_to_record_batch`] are `pub(crate)`
//! specifically so the lease module can resolve the same `state_table`
//! config value and build rows with the same fixed column order without
//! duplicating either; (2) no schema change was needed to add the lease --
//! it fits entirely inside the 14 columns below via the `trigger`/`status`
//! values `maintenance_lease` uses, so the operator's `CREATE TABLE` DDL is
//! unchanged.
//!
//! **Reader warning:** `sqe_system.maintenance_log` therefore holds TWO kinds
//! of row that look identical to a naive scan -- real job history
//! (`trigger` in `"scheduled"`/`"scheduler"`, appended by [`append_row`]) and
//! lease bookkeeping (`trigger = "lease"`, appended/updated by
//! `maintenance_lease.rs`'s claim/renew/release/steal). Nothing in this
//! module or `maintenance_lease.rs` reads the table back for reporting
//! today, so this has never mattered in practice -- but ANY future
//! read-back consumer (a `SHOW MAINTENANCE HISTORY`-style surface, a
//! last-run-per-table dedupe query, a debt/health dashboard, etc.) MUST
//! filter `trigger <> 'lease'` before treating a row as compaction job
//! history, or it will misreport lease claim/release churn as compaction
//! runs (inflated job counts, bogus `files_in`/`files_out`/`bytes_*`
//! entries -- lease rows zero those fields but a naive `SUM`/`COUNT` still
//! counts the row). See `maintenance_lease.rs`'s `LEASE_TRIGGER` constant
//! for the exact sentinel value to exclude.
//!
//! # Retention: one file per day, and only for aged history
//!
//! Because an append is one row in one file, this table grows by one file
//! per advisory/skip/job row and per lease operation, forever (review
//! finding 511). [`compact_history`] is the counterweight: rows older than
//! `retention_days` are rewritten into one data file per UTC day, in a
//! single `rewrite_files` CAS commit. It never drops a row, and it never
//! touches a file that holds a lease row or a recent row -- see that
//! function's docs. The scheduler calls it on a daily cadence; an operator
//! can call it directly with any `retention_days`.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{DataContentType, DataFile, ManifestStatus, PrimitiveLiteral};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};
use tracing::{debug, info, warn};
use uuid::Uuid;

use sqe_core::{SqeError, SqeErrorCode};

use crate::catalog_ops::is_namespace_not_found;
use crate::table_health::TableHealth;
use crate::write_handler::commit_with_retry;
use crate::writer::{
    new_upload_tracker, parse_parquet_compression, write_data_files, WriteCleanupGuard,
};

/// Default `ns.table` for the ledger, matching
/// `sqe_core::config::MaintenanceSchedulerConfig::state_table`'s default.
pub const DEFAULT_MAINTENANCE_LOG_TABLE: &str = "sqe_system.maintenance_log";

/// One row of the `sqe_system.maintenance_log` ledger.
///
/// See the module docs for the fixed Arrow/Iceberg column order this maps
/// to. Count fields are `i64` (not `u64`): Iceberg's `long` type is signed,
/// and there is no unsigned Arrow integer type this table's schema could
/// declare from SQL.
///
/// WARNING for future read-back consumers: a `MaintenanceLogRow` you build
/// here is always real job history, but the underlying Iceberg table also
/// carries `maintenance_lease.rs`'s lease bookkeeping rows (`trigger =
/// "lease"`). Any code that reads `sqe_system.maintenance_log` BACK (a
/// `SHOW`-style surface, last-run-per-table dedupe, a history/debt report)
/// must filter `trigger <> 'lease'` first -- see the module doc's "Reader
/// warning" for why a naive scan misreports lease churn as compaction runs.
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
pub fn advisory_row(
    table: &str,
    principal: &str,
    health: &TableHealth,
    ts_ms: i64,
) -> MaintenanceLogRow {
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

/// Build the `status = "running"` row a (later) caller could append at the
/// start of an active-mode compaction job, before the rewrite commits, so an
/// operator watching the ledger mid-run can distinguish "in flight" from
/// "never started" / "crashed before any row landed". Not yet called by the
/// Phase 4b scheduler (see `maintenance_scheduler.rs`'s active arm, which
/// only appends the terminal `success`/`failed`/`skipped` row); provided now
/// so the constructor's field mapping is fixed and unit-tested ahead of that
/// wiring.
///
/// `started_at_ms` and `finished_at_ms` are both `ts_ms`: the row is a
/// snapshot of an in-progress job, not a completed one, so "finished" has no
/// real value yet. `trigger` is `"scheduled"` (Phase 4b active-mode jobs),
/// distinct from [`advisory_row`]'s pre-existing `"scheduler"` (Phase 4a).
pub fn running_row(job_id: &str, table: &str, principal: &str, ts_ms: i64) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: job_id.to_string(),
        table: table.to_string(),
        trigger: "scheduled".to_string(),
        principal: principal.to_string(),
        started_at_ms: ts_ms,
        finished_at_ms: ts_ms,
        status: "running".to_string(),
        files_in: 0,
        files_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        rows_removed: 0,
        snapshot_id: None,
        error: None,
    }
}

/// Build the `status = "success"` row the Phase 4b active-mode scheduler
/// records for one table it actually compacted. `job_id` ties this row back
/// to the same job's audit event and metric samples.
#[allow(clippy::too_many_arguments)]
pub fn success_row(
    job_id: &str,
    table: &str,
    principal: &str,
    started_at_ms: i64,
    finished_at_ms: i64,
    files_in: i64,
    files_out: i64,
    bytes_in: i64,
    bytes_out: i64,
    rows_removed: i64,
    snapshot_id: Option<i64>,
) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: job_id.to_string(),
        table: table.to_string(),
        trigger: "scheduled".to_string(),
        principal: principal.to_string(),
        started_at_ms,
        finished_at_ms,
        status: "success".to_string(),
        files_in,
        files_out,
        bytes_in,
        bytes_out,
        rows_removed,
        snapshot_id,
        error: None,
    }
}

/// Build the `status = "failed"` row the Phase 4b active-mode scheduler
/// records when a table's compaction job raises an error (mint/refresh
/// failure, catalog error, or the rewrite itself failing). Counts are always
/// zero: a failed job has no reliable partial byte/file/row counts to
/// report, and reporting a partial number invites an operator to
/// misinterpret it as a completed rewrite's stats.
pub fn failed_row(
    job_id: &str,
    table: &str,
    principal: &str,
    started_at_ms: i64,
    finished_at_ms: i64,
    error: &str,
) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: job_id.to_string(),
        table: table.to_string(),
        trigger: "scheduled".to_string(),
        principal: principal.to_string(),
        started_at_ms,
        finished_at_ms,
        status: "failed".to_string(),
        files_in: 0,
        files_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        rows_removed: 0,
        snapshot_id: None,
        error: Some(error.to_string()),
    }
}

/// Build the `status = "partial"` row the active-mode scheduler records
/// (Phase 4d Task 3) when a distributed rewrite ran with
/// `distribution.partial_progress` opted in and a terminal group failure hit
/// after one or more batches had already committed. Unlike [`failed_row`],
/// the count fields here are real and non-zero: they reflect exactly the
/// batches that committed before the job stopped (see
/// `maintenance::RewriteOutcome`'s `partial`/`partial_error` fields). `error`
/// carries the terminal failure's message, same convention as
/// [`skipped_row`]'s `reason`.
#[allow(clippy::too_many_arguments)]
pub fn partial_row(
    job_id: &str,
    table: &str,
    principal: &str,
    started_at_ms: i64,
    finished_at_ms: i64,
    files_in: i64,
    files_out: i64,
    bytes_in: i64,
    bytes_out: i64,
    rows_removed: i64,
    snapshot_id: Option<i64>,
    error: &str,
) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: job_id.to_string(),
        table: table.to_string(),
        trigger: "scheduled".to_string(),
        principal: principal.to_string(),
        started_at_ms,
        finished_at_ms,
        status: "partial".to_string(),
        files_in,
        files_out,
        bytes_in,
        bytes_out,
        rows_removed,
        snapshot_id,
        error: Some(error.to_string()),
    }
}

/// Build the `status = "skipped"` row the Phase 4b active-mode scheduler
/// records for a due, opted-in table it deliberately did not compact (no
/// eligible compaction debt this tick). Distinct from [`advisory_row`]:
/// this row means "active mode considered this table and chose not to act",
/// not "advisory mode never acts on anything". `reason` is carried in the
/// `error` column -- the fixed schema has no dedicated "reason" column, and
/// "why nothing happened" is exactly what that column is for on a
/// non-`success` row.
pub fn skipped_row(
    job_id: &str,
    table: &str,
    principal: &str,
    ts_ms: i64,
    reason: &str,
) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: job_id.to_string(),
        table: table.to_string(),
        trigger: "scheduled".to_string(),
        principal: principal.to_string(),
        started_at_ms: ts_ms,
        finished_at_ms: ts_ms,
        status: "skipped".to_string(),
        files_in: 0,
        files_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        rows_removed: 0,
        snapshot_id: None,
        error: Some(reason.to_string()),
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
pub(crate) fn row_to_record_batch(row: &MaintenanceLogRow) -> sqe_core::Result<RecordBatch> {
    rows_to_record_batch(std::slice::from_ref(row))
}

/// Shape N `MaintenanceLogRow`s into one `RecordBatch` in the fixed column
/// order the module docs pin down.
///
/// [`append_row`] always passes exactly one row (one row per file is what
/// makes an append cheap and conflict-free); [`compact_history`] passes a
/// whole day's rows at once, which is the entire point of the retention
/// pass.
pub(crate) fn rows_to_record_batch(rows: &[MaintenanceLogRow]) -> sqe_core::Result<RecordBatch> {
    RecordBatch::try_new(
        maintenance_log_arrow_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.job_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.table.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.trigger.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.started_at_ms),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.finished_at_ms),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.status.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.files_in),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.files_out),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.bytes_in),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.bytes_out),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.rows_removed),
            )),
            Arc::new(rows.iter().map(|r| r.snapshot_id).collect::<Int64Array>()),
            Arc::new(
                rows.iter()
                    .map(|r| r.error.clone())
                    .collect::<StringArray>(),
            ),
        ],
    )
    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("maintenance_log: failed to build row batch: {e}")))
}

/// Resolve `state_table` (a plain `ns.table` string, e.g. the
/// `[maintenance] state_table` config value) into a `TableIdent`.
///
/// Falls back to [`DEFAULT_MAINTENANCE_LOG_TABLE`] when `state_table` is
/// empty or has no `.` qualifier, rather than guessing at a namespace.
///
/// `pub(crate)` so `maintenance_lease.rs` can resolve the same
/// `state_table` config value to the same `TableIdent` the ledger writer
/// uses: the lease lives in this table too (see `maintenance_lease.rs`'s
/// module docs), and the two resolvers must never diverge.
pub(crate) fn resolve_state_table_ident(state_table: &str) -> TableIdent {
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
        SqeError::Catalog(format!(
            "maintenance_log: failed to load state table '{ident}': {e}"
        ))
    })?;

    let batch = row_to_record_batch(row)?;
    let tracker = new_upload_tracker();
    let cleanup_guard = WriteCleanupGuard::new(
        table.file_io().clone(),
        tracker.clone(),
        "maintenance-log-append",
    );
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
    commit_with_retry(
        catalog.as_ref(),
        &ident,
        "maintenance-log-append",
        move |fresh_table| {
            let files = files_for_retry.clone();
            let cat = catalog_for_commit.clone();
            async move {
                let tx = Transaction::new(&fresh_table);
                let action = tx.fast_append().add_data_files(files);
                let tx = action.apply(tx)?;
                tx.commit(cat.as_ref()).await
            }
        },
    )
    .await
    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("maintenance_log: failed to commit append: {e}")))?;
    cleanup_guard.mark_committed();

    Ok(())
}

/// Milliseconds in one UTC day: the retention pass's bucket width.
const MS_PER_DAY: i64 = 86_400_000;

/// The UTC day number `ts_ms` falls in. `div_euclid`, not `/`, so
/// pre-epoch timestamps bucket downward instead of toward zero.
fn day_of(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(MS_PER_DAY)
}

/// What one [`compact_history`] pass did. `files_in == 0` means it found
/// nothing worth rewriting, which is the steady state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistoryRetentionOutcome {
    /// Ledger data files deleted (rewritten away).
    pub files_in: usize,
    /// Data files written in their place: one per UTC day, normally.
    pub files_out: usize,
    /// Rows carried across. Always equal to the rows in `files_in`:
    /// retention COMPACTS history, it never drops a row.
    pub rows: usize,
    /// Distinct UTC days those rows fell in.
    pub days: usize,
}

impl HistoryRetentionOutcome {
    pub fn is_noop(&self) -> bool {
        self.files_in == 0
    }
}

/// Collect every live (non-deleted, `DataContentType::Data`) data file in
/// `table`'s current snapshot.
///
/// `pub(crate)` because both readers of this table need it and must agree:
/// [`compact_history`] here, and `crate::maintenance_lease`'s
/// claim/renew/release, which re-derives the current lease state the same
/// way. Sequential rather than concurrency-limited: this is a small
/// control-plane table, not a hot read path.
pub(crate) async fn collect_live_data_files(
    table: &IcebergTable,
) -> sqe_core::Result<Vec<DataFile>> {
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else {
        return Ok(vec![]);
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| {
            SqeError::Execution(format!(
                "maintenance_log: failed to load manifest list: {e}"
            ))
        })?;

    let mut data_files = Vec::new();
    for mf in manifest_list.entries() {
        let manifest = cache.get_manifest(mf).await.map_err(|e| {
            SqeError::Execution(format!("maintenance_log: failed to load manifest: {e}"))
        })?;
        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Deleted
                && entry.data_file().content_type() == DataContentType::Data
            {
                data_files.push(entry.data_file().clone());
            }
        }
    }
    Ok(data_files)
}

/// Read one ledger data file's `RecordBatch`es via the table's `FileIO`.
///
/// `pub(crate)` for the same reason as [`collect_live_data_files`]:
/// `maintenance_lease` decodes rows out of this table too, and the two
/// readers must not drift apart.
pub(crate) async fn read_batches(
    table: &IcebergTable,
    file_path: &str,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let file_io = table.file_io();
    let input = file_io.new_input(file_path).map_err(|e| {
        SqeError::Execution(format!(
            "maintenance_log: failed to open '{file_path}': {e}"
        ))
    })?;
    let bytes = input.read().await.map_err(|e| {
        SqeError::Execution(format!(
            "maintenance_log: failed to read '{file_path}': {e}"
        ))
    })?;
    let reader = parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(bytes).map_err(|e| {
        SqeError::Execution(format!(
            "maintenance_log: failed to open parquet reader for '{file_path}': {e}"
        ))
    })?;
    let reader = reader.build().map_err(|e| {
        SqeError::Execution(format!(
            "maintenance_log: failed to build parquet reader for '{file_path}': {e}"
        ))
    })?;
    reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            SqeError::Execution(format!(
                "maintenance_log: failed to decode '{file_path}': {e}"
            ))
        })
}

/// Field id of `name` in the state table's current schema, if it has such a
/// column. Used to look up per-file column bounds, which are keyed on field
/// id (never on column name).
pub(crate) fn field_id(table: &IcebergTable, name: &str) -> Option<i32> {
    table
        .metadata()
        .current_schema()
        .field_by_name(name)
        .map(|f| f.id)
}

/// This file's `(lower, upper)` bounds for a `STRING` column, when the
/// writer recorded them. `None` means "unknown", never "empty range": a
/// caller must fall back to reading the file rather than assume anything.
pub(crate) fn string_bounds(df: &DataFile, field_id: i32) -> Option<(&str, &str)> {
    let lower = df.lower_bounds().get(&field_id)?;
    let upper = df.upper_bounds().get(&field_id)?;
    match (lower.literal(), upper.literal()) {
        (PrimitiveLiteral::String(lo), PrimitiveLiteral::String(hi)) => {
            Some((lo.as_str(), hi.as_str()))
        }
        _ => None,
    }
}

/// This file's `(lower, upper)` bounds for a `BIGINT` column, when the
/// writer recorded them. Same "`None` means unknown" contract as
/// [`string_bounds`].
fn i64_bounds(df: &DataFile, field_id: i32) -> Option<(i64, i64)> {
    let lower = df.lower_bounds().get(&field_id)?;
    let upper = df.upper_bounds().get(&field_id)?;
    match (lower.literal(), upper.literal()) {
        (PrimitiveLiteral::Long(lo), PrimitiveLiteral::Long(hi)) => Some((*lo, *hi)),
        _ => None,
    }
}

/// Decode every row of one ledger `RecordBatch`.
///
/// Strict on the non-nullable columns: a null where the fixed schema says
/// NOT NULL is an error, not a defaulted value. The only caller is the
/// retention pass, which REWRITES what it decodes -- silently turning a
/// null into `""`/`0` there would be data loss dressed up as robustness.
/// The caller skips the whole file instead, leaving it exactly as it is.
fn batch_to_rows(batch: &RecordBatch, file_path: &str) -> sqe_core::Result<Vec<MaintenanceLogRow>> {
    let str_col = |name: &str| -> sqe_core::Result<&StringArray> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "maintenance_log: '{file_path}' has no Utf8 column '{name}'"
                ))
            })
    };
    let i64_col = |name: &str| -> sqe_core::Result<&Int64Array> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "maintenance_log: '{file_path}' has no Int64 column '{name}'"
                ))
            })
    };

    let job_id = str_col("job_id")?;
    let table_name = str_col("table_name")?;
    let trigger = str_col("trigger")?;
    let principal = str_col("principal")?;
    let started_at_ms = i64_col("started_at_ms")?;
    let finished_at_ms = i64_col("finished_at_ms")?;
    let status = str_col("status")?;
    let files_in = i64_col("files_in")?;
    let files_out = i64_col("files_out")?;
    let bytes_in = i64_col("bytes_in")?;
    let bytes_out = i64_col("bytes_out")?;
    let rows_removed = i64_col("rows_removed")?;
    let snapshot_id = i64_col("snapshot_id")?;
    let error = str_col("error")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let required_null = job_id.is_null(i)
            || table_name.is_null(i)
            || trigger.is_null(i)
            || principal.is_null(i)
            || started_at_ms.is_null(i)
            || finished_at_ms.is_null(i)
            || status.is_null(i)
            || files_in.is_null(i)
            || files_out.is_null(i)
            || bytes_in.is_null(i)
            || bytes_out.is_null(i)
            || rows_removed.is_null(i);
        if required_null {
            return Err(SqeError::Execution(format!(
                "maintenance_log: '{file_path}' row {i} has a null in a NOT NULL column"
            )));
        }
        out.push(MaintenanceLogRow {
            job_id: job_id.value(i).to_string(),
            table: table_name.value(i).to_string(),
            trigger: trigger.value(i).to_string(),
            principal: principal.value(i).to_string(),
            started_at_ms: started_at_ms.value(i),
            finished_at_ms: finished_at_ms.value(i),
            status: status.value(i).to_string(),
            files_in: files_in.value(i),
            files_out: files_out.value(i),
            bytes_in: bytes_in.value(i),
            bytes_out: bytes_out.value(i),
            rows_removed: rows_removed.value(i),
            snapshot_id: if snapshot_id.is_null(i) {
                None
            } else {
                Some(snapshot_id.value(i))
            },
            error: if error.is_null(i) {
                None
            } else {
                Some(error.value(i).to_string())
            },
        });
    }
    Ok(out)
}

/// Widest `started_at_ms` day span a retention pass will take from manifest
/// bounds instead of reading the file. A ledger file normally holds one row
/// (one instant) or one day's rows; anything spanning more than a month is
/// odd enough that reading it is cheaper than materialising its day range.
const MAX_BOUNDS_DAY_SPAN: i64 = 31;

/// Most files one retention pass rewrites. Bounds the pass's memory and its
/// commit size; a bigger backlog drains over successive daily passes (see
/// the truncation site for why any subset is safe).
const MAX_FILES_PER_PASS: usize = 5_000;

/// One aged, lease-free ledger file being considered for rewrite, plus the
/// UTC days its rows fall in. `rows` is `Some` only when the file had to be
/// read to work those days out; when the manifest bounds answered, it stays
/// `None` until (and unless) the file is actually selected.
struct Candidate {
    df: DataFile,
    days: Vec<i64>,
    rows: Option<Vec<MaintenanceLogRow>>,
}

/// Read and decode every row of one ledger file, or `None` (with a warning)
/// if it cannot be read or decoded. A file this pass cannot fully decode is
/// a file it must not rewrite.
async fn read_ledger_rows(table: &IcebergTable, path: &str) -> Option<Vec<MaintenanceLogRow>> {
    let batches = match read_batches(table, path).await {
        Ok(b) => b,
        Err(e) => {
            warn!(
                file = %path,
                error = %e,
                "maintenance_log retention: unreadable ledger file, leaving it alone"
            );
            return None;
        }
    };
    let mut rows = Vec::new();
    for batch in &batches {
        match batch_to_rows(batch, path) {
            Ok(mut r) => rows.append(&mut r),
            Err(e) => {
                warn!(
                    file = %path,
                    error = %e,
                    "maintenance_log retention: undecodable ledger file, leaving it alone"
                );
                return None;
            }
        }
    }
    Some(rows)
}

/// True when every row is old enough to rewrite and none is lease
/// bookkeeping. The decoded-row authority behind the manifest-bounds
/// shortcuts: a file is rewritten only if this says so.
fn rows_are_rewritable(rows: &[MaintenanceLogRow], cutoff_ms: i64) -> bool {
    !rows.iter().any(|r| {
        r.trigger == crate::maintenance_lease::LEASE_TRIGGER || r.started_at_ms >= cutoff_ms
    })
}

/// Pick the files a retention pass rewrites, given each candidate file's
/// path and the UTC days its rows fall in.
///
/// A day is worth compacting when more than one file holds rows for it;
/// then EVERY candidate file touching such a day is rewritten, including
/// the days-with-one-file it also happens to touch. That "including" is
/// load-bearing: a rewritten file is deleted whole, so every row in it has
/// to be re-added, and a rule that only re-added the crowded days' rows
/// would silently drop the rest.
///
/// Idempotent by construction: after a pass, each day has exactly one file,
/// so no day has more than one and the next pass rewrites nothing.
fn plan_history_rewrite(files: &[(String, Vec<i64>)]) -> HashSet<String> {
    let mut files_per_day: HashMap<i64, HashSet<&str>> = HashMap::new();
    for (path, days) in files {
        for day in days {
            files_per_day.entry(*day).or_default().insert(path.as_str());
        }
    }
    let crowded_days: HashSet<i64> = files_per_day
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .map(|(day, _)| day)
        .collect();

    files
        .iter()
        .filter(|(_, days)| days.iter().any(|d| crowded_days.contains(d)))
        .map(|(path, _)| path.clone())
        .collect()
}

/// Retention pass for the ledger: rewrite `maintenance_log` rows older than
/// `retention_days` into one data file per UTC day (finding 511).
///
/// # Why this exists
///
/// Every append here is one Parquet FILE (one row). An idle fleet still
/// writes an advisory/skipped row per due table per tick, and every lease
/// claim/renew/release is another. Nothing expired them, and
/// `maintenance_lease` re-derives lease state by scanning live files, so
/// unbounded file growth turned lease acquisition -- on the critical path
/// of every scheduled compaction -- into a linearly slowing scan.
///
/// # What it will not touch
///
/// - Rows younger than `retention_days`, and any file that holds one. A
///   file is rewritten only when ALL of its rows are older than the cutoff.
/// - Lease rows (`trigger = "lease"`), and any file holding one, at all.
///   `crate::maintenance_lease`'s CAS deletes a claim by exact file path;
///   moving a live claim into a day bundle behind its back would break the
///   next renew/release. Lease rows are self-limiting anyway (one live
///   claim per table, replaced in place).
/// - Anything, when no UTC day has more than one file. See
///   [`plan_history_rewrite`].
///
/// # Rows survive
///
/// This is a compaction, not an expiry: every row in every deleted file is
/// re-written into its day's file. Nothing is dropped, so no operator's
/// history disappears because a retention job ran.
///
/// # Cost
///
/// Planning runs off the manifest's per-file column bounds
/// (`started_at_ms` for the day buckets, `trigger` for lease-freeness), so
/// a pass reads only the files it is about to rewrite. The steady state --
/// a ledger that is already one file per day -- therefore costs zero file
/// reads, which is the difference between a daily housekeeping pass and a
/// daily full-table decode of a table that only grows. Files whose writer
/// recorded no bounds fall back to being read, as before. One pass rewrites
/// at most [`MAX_FILES_PER_PASS`] files; a larger backlog drains over
/// successive daily passes.
///
/// # Concurrency
///
/// The commit is a `rewrite_files` with `set_check_file_existence(true)` --
/// the same primitive the lease CAS uses. Two coordinators running this
/// pass at the same time cannot both win: the loser sees
/// `ErrorKind::DataInvalid`, cleans up the files it wrote, and reports a
/// no-op. No lease is taken, deliberately: losing this race costs one
/// wasted pass, and the pass is not on any critical path.
///
/// Best-effort like every other operation in this module: a missing state
/// table (or namespace) is a no-op, not an error.
pub async fn compact_history(
    catalog: &Arc<dyn Catalog>,
    state_table: &str,
    retention_days: i64,
    now_ms: i64,
) -> sqe_core::Result<HistoryRetentionOutcome> {
    let ident = resolve_state_table_ident(state_table);
    match catalog.table_exists(&ident).await {
        Ok(true) => {}
        Ok(false) => {
            debug!(
                table = %ident,
                "maintenance_log retention: state table absent, nothing to compact"
            );
            return Ok(HistoryRetentionOutcome::default());
        }
        Err(e) if is_namespace_not_found(&e) => {
            debug!(
                table = %ident,
                error = %e,
                "maintenance_log retention: state table's namespace absent, nothing to compact"
            );
            return Ok(HistoryRetentionOutcome::default());
        }
        Err(e) => {
            return Err(SqeError::Catalog(format!(
                "maintenance_log retention: failed to check existence of state table '{ident}': {e}"
            )));
        }
    }

    let table = catalog.load_table(&ident).await.map_err(|e| {
        SqeError::Catalog(format!(
            "maintenance_log retention: failed to load state table '{ident}': {e}"
        ))
    })?;

    let cutoff_ms = now_ms - retention_days.max(0) * MS_PER_DAY;
    let trigger_fid = field_id(&table, "trigger");
    let started_fid = field_id(&table, "started_at_ms");

    // --- Plan from manifest bounds; read only what will be rewritten ---
    //
    // Planning needs one thing per file: which UTC days its rows fall in.
    // The manifest already answers that for any file whose writer recorded
    // `started_at_ms` bounds, so the steady state (a ledger that is already
    // one file per day) costs zero file reads -- which matters, because
    // this runs daily forever against a table that only grows.
    let mut candidates: Vec<Candidate> = Vec::new();
    for df in collect_live_data_files(&table).await? {
        let path = df.file_path().to_string();
        let started_bounds = started_fid.and_then(|fid| i64_bounds(&df, fid));
        if let Some((lower_ms, _)) = started_bounds {
            if lower_ms >= cutoff_ms {
                // Every row is newer than the cutoff.
                continue;
            }
        }
        // A file whose `trigger` bounds do not PROVE it lease-free may hold
        // a lease row, and retention never rewrites one of those. Asked via
        // the lease module's own predicate so the two modules cannot answer
        // "could this file hold a lease row?" differently. Absent bounds
        // fall through to the decoded-row check, which is the authority
        // either way.
        let trigger_bounds_known = trigger_fid
            .and_then(|fid| string_bounds(&df, fid))
            .is_some();
        let proven_lease_free = trigger_fid
            .is_some_and(|fid| crate::maintenance_lease::file_excludes_lease_rows(&df, fid));
        if trigger_bounds_known && !proven_lease_free {
            continue;
        }

        // Days from bounds when they prove the whole file is aged and
        // lease-free and spans a sane number of days; otherwise read it.
        let days_from_bounds = match started_bounds {
            Some((lower_ms, upper_ms))
                if proven_lease_free
                    && upper_ms < cutoff_ms
                    && day_of(upper_ms) - day_of(lower_ms) <= MAX_BOUNDS_DAY_SPAN =>
            {
                // The day RANGE, not the days that actually have rows: an
                // over-counted day can only make a file look worth
                // rewriting, never make one that must be rewritten look
                // safe to leave, and the pass converges either way.
                Some((day_of(lower_ms)..=day_of(upper_ms)).collect::<Vec<i64>>())
            }
            _ => None,
        };

        match days_from_bounds {
            Some(days) => candidates.push(Candidate {
                df,
                days,
                rows: None,
            }),
            None => {
                let Some(rows) = read_ledger_rows(&table, &path).await else {
                    continue;
                };
                if rows.is_empty() || !rows_are_rewritable(&rows, cutoff_ms) {
                    continue;
                }
                let days = rows.iter().map(|r| day_of(r.started_at_ms)).collect();
                candidates.push(Candidate {
                    df,
                    days,
                    rows: Some(rows),
                });
            }
        }
    }

    let per_file_days: Vec<(String, Vec<i64>)> = candidates
        .iter()
        .map(|c| (c.df.file_path().to_string(), c.days.clone()))
        .collect();
    let mut to_rewrite = plan_history_rewrite(&per_file_days);
    if to_rewrite.is_empty() {
        return Ok(HistoryRetentionOutcome::default());
    }
    if to_rewrite.len() > MAX_FILES_PER_PASS {
        // Bound one pass's memory and commit size. Any SUBSET of files is
        // safe to rewrite on its own (whole files in, all their rows out),
        // and the days left over stay crowded, so the next pass picks them
        // up: a large backlog drains over several days instead of in one
        // enormous commit.
        let mut capped: Vec<String> = to_rewrite.into_iter().collect();
        capped.sort();
        capped.truncate(MAX_FILES_PER_PASS);
        info!(
            table = %ident,
            capped_at = MAX_FILES_PER_PASS,
            "maintenance_log retention: more aged files than one pass rewrites; draining the rest on later passes"
        );
        to_rewrite = capped.into_iter().collect();
    }

    let mut rows_by_day: BTreeMap<i64, Vec<MaintenanceLogRow>> = BTreeMap::new();
    let mut delete_targets: Vec<DataFile> = Vec::new();
    let mut rows_total = 0usize;
    for candidate in candidates {
        if !to_rewrite.contains(candidate.df.file_path()) {
            continue;
        }
        let path = candidate.df.file_path().to_string();
        // Read now if planning got by on bounds alone, and re-check the
        // decoded rows either way: bounds are a planning shortcut, the rows
        // are the authority on "aged and lease-free", and this is the last
        // point before a file is deleted.
        let rows = match candidate.rows {
            Some(rows) => rows,
            None => match read_ledger_rows(&table, &path).await {
                Some(rows) => rows,
                None => continue,
            },
        };
        if rows.is_empty() || !rows_are_rewritable(&rows, cutoff_ms) {
            warn!(
                file = %path,
                "maintenance_log retention: file's rows disagree with its manifest bounds; leaving it alone"
            );
            continue;
        }
        for row in rows {
            rows_total += 1;
            rows_by_day
                .entry(day_of(row.started_at_ms))
                .or_default()
                .push(row);
        }
        delete_targets.push(candidate.df);
    }

    if delete_targets.is_empty() {
        return Ok(HistoryRetentionOutcome::default());
    }

    // One `write_data_files` call per day: the rolling writer rolls on
    // SIZE, so handing it every day's batches at once would produce one
    // file for all of them, not one per day.
    let tracker = new_upload_tracker();
    let cleanup_guard = WriteCleanupGuard::new(
        table.file_io().clone(),
        tracker.clone(),
        "maintenance-log-retention",
    );
    let compression = parse_parquet_compression("zstd");
    let days = rows_by_day.len();
    let mut new_files: Vec<DataFile> = Vec::new();
    for (_day, rows) in rows_by_day {
        let batch = rows_to_record_batch(&rows)?;
        let written = write_data_files(
            &table,
            vec![batch],
            "maintenance-log-retention",
            compression,
            tracker.clone(),
        )
        .await?;
        new_files.extend(written);
    }

    if new_files.is_empty() {
        // Defensive: `rows_by_day` is non-empty here, so this cannot
        // happen; leaving the ledger untouched is the safe reading of it if
        // it ever does.
        return Ok(HistoryRetentionOutcome::default());
    }

    let files_in = delete_targets.len();
    let files_out = new_files.len();
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_files()
        .delete_files(delete_targets)
        .add_data_files(new_files)
        .set_check_file_existence(true);
    let tx = action.apply(tx).map_err(|e| {
        SqeError::Execution(format!(
            "maintenance_log retention: failed to apply rewrite: {e}"
        ))
    })?;
    match tx.commit(catalog.as_ref()).await {
        Ok(_) => {
            cleanup_guard.mark_committed();
            info!(
                table = %ident,
                files_in,
                files_out,
                rows = rows_total,
                days,
                "maintenance_log retention: compacted aged ledger history"
            );
            Ok(HistoryRetentionOutcome {
                files_in,
                files_out,
                rows: rows_total,
                days,
            })
        }
        Err(e) if e.kind() == ErrorKind::DataInvalid => {
            // Another coordinator's pass deleted these files first. Not an
            // error: drop the cleanup guard unmarked so the files this one
            // wrote are removed, and report a no-op.
            debug!(
                table = %ident,
                error = %e,
                "maintenance_log retention: lost the rewrite race to another coordinator"
            );
            Ok(HistoryRetentionOutcome::default())
        }
        Err(e) => Err(SqeError::Execution(format!(
            "maintenance_log retention: failed to commit rewrite: {e}"
        ))),
    }
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
        assert_eq!(
            row.bytes_in,
            (health.avg_file_bytes * health.live_data_files) as i64
        );
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
        assert!(
            snapshot_id.is_null(0),
            "None must round-trip as a null cell"
        );

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
    fn running_row_maps_status_and_trigger() {
        let row = running_row("job-1", "ns.t", "svc", 1_000);
        assert_eq!(row.status, "running");
        assert_eq!(row.trigger, "scheduled");
        assert_eq!(row.job_id, "job-1");
        assert_eq!(row.table, "ns.t");
        assert_eq!(row.principal, "svc");
        assert_eq!(row.started_at_ms, 1_000);
        assert_eq!(row.finished_at_ms, 1_000);
    }

    #[test]
    fn running_row_zeroes_counts_and_has_no_snapshot_or_error() {
        let row = running_row("job-1", "ns.t", "svc", 1_000);
        assert_eq!(row.files_in, 0);
        assert_eq!(row.files_out, 0);
        assert_eq!(row.bytes_in, 0);
        assert_eq!(row.bytes_out, 0);
        assert_eq!(row.rows_removed, 0);
        assert_eq!(row.snapshot_id, None);
        assert_eq!(row.error, None);
    }

    #[test]
    fn success_row_maps_status_trigger_and_all_counts() {
        let row = success_row(
            "job-2",
            "ns.orders",
            "svc",
            100,
            200,
            10,
            3,
            1000,
            300,
            42,
            Some(99),
        );
        assert_eq!(row.status, "success");
        assert_eq!(row.trigger, "scheduled");
        assert_eq!(row.job_id, "job-2");
        assert_eq!(row.table, "ns.orders");
        assert_eq!(row.principal, "svc");
        assert_eq!(row.started_at_ms, 100);
        assert_eq!(row.finished_at_ms, 200);
        assert_eq!(row.files_in, 10);
        assert_eq!(row.files_out, 3);
        assert_eq!(row.bytes_in, 1000);
        assert_eq!(row.bytes_out, 300);
        assert_eq!(row.rows_removed, 42);
        assert_eq!(row.snapshot_id, Some(99));
        assert_eq!(row.error, None);
    }

    #[test]
    fn success_row_allows_none_snapshot_id() {
        // Defensive: a caller could construct a success row before it has
        // resolved a snapshot id (should not happen in practice, but the
        // constructor must not silently coerce None into something else).
        let row = success_row(
            "job-2",
            "ns.orders",
            "svc",
            100,
            200,
            10,
            3,
            1000,
            300,
            42,
            None,
        );
        assert_eq!(row.snapshot_id, None);
    }

    #[test]
    fn partial_row_maps_status_trigger_counts_and_error() {
        // Unlike failed_row, a partial row's counts are real (whatever the
        // already-committed batches produced), not zeroed.
        let row = partial_row(
            "job-4",
            "ns.orders",
            "svc",
            100,
            300,
            10,
            2,
            1000,
            200,
            15,
            Some(77),
            "distributed compaction job job-4: group 2 exhausted retries",
        );
        assert_eq!(row.status, "partial");
        assert_eq!(row.trigger, "scheduled");
        assert_eq!(row.job_id, "job-4");
        assert_eq!(row.table, "ns.orders");
        assert_eq!(row.principal, "svc");
        assert_eq!(row.started_at_ms, 100);
        assert_eq!(row.finished_at_ms, 300);
        assert_eq!(row.files_in, 10);
        assert_eq!(row.files_out, 2);
        assert_eq!(row.bytes_in, 1000);
        assert_eq!(row.bytes_out, 200);
        assert_eq!(row.rows_removed, 15);
        assert_eq!(row.snapshot_id, Some(77));
        assert_eq!(
            row.error,
            Some("distributed compaction job job-4: group 2 exhausted retries".to_string())
        );
    }

    #[test]
    fn failed_row_maps_status_trigger_and_error_zeroes_counts() {
        let row = failed_row("job-3", "ns.t", "svc", 100, 250, "boom: commit conflict");
        assert_eq!(row.status, "failed");
        assert_eq!(row.trigger, "scheduled");
        assert_eq!(row.job_id, "job-3");
        assert_eq!(row.table, "ns.t");
        assert_eq!(row.principal, "svc");
        assert_eq!(row.started_at_ms, 100);
        assert_eq!(row.finished_at_ms, 250);
        assert_eq!(row.error, Some("boom: commit conflict".to_string()));
        assert_eq!(row.files_in, 0);
        assert_eq!(row.files_out, 0);
        assert_eq!(row.bytes_in, 0);
        assert_eq!(row.bytes_out, 0);
        assert_eq!(row.rows_removed, 0);
        assert_eq!(row.snapshot_id, None);
    }

    #[test]
    fn skipped_row_maps_status_trigger_and_reason_zeroes_counts() {
        let row = skipped_row("job-4", "ns.t", "svc", 500, "no eligible compaction debt");
        assert_eq!(row.status, "skipped");
        assert_eq!(row.trigger, "scheduled");
        assert_eq!(row.job_id, "job-4");
        assert_eq!(row.table, "ns.t");
        assert_eq!(row.principal, "svc");
        assert_eq!(row.started_at_ms, 500);
        assert_eq!(row.finished_at_ms, 500);
        assert_eq!(row.error, Some("no eligible compaction debt".to_string()));
        assert_eq!(row.files_in, 0);
        assert_eq!(row.files_out, 0);
        assert_eq!(row.snapshot_id, None);
    }

    #[test]
    fn resolve_state_table_ident_defaults_when_only_dot() {
        // "." alone: namespace and name both empty -> falls back to default
        // rather than producing a TableIdent with an empty name.
        let ident = resolve_state_table_ident(".");
        assert_eq!(ident.namespace().as_ref(), &vec!["sqe_system".to_string()]);
        assert_eq!(ident.name(), "maintenance_log");
    }

    // --- Retention (finding 511, arm 4) ---

    #[test]
    fn day_of_buckets_by_utc_day_and_handles_pre_epoch() {
        assert_eq!(day_of(0), 0);
        assert_eq!(day_of(MS_PER_DAY - 1), 0);
        assert_eq!(day_of(MS_PER_DAY), 1);
        // div_euclid, not `/`: -1ms is the day BEFORE the epoch, not day 0.
        assert_eq!(day_of(-1), -1);
    }

    #[test]
    fn plan_history_rewrite_leaves_a_day_that_already_has_one_file() {
        let files = vec![
            ("a.parquet".to_string(), vec![10]),
            ("b.parquet".to_string(), vec![11]),
        ];
        assert!(
            plan_history_rewrite(&files).is_empty(),
            "one file per day is the goal state; rewriting it again is pure churn"
        );
    }

    #[test]
    fn plan_history_rewrite_compacts_a_day_split_across_files() {
        let files = vec![
            ("a.parquet".to_string(), vec![10]),
            ("b.parquet".to_string(), vec![10]),
            ("c.parquet".to_string(), vec![11]),
        ];
        let plan = plan_history_rewrite(&files);
        assert_eq!(plan.len(), 2);
        assert!(plan.contains("a.parquet"));
        assert!(plan.contains("b.parquet"));
        assert!(
            !plan.contains("c.parquet"),
            "day 11 has only one file; leave it alone"
        );
    }

    #[test]
    fn plan_history_rewrite_includes_a_lonely_day_carried_by_a_rewritten_file() {
        // `a` holds rows for day 10 (crowded, with `b`) AND day 99 (its
        // own). Rewriting `a` deletes it whole, so day 99's rows have to
        // come along or they would be dropped.
        let files = vec![
            ("a.parquet".to_string(), vec![10, 99]),
            ("b.parquet".to_string(), vec![10]),
        ];
        let plan = plan_history_rewrite(&files);
        assert!(plan.contains("a.parquet"));
        assert!(plan.contains("b.parquet"));
    }

    #[test]
    fn plan_history_rewrite_is_empty_for_no_candidates() {
        assert!(plan_history_rewrite(&[]).is_empty());
    }

    #[test]
    fn plan_history_rewrite_is_idempotent_on_its_own_output() {
        // After a pass, every day has exactly one file: the second pass
        // must find nothing.
        let after = vec![
            ("day-10.parquet".to_string(), vec![10, 10, 10]),
            ("day-11.parquet".to_string(), vec![11, 11]),
        ];
        assert!(plan_history_rewrite(&after).is_empty());
    }

    #[test]
    fn rows_to_record_batch_round_trips_every_column_for_many_rows() {
        let rows = vec![
            MaintenanceLogRow {
                job_id: "job-a".to_string(),
                table: "ns.t1".to_string(),
                trigger: "scheduled".to_string(),
                principal: "svc".to_string(),
                started_at_ms: 1,
                finished_at_ms: 2,
                status: "success".to_string(),
                files_in: 3,
                files_out: 1,
                bytes_in: 40,
                bytes_out: 30,
                rows_removed: 5,
                snapshot_id: Some(99),
                error: None,
            },
            MaintenanceLogRow {
                job_id: "job-b".to_string(),
                table: "ns.t2".to_string(),
                trigger: "scheduler".to_string(),
                principal: "svc".to_string(),
                started_at_ms: 7,
                finished_at_ms: 7,
                status: "skipped".to_string(),
                files_in: 0,
                files_out: 0,
                bytes_in: 0,
                bytes_out: 0,
                rows_removed: 0,
                snapshot_id: None,
                error: Some("no eligible compaction debt".to_string()),
            },
        ];

        let batch = rows_to_record_batch(&rows).expect("batch builds");
        assert_eq!(batch.num_rows(), 2);
        let decoded = batch_to_rows(&batch, "test.parquet").expect("batch decodes");
        assert_eq!(decoded, rows);
    }

    #[test]
    fn batch_to_rows_rejects_a_null_in_a_not_null_column() {
        // A retention pass REWRITES what it decodes, so an undecodable row
        // must stop the file being rewritten rather than be defaulted.
        // Built on an all-nullable copy of the fixed schema: the real one
        // would (correctly) refuse the null at `RecordBatch::try_new`, and
        // what is under test here is the decoder's own guard for a file
        // that somehow already holds one.
        let nullable_schema = Arc::new(ArrowSchema::new(
            maintenance_log_arrow_schema()
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            nullable_schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["ns.t"])),
                Arc::new(StringArray::from(vec!["scheduled"])),
                Arc::new(StringArray::from(vec!["svc"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["skipped"])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .expect("batch builds");
        assert!(batch_to_rows(&batch, "test.parquet").is_err());
    }
}
