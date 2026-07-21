//! Catalog-native HA lease (claim / renew / release / steal) over the
//! `sqe_system.maintenance_log` state table (Phase 4d, Task 2).
//!
//! # Correctness never depends on this lease
//!
//! Iceberg's optimistic-concurrency commit (`Transaction::commit`, backed by
//! the catalog's atomic metadata-location CAS) already prevents two
//! coordinators from double-committing a compaction rewrite: whichever
//! commits second loses the race and reloads. This lease exists ONLY to
//! avoid two coordinators redundantly doing the same (expensive) rewrite
//! work concurrently, when Iceberg would throw one of their results away
//! anyway. A coordinator that runs a compaction job without ever calling
//! this module, or that loses/never gets a lease and does the work anyway,
//! cannot corrupt the table -- it can only waste CPU/IO. Nothing in this
//! module is allowed to become a correctness dependency; if a future change
//! makes it one, that is a bug in that change, not a feature of this one.
//!
//! # Design: leases are rows in `maintenance_log`, not a new table
//!
//! A lease claim is one row appended to `sqe_system.maintenance_log`
//! (`crate::maintenance_log`), reusing that table's existing fixed 14-column
//! schema with no schema change and no new operator DDL:
//!
//! | column          | lease meaning                                         |
//! |-----------------|--------------------------------------------------------|
//! | `table_name`    | the lease's `job_key`                                 |
//! | `trigger`       | always `"lease"` (marks this row as lease bookkeeping, distinct from `"scheduler"`/`"scheduled"` job rows) |
//! | `principal`     | `holder_id`                                           |
//! | `started_at_ms` | when this claim/release row was written               |
//! | `finished_at_ms`| `expires_at_ms` (a release row's is just "now", already expired) |
//! | `status`        | `"claimed"` (live) or `"released"` (tombstone)         |
//! | `error`         | `None`, or an audit note on a steal: `"stolen lease from holder '<id>' (expired)"` |
//! | everything else | zeroed / `None`, unused by the lease                   |
//!
//! # The exclusion primitive (Task 1 spike, do not re-derive)
//!
//! `crates/sqe-coordinator/tests/lease_cas_spike_test.rs` (and
//! `.superpowers/sdd/task-1-report.md`) established empirically that a plain
//! `fast_append` gives NO mutual exclusion (both racers' appends land,
//! rebased by `Transaction::commit`'s internal reload-and-retry), but
//! `Transaction::rewrite_files().delete_files([current_live_claim_files]).add_data_files([new_claim_file]).set_check_file_existence(true)`
//! DOES: exactly one concurrent commit wins, and the loser's reload rebuilds
//! the rewrite against the post-winner table, where `check_file_existence`
//! finds its delete target already gone and hard-fails with
//! `ErrorKind::DataInvalid` / `retryable() == false`. That error does NOT
//! match `write_handler::is_conflict_message`'s retryable-conflict heuristic
//! -- it must be classified separately, as "lost the lease race", not
//! retried.
//!
//! Every claim/renew/release commit in this module deletes the CURRENT live
//! lease file(s) for `job_key` (looked up fresh, by scanning the actually
//! committed table state on every attempt, never a cached `DataFile`) and
//! replaces them with one new file. `check_file_existence(true)` is what
//! turns that replace into a hard single-winner race.
//!
//! # The bootstrap gap (accepted, documented, not a correctness issue)
//!
//! The CAS primitive above requires an existing file to delete. The very
//! first-ever claim for a brand new `job_key` (no lease rows at all yet) has
//! nothing to delete, so [`try_acquire`] falls back to a plain `fast_append`
//! for that one case -- which, per the spike, gives NO exclusion. Two
//! coordinators racing the FIRST EVER claim for a `job_key` can therefore
//! both land a `"claimed"` row and both get `Some` back. This is acceptable
//! per this module's correctness contract above (worst case: one extra
//! redundant compaction pass, once, the first time a table is ever
//! compacted) and is called out explicitly rather than hidden: every commit
//! AFTER that first one has a real file to CAS against (a claim or a
//! `"released"` tombstone), so exclusion is real from the second claim
//! onward. [`release`] always writes a `"released"` tombstone row rather
//! than leaving zero live rows, specifically so the table never returns to
//! the unprotected "no rows" state after its first claim.
//!
//! # Reader cost (accepted, documented, a later-phase concern)
//!
//! [`try_acquire`]/[`renew`]/[`release`] each re-derive the current lease
//! state for `job_key` by decoding every live data file in
//! `maintenance_log` (there is no partitioning or file-level index on
//! `table_name`/`trigger`), because the ledger's own advisory/success/
//! failed/skipped job rows share the same table and file-per-row shape.
//! This is O(total rows ever written to the ledger) per lease operation.
//! Acceptable at Phase 4d's scale (a lease check per due table per
//! scheduler tick, TTLs in minutes); if `maintenance_log` grows very large
//! this should be revisited (e.g. partitioning by `table_name`), but that is
//! out of scope here.
//!
//! Run the integration test with
//! `cargo test -p sqe-coordinator --features test-sqlite --test maintenance_lease_test`.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind};
use tracing::{info, warn};
use uuid::Uuid;

use sqe_core::SqeError;

use crate::maintenance_log::{resolve_state_table_ident, row_to_record_batch, MaintenanceLogRow};
use crate::write_handler::is_conflict_message;
use crate::writer::{new_upload_tracker, parse_parquet_compression, write_data_files, WriteCleanupGuard};

/// `trigger` value marking a `maintenance_log` row as lease bookkeeping
/// rather than a job/advisory row.
const LEASE_TRIGGER: &str = "lease";
/// `status` value for a live claim.
const LEASE_STATUS_CLAIMED: &str = "claimed";
/// `status` value for a released tombstone (see the module docs: `release`
/// always writes one of these rather than leaving zero live rows).
const LEASE_STATUS_RELEASED: &str = "released";

/// Bounded attempts for a single claim/renew/release commit: enough to ride
/// out a few genuine transient `CatalogCommitConflicts`, not enough to spin
/// forever against a lease someone else legitimately holds (that case exits
/// immediately via the `ErrorKind::DataInvalid` classification, not via
/// exhausting this bound).
const MAX_LEASE_ATTEMPTS: u32 = 5;

/// Generate a fresh, per-process-lifetime `holder_id`. Callers that want a
/// stable identity across restarts (e.g. hostname+pid) may build their own
/// string instead; this is just a convenient default so nothing hardcodes a
/// single-coordinator assumption.
pub fn generate_holder_id() -> String {
    Uuid::new_v4().to_string()
}

/// A held (or just-acquired) lease. `claim_path` is the live data file
/// backing this claim in `maintenance_log` -- the exact file [`renew`] and
/// [`release`] must delete-and-replace to keep the CAS chain unbroken.
///
/// `stolen_from` (Phase 4d Task 3) is `Some(previous_holder_id)` only when
/// THIS acquisition won the claim by stealing an expired lease from a
/// different holder (see [`AcquireDecision::Acquirable`]'s `stolen_from`);
/// it is `None` for a fresh claim (no prior row), a re-acquire of an
/// already-released row, or an idempotent re-acquire of this same holder's
/// own still-live claim ([`AcquireDecision::OwnLive`]). Exists so a caller
/// (the scheduler's audit emission) can tell a steal apart from a routine
/// acquire without re-deriving it -- [`try_acquire`] already computed this
/// exact distinction internally (see its own `info!` log on a steal) but
/// previously did not surface it past that log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHandle {
    pub job_key: String,
    pub holder_id: String,
    pub expires_at_ms: i64,
    pub claim_path: String,
    pub stolen_from: Option<String>,
}

/// The pure, catalog-free view of "the latest lease row for one `job_key`"
/// that [`lease_acquirable`] decides against. Built from a real
/// `maintenance_log` row by [`read_lease_rows`], but deliberately has no
/// dependency on `iceberg`/`Catalog` types so the decision logic is
/// unit-testable with zero I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseState {
    pub job_key: String,
    pub holder_id: String,
    pub acquired_at_ms: i64,
    pub expires_at_ms: i64,
    pub released: bool,
    pub claim_path: String,
}

/// The outcome of evaluating [`lease_acquirable`] against the latest known
/// state for a `job_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireDecision {
    /// No live claim exists (no rows ever written, the last row is a
    /// released tombstone, or the last claim has expired). `stolen_from` is
    /// `Some(previous_holder_id)` only in the expired case, so a caller can
    /// log/audit the steal; it is `None` for "no rows" and "released".
    Acquirable { stolen_from: Option<String> },
    /// A live, unexpired claim exists, held by a DIFFERENT holder.
    HeldByOther { holder_id: String, expires_at_ms: i64 },
    /// A live, unexpired claim exists, already held by THIS holder --
    /// renewable, and [`try_acquire`] treats re-acquiring it as an
    /// idempotent success rather than a fresh commit.
    OwnLive,
}

/// Pure decision function: given the latest `maintenance_log` lease row for
/// `job_key` (or `None` if it has never been claimed), decide whether
/// `holder_id` can acquire it at `now_ms`. No I/O, no catalog, no clock
/// reads -- exhaustively unit-tested below.
///
/// Order of checks matters: an expired claim is `Acquirable` (steal) even
/// if it happens to be held by `holder_id` itself (a holder that let its
/// own lease lapse re-acquires via the same steal path, not a fast-path
/// "still mine"); only an UNEXPIRED claim can be `OwnLive`/`HeldByOther`.
pub fn lease_acquirable(latest: Option<&LeaseState>, holder_id: &str, now_ms: i64) -> AcquireDecision {
    let Some(state) = latest else {
        return AcquireDecision::Acquirable { stolen_from: None };
    };
    if state.released {
        return AcquireDecision::Acquirable { stolen_from: None };
    }
    if now_ms >= state.expires_at_ms {
        return AcquireDecision::Acquirable {
            stolen_from: Some(state.holder_id.clone()),
        };
    }
    if state.holder_id == holder_id {
        return AcquireDecision::OwnLive;
    }
    AcquireDecision::HeldByOther {
        holder_id: state.holder_id.clone(),
        expires_at_ms: state.expires_at_ms,
    }
}

/// A live lease row paired with the real `DataFile` backing it (needed to
/// build the CAS `delete_files` target -- `lease_acquirable` itself never
/// sees this, only the derived [`LeaseState`]).
struct LiveLeaseRow {
    state: LeaseState,
    data_file: DataFile,
}

/// Collect every live (non-deleted, `DataContentType::Data`) data file in
/// `table`'s current snapshot. Mirrors `write_handler::collect_data_files`
/// (sequential rather than concurrency-limited: this table is small and
/// this is a control-plane operation, not a hot write path).
async fn collect_data_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else {
        return Ok(vec![]);
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to load manifest list: {e}")))?;

    let mut data_files = Vec::new();
    for mf in manifest_list.entries() {
        let manifest = cache
            .get_manifest(mf)
            .await
            .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to load manifest: {e}")))?;
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

/// Read one data file's single-row `RecordBatch` via the table's `FileIO`.
/// Every `maintenance_log` row (lease or not) is written as a one-row batch
/// (see `maintenance_log::row_to_record_batch`), so this always expects
/// exactly one non-empty batch back.
async fn read_single_row_batch(table: &IcebergTable, file_path: &str) -> sqe_core::Result<RecordBatch> {
    let file_io = table.file_io();
    let input = file_io
        .new_input(file_path)
        .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to open '{file_path}': {e}")))?;
    let bytes = input
        .read()
        .await
        .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to read '{file_path}': {e}")))?;
    let reader = parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(bytes).map_err(|e| {
        SqeError::Execution(format!("maintenance_lease: failed to open parquet reader for '{file_path}': {e}"))
    })?;
    let reader = reader
        .build()
        .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to build parquet reader for '{file_path}': {e}")))?;
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to decode '{file_path}': {e}")))?;
    batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| SqeError::Execution(format!("maintenance_lease: '{file_path}' contains no rows")))
}

fn get_str_col(batch: &RecordBatch, name: &str, file_path: &str) -> sqe_core::Result<String> {
    let col = batch.column_by_name(name).ok_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: '{file_path}' is missing column '{name}'"))
    })?;
    let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: '{file_path}' column '{name}' is not Utf8"))
    })?;
    if arr.is_null(0) {
        return Err(SqeError::Execution(format!(
            "maintenance_lease: '{file_path}' column '{name}' is unexpectedly null"
        )));
    }
    Ok(arr.value(0).to_string())
}

fn get_i64_col(batch: &RecordBatch, name: &str, file_path: &str) -> sqe_core::Result<i64> {
    let col = batch.column_by_name(name).ok_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: '{file_path}' is missing column '{name}'"))
    })?;
    let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: '{file_path}' column '{name}' is not Int64"))
    })?;
    if arr.is_null(0) {
        return Err(SqeError::Execution(format!(
            "maintenance_lease: '{file_path}' column '{name}' is unexpectedly null"
        )));
    }
    Ok(arr.value(0))
}

/// Scan every live data file in `table` and return every row that is lease
/// bookkeeping (`trigger = "lease"`) for `job_key`. Normally at most one
/// (the design keeps exactly one live lease file per `job_key` by always
/// deleting the old one when writing a new one), but callers must tolerate
/// more than one defensively: the very first claim for a `job_key` is
/// written via an unprotected `fast_append` (see the module docs' bootstrap
/// gap), so a genuine race there can leave two.
async fn read_lease_rows(table: &IcebergTable, job_key: &str) -> sqe_core::Result<Vec<LiveLeaseRow>> {
    let data_files = collect_data_files(table).await?;
    let mut out = Vec::new();
    for df in data_files {
        let path = df.file_path().to_string();
        let batch = read_single_row_batch(table, &path).await?;
        let trigger = get_str_col(&batch, "trigger", &path)?;
        if trigger != LEASE_TRIGGER {
            continue;
        }
        let table_name = get_str_col(&batch, "table_name", &path)?;
        if table_name != job_key {
            continue;
        }
        let holder_id = get_str_col(&batch, "principal", &path)?;
        let acquired_at_ms = get_i64_col(&batch, "started_at_ms", &path)?;
        let expires_at_ms = get_i64_col(&batch, "finished_at_ms", &path)?;
        let status = get_str_col(&batch, "status", &path)?;
        out.push(LiveLeaseRow {
            state: LeaseState {
                job_key: job_key.to_string(),
                holder_id,
                acquired_at_ms,
                expires_at_ms,
                released: status == LEASE_STATUS_RELEASED,
                claim_path: path,
            },
            data_file: df,
        });
    }
    Ok(out)
}

/// Build the `MaintenanceLogRow` for a claim or release row. `note` becomes
/// the `error` column, used only as a free-text audit note (e.g. a steal).
fn lease_row(
    job_key: &str,
    holder_id: &str,
    written_at_ms: i64,
    expires_at_ms: i64,
    status: &str,
    note: Option<&str>,
) -> MaintenanceLogRow {
    MaintenanceLogRow {
        job_id: Uuid::now_v7().to_string(),
        table: job_key.to_string(),
        trigger: LEASE_TRIGGER.to_string(),
        principal: holder_id.to_string(),
        started_at_ms: written_at_ms,
        finished_at_ms: expires_at_ms,
        status: status.to_string(),
        files_in: 0,
        files_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        rows_removed: 0,
        snapshot_id: None,
        error: note.map(|s| s.to_string()),
    }
}

fn backoff_duration(attempt: u32) -> Duration {
    let base_ms: u64 = 25_u64.saturating_mul(1_u64 << attempt.saturating_sub(1));
    Duration::from_millis(base_ms.min(500))
}

/// Outcome of committing one claim/renew/release row via the CAS primitive.
enum CasOutcome {
    /// Committed; the new file's path is the new live claim/tombstone.
    Committed,
    /// Lost the race (`ErrorKind::DataInvalid`): someone else's commit beat
    /// this one to the same delete target. Not retryable -- the caller must
    /// re-read the fresh state, not retry this same commit.
    Lost,
    /// A genuine transient conflict (`retryable()` or the conflict-message
    /// heuristic): worth retrying with freshly re-read state.
    Transient(iceberg::Error),
}

/// Commit one CAS step: delete `delete_targets` (the current live lease
/// file(s) for `job_key`, empty only for the unprotected bootstrap case) and
/// add `new_file`. Empty `delete_targets` uses a plain `fast_append` (no
/// exclusion, see the module docs' bootstrap gap); non-empty uses
/// `rewrite_files().set_check_file_existence(true)` (the Task 1 primitive).
async fn commit_cas(
    catalog: &Arc<dyn Catalog>,
    table: &IcebergTable,
    delete_targets: Vec<DataFile>,
    new_file: DataFile,
) -> sqe_core::Result<CasOutcome> {
    let tx = Transaction::new(table);
    let commit_result = if delete_targets.is_empty() {
        let action = tx.fast_append().add_data_files(vec![new_file]);
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to apply bootstrap claim: {e}")))?;
        tx.commit(catalog.as_ref()).await
    } else {
        let action = tx
            .rewrite_files()
            .delete_files(delete_targets)
            .add_data_files(vec![new_file])
            .set_check_file_existence(true);
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("maintenance_lease: failed to apply claim rewrite: {e}")))?;
        tx.commit(catalog.as_ref()).await
    };

    match commit_result {
        Ok(_) => Ok(CasOutcome::Committed),
        Err(e) if e.kind() == ErrorKind::DataInvalid => Ok(CasOutcome::Lost),
        Err(e) if e.retryable() || is_conflict_message(&e.to_string()) => Ok(CasOutcome::Transient(e)),
        Err(e) => Err(SqeError::Execution(format!("maintenance_lease: commit failed: {e}"))),
    }
}

/// Write one lease row (claim or release) as a Parquet data file, returning
/// the written `DataFile` (not yet committed).
async fn write_lease_file(
    table: &IcebergTable,
    row: &MaintenanceLogRow,
    op: &'static str,
) -> sqe_core::Result<(DataFile, WriteCleanupGuard)> {
    let batch = row_to_record_batch(row)?;
    let tracker = new_upload_tracker();
    let cleanup_guard = WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), op);
    let compression = parse_parquet_compression("zstd");
    let mut files = write_data_files(table, vec![batch], op, compression, tracker).await?;
    if files.is_empty() {
        cleanup_guard.mark_committed();
        return Err(SqeError::Execution(format!(
            "maintenance_lease: writing a {op} row produced no data file"
        )));
    }
    Ok((files.remove(0), cleanup_guard))
}

/// Try to acquire the lease for `job_key` as `holder_id`. Returns:
/// - `Ok(Some(handle))` -- acquired (fresh claim, steal, or an idempotent
///   re-acquire of this holder's own still-live claim).
/// - `Ok(None)` -- another holder has a live, unexpired claim.
/// - `Err(_)` -- a real operational failure (state table missing/unreadable,
///   catalog error, or attempts exhausted against transient conflicts).
///
/// Re-reads the latest lease state fresh on every attempt (never reuses a
/// stale delete target), and retries a bounded number of times only on a
/// genuine transient conflict; a lost CAS race (`ErrorKind::DataInvalid`)
/// returns `Ok(None)` immediately, it is never retried (see the module docs
/// and the Task 1 spike: retrying a lost lease race is the wrong behavior,
/// not just a wasted attempt).
pub async fn try_acquire(
    catalog: &Arc<dyn Catalog>,
    state_table: &str,
    job_key: &str,
    holder_id: &str,
    ttl_secs: u64,
    now_ms: i64,
) -> sqe_core::Result<Option<LeaseHandle>> {
    let ident = resolve_state_table_ident(state_table);
    let mut last_err: Option<SqeError> = None;

    for attempt in 1..=MAX_LEASE_ATTEMPTS {
        let table = catalog.load_table(&ident).await.map_err(|e| {
            SqeError::Catalog(format!("maintenance_lease: failed to load state table '{ident}': {e}"))
        })?;

        let rows = read_lease_rows(&table, job_key).await?;
        let latest_state = rows.iter().map(|r| &r.state).max_by_key(|s| s.acquired_at_ms);

        match lease_acquirable(latest_state, holder_id, now_ms) {
            AcquireDecision::HeldByOther { .. } => return Ok(None),
            AcquireDecision::OwnLive => {
                let state = latest_state.expect("OwnLive implies a live row was found").clone();
                return Ok(Some(LeaseHandle {
                    job_key: job_key.to_string(),
                    holder_id: holder_id.to_string(),
                    expires_at_ms: state.expires_at_ms,
                    claim_path: state.claim_path,
                    stolen_from: None,
                }));
            }
            AcquireDecision::Acquirable { stolen_from } => {
                let expires_at_ms = now_ms.saturating_add((ttl_secs as i64).saturating_mul(1000));
                let note = stolen_from
                    .as_deref()
                    .map(|prev| format!("stolen lease from holder '{prev}' (expired)"));
                let row = lease_row(job_key, holder_id, now_ms, expires_at_ms, LEASE_STATUS_CLAIMED, note.as_deref());
                let (new_file, cleanup_guard) = write_lease_file(&table, &row, "maintenance-lease-claim").await?;
                let new_path = new_file.file_path().to_string();
                let delete_targets: Vec<DataFile> = rows.into_iter().map(|r| r.data_file).collect();

                match commit_cas(catalog, &table, delete_targets, new_file).await? {
                    CasOutcome::Committed => {
                        cleanup_guard.mark_committed();
                        if let Some(prev) = &stolen_from {
                            info!(job_key, holder_id, prev, "maintenance_lease: stole expired lease");
                        }
                        return Ok(Some(LeaseHandle {
                            job_key: job_key.to_string(),
                            holder_id: holder_id.to_string(),
                            expires_at_ms,
                            claim_path: new_path,
                            stolen_from: stolen_from.clone(),
                        }));
                    }
                    CasOutcome::Lost => return Ok(None),
                    CasOutcome::Transient(e) => {
                        last_err = Some(SqeError::Execution(format!(
                            "maintenance_lease: transient conflict claiming '{job_key}': {e}"
                        )));
                        if attempt < MAX_LEASE_ATTEMPTS {
                            warn!(job_key, attempt, error = %e, "maintenance_lease: transient conflict, retrying");
                            tokio::time::sleep(backoff_duration(attempt)).await;
                        }
                    }
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: exhausted attempts acquiring '{job_key}'"))
    }))
}

/// Renew `handle`'s lease, extending `expires_at_ms` from `now_ms`.
/// Requires `handle`'s claim to still be the live claim, held by
/// `handle.holder_id` -- if the lease was lost (expired and stolen, or
/// released out from under the caller), returns `Err`, since a caller with
/// no lease must stop treating itself as the sole worker for `job_key`.
pub async fn renew(
    handle: &mut LeaseHandle,
    catalog: &Arc<dyn Catalog>,
    state_table: &str,
    ttl_secs: u64,
    now_ms: i64,
) -> sqe_core::Result<()> {
    let ident = resolve_state_table_ident(state_table);
    let mut last_err: Option<SqeError> = None;

    for attempt in 1..=MAX_LEASE_ATTEMPTS {
        let table = catalog.load_table(&ident).await.map_err(|e| {
            SqeError::Catalog(format!("maintenance_lease: failed to load state table '{ident}': {e}"))
        })?;

        let rows = read_lease_rows(&table, &handle.job_key).await?;
        let current = rows.iter().find(|r| r.state.claim_path == handle.claim_path && !r.state.released);
        let Some(current) = current else {
            return Err(SqeError::Execution(format!(
                "maintenance_lease: lease for '{}' is no longer live (expired/released/stolen), cannot renew",
                handle.job_key
            )));
        };
        if current.state.holder_id != handle.holder_id {
            return Err(SqeError::Execution(format!(
                "maintenance_lease: lease for '{}' is held by '{}', not '{}'; cannot renew",
                handle.job_key, current.state.holder_id, handle.holder_id
            )));
        }

        let expires_at_ms = now_ms.saturating_add((ttl_secs as i64).saturating_mul(1000));
        let row = lease_row(&handle.job_key, &handle.holder_id, now_ms, expires_at_ms, LEASE_STATUS_CLAIMED, None);
        let (new_file, cleanup_guard) = write_lease_file(&table, &row, "maintenance-lease-renew").await?;
        let new_path = new_file.file_path().to_string();
        let delete_targets: Vec<DataFile> = rows.into_iter().map(|r| r.data_file).collect();

        match commit_cas(catalog, &table, delete_targets, new_file).await? {
            CasOutcome::Committed => {
                cleanup_guard.mark_committed();
                handle.expires_at_ms = expires_at_ms;
                handle.claim_path = new_path;
                return Ok(());
            }
            CasOutcome::Lost => {
                return Err(SqeError::Execution(format!(
                    "maintenance_lease: lost the lease for '{}' while renewing (raced out by another holder)",
                    handle.job_key
                )));
            }
            CasOutcome::Transient(e) => {
                last_err = Some(SqeError::Execution(format!(
                    "maintenance_lease: transient conflict renewing '{}': {e}",
                    handle.job_key
                )));
                if attempt < MAX_LEASE_ATTEMPTS {
                    warn!(job_key = %handle.job_key, attempt, error = %e, "maintenance_lease: transient conflict renewing, retrying");
                    tokio::time::sleep(backoff_duration(attempt)).await;
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: exhausted attempts renewing '{}'", handle.job_key))
    }))
}

/// Release `handle`'s lease: replaces the live claim file with a
/// `"released"` tombstone row (never leaves zero live rows -- see the
/// module docs' bootstrap-gap rationale). Best-effort past the first
/// attempt: if the claim is already gone (expired and stolen by someone
/// else, or already released), that means the caller no longer holds the
/// lease anyway, which is exactly the postcondition `release` promises, so
/// this returns `Ok(())` rather than an error.
pub async fn release(
    handle: LeaseHandle,
    catalog: &Arc<dyn Catalog>,
    state_table: &str,
    now_ms: i64,
) -> sqe_core::Result<()> {
    let ident = resolve_state_table_ident(state_table);
    let mut last_err: Option<SqeError> = None;

    for attempt in 1..=MAX_LEASE_ATTEMPTS {
        let table = catalog.load_table(&ident).await.map_err(|e| {
            SqeError::Catalog(format!("maintenance_lease: failed to load state table '{ident}': {e}"))
        })?;

        let rows = read_lease_rows(&table, &handle.job_key).await?;
        let delete_targets: Vec<DataFile> = rows.into_iter().map(|r| r.data_file).collect();
        if delete_targets.is_empty() {
            // Already gone (raced by someone else's steal, or already
            // released): the postcondition "this holder doesn't have it
            // live" already holds.
            return Ok(());
        }

        let row = lease_row(&handle.job_key, &handle.holder_id, now_ms, now_ms, LEASE_STATUS_RELEASED, None);
        let (new_file, cleanup_guard) = write_lease_file(&table, &row, "maintenance-lease-release").await?;

        match commit_cas(catalog, &table, delete_targets, new_file).await? {
            CasOutcome::Committed => {
                cleanup_guard.mark_committed();
                return Ok(());
            }
            CasOutcome::Lost => {
                // Someone else's commit (a steal, most likely) already
                // replaced our claim -- we no longer hold it either way.
                return Ok(());
            }
            CasOutcome::Transient(e) => {
                last_err = Some(SqeError::Execution(format!(
                    "maintenance_lease: transient conflict releasing '{}': {e}",
                    handle.job_key
                )));
                if attempt < MAX_LEASE_ATTEMPTS {
                    warn!(job_key = %handle.job_key, attempt, error = %e, "maintenance_lease: transient conflict releasing, retrying");
                    tokio::time::sleep(backoff_duration(attempt)).await;
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        SqeError::Execution(format!("maintenance_lease: exhausted attempts releasing '{}'", handle.job_key))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(holder_id: &str, acquired_at_ms: i64, expires_at_ms: i64, released: bool) -> LeaseState {
        LeaseState {
            job_key: "ns.orders".to_string(),
            holder_id: holder_id.to_string(),
            acquired_at_ms,
            expires_at_ms,
            released,
            claim_path: "s3://bucket/claim.parquet".to_string(),
        }
    }

    #[test]
    fn no_rows_is_acquirable_with_no_steal() {
        let decision = lease_acquirable(None, "me", 1_000);
        assert_eq!(decision, AcquireDecision::Acquirable { stolen_from: None });
    }

    #[test]
    fn released_row_is_acquirable_with_no_steal() {
        let s = state("someone-else", 500, 2_000, true);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(decision, AcquireDecision::Acquirable { stolen_from: None });
    }

    #[test]
    fn released_row_is_acquirable_even_if_still_before_its_expiry() {
        // A released row's expires_at_ms is meaningless (release sets it to
        // its own written_at_ms); `released` alone must decide, regardless
        // of now_ms vs expires_at_ms.
        let s = state("someone-else", 500, 999_999, true);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(decision, AcquireDecision::Acquirable { stolen_from: None });
    }

    #[test]
    fn live_claim_by_other_holder_is_held_by_other() {
        let s = state("other-holder", 500, 5_000, false);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(
            decision,
            AcquireDecision::HeldByOther {
                holder_id: "other-holder".to_string(),
                expires_at_ms: 5_000
            }
        );
    }

    #[test]
    fn live_claim_by_own_holder_is_own_live() {
        let s = state("me", 500, 5_000, false);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(decision, AcquireDecision::OwnLive);
    }

    #[test]
    fn expired_claim_by_other_holder_is_acquirable_via_steal() {
        let s = state("other-holder", 500, 900, false);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(
            decision,
            AcquireDecision::Acquirable {
                stolen_from: Some("other-holder".to_string())
            }
        );
    }

    #[test]
    fn expired_claim_by_own_holder_is_still_acquirable_via_steal_path() {
        // Order-of-checks: expiry is checked before the own-holder
        // fast-path, so a holder that let its own lease lapse re-acquires
        // via the same steal branch rather than a special "still mine" case.
        let s = state("me", 500, 900, false);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(
            decision,
            AcquireDecision::Acquirable {
                stolen_from: Some("me".to_string())
            }
        );
    }

    #[test]
    fn now_equal_to_expiry_is_treated_as_expired() {
        let s = state("other-holder", 500, 1_000, false);
        let decision = lease_acquirable(Some(&s), "me", 1_000);
        assert_eq!(
            decision,
            AcquireDecision::Acquirable {
                stolen_from: Some("other-holder".to_string())
            }
        );
    }

    #[test]
    fn one_ms_before_expiry_is_still_held() {
        let s = state("other-holder", 500, 1_000, false);
        let decision = lease_acquirable(Some(&s), "me", 999);
        assert_eq!(
            decision,
            AcquireDecision::HeldByOther {
                holder_id: "other-holder".to_string(),
                expires_at_ms: 1_000
            }
        );
    }

    #[test]
    fn generate_holder_id_is_nonempty_and_unique() {
        let a = generate_holder_id();
        let b = generate_holder_id();
        assert!(!a.is_empty());
        assert_ne!(a, b);
    }
}
