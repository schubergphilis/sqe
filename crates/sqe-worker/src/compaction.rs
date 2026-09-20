//! Worker-side executor for the distributed `compact_file_group` Flight
//! action (Phase 4c Task 3).
//!
//! The coordinator (Task 4, not yet implemented) signs and sends one
//! [`CompactGroupRequest`] per bin-packed file group to a worker's
//! `do_action("compact_file_group")`. This module does the actual rewrite:
//! build a catalog-free `FileIO` from the request's S3 credentials, pin a
//! [`StaticTable`] to the requested `metadata_location`, verify it is still
//! at the snapshot the coordinator planned against, apply deletes while
//! reading the group's files, optionally sort, and write fresh Parquet data
//! files back to the table's data location.
//!
//! Workers never see a catalog token and never commit: the coordinator
//! (Task 4) commits the returned, Avro-encoded `DataFile`s via
//! `RewriteFilesAction` after collecting every group's response.
//!
//! Delete application and the post-delete row cross-check are NOT
//! reimplemented here: both come from `sqe_compaction::rewrite::rewrite_group`
//! (Task 1), the same primitive `CALL system.rewrite_data_files` uses on the
//! coordinator. This module only adapts a worker's inputs (S3 creds instead
//! of a catalog session, one pre-selected group instead of a bin-packed
//! set) to that shared primitive. The live-delete-file collection
//! (`collect_live_delete_files`) is likewise imported from `sqe-compaction`
//! rather than duplicated: both this module and the coordinator's
//! `maintenance.rs` used to carry a byte-identical copy, deduplicated into
//! `sqe_compaction::rewrite` as a follow-up cleanup.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::scan::FileScanTask;
use iceberg::spec::{write_data_files_to_avro, DataFile, DataFileBuilder, Struct};
use iceberg::table::{StaticTable, Table as IcebergTable};
use iceberg::TableIdent;
use sqe_compaction::wire::{CompactGroupRequest, CompactGroupResponse, S3Conn};
use sqe_compaction::writer::{new_upload_tracker, parse_parquet_compression, UploadedPaths};
use sqe_compaction::{
    collect_live_delete_files, plan_delete_aware_read, rewrite_group, ProgressReporter, SortCtx,
};
use sqe_core::{Result as SqeResult, SqeError, SqeErrorCode};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// How many record batches the worker rewrite loop processes between
/// `CompactGroupFrame::Progress` frames. Chosen small enough that a hang on
/// any single batch (deletes-applying decode, or a rolling-writer flush) is
/// bounded well under the coordinator's `group_heartbeat_timeout` (default
/// on the order of tens of seconds), without spamming a frame per single
/// (often small) Arrow batch. Not currently operator-configurable: the
/// coordinator's heartbeat timeout is the tunable knob for how much slack a
/// deployment wants; this interval only needs to be "frequent enough" under
/// any reasonable heartbeat_timeout.
const PROGRESS_INTERVAL_BATCHES: usize = 8;

/// How many orphan paths the failure `Status` message names inline before it
/// switches to a count. The full list always travels in the `Status` details
/// payload ([`CleanupOutcome::to_details_json`]); the message is for humans
/// reading a log line, and an unbounded path list in a gRPC trailer is how
/// you get a header-size rejection on a group that uploaded 400 files.
pub const MAX_INLINE_ORPHAN_PATHS: usize = 8;

/// Everything one `compact_file_group` rewrite needs, assembled by the Flight
/// layer rather than by the rewrite itself (finding 608).
///
/// The two fields that matter for supervision are `tracker` and `cancel`:
/// both are owned by the caller, so the RPC handler can cancel the rewrite
/// and still read back the object paths it had already uploaded *after* the
/// rewrite future is gone. Building them inside the rewrite (as the original
/// `compact_file_group` did) meant the tracker died with the failure, and the
/// coordinator had nothing but the age-thresholded orphan sweep to fall back
/// on.
pub struct GroupRewriteCtx {
    /// Worker session context; supplies the `RuntimeEnv` (memory pool + disk
    /// manager) a spillable sort compaction runs under.
    pub session_ctx: SessionContext,
    /// The decoded, signature-verified request.
    pub request: CompactGroupRequest,
    /// Bounded progress channel back to the RPC response stream. Bounded on
    /// purpose (finding 608): progress is advisory, so frames are dropped
    /// rather than queued when the coordinator stops reading.
    pub progress_tx: Option<tokio::sync::mpsc::Sender<u64>>,
    /// Paths the rolling writer has generated so far. Shared with the caller
    /// so orphan cleanup is possible on every failure path, including one
    /// where the rewrite task was aborted outright.
    pub tracker: UploadedPaths,
    /// Cancelled by the RPC layer when the response stream is dropped or the
    /// worker-side deadline fires; the rewrite stops at its next await point.
    pub cancel: CancellationToken,
    /// `FileIO` for the table's object store, built once from
    /// `request.s3` by the caller ([`build_file_io`]) and used both to pin
    /// the `StaticTable` and to delete orphan uploads on the failure path.
    pub file_io: FileIO,
}

/// What a best-effort orphan cleanup managed to do.
///
/// `leaked` is the part the coordinator cares about: those objects are still
/// on the table's storage, uncommitted, and the coordinator can retry their
/// deletion instead of waiting for the age-thresholded orphan sweep.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CleanupOutcome {
    /// Paths recorded by the tracker at the time of the failure.
    pub attempted: Vec<String>,
    /// Paths successfully deleted.
    pub deleted: Vec<String>,
    /// Paths that could NOT be deleted and remain on object storage.
    pub leaked: Vec<String>,
}

impl CleanupOutcome {
    /// True when the rewrite had not uploaded anything yet (the common case
    /// for a failure in planning or the snapshot-pin guard).
    pub fn is_empty(&self) -> bool {
        self.attempted.is_empty()
    }

    /// Machine-readable payload for `Status::with_details`, so the
    /// coordinator can retry the deletion of `leaked` without scraping the
    /// human-readable message.
    pub fn to_details_json(&self, job_id: &str, group_id: u32) -> Vec<u8> {
        serde_json::json!({
            "kind": "compact_file_group_cleanup",
            "job_id": job_id,
            "group_id": group_id,
            "uploaded_paths": self.attempted,
            "deleted_paths": self.deleted,
            "leaked_paths": self.leaked,
        })
        .to_string()
        .into_bytes()
    }

    /// One-line human summary appended to the failure `Status` message.
    /// Empty when nothing had been uploaded, so clean failures keep their
    /// original wording.
    pub fn summary(&self) -> String {
        if self.is_empty() {
            return String::new();
        }
        let mut s = format!(
            "; uploaded {} orphan file(s), deleted {}, {} still on object storage",
            self.attempted.len(),
            self.deleted.len(),
            self.leaked.len()
        );
        if !self.leaked.is_empty() {
            let shown: Vec<&str> = self
                .leaked
                .iter()
                .take(MAX_INLINE_ORPHAN_PATHS)
                .map(String::as_str)
                .collect();
            s.push_str(&format!(": [{}]", shown.join(", ")));
            if self.leaked.len() > shown.len() {
                s.push_str(&format!(
                    " and {} more (full list in the error details)",
                    self.leaked.len() - shown.len()
                ));
            }
        }
        s
    }
}

/// Drain the tracker. Draining (rather than cloning) is deliberate: it makes
/// cleanup idempotent, so the in-task cleanup and the RPC guard's
/// abort-path cleanup can both run without double-deleting a path that a
/// concurrent writer might legitimately have re-created under the same name.
pub fn take_uploaded_paths(tracker: &UploadedPaths) -> Vec<String> {
    match tracker.lock() {
        Ok(mut g) => std::mem::take(&mut *g),
        // Poisoned only if a writer panicked mid-push. Recover the paths
        // anyway: leaking a full-size data file is worse than reading past a
        // poisoned lock whose only content is a Vec<String>.
        Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
    }
}

/// Best-effort deletion of every path the rewrite uploaded before it failed
/// (finding 608, proposed change 3).
///
/// Mirrors `sqe_compaction::writer::WriteCleanupGuard`'s semantics for the
/// local write path, but as an awaitable function rather than a `Drop`
/// impl, so the RPC handler can report what happened in the error it returns
/// instead of only logging it. The orphan sweep stays as the backstop for
/// whatever lands in [`CleanupOutcome::leaked`].
pub async fn delete_orphan_uploads(file_io: &FileIO, tracker: &UploadedPaths) -> CleanupOutcome {
    let attempted = take_uploaded_paths(tracker);
    let mut outcome = CleanupOutcome {
        deleted: Vec::with_capacity(attempted.len()),
        leaked: Vec::new(),
        attempted,
    };
    for path in &outcome.attempted {
        match file_io.delete(path).await {
            Ok(()) => outcome.deleted.push(path.clone()),
            Err(e) => {
                warn!(path = %path, error = %e, "compact_file_group: orphan cleanup delete failed");
                outcome.leaked.push(path.clone());
            }
        }
    }
    if !outcome.is_empty() {
        warn!(
            attempted = outcome.attempted.len(),
            deleted = outcome.deleted.len(),
            leaked = outcome.leaked.len(),
            "compact_file_group: cleaned up orphan uploads after a failed rewrite"
        );
    }
    outcome
}

/// Error returned when the RPC layer cancelled the rewrite (response stream
/// dropped, or worker-side deadline fired).
fn cancelled_error(request: &CompactGroupRequest, stage: &str) -> SqeError {
    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
        "compact_file_group: group {} of job '{}' was cancelled during {stage} \
         (the coordinator dropped the response stream, or the worker-side \
         compaction deadline fired)",
        request.group_id, request.job_id
    ))
}

/// Build a catalog-free `FileIO` from a [`S3Conn`]. Mirrors the S3 property
/// names the coordinator's per-session `RestCatalog` already injects for
/// FileIO configuration (`crates/sqe-catalog/src/rest_catalog.rs`), but talks
/// directly to `iceberg::io::FileIOBuilder` since a worker has no catalog to
/// go through.
///
/// `allow_http` is not translated into a separate property: opendal's S3
/// backend infers the scheme from the endpoint URL itself (`http://` vs
/// `https://`), the same way the executor's own S3 object-store builder
/// treats it (`executor.rs::build_object_store_with_creds`).
pub fn build_file_io(s3: &S3Conn) -> SqeResult<FileIO> {
    let mut builder = FileIOBuilder::new("s3");
    if !s3.endpoint.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_ENDPOINT, s3.endpoint.clone());
    }
    if !s3.region.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_REGION, s3.region.clone());
    }
    if !s3.access_key.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_ACCESS_KEY_ID, s3.access_key.clone());
    }
    if !s3.secret_key.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, s3.secret_key.clone());
    }
    if !s3.session_token.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_SESSION_TOKEN, s3.session_token.clone());
    }
    if s3.path_style {
        builder = builder.with_prop(iceberg::io::S3_PATH_STYLE_ACCESS, "true");
    }
    builder.build().map_err(|e| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "compact_file_group: failed to build FileIO from S3 config: {e}"
        ))
    })
}

/// Build a synthetic [`DataFile`] from a single [`FileScanTask`]. Used both
/// to resolve the requested group's data files and to reconstruct delete
/// files from a data file task's attached `deletes`; a data file's
/// `FileScanTask` (built by `plan_files`, before any row-group splitting
/// happens in the reader) carries the manifest entry's exact
/// `record_count`/`file_size_in_bytes`, so this is not a lossy re-derivation.
///
/// Only the fields [`rewrite_group`]/`expected_rows_after_deletes` actually
/// read (`content`, `file_path`, `record_count`) plus enough metadata to
/// build a valid `DataFile` are populated; partition/spec-id are not needed
/// because the worker rewrites a single pre-selected group rather than
/// bin-packing by partition.
fn data_file_from_task(task: &FileScanTask) -> SqeResult<DataFile> {
    DataFileBuilder::default()
        .content(task.data_file_content)
        .file_path(task.data_file_path.clone())
        .file_format(task.data_file_format)
        .record_count(task.record_count.unwrap_or(0))
        .file_size_in_bytes(task.file_size_in_bytes)
        .partition(task.partition.clone().unwrap_or_else(Struct::empty))
        .equality_ids(task.equality_ids.clone())
        .referenced_data_file(task.referenced_data_file.clone())
        .build()
        .map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "compact_file_group: failed to build data file metadata for '{}': {e}",
                task.data_file_path
            ))
        })
}

/// Resolve every path in `group_file_paths` to its planned [`DataFile`] via
/// `tasks_by_path` (the delete-aware read plan's per-path task map).
///
/// This is the resurrection guard: a data file the coordinator asked us to
/// compact but that is missing from the plan means the worker would read it
/// without knowing which delete files apply, silently resurrecting deleted
/// rows. Fail loud instead, matching `rewrite_group`'s own guard for the
/// same situation (`crates/sqe-compaction/src/rewrite.rs`), which this
/// duplicates at the group-resolution boundary so the error surfaces before
/// any read/write I/O, not partway through a stream.
fn resolve_group_data_files(
    tasks_by_path: &HashMap<String, Vec<FileScanTask>>,
    group_file_paths: &[String],
) -> SqeResult<Vec<DataFile>> {
    group_file_paths
        .iter()
        .map(|path| {
            let tasks = tasks_by_path.get(path).ok_or_else(|| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "compact_file_group: requested data file '{path}' is missing from the \
                     delete-aware scan plan; refusing to compact it without confirming its \
                     applicable deletes (resurrection guard)"
                ))
            })?;
            let first = tasks.first().ok_or_else(|| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "compact_file_group: data file '{path}' resolved to zero scan tasks"
                ))
            })?;
            data_file_from_task(first)
        })
        .collect()
}

/// Parse `table_ident` (`catalog.namespace.table`, dot-separated) into a
/// `TableIdent`. Purely cosmetic identity for the `StaticTable` -- the actual
/// read/write never goes through a catalog, so this can never resolve to the
/// wrong table the way a real catalog lookup could.
fn parse_table_ident(table_ident: &str) -> SqeResult<TableIdent> {
    TableIdent::from_strs(table_ident.split('.')).map_err(|e| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "compact_file_group: invalid table_ident '{table_ident}': {e}"
        ))
    })
}

/// Rewrite one file group against an already-pinned [`IcebergTable`]. Split
/// out from [`compact_file_group`] so the snapshot-pin assertion and the
/// group-resolution guard are reachable in tests without needing a real
/// `StaticTable::from_metadata_file` load (see the unit tests below); the
/// public entry point below is the thin S3/StaticTable-loading wrapper
/// around this.
///
/// `tracker` is supplied by the caller rather than allocated here (finding
/// 608): the RPC layer must be able to read the uploaded-path list on the
/// failure path, which means it has to outlive this future.
async fn compact_pinned_table(
    session_ctx: &SessionContext,
    table: IcebergTable,
    request: &CompactGroupRequest,
    progress_tx: Option<tokio::sync::mpsc::Sender<u64>>,
    tracker: UploadedPaths,
    cancel: &CancellationToken,
) -> SqeResult<CompactGroupResponse> {
    // Snapshot pin assertion. The coordinator planned this group's file list
    // and delete accounting against a specific snapshot; if the table has
    // since advanced (or the metadata_location resolves to a different
    // snapshot than the coordinator observed), the plan is stale and must
    // not be executed. Checked BEFORE any manifest I/O (plan_delete_aware_read
    // below), so a mismatch never triggers a scan.
    let actual_snapshot_id = table.metadata().current_snapshot_id();
    if actual_snapshot_id != Some(request.snapshot_id) {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "compact_file_group: snapshot mismatch for table '{}': request is pinned to \
             snapshot {}, but '{}' resolves to current snapshot {:?}; the read plan is only \
             valid against the pinned snapshot, refusing to compact",
            request.table_ident, request.snapshot_id, request.metadata_location, actual_snapshot_id
        )));
    }

    let plan = plan_delete_aware_read(&table).await.map_err(|e| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "compact_file_group: failed to plan delete-aware read: {e}"
        ))
    })?;

    let group = resolve_group_data_files(&plan.tasks_by_path, &request.group_file_paths)?;
    let live_deletes = collect_live_delete_files(&table).await?;

    let arrow_schema: arrow_schema::SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
            .map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("compact_file_group: schema conversion failed: {e}"))
            })?,
    );

    let sort_ctx: Option<SortCtx> = request.sort.as_ref().map(|s| SortCtx {
        runtime: session_ctx.runtime_env(),
        spec: s.to_sort_spec(),
    });

    let compression = parse_parquet_compression(&request.compression);

    // Wrap the caller's progress channel (if any) in a `ProgressReporter` so
    // the streaming writer only has to call `on_batch` unconditionally; the
    // interval throttling lives here, not in the writer loop. `do_action`
    // (flight_service.rs) forwards every value received on `progress_tx` as
    // a `CompactGroupFrame::Progress` frame as soon as it arrives, so the
    // coordinator's `group_heartbeat_timeout` -- which rearms on every frame
    // -- now bounds a stall in the delete-applying read or the rolling write
    // itself, not just the final frame delivery.
    let progress = progress_tx.map(|tx| {
        ProgressReporter::new(
            PROGRESS_INTERVAL_BATCHES,
            Box::new(move |rows_read| {
                // `try_send`, never `send`: the channel is bounded (finding
                // 608) and progress is advisory, so a coordinator that has
                // stopped reading must cost us a dropped frame, not a
                // growing queue and not backpressure on the rewrite. The
                // receiver also goes away entirely once `do_action` has
                // returned (client disconnected, response stream dropped),
                // which is equally fine to ignore.
                let _ = tx.try_send(rows_read);
            }),
        )
    });

    // Cancellation (finding 608): the rewrite future is dropped at its next
    // await point when the token fires, which is at most one record batch of
    // work away -- the delete-applying read and the rolling write both yield
    // per batch. `biased` polls the token first on every wakeup so a
    // ready-to-proceed rewrite cannot starve the cancellation.
    //
    // Done here, at the call boundary, rather than by threading the token
    // through `rewrite_group`: that primitive is shared with the
    // coordinator's local `CALL system.rewrite_data_files` path, and dropping
    // the future stops the writer at least as promptly as an in-loop check
    // would. What the token buys over a plain `JoinHandle::abort()` is that
    // control returns *here*, so the caller's orphan cleanup runs instead of
    // the task simply vanishing.
    let rewrite = rewrite_group(
        &table,
        &plan,
        &live_deletes,
        &arrow_schema,
        sort_ctx.as_ref(),
        group,
        compression,
        tracker.clone(),
        request.target_file_size_bytes,
        progress,
    );
    tokio::pin!(rewrite);

    let (new_files, _old_files, rows_written) = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(cancelled_error(request, "the group rewrite")),
        result = &mut rewrite => result
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("compact_file_group: rewrite failed: {e}")))?,
    };

    let partition_type = table.metadata().default_partition_type().clone();
    let format_version = table.metadata().format_version();
    let mut avro_buf = Vec::new();
    write_data_files_to_avro(
        &mut avro_buf,
        new_files.clone(),
        &partition_type,
        format_version,
    )
    .map_err(|e| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "compact_file_group: failed to encode data files to avro: {e}"
        ))
    })?;

    let uploaded_paths = tracker
        .lock()
        .map_err(|_| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, "compact_file_group: upload tracker mutex poisoned".to_string())
        })?
        .clone();

    let bytes_written: u64 = new_files.iter().map(|f| f.file_size_in_bytes()).sum();

    Ok(CompactGroupResponse {
        group_id: request.group_id,
        new_data_files_avro: avro_buf,
        rows_written,
        bytes_written,
        uploaded_paths,
    })
}

/// Rewrite one file group for a `compact_file_group` action: the worker-side
/// executor for Phase 4c distributed compaction.
///
/// Pins a `StaticTable` to `ctx.request.metadata_location` using the
/// caller-built `FileIO`, then delegates to [`compact_pinned_table`] for the
/// snapshot check, delete-aware rewrite, and Avro encoding. The worker never
/// obtains a catalog token and never commits; the coordinator commits the
/// returned `DataFile`s.
///
/// `ctx.progress_tx`, when `Some`, receives one cumulative rows-read value
/// every [`PROGRESS_INTERVAL_BATCHES`] record batches processed during the
/// delete-applying read + rolling write; `do_action` (`flight_service.rs`)
/// drains it concurrently with this future and turns each value into a
/// `CompactGroupFrame::Progress` frame sent back to the coordinator
/// immediately, before the rewrite as a whole finishes. `None` disables
/// progress reporting entirely; the rewrite itself is byte-for-byte
/// identical either way.
///
/// This function does NOT clean up after itself on failure. That is the
/// caller's job (`flight_service.rs`), because the caller also has to handle
/// the two failure modes this future cannot observe: its own deadline and an
/// outright task abort. `ctx.tracker` is the shared handle that makes that
/// possible; see [`delete_orphan_uploads`].
pub async fn run_group_rewrite(ctx: GroupRewriteCtx) -> SqeResult<CompactGroupResponse> {
    let GroupRewriteCtx {
        session_ctx,
        request,
        progress_tx,
        tracker,
        cancel,
        file_io,
    } = ctx;

    let ident = parse_table_ident(&request.table_ident)?;

    // Metadata load is an unbounded network read against the table's object
    // store: check the token before it and race it against the load, so a
    // group cancelled while its worker is stuck on a slow/hanging S3 does not
    // sit there until the TCP timeout.
    if cancel.is_cancelled() {
        return Err(cancelled_error(&request, "startup"));
    }
    let static_table = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(cancelled_error(&request, "the table metadata load")),
        loaded = StaticTable::from_metadata_file(&request.metadata_location, ident, file_io) => {
            loaded.map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "compact_file_group: failed to load table metadata '{}': {e}",
                    request.metadata_location
                ))
            })?
        }
    };

    compact_pinned_table(
        &session_ctx,
        static_table.into_table(),
        &request,
        progress_tx,
        tracker,
        &cancel,
    )
    .await
}

/// Convenience entry point for callers that do not supervise the rewrite:
/// builds the `FileIO`, a fresh tracker and an un-cancelled token, then runs
/// [`run_group_rewrite`]. Used by the live S3 test in
/// `tests/protocol/compaction_live.rs`; the Flight handler builds its own
/// [`GroupRewriteCtx`] so it can supervise.
pub async fn compact_file_group(
    session_ctx: &SessionContext,
    request: &CompactGroupRequest,
    progress_tx: Option<tokio::sync::mpsc::Sender<u64>>,
) -> SqeResult<CompactGroupResponse> {
    let file_io = build_file_io(&request.s3)?;
    run_group_rewrite(GroupRewriteCtx {
        session_ctx: session_ctx.clone(),
        request: request.clone(),
        progress_tx,
        tracker: new_upload_tracker(),
        cancel: CancellationToken::new(),
        file_io,
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType,
    };
    use sqe_compaction::wire::SortSpecWire;
    use std::io::Write as _;

    fn sample_task(path: &str, record_count: Option<u64>, size: u64) -> FileScanTask {
        let schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .unwrap(),
        );
        FileScanTask {
            file_size_in_bytes: size,
            start: 0,
            length: size,
            record_count,
            data_file_path: path.to_string(),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            partition: Some(Struct::empty()),
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn sample_request(group_file_paths: Vec<String>, snapshot_id: i64) -> CompactGroupRequest {
        CompactGroupRequest {
            audience: String::new(),
            expires_at: 0,
            job_id: "job-1".to_string(),
            group_id: 0,
            table_ident: "catalog.ns.tbl".to_string(),
            metadata_location: "s3://bucket/warehouse/tbl/metadata/v1.metadata.json".to_string(),
            snapshot_id,
            group_file_paths,
            target_file_size_bytes: 128 * 1024 * 1024,
            compression: "zstd".to_string(),
            sort: None,
            s3: sqe_compaction::wire::S3Conn {
                endpoint: String::new(),
                region: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                session_token: String::new(),
                path_style: false,
                allow_http: false,
            },
        }
    }

    // ---- resolve_group_data_files (resurrection guard) ---------------

    #[test]
    fn resolve_group_data_files_succeeds_for_known_paths() {
        let mut tasks_by_path = HashMap::new();
        tasks_by_path.insert(
            "s3://bucket/data/f1.parquet".to_string(),
            vec![sample_task("s3://bucket/data/f1.parquet", Some(100), 4096)],
        );
        let group =
            resolve_group_data_files(&tasks_by_path, &["s3://bucket/data/f1.parquet".to_string()])
                .unwrap();
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].file_path(), "s3://bucket/data/f1.parquet");
        assert_eq!(group[0].record_count(), 100);
        assert_eq!(group[0].file_size_in_bytes(), 4096);
    }

    #[test]
    fn resolve_group_data_files_fails_loud_on_missing_path() {
        // Resurrection guard: a requested path absent from the plan must
        // error out before any read, not silently skip the file (which
        // would resurrect its deleted rows by never applying its deletes).
        let mut tasks_by_path = HashMap::new();
        tasks_by_path.insert(
            "s3://bucket/data/f1.parquet".to_string(),
            vec![sample_task("s3://bucket/data/f1.parquet", Some(100), 4096)],
        );
        let err = resolve_group_data_files(
            &tasks_by_path,
            &[
                "s3://bucket/data/f1.parquet".to_string(),
                "s3://bucket/data/GHOST.parquet".to_string(),
            ],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("GHOST.parquet"),
            "error must name the missing path: {msg}"
        );
        assert!(
            msg.contains("resurrection guard") || msg.contains("missing"),
            "error must explain the guard: {msg}"
        );
    }

    #[test]
    fn resolve_group_data_files_uses_task_record_count_when_present() {
        let mut tasks_by_path = HashMap::new();
        // record_count is None when a task does not cover the whole file;
        // the resolver must not fabricate a nonzero count in that case.
        tasks_by_path.insert(
            "s3://bucket/data/partial.parquet".to_string(),
            vec![sample_task("s3://bucket/data/partial.parquet", None, 4096)],
        );
        let group = resolve_group_data_files(
            &tasks_by_path,
            &["s3://bucket/data/partial.parquet".to_string()],
        )
        .unwrap();
        assert_eq!(group[0].record_count(), 0);
    }

    // ---- snapshot pin assertion ---------------------------------------

    /// Minimal local (fs, zero S3/network) table with a real current
    /// snapshot, built the same way `iceberg::table::StaticTable`'s own doc
    /// example does: parse a `TableMetadata` and attach it via
    /// `Table::builder()`. The manifest list this snapshot points at does
    /// not need to exist on disk: the snapshot mismatch guard in
    /// `compact_pinned_table` returns before `plan_delete_aware_read` ever
    /// reads it.
    fn local_table_with_snapshot(snapshot_id: i64) -> IcebergTable {
        let json = format!(
            r#"{{
              "format-version": 2,
              "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
              "location": "s3://bucket/test/location",
              "last-sequence-number": 1,
              "last-updated-ms": 1602638573590,
              "last-column-id": 1,
              "current-schema-id": 0,
              "schemas": [
                {{"type": "struct", "schema-id": 0, "fields": [
                  {{"id": 1, "name": "x", "required": true, "type": "long"}}
                ]}}
              ],
              "default-spec-id": 0,
              "partition-specs": [{{"spec-id": 0, "fields": []}}],
              "last-partition-id": 999,
              "default-sort-order-id": 0,
              "sort-orders": [{{"order-id": 0, "fields": []}}],
              "properties": {{}},
              "current-snapshot-id": {snapshot_id},
              "snapshots": [
                {{
                  "snapshot-id": {snapshot_id},
                  "timestamp-ms": 1555100955770,
                  "sequence-number": 1,
                  "summary": {{"operation": "append"}},
                  "manifest-list": "s3://a/b/nonexistent.avro"
                }}
              ],
              "snapshot-log": [
                {{"snapshot-id": {snapshot_id}, "timestamp-ms": 1555100955770}}
              ],
              "metadata-log": []
            }}"#
        );
        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json).unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        IcebergTable::builder()
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "tbl"]).unwrap())
            .file_io(file_io)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn compact_pinned_table_rejects_snapshot_mismatch() {
        let table = local_table_with_snapshot(555);
        let request = sample_request(vec!["s3://bucket/data/f1.parquet".to_string()], 999);
        let ctx = SessionContext::new();
        let err = compact_pinned_table(
            &ctx,
            table,
            &request,
            None,
            new_upload_tracker(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("snapshot mismatch"),
            "error must name the mismatch: {msg}"
        );
        assert!(
            msg.contains("999") && msg.contains("555"),
            "error must cite both ids: {msg}"
        );
    }

    #[tokio::test]
    async fn compact_pinned_table_accepts_matching_snapshot_then_fails_later() {
        // A matching snapshot id must pass the guard; the call still fails
        // downstream because the manifest list is a placeholder path, but
        // the error must NOT be the snapshot-mismatch error.
        let table = local_table_with_snapshot(555);
        let request = sample_request(vec!["s3://bucket/data/f1.parquet".to_string()], 555);
        let ctx = SessionContext::new();
        let err = compact_pinned_table(
            &ctx,
            table,
            &request,
            None,
            new_upload_tracker(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(
            !err.to_string().contains("snapshot mismatch"),
            "matching snapshot must pass the pin guard: {err}"
        );
    }

    // ---- finding 608: cancellation + orphan cleanup --------------------

    /// In-memory `FileIO`. Every clone shares one `Arc<RwLock<HashMap>>`
    /// (`iceberg::io::storage::memory`), so a "writer" and the cleanup path
    /// see the same store without any S3 endpoint.
    fn memory_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_object(io: &FileIO, path: &str) {
        io.new_output(path)
            .unwrap()
            .write(bytes::Bytes::from_static(b"parquet"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_orphan_uploads_removes_recorded_paths_and_drains_the_tracker() {
        let io = memory_io();
        let tracker = new_upload_tracker();
        for i in 0..3 {
            let path = format!("/warehouse/ns/tbl/data/out-{i}.parquet");
            write_object(&io, &path).await;
            tracker.lock().unwrap().push(path);
        }
        // Not recorded by the writer => must survive cleanup untouched.
        write_object(&io, "/warehouse/ns/tbl/data/committed.parquet").await;

        let outcome = delete_orphan_uploads(&io, &tracker).await;

        assert_eq!(outcome.attempted.len(), 3);
        assert_eq!(outcome.deleted.len(), 3);
        assert!(outcome.leaked.is_empty(), "memory io deletes never fail");
        for path in &outcome.attempted {
            assert!(
                !io.exists(path).await.unwrap(),
                "cleanup must remove {path} from object storage"
            );
        }
        assert!(
            io.exists("/warehouse/ns/tbl/data/committed.parquet")
                .await
                .unwrap(),
            "cleanup must only touch paths the tracker recorded"
        );
        assert!(
            tracker.lock().unwrap().is_empty(),
            "the tracker must be drained so a second cleanup is a no-op"
        );

        let second = delete_orphan_uploads(&io, &tracker).await;
        assert!(second.is_empty(), "second cleanup must attempt nothing");
    }

    #[tokio::test]
    async fn delete_orphan_uploads_is_a_noop_when_nothing_was_uploaded() {
        let outcome = delete_orphan_uploads(&memory_io(), &new_upload_tracker()).await;
        assert!(outcome.is_empty());
        assert_eq!(outcome.summary(), "", "clean failures keep their wording");
    }

    #[test]
    fn cleanup_summary_truncates_inline_paths_but_details_carry_them_all() {
        let leaked: Vec<String> = (0..12)
            .map(|i| format!("s3://b/data/{i}.parquet"))
            .collect();
        let outcome = CleanupOutcome {
            attempted: leaked.clone(),
            deleted: vec![],
            leaked: leaked.clone(),
        };
        let summary = outcome.summary();
        assert!(summary.contains("12 still on object storage"), "{summary}");
        assert!(summary.contains("and 4 more"), "{summary}");
        assert!(summary.contains("s3://b/data/0.parquet"), "{summary}");
        assert!(
            !summary.contains("s3://b/data/11.parquet"),
            "the 12th path must not be inlined: {summary}"
        );

        let details: serde_json::Value =
            serde_json::from_slice(&outcome.to_details_json("job-1", 7)).unwrap();
        assert_eq!(details["job_id"], "job-1");
        assert_eq!(details["group_id"], 7);
        assert_eq!(details["leaked_paths"].as_array().unwrap().len(), 12);
    }

    #[tokio::test]
    async fn run_group_rewrite_returns_immediately_when_already_cancelled() {
        // A token cancelled before the task is polled must short-circuit
        // before the metadata load: the whole point of the abort-on-drop
        // guard is that a coordinator that walked away costs us no further
        // object-store I/O.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = run_group_rewrite(GroupRewriteCtx {
            session_ctx: SessionContext::new(),
            request: sample_request(vec!["s3://bucket/data/f1.parquet".to_string()], 555),
            progress_tx: None,
            tracker: new_upload_tracker(),
            cancel,
            file_io: memory_io(),
        })
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cancelled"), "{msg}");
        assert!(msg.contains("job-1"), "error must name the job: {msg}");
    }

    // ---- SortSpecWire wiring (no I/O) ----------------------------------

    #[test]
    fn sort_spec_wire_builds_a_sort_ctx_spec() {
        let wire = SortSpecWire::Columns(vec![("a".to_string(), true)]);
        let spec = wire.to_sort_spec();
        assert_eq!(spec.columns(), vec!["a"]);
    }

    // ---- Avro encode/decode contract for CompactGroupResponse ----------
    //
    // Pins the exact (version, partition_type, partition_spec_id) contract
    // `compact_pinned_table` uses to encode `new_data_files_avro`, so Task 4
    // (the coordinator decoder) has a concrete round-trip to match rather
    // than re-deriving it from the vendored `write_data_files_to_avro` source.

    #[test]
    fn data_files_avro_round_trips_through_write_and_read() {
        let partition_type = iceberg::spec::StructType::new(vec![]);
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/data/out-0.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(42)
            .file_size_in_bytes(2048)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        let mut buf = Vec::new();
        write_data_files_to_avro(
            &mut buf,
            vec![file.clone()],
            &partition_type,
            FormatVersion::V2,
        )
        .unwrap();
        buf.flush().unwrap();

        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap();

        let decoded = iceberg::spec::read_data_files_from_avro(
            &mut buf.as_slice(),
            &schema,
            0,
            &partition_type,
            FormatVersion::V2,
        )
        .unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file_path(), file.file_path());
        assert_eq!(decoded[0].record_count(), 42);
        assert_eq!(decoded[0].file_size_in_bytes(), 2048);
    }

    // The live end-to-end rewrite against a real S3 endpoint and Iceberg
    // table lives in `tests/protocol/compaction_live.rs` (finding 611 item 5,
    // finding 807 item 5): production code reads no integration-env variables.
}
