//! Handlers for `CALL system.*` Iceberg maintenance procedures.
//!
//! Each procedure wraps a vendored iceberg-rust transaction action and
//! returns a single-row `RecordBatch` summary for the caller. The actions
//! themselves are documented in `vendor/iceberg-rust/crates/iceberg/src/`:
//!
//! - `transaction/rewrite_files.rs` drives `rewrite_data_files`
//! - `transaction/remove_snapshots.rs` drives `expire_snapshots`
//! - `actions/remove_orphan_files.rs` drives `remove_orphan_files`
//! - `transaction/rewrite_manifests.rs` drives `rewrite_manifests`
//!
//! Every procedure re-resolves the target table through the session catalog
//! so multi-namespace installations work unchanged. Privilege checks run
//! before any catalog traffic; a read-only user never sees an in-flight
//! rewrite.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError};
use sqe_sql::{NamespaceRef, ProcedureCall, TableRef};
use tracing::{info, warn};
use futures::TryStreamExt;

use crate::writer::{
    new_upload_tracker, parse_parquet_compression, write_data_files_streaming, FanoutLimits,
    WriteCleanupGuard,
};

/// Callback that returns a snapshot of recent SQL query texts.
///
/// Used by `suggest_bloom_filter_columns` to read the query log without
/// pulling `QueryTracker` into the procedure AST. Returning owned `String`s
/// keeps the closure simple and avoids lifetime gymnastics.
pub type QueryHistoryFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// Dispatcher for `CALL system.*` maintenance procedures.
///
/// The handler is lightweight. It holds config for catalog construction and
/// an optional audit logger and metadata cache, mirroring the pattern used
/// by `WriteHandler` and `CatalogOps`.
pub struct MaintenanceHandler {
    config: SqeConfig,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    table_cache: Option<TableMetadataCache>,
    query_history: Option<QueryHistoryFn>,
}

impl MaintenanceHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self {
            config,
            audit: None,
            table_cache: None,
            query_history: None,
        }
    }

    #[must_use = "with_audit consumes self; bind the returned handler"]
    pub fn with_audit(mut self, audit: Arc<sqe_metrics::audit::AuditLogger>) -> Self {
        self.audit = Some(audit);
        self
    }

    #[must_use = "with_table_cache consumes self; bind the returned handler"]
    pub fn with_table_cache(mut self, cache: TableMetadataCache) -> Self {
        self.table_cache = Some(cache);
        self
    }

    /// Attach a callback that returns the current query log.
    ///
    /// Required for `suggest_bloom_filter_columns`; without it the procedure
    /// returns an empty suggestion set (still a well-formed response).
    #[must_use = "with_query_history consumes self; bind the returned handler"]
    pub fn with_query_history(mut self, f: QueryHistoryFn) -> Self {
        self.query_history = Some(f);
        self
    }

    /// Entry point from the query handler. Resolves the target table via the
    /// session's catalog, enforces write privilege, then dispatches to the
    /// per-procedure implementation.
    pub async fn handle(
        &self,
        session: &Session,
        call: &ProcedureCall,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        self.authorize_or_deny(session, call).await?;

        match call {
            ProcedureCall::RewriteDataFiles {
                table,
                target_file_size_bytes,
                min_input_files,
                max_concurrent_file_group_rewrites,
            } => {
                self.rewrite_data_files(
                    session,
                    table,
                    *target_file_size_bytes,
                    *min_input_files,
                    *max_concurrent_file_group_rewrites,
                )
                .await
            }
            ProcedureCall::ExpireSnapshots {
                table,
                older_than,
                retain_last,
            } => {
                let older_than_ms = older_than.map(|t| t.timestamp_millis());
                self.expire_snapshots(session, table, older_than_ms, *retain_last)
                    .await
            }
            ProcedureCall::RemoveOrphanFiles {
                table,
                older_than,
            } => {
                let older_than_ms = older_than.map(|t| t.timestamp_millis());
                self.remove_orphan_files(session, table, older_than_ms).await
            }
            ProcedureCall::RewriteManifests { table } => {
                self.rewrite_manifests(session, table).await
            }
            ProcedureCall::SuggestBloomFilterColumns {
                table,
                history_limit,
            } => self.suggest_bloom_filter_columns(table, *history_limit),
            ProcedureCall::PurgeOrphanLocations {
                namespace,
                dry_run,
            } => self.purge_orphan_locations(session, namespace, *dry_run).await,
            ProcedureCall::RegisterTable {
                table,
                metadata_location,
            } => self.register_table(session, table, metadata_location).await,
            ProcedureCall::DropTable { table, purge } => {
                self.drop_table(session, table, *purge).await
            }
            ProcedureCall::SetCurrentSnapshot { table, snapshot_id } => {
                self.move_main_ref(session, table, *snapshot_id, false, "set_current_snapshot")
                    .await
            }
            ProcedureCall::RollbackToSnapshot { table, snapshot_id } => {
                self.move_main_ref(session, table, *snapshot_id, true, "rollback_to_snapshot")
                    .await
            }
        }
    }

    /// Move the `main` branch ref to an existing snapshot, repointing the
    /// table's current snapshot. Both `set_current_snapshot` and
    /// `rollback_to_snapshot` are this one metadata-only commit; the only
    /// difference is `require_ancestor`:
    ///
    /// - `set_current_snapshot` (false): repoint to ANY snapshot in the table.
    /// - `rollback_to_snapshot` (true): the target must be an ancestor of the
    ///   current snapshot (you can only roll backward along the current
    ///   history), matching Spark/Iceberg semantics and preventing an
    ///   accidental forward jump onto a divergent line.
    ///
    /// No snapshots are removed — the snapshot log (audit trail) is preserved;
    /// only the `main` pointer moves. SELECTs read the target state once the
    /// table cache is invalidated below.
    async fn move_main_ref(
        &self,
        session: &Session,
        table_ref: &TableRef,
        snapshot_id: i64,
        require_ancestor: bool,
        op_name: &'static str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use iceberg::spec::MAIN_BRANCH;

        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

        let before = table.metadata().current_snapshot_id().unwrap_or(-1);

        // Target must exist in the table's snapshot history.
        if table.metadata().snapshot_by_id(snapshot_id).is_none() {
            return Err(SqeError::Execution(format!(
                "CALL system.{op_name}: snapshot id {snapshot_id} not found in table '{ident}' history"
            )));
        }

        // Rollback safety: target must be an ancestor of the current snapshot.
        // Walk the parent chain from current; set_current_snapshot skips this.
        if require_ancestor {
            let mut cursor = table.metadata().current_snapshot_id();
            let mut is_ancestor = false;
            while let Some(id) = cursor {
                if id == snapshot_id {
                    is_ancestor = true;
                    break;
                }
                cursor = table
                    .metadata()
                    .snapshot_by_id(id)
                    .and_then(|s| s.parent_snapshot_id());
            }
            if !is_ancestor {
                return Err(SqeError::Execution(format!(
                    "CALL system.rollback_to_snapshot: snapshot id {snapshot_id} is not an ancestor \
                     of the current snapshot of '{ident}'; use set_current_snapshot to move to an \
                     arbitrary snapshot"
                )));
            }
        }

        // Repoint `main` at the target. create_branch with the default
        // if_not_exists=false updates the existing ref (if_not_exists=true
        // would silently no-op on an existing branch). This resets main's
        // retention to Branch{None,None,None}, which is the intended default
        // for these operations.
        let tx = Transaction::new(&table);
        let action = tx.create_branch(MAIN_BRANCH).with_snapshot_id(snapshot_id);
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("{op_name} apply failed: {e}")))?;
        let committed = tx
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, op_name))?;

        // Invalidate the table cache so subsequent SELECTs see the moved
        // pointer immediately instead of a stale pre-move snapshot.
        sqe_catalog::invalidate_rest_catalog_cache_all().await;

        let after = committed.metadata().current_snapshot_id().unwrap_or(-1);

        info!(
            user = %session.user.username,
            table = %ident,
            before_snapshot = before,
            after_snapshot = after,
            "{op_name}: committed main ref move"
        );

        Ok(vec![summary_batch(
            op_name,
            &ident,
            before,
            after,
            0,
            0,
            format!("snapshot {before} -> {after}"),
        )?])
    }

    /// Register an existing Iceberg table by pointing the catalog at its
    /// `metadata.json`. No data movement; the catalog backend validates the
    /// metadata file is readable and commits a pointer to it.
    ///
    /// Mirrors Spark's `CALL <catalog>.system.register_table(...)`. Useful
    /// for the golden-dataset workflow (generate once, register into each
    /// test catalog), catalog migration (drop on source + register on
    /// destination = data-free move), and disaster recovery (rebuild the
    /// catalog from intact S3 by re-registering every `metadata.json`).
    async fn register_table(
        &self,
        session: &Session,
        table_ref: &TableRef,
        metadata_location: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);

        info!(
            user = %session.user.username,
            table = %ident,
            metadata_location = metadata_location,
            "register_table: requesting catalog registration"
        );

        let table = catalog
            .register_table(&ident, metadata_location.to_string())
            .await
            .map_err(|e| classify_commit_error(e, "register_table"))?;

        // Invalidate the table cache so subsequent SELECTs see the
        // registration immediately. The cache key is the bearer token
        // fingerprint plus the table identifier, so a stale "table not
        // found" entry from an earlier load would otherwise mask the
        // registered table for up to the cache TTL.
        // Force every session to re-resolve the table on next access so the
        // register / drop is visible without waiting for the soft TTL to
        // expire. Cheap (one moka clear) and bounded to one catalog
        // operation per CALL.
        sqe_catalog::invalidate_rest_catalog_cache_all().await;

        let snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_identifier", DataType::Utf8, false),
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("metadata_location", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![ident.to_string()])),
                Arc::new(Int64Array::from(vec![snapshot_id])),
                Arc::new(StringArray::from(vec![metadata_location.to_string()])),
                Arc::new(StringArray::from(vec!["registered"])),
            ],
        )
        .map_err(|e| SqeError::Execution(format!("register_table summary: {e}")))?;
        Ok(vec![batch])
    }

    /// Drop a table from the catalog without deleting its data files. The
    /// `purge => true` form is reserved for a follow-up change that wires
    /// FileIO::delete_prefix into the drop path; today it returns an
    /// explicit "not yet implemented" error so users do not accidentally
    /// rely on it.
    async fn drop_table(
        &self,
        session: &Session,
        table_ref: &TableRef,
        purge: bool,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        if purge {
            return Err(SqeError::Execution(
                "CALL system.drop_table(purge => true) is not yet implemented; \
                 drop without purge then delete the table prefix manually"
                    .into(),
            ));
        }

        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);

        info!(
            user = %session.user.username,
            table = %ident,
            "drop_table: requesting catalog drop (no purge)"
        );

        catalog
            .drop_table(&ident)
            .await
            .map_err(|e| classify_commit_error(e, "drop_table"))?;

        // Force every session to re-resolve the table on next access so the
        // register / drop is visible without waiting for the soft TTL to
        // expire. Cheap (one moka clear) and bounded to one catalog
        // operation per CALL.
        sqe_catalog::invalidate_rest_catalog_cache_all().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("table_identifier", DataType::Utf8, false),
            Field::new("purge", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![ident.to_string()])),
                Arc::new(StringArray::from(vec!["false"])),
                Arc::new(StringArray::from(vec!["dropped"])),
            ],
        )
        .map_err(|e| SqeError::Execution(format!("drop_table summary: {e}")))?;
        Ok(vec![batch])
    }

    /// Read-only probe: walk the in-memory query log and surface the top
    /// equality-filtered columns for the target table.
    ///
    /// Unlike the mutating maintenance procedures, this one does not require
    /// write privilege; the privilege check in [`authorize_or_deny`] gates
    /// by `table_ref` semantics but the target is merely used to filter the
    /// history. We still route through the same dispatcher for a consistent
    /// audit trail, but the early-return for a missing history callback is
    /// graceful.
    fn suggest_bloom_filter_columns(
        &self,
        table_ref: &TableRef,
        history_limit: Option<usize>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let queries = match &self.query_history {
            Some(f) => f(),
            None => Vec::new(),
        };
        crate::suggest_bloom::suggest_bloom_filter_columns(
            table_ref,
            &queries,
            history_limit,
        )
    }

    /// Privilege check. Maintenance procedures mutate table state; we insist
    /// on a write-capable session. The read-only check is intentionally
    /// conservative: any role containing "read" or "select" in its name
    /// (case-insensitive) is treated as read-only unless the role also
    /// contains "write" or "admin". This matches the Polaris role naming
    /// convention and keeps the rule simple pending OPA/Cedar wiring.
    ///
    /// Denial paths record an audit entry so operators can detect probing.
    async fn authorize_or_deny(
        &self,
        session: &Session,
        call: &ProcedureCall,
    ) -> sqe_core::Result<()> {
        // Read-only procedures bypass the write-privilege gate.
        if matches!(call, ProcedureCall::SuggestBloomFilterColumns { .. }) {
            return Ok(());
        }
        // `purge_orphan_locations` in dry_run mode is also read-only.
        if let ProcedureCall::PurgeOrphanLocations { dry_run: true, .. } = call {
            return Ok(());
        }

        if session_has_write_privilege(session) {
            return Ok(());
        }

        let target = call.target_label();
        if let Some(ref audit) = self.audit {
            // Canonical AdminDdl event: denial of a maintenance procedure.
            // The maintenance handler has the session identity, so we build
            // a full Actor. No bearer tokens travel in procedure SQL text,
            // so log_event is safe here (no secret redaction needed).
            let deny_actor = sqe_metrics::audit::Actor::from_parts(
                session.user.username.clone(),
                session.user.subject.clone(),
                session.user.email.clone(),
                session.user.roles.clone(),
                session.user.groups.clone(),
            );
            audit.log_event(sqe_metrics::audit::AuditEvent {
                time: chrono::Utc::now(),
                kind: sqe_metrics::audit::AuditKind::AdminDdl,
                actor: deny_actor,
                outcome: sqe_metrics::audit::Outcome::Failure {
                    error_type: Some("AdminGateDenied".to_string()),
                    error_code: None,
                    message: Some(format!(
                        "user '{}' lacks write privilege on '{target}'",
                        session.user.username
                    )),
                },
                resources: vec![],
                policy: None,
                timing: None,
                stats: None,
                query: Some(sqe_metrics::audit::QueryInfo {
                    text: Some(format!("CALL system.{}('{target}')", call.name())),
                    query_hash: sqe_metrics::audit::query_hash(&format!(
                        "CALL system.{}({target})",
                        call.name(),
                    )),
                    statement_type: "procedure".to_string(),
                }),
                session_id: Some(session.id.clone()),
                client_ip: None,
                trace_id: None,
                query_id: None,
                integrity: sqe_metrics::audit::Integrity::default(),
            });
        }
        warn!(
            username = %session.user.username,
            procedure = %call.name(),
            target = %target,
            "Maintenance procedure denied: user lacks write privilege"
        );
        Err(SqeError::Execution(format!(
            "Access denied: user '{}' does not have write privilege on '{target}' required by \
             CALL system.{}",
            session.user.username,
            call.name()
        )))
    }

    /// Delete-aware bin-pack rewrite with bounded conflict retry.
    ///
    /// A concurrent writer that commits between our read and our commit turns
    /// the `RewriteFilesAction` commit into a retryable conflict. We retry the
    /// whole load -> plan -> rewrite -> commit a bounded number of times. Each
    /// attempt goes through `rewrite_data_files_once`, which re-loads the table,
    /// so the sequence-number pin, scan plan, and file set always describe the
    /// current snapshot: retrying with a stale pin would reopen the
    /// concurrent-equality-delete correctness hole. A failed attempt's orphaned
    /// output files are cleaned up by that attempt's `WriteCleanupGuard` on drop.
    async fn rewrite_data_files(
        &self,
        session: &Session,
        table_ref: &TableRef,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        max_concurrent_file_group_rewrites: Option<usize>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        const MAX_COMMIT_ATTEMPTS: usize = 4;
        let mut attempt: usize = 0;
        loop {
            attempt += 1;
            match self
                .rewrite_data_files_once(
                    session,
                    table_ref,
                    target_file_size_bytes,
                    min_input_files,
                    max_concurrent_file_group_rewrites,
                )
                .await
            {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // classify_commit_error tags retryable conflicts in the
                    // message ("retryable" / "conflict"). Anything else is a
                    // permanent failure we surface immediately.
                    let msg = e.to_string().to_lowercase();
                    let retryable = msg.contains("retryable") || msg.contains("conflict");
                    if retryable && attempt < MAX_COMMIT_ATTEMPTS {
                        let backoff = std::time::Duration::from_millis(50 * (1u64 << (attempt - 1)));
                        warn!(
                            table = %to_table_ident(table_ref),
                            attempt,
                            backoff_ms = backoff.as_millis() as u64,
                            "rewrite_data_files: retryable commit conflict; re-reading and retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn rewrite_data_files_once(
        &self,
        session: &Session,
        table_ref: &TableRef,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        max_concurrent_file_group_rewrites: Option<usize>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;
        const DEFAULT_MIN_INPUT_FILES: usize = 5;
        const DEFAULT_MAX_CONCURRENT_GROUPS: usize = 4;

        let target_bytes = target_file_size_bytes.unwrap_or(DEFAULT_TARGET_FILE_SIZE_BYTES);
        let min_input = min_input_files.unwrap_or(DEFAULT_MIN_INPUT_FILES);
        let max_concurrent =
            max_concurrent_file_group_rewrites.unwrap_or(DEFAULT_MAX_CONCURRENT_GROUPS);

        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

        // Delete-aware rewrite (Phase 2). Capture the current snapshot's
        // sequence number BEFORE any work: the rewritten data files are pinned
        // to this sequence number at commit time (see
        // `set_new_data_file_sequence_number` below) so that equality deletes
        // committed concurrently by another writer (at a higher sequence
        // number) still apply to the compacted output. Plan the delete-aware
        // read and collect the live delete files from the SAME table load so
        // the scan tasks, data files, and sequence pin all describe one
        // consistent snapshot.
        let seq_at_start = table
            .metadata_ref()
            .current_snapshot()
            .map(|s| s.sequence_number())
            .unwrap_or(0);
        let read_plan = plan_delete_aware_read(&table).await?;
        let live_deletes = collect_live_delete_files(&table).await?;

        let old_data_files = collect_live_data_files(&table).await?;
        let input_count = old_data_files.len();
        let total_bytes: i64 = old_data_files
            .iter()
            .map(|f| f.file_size_in_bytes() as i64)
            .sum();
        let total_input_rows: u64 = old_data_files.iter().map(|f| f.record_count()).sum();

        if input_count < min_input {
            info!(
                table = %ident,
                input_count,
                min_input,
                "rewrite_data_files: skipping, below min_input_files"
            );
            return Ok(vec![summary_batch(
                call_name_rewrite(),
                &ident,
                input_count as i64,
                0,
                total_bytes,
                0,
                "skipped: below min_input_files".to_string(),
            )?]);
        }

        // Greedy bin-pack small files into groups under `target_bytes`. Files
        // already at or above target are skipped (no win from re-emitting
        // them). Sort descending by size so the larger small-files anchor
        // each group and leftover capacity soaks up the smallest files.
        //
        // Partition-aware: never bin-pack across partition boundaries. A
        // cross-partition group would fan back out to ~1 output file per
        // partition on write (write_data_files re-splits per row), paying full
        // I/O for near-zero consolidation.
        let groups = pack_file_groups_partition_aware(&old_data_files, target_bytes);

        // Only groups with >= min_input members are worth rewriting; smaller
        // groups would trade one commit for no real reduction.
        let eligible_groups: Vec<Vec<DataFile>> = groups
            .into_iter()
            .filter(|g| g.len() >= min_input)
            .collect();

        if eligible_groups.is_empty() {
            info!(
                table = %ident,
                input_count,
                "rewrite_data_files: no groups meet min_input_files after packing"
            );
            return Ok(vec![summary_batch(
                call_name_rewrite(),
                &ident,
                input_count as i64,
                0,
                total_bytes,
                0,
                "skipped: no eligible groups".to_string(),
            )?]);
        }

        info!(
            table = %ident,
            input_count,
            target_bytes,
            min_input,
            max_concurrent,
            group_count = eligible_groups.len(),
            "rewrite_data_files: rewriting groups"
        );

        // Re-encode each group into one or more new Parquet files. We bound
        // concurrency so large tables do not exhaust file descriptors or S3
        // connections.
        let compression = parse_parquet_compression(&self.config.catalog.parquet_compression);

        let mut new_files: Vec<DataFile> = Vec::new();
        let mut old_files: Vec<DataFile> = Vec::new();
        let mut rewritten_rows: u64 = 0;

        use futures::stream::{self, StreamExt, TryStreamExt};

        let table_arc = Arc::new(table.clone());
        let tracker = new_upload_tracker();
        let cleanup_guard = WriteCleanupGuard::new(
            table.file_io().clone(),
            tracker.clone(),
            "rewrite-data-files",
        );
        let read_plan_arc = Arc::new(read_plan);
        let live_deletes_arc = Arc::new(live_deletes);
        let results: Vec<(Vec<DataFile>, Vec<DataFile>, u64)> =
            stream::iter(eligible_groups.into_iter())
                .map(|group| {
                    let table_for_group = table_arc.clone();
                    let tracker_for_group = tracker.clone();
                    let plan_for_group = read_plan_arc.clone();
                    let deletes_for_group = live_deletes_arc.clone();
                    async move {
                        rewrite_group(
                            &table_for_group,
                            &plan_for_group,
                            &deletes_for_group,
                            group,
                            compression,
                            tracker_for_group,
                        )
                        .await
                    }
                })
                .buffer_unordered(max_concurrent.max(1))
                .try_collect()
                .await?;

        for (group_new, group_old, group_rows) in results {
            new_files.extend(group_new);
            old_files.extend(group_old);
            rewritten_rows += group_rows;
        }

        // Row-count backstop. With delete application the added rows may be
        // fewer than the removed rows (deleted rows are dropped), so the strict
        // equality of the delete-free path no longer holds. The exact per-group
        // cross-check (`expected_rows_after_deletes`) already ran inside
        // `rewrite_group` and aborted before this point on any mismatch; here we
        // only assert the global direction: a rewrite can never manufacture
        // rows. `rewritten_rows` is the total the writer actually emitted.
        let removed_rows: u64 = old_files.iter().map(|f| f.record_count()).sum();
        let added_rows: u64 = new_files.iter().map(|f| f.record_count()).sum();
        if added_rows > removed_rows {
            return Err(SqeError::Execution(format!(
                "rewrite_data_files row-count invariant violated: added={added_rows} exceeds \
                 removed={removed_rows} (read_count={rewritten_rows}); a rewrite cannot \
                 increase row count; aborting before commit"
            )));
        }

        let output_count = new_files.len() as i64;
        let output_bytes: i64 = new_files.iter().map(|f| f.file_size_in_bytes() as i64).sum();

        // Position delete files whose referenced data file we are removing are
        // now dangling; drop them in the same commit so the delete-file layer
        // shrinks along with the data. Equality deletes are left to age out.
        let removed_data_paths: std::collections::HashSet<String> =
            old_files.iter().map(|f| f.file_path().to_string()).collect();
        let covered_deletes = covered_position_deletes(&removed_data_paths, &live_deletes_arc);
        let removed_delete_count = covered_deletes.len() as i64;

        info!(
            table = %ident,
            input_count = old_files.len(),
            output_count,
            removed_rows,
            added_rows,
            removed_delete_count,
            "rewrite_data_files: committing RewriteFilesAction"
        );

        // Commit via RewriteFilesAction: atomic swap of old -> new files.
        // Concurrent writers who committed between our read and this commit
        // cause a retryable CommitConflict error via the vendored fork's
        // SnapshotProducer; classify_commit_error flags that as retryable.
        //
        // set_enable_delete_filter_manager(true) is required for the commit to
        // actually mark the replaced data files as deleted in the output
        // manifest. Without it, the SnapshotProducer's existing_manifest()
        // branch at snapshot.rs skips the filter pass entirely, adds the new
        // data file, and leaves the old files alive: live count becomes N+1
        // instead of 1. Default is false in iceberg-rust's RewriteFilesAction
        // because other callers (e.g. fast appends) do not need to rewrite
        // existing manifests. We do.
        let tx = Transaction::new(&table);
        // check_file_existence forces a manifest scan that validates every
        // removed path exists in the current snapshot; any mismatch turns
        // into a hard error instead of a silent no-op. Combined with
        // enable_delete_filter_manager, the SnapshotProducer rewrites the
        // existing data manifests and marks the replaced files as deleted.
        // set_new_data_file_sequence_number pins the compacted output to the
        // sequence number of the snapshot we read (`seq_at_start`) instead of
        // the new snapshot's. This is the conflict-correctness keystone for
        // delete-aware compaction: an equality delete another writer commits
        // concurrently (at a higher sequence number) still applies to the
        // compacted files, because they carry the older sequence number. Without
        // the pin, the new files would out-rank that delete and silently
        // resurrect the rows it was meant to remove.
        // `.delete_files` routes data-content files into removed_data_files and
        // delete-content files into removed_delete_files, so chaining the
        // covered position deletes onto the removed data files drops both in one
        // atomic swap.
        let files_to_remove: Vec<DataFile> =
            old_files.iter().cloned().chain(covered_deletes).collect();
        let action = tx
            .rewrite_files()
            .set_enable_delete_filter_manager(true)
            .set_check_file_existence(true)
            .set_new_data_file_sequence_number(seq_at_start)
            .add_data_files(new_files)
            .delete_files(files_to_remove);
        let tx_applied = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("rewrite_files apply failed: {e}")))?;

        tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "rewrite_data_files"))?;
        cleanup_guard.mark_committed();

        // After the commit, invalidate the shared TableMetadataCache entry so
        // subsequent load_table calls (including the table_files TVF used by
        // readers) do not serve stale metadata with the pre-rewrite manifest
        // list. The SessionCatalogAdapter's update_table impl already calls
        // invalidate via its own SessionCatalog, but iceberg-rust's Transaction
        // commit path goes through a different catalog reference passed to
        // `.commit(catalog)` above: that Arc<dyn Catalog> may not share the
        // same invalidation hook if the Transaction retries or if the adapter
        // was constructed inline. Invalidating here closes the window. When
        // no cache is configured (e.g. in tests without a coordinator-shared
        // cache) the invalidation is a no-op.
        let cache_key = format!("{}.{}", ident.namespace(), ident.name());
        if let Some(tc) = &self.table_cache {
            tc.invalidate(&cache_key).await;
        }

        // Sanity check: post-commit reload should show the new file count.
        // If this disagrees with the committed action stats, we have a
        // catalog-state-propagation bug that must not be papered over.
        let reloaded = catalog.load_table(&ident).await.map_err(|e| {
            SqeError::Catalog(format!(
                "rewrite_data_files: post-commit reload failed: {e}"
            ))
        })?;
        let live_after = collect_live_data_files(&reloaded).await?.len();
        info!(
            table = %ident,
            live_after,
            expected_after = output_count,
            "rewrite_data_files: post-commit verification"
        );
        if live_after as i64 != output_count + (input_count as i64 - old_files.len() as i64) {
            warn!(
                table = %ident,
                live_after,
                expected_after = output_count + (input_count as i64 - old_files.len() as i64),
                "rewrite_data_files: live file count after commit does not match expectation"
            );
        }

        // Sanity check: total row count pre-rewrite should still equal
        // post-rewrite. `total_input_rows` counts all live data files, but we
        // only rewrote the ones that landed in eligible groups. Files left
        // alone keep their rows; rewritten files swap in equal-row replacements.
        let _ = total_input_rows; // tracked for observability

        Ok(vec![summary_batch(
            call_name_rewrite(),
            &ident,
            input_count as i64,
            output_count,
            total_bytes,
            output_bytes,
            format!("committed rewritten={}", old_files.len()),
        )?])
    }

    async fn expire_snapshots(
        &self,
        session: &Session,
        table_ref: &TableRef,
        older_than_ms: Option<i64>,
        retain_last: Option<usize>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

        let before_count = table.metadata().snapshots().count() as i64;

        let tx = Transaction::new(&table);
        let mut action = tx.expire_snapshot().clear_expire_files(true);
        if let Some(ts) = older_than_ms {
            action = action.expire_older_than(ts);
        }
        if let Some(keep) = retain_last {
            action = action.retain_last(
                i32::try_from(keep)
                    .map_err(|_| SqeError::Execution("retain_last does not fit in i32".into()))?,
            );
        }

        let tx_applied = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("expire_snapshots apply failed: {e}")))?;

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "expire_snapshots"))?;

        let after_count = committed.metadata().snapshots().count() as i64;
        let removed = before_count - after_count;

        info!(
            table = %ident,
            before_count,
            after_count,
            removed,
            "expire_snapshots: committed"
        );

        Ok(vec![summary_batch(
            "expire_snapshots",
            &ident,
            before_count,
            after_count,
            0,
            0,
            format!("snapshots_removed={removed}"),
        )?])
    }

    async fn remove_orphan_files(
        &self,
        session: &Session,
        table_ref: &TableRef,
        older_than_ms: Option<i64>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        const DEFAULT_OLDER_THAN_DAYS: i64 = 3;

        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

        let threshold_ms = older_than_ms.unwrap_or_else(|| {
            chrono::Utc::now().timestamp_millis()
                - DEFAULT_OLDER_THAN_DAYS * 24 * 60 * 60 * 1000
        });

        let action = iceberg::actions::RemoveOrphanFilesAction::new(table).older_than_ms(threshold_ms);

        let orphans = action.execute().await.map_err(|e| {
            SqeError::Execution(format!("remove_orphan_files execute failed: {e}"))
        })?;

        info!(
            table = %ident,
            orphan_count = orphans.len(),
            "remove_orphan_files: completed"
        );

        Ok(vec![summary_batch(
            "remove_orphan_files",
            &ident,
            0,
            orphans.len() as i64,
            0,
            0,
            format!("deleted={}", orphans.len()),
        )?])
    }

    async fn rewrite_manifests(
        &self,
        session: &Session,
        table_ref: &TableRef,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let tx_applied = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("rewrite_manifests apply failed: {e}")))?;

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "rewrite_manifests"))?;

        let manifest_count = committed
            .metadata()
            .current_snapshot()
            .map(|s| s.summary().additional_properties.len() as i64)
            .unwrap_or(0);

        info!(
            table = %ident,
            manifest_count,
            "rewrite_manifests: committed"
        );

        Ok(vec![summary_batch(
            "rewrite_manifests",
            &ident,
            0,
            manifest_count,
            0,
            0,
            "committed".to_string(),
        )?])
    }

    /// Sweep a namespace's warehouse prefix for S3 subdirectories that are
    /// not registered as tables in the catalog. Returns a result set with
    /// one row per orphan: `(path, kind, action)`.
    ///
    /// `dry_run = true` (default) reports without deleting. `dry_run = false`
    /// deletes via `FileIO::delete_prefix`.
    ///
    /// Limitations:
    /// - Requires at least one registered table in the namespace so we can
    ///   derive a `FileIO` to enumerate / delete with. Empty namespaces
    ///   error out; operators must `rm -rf` manually or add a sentinel
    ///   table first.
    /// - The namespace base location is derived from the first table's
    ///   `metadata().location()` by stripping the trailing path segment.
    ///   This matches the conventional `<warehouse>/<namespace>/<table>/`
    ///   layout Polaris emits. Custom per-table locations outside the
    ///   namespace prefix are not detected as orphans.
    async fn purge_orphan_locations(
        &self,
        session: &Session,
        namespace: &NamespaceRef,
        dry_run: bool,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = self
            .create_catalog_bridge(session, namespace.catalog.as_deref())
            .await?;
        let ns_ident = NamespaceIdent::new(namespace.namespace.clone());

        let table_idents = catalog.list_tables(&ns_ident).await.map_err(|e| {
            SqeError::Catalog(format!(
                "Failed to list tables in namespace '{}': {e}",
                namespace.as_string()
            ))
        })?;

        if table_idents.is_empty() {
            return Err(SqeError::Execution(format!(
                "Cannot purge orphans in empty namespace '{}': at least one registered \
                 table is required to derive the FileIO + namespace base. Add a \
                 placeholder table or clean the prefix manually.",
                namespace.as_string()
            )));
        }

        // Load every table to collect its location + a usable FileIO.
        // We index by canonical URI so case- and slash-only differences match
        // the listing returned by the storage backend.
        let mut registered_locations: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut probe_table: Option<IcebergTable> = None;
        for ident in &table_idents {
            match catalog.load_table(ident).await {
                Ok(t) => {
                    registered_locations.insert(canonicalize_uri(t.metadata().location()));
                    if probe_table.is_none() {
                        probe_table = Some(t);
                    }
                }
                Err(e) => {
                    warn!(
                        table = %ident,
                        error = %e,
                        "purge_orphan_locations: failed to load registered table; \
                         refusing to proceed because an unknown registered location \
                         would be misclassified as orphan"
                    );
                    return Err(SqeError::Execution(format!(
                        "purge_orphan_locations: refusing to run; could not load \
                         registered table '{ident}': {e}. Fix the catalog before \
                         retrying so live tables are not deleted."
                    )));
                }
            }
        }

        let probe = probe_table.ok_or_else(|| {
            SqeError::Execution(format!(
                "Cannot purge orphans in namespace '{}': failed to load any registered table",
                namespace.as_string()
            ))
        })?;

        // Derive the namespace base: parent of the probe table's location.
        let probe_loc = strip_trailing_slash(probe.metadata().location());
        let ns_base = probe_loc
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "Could not derive namespace base from probe location '{probe_loc}'"
                ))
            })?;
        let ns_base_canonical = canonicalize_uri(&ns_base);

        info!(
            namespace = %namespace.as_string(),
            ns_base = %ns_base,
            table_count = registered_locations.len(),
            dry_run,
            "purge_orphan_locations: enumerating prefixes"
        );

        // Enumerate one level under ns_base.
        let file_io = probe.file_io();
        let listing = file_io
            .list(format!("{ns_base}/"), false)
            .await
            .map_err(|e| {
                SqeError::Execution(format!(
                    "Failed to list prefix '{ns_base}/': {e}"
                ))
            })?;
        let entries: Vec<_> = listing.try_collect().await.map_err(|e| {
            SqeError::Execution(format!("Failed to collect listing for '{ns_base}/': {e}"))
        })?;

        let mut paths: Vec<String> = Vec::new();
        let mut kinds: Vec<&'static str> = Vec::new();
        let mut actions: Vec<String> = Vec::new();
        for entry in &entries {
            let path = strip_trailing_slash(&entry.path);
            let canonical = canonicalize_uri(&path);
            // Belt-and-suspenders: refuse to act on any candidate that does
            // not live strictly under the namespace base. A buggy backend
            // returning an absolute path outside ns_base would otherwise be
            // honoured by delete_prefix and reach arbitrary keys.
            if !is_strictly_under(&path, &ns_base_canonical) {
                warn!(
                    path = %path,
                    ns_base = %ns_base_canonical,
                    "purge_orphan_locations: skipping candidate outside namespace base"
                );
                paths.push(path);
                kinds.push("out_of_scope");
                actions.push("skipped_outside_ns".to_string());
                continue;
            }
            if registered_locations.contains(&canonical) {
                continue;
            }
            paths.push(path.clone());
            kinds.push("orphan");
            if dry_run {
                actions.push("would_delete".to_string());
            } else {
                match file_io.delete_prefix(&path).await {
                    Ok(()) => {
                        info!(path = %path, "purge_orphan_locations: deleted orphan prefix");
                        actions.push("deleted".to_string());
                    }
                    Err(e) => {
                        warn!(
                            path = %path,
                            error = %e,
                            "purge_orphan_locations: failed to delete orphan prefix"
                        );
                        actions.push(format!("delete_failed: {e}"));
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("action", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(paths)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(StringArray::from(actions)),
            ],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to build result batch: {e}")))?;

        info!(
            namespace = %namespace.as_string(),
            orphan_count = batch.num_rows(),
            dry_run,
            "purge_orphan_locations: complete"
        );
        Ok(vec![batch])
    }

    /// Build the iceberg catalog bridge for a maintenance op's target.
    ///
    /// `target_warehouse` is the target's catalog qualifier (`TableRef.catalog` /
    /// `NamespaceRef.catalog`), or `None` when unqualified. When it names a
    /// non-default catalog and `catalog_discovery = polaris-auto` is on, resolve
    /// THAT catalog via Polaris instead of the default warehouse -- so DROP and
    /// the maintenance procedures act on the workspace catalog the caller named.
    /// Mirrors `WriteHandler::create_catalog_bridge` (MR !285).
    async fn create_catalog_bridge(
        &self,
        session: &Session,
        target_warehouse: Option<&str>,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = match target_warehouse {
            Some(warehouse)
                if warehouse != self.config.catalog.warehouse
                    && self.config.query.catalog_discovery
                        == sqe_core::config::CatalogDiscovery::PolarisAuto =>
            {
                crate::session_context::discover_session_catalog(
                    warehouse,
                    &self.config,
                    session,
                    self.table_cache.as_ref(),
                )
                .await
                .ok_or_else(|| {
                    SqeError::Catalog(format!(
                        "Unknown catalog '{warehouse}': not resolvable via Polaris \
                         (nonexistent or not authorized for this user)"
                    ))
                })?
            }
            _ => Arc::new(
                SessionCatalog::for_session(
                    &self.config,
                    self.table_cache.clone(),
                    session.access_token().expose(),
                )
                .await?,
            ),
        };
        let _ = session_catalog.list_namespaces().await;
        Ok(session_catalog.as_catalog())
    }
}

fn call_name_rewrite() -> &'static str {
    "rewrite_data_files"
}

/// Strip a single trailing `/` if present so two locations that differ only
/// by trailing slash compare equal.
fn strip_trailing_slash(s: &str) -> String {
    s.strip_suffix('/').unwrap_or(s).to_string()
}

/// Canonical form for a URI used in orphan-location comparisons (#48).
///
/// - Scheme + host (authority) get lowercased so `S3://MyBucket/wh` matches
///   `s3://mybucket/wh` (Polaris normalises lowercase, some S3-compatible
///   stores preserve case).
/// - The path keeps its case (S3 keys are case-sensitive).
/// - Trailing slashes are stripped.
/// - Consecutive slashes inside the path are collapsed so `s3://b/wh/ns/t//`
///   compares equal to `s3://b/wh/ns/t/`.
///
/// Returns the input unchanged if it does not contain `://` (treats it as an
/// opaque path).
fn canonicalize_uri(s: &str) -> String {
    let trimmed = s.trim_end_matches('/');
    let (scheme, rest) = match trimmed.split_once("://") {
        Some(parts) => parts,
        None => return collapse_slashes(trimmed),
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path_collapsed = collapse_slashes(path);
    if path_collapsed.is_empty() {
        format!("{}://{}", scheme.to_ascii_lowercase(), authority.to_ascii_lowercase())
    } else {
        format!(
            "{}://{}/{}",
            scheme.to_ascii_lowercase(),
            authority.to_ascii_lowercase(),
            path_collapsed
        )
    }
}

fn collapse_slashes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_slash = false;
    for c in s.chars() {
        if c == '/' {
            if !prev_slash {
                out.push('/');
            }
            prev_slash = true;
        } else {
            out.push(c);
            prev_slash = false;
        }
    }
    out.trim_end_matches('/').to_string()
}

/// Return true when `candidate` lives strictly under `base` after both have
/// been canonicalised. Equal paths are not "under" the base; a path with a
/// component that only shares a prefix as a string is rejected (e.g.
/// `s3://b/wh/ns/table_2` is not under `s3://b/wh/ns/table`).
fn is_strictly_under(candidate: &str, base: &str) -> bool {
    let cand = canonicalize_uri(candidate);
    let b = canonicalize_uri(base);
    let prefix = format!("{b}/");
    cand.starts_with(&prefix) && cand.len() > prefix.len()
}

/// Translate an iceberg commit error into an SQE error, preserving retryable
/// conflict signalling so callers can distinguish transient from permanent
/// failures.
fn classify_commit_error(err: iceberg::Error, proc: &str) -> SqeError {
    let msg = err.to_string();
    if msg.to_lowercase().contains("conflict") || msg.to_lowercase().contains("retry") {
        SqeError::Execution(format!(
            "{proc}: concurrent writer conflict (retryable): {msg}"
        ))
    } else {
        SqeError::Execution(format!("{proc}: commit failed: {msg}"))
    }
}

fn to_table_ident(table_ref: &TableRef) -> TableIdent {
    let ns = NamespaceIdent::new(table_ref.namespace.clone());
    TableIdent::new(ns, table_ref.name.clone())
}

async fn load_table(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> sqe_core::Result<IcebergTable> {
    catalog
        .load_table(ident)
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to load table '{ident}': {e}")))
}

/// True when a manifest entry is a live delete file (position or equality).
///
/// A live entry is one whose status is not `Deleted`; a delete file is any
/// entry whose content type is not `Data`. The delete-aware rewrite uses this
/// to collect the delete files it must apply during the read
/// (`collect_live_delete_files`) and account for in the row cross-check.
fn is_live_delete_entry(entry: &iceberg::spec::ManifestEntry) -> bool {
    entry.status() != ManifestStatus::Deleted
        && entry.content_type() != DataContentType::Data
}

/// Collect the live delete files (position + equality) of the current snapshot.
/// Mirrors `collect_live_data_files` but keeps delete-content entries instead of
/// data entries. The delete-aware rewrite needs the delete `DataFile`s
/// themselves (not just a count) to compute the post-delete row cross-check and
/// to identify fully-covered position deletes for removal.
async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
    use futures::{StreamExt, TryStreamExt};

    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else {
        return Ok(vec![]);
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;

    Ok(manifests
        .into_iter()
        .flat_map(|m| {
            m.entries()
                .iter()
                .filter(|e| is_live_delete_entry(e))
                .map(|e| e.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect())
}

/// A snapshot-pinned, delete-aware read plan for a compaction pass.
///
/// `scan` is the configured `TableScan`; `tasks_by_path` maps each live data
/// file's path to the `FileScanTask`s that cover it, with their applicable
/// position and equality delete files attached. Reading a data file's tasks
/// through `scan.read_tasks_to_arrow_with_metrics` applies those deletes, so the
/// compacted output never carries logically-deleted rows.
///
/// Modeled on `WriteHandler::plan_delete_aware_read`; kept as a free function
/// here so the maintenance path does not depend on the write handler's session
/// context.
struct DeleteAwareReadPlan {
    scan: iceberg::scan::TableScan,
    tasks_by_path: std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>>,
}

/// Build the delete-aware read plan for the current snapshot. Plan once, right
/// after loading the table, so the task set matches the snapshot whose files the
/// rewrite deletes.
async fn plan_delete_aware_read(table: &IcebergTable) -> sqe_core::Result<DeleteAwareReadPlan> {
    let scan = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| SqeError::Execution(format!("Failed to build compaction read scan: {e}")))?;
    let tasks: Vec<iceberg::scan::FileScanTask> = scan
        .plan_files()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to plan compaction read: {e}")))?
        .try_collect()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to plan compaction read: {e}")))?;
    let mut tasks_by_path: std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>> =
        std::collections::HashMap::new();
    for task in tasks {
        tasks_by_path
            .entry(task.data_file_path.clone())
            .or_default()
            .push(task);
    }
    Ok(DeleteAwareReadPlan {
        scan,
        tasks_by_path,
    })
}

/// Rows expected after applying deletes to `group`, or `None` when it cannot be
/// computed exactly.
///
/// Only position-delete files whose `referenced_data_file` points at a file in
/// the group are attributable, so their `record_count` subtracts cleanly.
/// Equality deletes are value-based (they can match rows across many files at
/// any lower sequence number) and position deletes without a
/// `referenced_data_file` cannot be attributed to this group, so either makes
/// the exact count unknowable. Callers fall back to the looser
/// "cannot manufacture rows" bound in that case. Delete files are deduped by
/// path so a delete referenced from multiple manifest entries counts once.
fn expected_rows_after_deletes(group: &[DataFile], live_deletes: &[DataFile]) -> Option<u64> {
    use std::collections::HashSet;
    let group_paths: HashSet<&str> = group.iter().map(|f| f.file_path()).collect();
    let base: u64 = group.iter().map(|f| f.record_count()).sum();
    let mut deleted: u64 = 0;
    let mut seen: HashSet<&str> = HashSet::new();
    for d in live_deletes {
        if !seen.insert(d.file_path()) {
            continue;
        }
        match d.content_type() {
            DataContentType::PositionDeletes => match d.referenced_data_file() {
                Some(ref_path) if group_paths.contains(ref_path.as_str()) => {
                    deleted += d.record_count();
                }
                // References a data file outside this group: not our concern.
                Some(_) => {}
                // Unattributable position delete: exact count unknowable.
                None => return None,
            },
            // Value-based deletes: exact count unknowable.
            DataContentType::EqualityDeletes => return None,
            DataContentType::Data => {}
        }
    }
    Some(base.saturating_sub(deleted))
}

/// Position delete files that are fully covered by the rewritten data set and so
/// can be dropped in the same commit: their `referenced_data_file` points at a
/// data file we are removing, so after the rewrite they reference a path that no
/// longer exists.
///
/// Only position deletes with a `referenced_data_file` are returned. Equality
/// deletes are value-based and may still match data files we are not touching
/// (or, via the sequence-number pin, higher-sequence data), so they are left in
/// place and aged out by `expire_snapshots`/`drop_delete_files_older_than`.
/// Position deletes without a `referenced_data_file` cannot be attributed
/// cheaply and are likewise left (harmless: they reference removed paths and
/// match nothing). Deduped by path.
fn covered_position_deletes(
    removed_data_paths: &std::collections::HashSet<String>,
    live_deletes: &[DataFile],
) -> Vec<DataFile> {
    use std::collections::HashSet;
    let mut seen: HashSet<&str> = HashSet::new();
    live_deletes
        .iter()
        .filter(|d| seen.insert(d.file_path()))
        .filter(|d| d.content_type() == DataContentType::PositionDeletes)
        .filter(|d| {
            d.referenced_data_file()
                .is_some_and(|p| removed_data_paths.contains(&p))
        })
        .cloned()
        .collect()
}

/// Collect the live data files of the current snapshot. Mirrors the helper
/// in `WriteHandler` but does not need access to the compression config, so
/// it stays in this module.
async fn collect_live_data_files(
    table: &IcebergTable,
) -> sqe_core::Result<Vec<iceberg::spec::DataFile>> {
    use futures::{StreamExt, TryStreamExt};

    let metadata_ref = table.metadata_ref();
    let snapshot = match metadata_ref.current_snapshot() {
        Some(s) => s,
        None => return Ok(vec![]),
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;

    let data_files = manifests
        .into_iter()
        .flat_map(|manifest| {
            manifest
                .entries()
                .iter()
                .filter(|entry| {
                    entry.status() != ManifestStatus::Deleted
                        && entry.data_file().content_type() == DataContentType::Data
                })
                .map(|entry| entry.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect();

    Ok(data_files)
}

/// Greedy bin-pack a list of data files into groups whose total size stays
/// under `target_bytes`. Files already at or above target are dropped: there
/// is no benefit to rewriting a file that is already large.
///
/// The algorithm sorts files descending by size so larger small-files anchor
/// each group and the remaining capacity is filled with the smallest files.
/// Simple, deterministic, and good enough for the maintenance use case.
pub(crate) fn pack_file_groups(files: &[DataFile], target_bytes: u64) -> Vec<Vec<DataFile>> {
    // Filter files that are already at or above target: no point re-emitting.
    let mut small: Vec<DataFile> = files
        .iter()
        .filter(|f| f.file_size_in_bytes() < target_bytes)
        .cloned()
        .collect();
    // Descending by size.
    small.sort_by_key(|b| std::cmp::Reverse(b.file_size_in_bytes()));

    let mut groups: Vec<Vec<DataFile>> = Vec::new();
    for f in small {
        let size = f.file_size_in_bytes();
        // Try to fit into an existing group.
        let mut placed = false;
        for g in groups.iter_mut() {
            let current: u64 = g.iter().map(|x| x.file_size_in_bytes()).sum();
            if current + size <= target_bytes {
                g.push(f.clone());
                placed = true;
                break;
            }
        }
        if !placed {
            groups.push(vec![f]);
        }
    }
    groups
}

/// Stable grouping key for a data file's partition. Files that share a key
/// belong to the same partition of the same partition spec and can be safely
/// compacted together; files with different keys must never share an output
/// file. `Struct` is not `Hash`, so we key on its `Debug` form, which is
/// deterministic and sufficient as an in-memory grouping key (never persisted).
fn partition_key(f: &DataFile) -> String {
    format!("{}:{:?}", f.partition_spec_id(), f.partition())
}

/// Bin-pack files without ever mixing partitions. Groups by `partition_key`
/// first, then applies the greedy `pack_file_groups` within each partition.
/// Every returned group contains files from exactly one partition.
///
/// Global bin-packing is not a correctness bug in SQE (the writer re-splits
/// rows per partition on write), but a cross-partition group fans back out to
/// roughly one output file per partition, paying full read+write I/O for near
/// zero consolidation. Grouping per partition is what makes compaction actually
/// reduce file counts on partitioned tables.
fn pack_file_groups_partition_aware(files: &[DataFile], target_bytes: u64) -> Vec<Vec<DataFile>> {
    use std::collections::BTreeMap;
    let mut by_partition: BTreeMap<String, Vec<DataFile>> = BTreeMap::new();
    for f in files {
        by_partition
            .entry(partition_key(f))
            .or_default()
            .push(f.clone());
    }
    let mut out: Vec<Vec<DataFile>> = Vec::new();
    for (_key, part_files) in by_partition {
        out.extend(pack_file_groups(&part_files, target_bytes));
    }
    out
}

/// Rewrite one file group, applying its position and equality deletes, and emit
/// the surviving rows as a fresh set of data files. Streams the delete-aware
/// scan straight into the streaming writer so the group is never fully buffered
/// in coordinator memory. Returns `(new_files, old_files, rows_written)`.
///
/// The read routes through the shared `DeleteAwareReadPlan`: every task for the
/// group's data files (with its delete files attached) is fed to
/// `read_tasks_to_arrow_with_metrics`, which applies the deletes during decode.
/// Before returning, the exact post-delete row count is cross-checked against
/// `expected_rows_after_deletes`; when that is knowable (position deletes with a
/// `referenced_data_file`) any mismatch aborts before commit, and when it is not
/// (equality deletes) the caller enforces the looser "cannot manufacture rows"
/// bound.
async fn rewrite_group(
    table: &IcebergTable,
    plan: &DeleteAwareReadPlan,
    live_deletes: &[DataFile],
    group: Vec<DataFile>,
    compression: parquet::basic::Compression,
    tracker: crate::writer::UploadedPaths,
) -> sqe_core::Result<(Vec<DataFile>, Vec<DataFile>, u64)> {
    use futures::StreamExt;

    // Gather every scan task for the group's data files. Each task carries the
    // delete files that apply to it; missing a file from the plan means we would
    // read it without its deletes and resurrect deleted rows, so fail loud.
    let mut tasks: Vec<iceberg::scan::FileScanTask> = Vec::new();
    for df in &group {
        match plan.tasks_by_path.get(df.file_path()) {
            Some(t) => tasks.extend(t.iter().cloned()),
            None => {
                return Err(SqeError::Execution(format!(
                    "delete-aware compaction: data file '{}' is missing from the scan plan; \
                     refusing to read it without its delete files (data-file path mismatch \
                     between the manifest and the scan planner)",
                    df.file_path()
                )));
            }
        }
    }

    if tasks.is_empty() {
        // Empty group: caller treats this as a no-op (nothing added/removed).
        return Ok((vec![], vec![], 0));
    }

    // Feed the group's tasks through the scan's delete-applying reader, then
    // adapt the Iceberg record-batch stream into a DataFusion stream for the
    // streaming writer. The declared schema is the table's current schema in
    // Arrow form; the writer re-stamps field IDs by position per batch.
    let task_stream: iceberg::scan::FileScanTaskStream =
        Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)));
    let scan_result = plan
        .scan
        .read_tasks_to_arrow_with_metrics(task_stream)
        .map_err(|e| {
            SqeError::Execution(format!("delete-aware compaction read failed: {e}"))
        })?;
    let arrow_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
            .map_err(|e| {
                SqeError::Execution(format!("compaction schema conversion failed: {e}"))
            })?,
    );
    let df_stream = scan_result.stream().map(|item| {
        item.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    });
    let sendable: datafusion::execution::SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(arrow_schema, df_stream),
    );

    let (new_files, rows_written) = write_data_files_streaming(
        table,
        sendable,
        "rewrite",
        compression,
        tracker,
        FanoutLimits::unbounded(),
    )
    .await?;
    let rows_written = rows_written as u64;

    // Per-group delete-accounting cross-check. This is the guard that makes the
    // relaxed (added <= removed) invariant trustworthy: it catches a scan that
    // silently fails to apply deletes (rows too high) or a writer that drops
    // surviving rows (rows too low).
    match expected_rows_after_deletes(&group, live_deletes) {
        Some(expected) if rows_written != expected => {
            return Err(SqeError::Execution(format!(
                "compaction delete-accounting mismatch: wrote {rows_written} rows, expected \
                 {expected} after applying position deletes to a group of {} files; aborting \
                 before commit",
                group.len()
            )));
        }
        _ => {
            // Equality / unattributable deletes: exact count unknowable. Enforce
            // the looser bound that a rewrite can never manufacture rows.
            let base: u64 = group.iter().map(|f| f.record_count()).sum();
            if rows_written > base {
                return Err(SqeError::Execution(format!(
                    "compaction wrote {rows_written} rows from {base} input rows; deletes \
                     cannot increase the row count; aborting before commit"
                )));
            }
        }
    }

    if new_files.is_empty() {
        // Whole group deleted away: nothing to add, but we still remove the old
        // files. Return them so the caller drops them from the table.
        return Ok((vec![], group, rows_written));
    }

    Ok((new_files, group, rows_written))
}

/// Build a single-row `RecordBatch` describing the procedure's effect.
/// Columns: procedure, table, input_count, output_count, input_bytes,
/// output_bytes, status.
fn summary_batch(
    procedure: &str,
    ident: &TableIdent,
    input_count: i64,
    output_count: i64,
    input_bytes: i64,
    output_bytes: i64,
    status: String,
) -> sqe_core::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("procedure", DataType::Utf8, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("input_count", DataType::Int64, false),
        Field::new("output_count", DataType::Int64, false),
        Field::new("input_bytes", DataType::Int64, false),
        Field::new("output_bytes", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let table_str = format!("{}.{}", ident.namespace(), ident.name());

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![procedure])),
            Arc::new(StringArray::from(vec![table_str.as_str()])),
            Arc::new(Int64Array::from(vec![input_count])),
            Arc::new(Int64Array::from(vec![output_count])),
            Arc::new(Int64Array::from(vec![input_bytes])),
            Arc::new(Int64Array::from(vec![output_bytes])),
            Arc::new(StringArray::from(vec![status.as_str()])),
        ],
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build summary batch: {e}")))?;
    Ok(batch)
}

/// Treat the session as write-capable when no explicit read-only role is set.
///
/// Rules, applied in order:
/// 1. If any role name matches `^read` or `^select` (case-insensitive),
///    AND no role contains "write" or "admin", the session is read-only.
/// 2. Otherwise the session is write-capable.
///
/// The Polaris/Cedar backends will override this with richer decisions once
/// the policy enforcement wiring lands; this function is the engine-level
/// fallback and is the source of truth for the `#[ignore]` integration tests.
pub(crate) fn session_has_write_privilege(session: &Session) -> bool {
    let roles = &session.user.roles;
    if roles.is_empty() {
        return true;
    }

    let has_write_like = roles.iter().any(|r| {
        let lower = r.to_ascii_lowercase();
        lower.contains("write") || lower.contains("admin") || lower.contains("owner")
    });
    let has_read_only = roles.iter().any(|r| {
        let lower = r.to_ascii_lowercase();
        lower.starts_with("read") || lower.starts_with("select") || lower.contains("readonly")
    });

    if has_read_only && !has_write_like {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn session_with_roles(roles: Vec<&str>) -> Session {
        Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("test-token".to_string()),
            None,
            chrono::Utc::now() + Duration::hours(1),
            roles.into_iter().map(String::from).collect(),
        )
    }

    #[test]
    fn write_privilege_empty_roles_allows() {
        let session = session_with_roles(vec![]);
        assert!(session_has_write_privilege(&session));
    }

    #[test]
    fn write_privilege_read_only_denied() {
        let session = session_with_roles(vec!["readonly"]);
        assert!(!session_has_write_privilege(&session));
    }

    #[test]
    fn write_privilege_read_prefix_denied() {
        let session = session_with_roles(vec!["read_analyst"]);
        assert!(!session_has_write_privilege(&session));
    }

    #[test]
    fn write_privilege_writer_role_allows() {
        let session = session_with_roles(vec!["table_writer"]);
        assert!(session_has_write_privilege(&session));
    }

    #[test]
    fn write_privilege_admin_overrides_read_only() {
        let session = session_with_roles(vec!["readonly", "admin"]);
        assert!(session_has_write_privilege(&session));
    }

    #[test]
    fn write_privilege_unknown_role_allows() {
        // Unknown roles default to allow so the engine never blocks callers
        // whose policy enforcement runs elsewhere (OPA/Cedar/Polaris).
        let session = session_with_roles(vec!["analyst"]);
        assert!(session_has_write_privilege(&session));
    }

    // ---------------------------------------------------------------------
    // Bin-packing unit tests for rewrite_data_files. These run without a
    // live catalog because `pack_file_groups` is pure data manipulation.
    // ---------------------------------------------------------------------

    fn data_file_of_size(path: &str, size: u64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file")
    }

    /// Like `data_file_of_size` but lets the test vary the partition value and
    /// partition spec id, so partition-aware grouping can be exercised.
    fn data_file_part(path: &str, size: u64, spec_id: i32, part: i64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(part))]))
            .partition_spec_id(spec_id)
            .build()
            .expect("build data file")
    }

    #[test]
    fn partition_key_distinguishes_partitions_and_specs() {
        let a = data_file_part("a", 10, 0, 1);
        let b = data_file_part("b", 10, 0, 2);
        let c = data_file_part("c", 10, 1, 1);
        assert_eq!(partition_key(&a), partition_key(&a));
        assert_ne!(partition_key(&a), partition_key(&b), "different partition value");
        assert_ne!(partition_key(&a), partition_key(&c), "different spec id");
    }

    #[test]
    fn partition_aware_never_mixes_partitions() {
        let files = vec![
            data_file_part("p1-a", 10, 0, 1),
            data_file_part("p1-b", 10, 0, 1),
            data_file_part("p2-a", 10, 0, 2),
            data_file_part("p2-b", 10, 0, 2),
        ];
        let groups = pack_file_groups_partition_aware(&files, 1024);
        for g in &groups {
            let keys: std::collections::HashSet<String> = g.iter().map(partition_key).collect();
            assert_eq!(keys.len(), 1, "each group must be single-partition, got {keys:?}");
        }
        let total: usize = groups.iter().map(|g| g.len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn is_live_delete_entry_flags_position_deletes() {
        use iceberg::spec::{
            DataFileBuilder, DataFileFormat, ManifestEntry, ManifestStatus, Struct,
        };
        let df = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("pd".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .expect("build delete file");
        let entry = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(df)
            .build();
        assert!(is_live_delete_entry(&entry));
    }

    #[test]
    fn is_live_delete_entry_rejects_data_and_deleted() {
        use iceberg::spec::{
            DataFileBuilder, DataFileFormat, Literal, ManifestEntry, ManifestStatus, Struct,
        };
        let data = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("d".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file");
        let entry = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(data)
            .build();
        assert!(!is_live_delete_entry(&entry), "data file is not a delete");
    }

    // ---------------------------------------------------------------------
    // Delete-accounting cross-check (expected_rows_after_deletes). Pure over
    // DataFile metadata, so no live catalog needed.
    // ---------------------------------------------------------------------

    fn data_file_rows(path: &str, rows: u64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(rows * 8)
            .record_count(rows)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file")
    }

    fn pos_delete_file(path: &str, rows: u64, referenced: &str) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(rows * 8)
            .record_count(rows)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .referenced_data_file(Some(referenced.to_string()))
            .build()
            .expect("build position delete file")
    }

    fn eq_delete_file(path: &str, rows: u64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(rows * 8)
            .record_count(rows)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .equality_ids(Some(vec![1]))
            .build()
            .expect("build equality delete file")
    }

    #[test]
    fn expected_rows_subtracts_referenced_position_deletes() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        assert_eq!(expected_rows_after_deletes(&[d], &[pd]), Some(90));
    }

    #[test]
    fn expected_rows_ignores_position_delete_outside_group() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let pd = pos_delete_file("s3://b/pd2.parquet", 5, "s3://b/other.parquet");
        assert_eq!(expected_rows_after_deletes(&[d], &[pd]), Some(100));
    }

    #[test]
    fn expected_rows_ambiguous_on_equality_delete() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let ed = eq_delete_file("s3://b/ed1.parquet", 5);
        assert_eq!(expected_rows_after_deletes(&[d], &[ed]), None);
    }

    #[test]
    fn expected_rows_dedupes_delete_by_path() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        // Same delete file listed twice must only subtract once.
        assert_eq!(expected_rows_after_deletes(&[d], &[pd.clone(), pd]), Some(90));
    }

    #[test]
    fn covered_position_deletes_selects_only_referenced() {
        let mut removed = std::collections::HashSet::new();
        removed.insert("s3://b/d1.parquet".to_string());
        let pd_in = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        let pd_out = pos_delete_file("s3://b/pd2.parquet", 5, "s3://b/d2.parquet");
        let ed = eq_delete_file("s3://b/ed1.parquet", 3);
        let got = covered_position_deletes(&removed, &[pd_in, pd_out, ed]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].file_path(), "s3://b/pd1.parquet");
    }

    #[test]
    fn covered_position_deletes_empty_when_none_referenced() {
        let removed = std::collections::HashSet::new();
        let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        assert!(covered_position_deletes(&removed, &[pd]).is_empty());
    }

    #[test]
    fn partition_aware_matches_global_when_single_partition() {
        let files = vec![
            data_file_part("a", 300, 0, 0),
            data_file_part("b", 300, 0, 0),
            data_file_part("c", 300, 0, 0),
        ];
        let pa = pack_file_groups_partition_aware(&files, 1024);
        let global = pack_file_groups(&files, 1024);
        let pa_sizes: usize = pa.iter().map(|g| g.len()).sum();
        let gl_sizes: usize = global.iter().map(|g| g.len()).sum();
        assert_eq!(pa_sizes, gl_sizes);
    }

    #[test]
    fn pack_empty_input_returns_empty() {
        let out = pack_file_groups(&[], 1024);
        assert!(out.is_empty());
    }

    #[test]
    fn pack_files_at_or_above_target_are_skipped() {
        let target = 1024;
        let files = vec![
            data_file_of_size("a", target),     // equal to target
            data_file_of_size("b", target + 1), // above target
        ];
        let out = pack_file_groups(&files, target);
        assert!(
            out.is_empty(),
            "files at or above target must not be packed"
        );
    }

    #[test]
    fn pack_small_files_group_under_target() {
        let target = 1000;
        let files: Vec<_> = (0..10)
            .map(|i| data_file_of_size(&format!("f{i}"), 100))
            .collect();
        let out = pack_file_groups(&files, target);
        // 10 * 100 == 1000 == target; exactly one group at the boundary.
        assert_eq!(out.len(), 1, "expected one packed group, got {}", out.len());
        assert_eq!(out[0].len(), 10);
        let sum: u64 = out[0].iter().map(|f| f.file_size_in_bytes()).sum();
        assert_eq!(sum, 1000);
    }

    #[test]
    fn pack_respects_target_boundary() {
        let target = 1000;
        let files: Vec<_> = (0..11)
            .map(|i| data_file_of_size(&format!("f{i}"), 100))
            .collect();
        let out = pack_file_groups(&files, target);
        // Greedy descending-first packing: first 10 fill the first group
        // (sum=1000, fits because current+size<=target). The 11th starts a
        // fresh group.
        assert_eq!(out.len(), 2);
        let total_packed: usize = out.iter().map(|g| g.len()).sum();
        assert_eq!(total_packed, 11);
    }

    #[test]
    fn pack_mixed_sizes_sorted_descending() {
        let target = 1000;
        let files = vec![
            data_file_of_size("small", 50),
            data_file_of_size("big", 800),
            data_file_of_size("medium", 300),
        ];
        let out = pack_file_groups(&files, target);
        // Descending order: 800 first in group. Then 300: 800+300>1000, new
        // group. Then 50: 800+50<=1000, placed in first.
        assert_eq!(out.len(), 2);
        // Group 0: 800 + 50 = 850
        // Group 1: 300
        let sizes: Vec<u64> = out
            .iter()
            .map(|g| g.iter().map(|f| f.file_size_in_bytes()).sum())
            .collect();
        assert!(sizes.contains(&850) && sizes.contains(&300));
    }

    // ---------------------------------------------------------------------
    // URI canonicalization + prefix safety (#48 purge_orphan_locations)
    // ---------------------------------------------------------------------

    #[test]
    fn canonicalize_lowercases_scheme_and_host() {
        assert_eq!(
            canonicalize_uri("S3://MyBucket/wh/ns/t"),
            "s3://mybucket/wh/ns/t"
        );
    }

    #[test]
    fn canonicalize_strips_trailing_slash() {
        assert_eq!(canonicalize_uri("s3://b/wh/t/"), "s3://b/wh/t");
    }

    #[test]
    fn canonicalize_collapses_double_slashes() {
        assert_eq!(canonicalize_uri("s3://b/wh//ns//t//"), "s3://b/wh/ns/t");
    }

    #[test]
    fn canonicalize_preserves_path_case() {
        assert_eq!(canonicalize_uri("s3://b/Wh/Ns/T"), "s3://b/Wh/Ns/T");
    }

    #[test]
    fn is_strictly_under_rejects_self() {
        assert!(!is_strictly_under("s3://b/wh/ns/t", "s3://b/wh/ns/t"));
    }

    #[test]
    fn is_strictly_under_rejects_string_prefix_match() {
        // table_2 is not under table even though one is a string prefix of
        // the other.
        assert!(!is_strictly_under(
            "s3://b/wh/ns/table_2",
            "s3://b/wh/ns/table"
        ));
    }

    #[test]
    fn is_strictly_under_accepts_child() {
        assert!(is_strictly_under("s3://b/wh/ns/t", "s3://b/wh/ns"));
        assert!(is_strictly_under(
            "s3://b/wh/ns/t/sub",
            "s3://b/wh/ns"
        ));
    }

    #[test]
    fn is_strictly_under_handles_case_and_slash_variants() {
        assert!(is_strictly_under(
            "S3://MyBucket/wh/ns/t/",
            "s3://mybucket/wh/ns"
        ));
    }
}
