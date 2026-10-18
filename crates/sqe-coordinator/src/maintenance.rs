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
use futures::TryStreamExt;
use iceberg::spec::{DataContentType, DataFile};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError, SqeErrorCode};
use sqe_sql::{NamespaceRef, ProcedureCall, TableRef};
use tracing::{info, warn};

use crate::worker_registry::{WorkerLoadTracker, WorkerRegistry};
use crate::writer::{new_upload_tracker, parse_parquet_compression, WriteCleanupGuard};

/// Lives in `writer.rs` since MR A of the coordinator crate split (the sqlite
/// test rig that moves with the write path needs it); re-exported here so the
/// procedures keep calling `collect_live_data_files` unqualified.
pub(crate) use crate::writer::collect_live_data_files;
pub(crate) use sqe_compaction::{
    collect_live_delete_files, covered_position_deletes, delete_heavy_files,
    delete_ratio_heavy_files, pack_file_groups_partition_aware,
};
/// Delete-aware bin-pack/sort rewrite primitives moved to `sqe-compaction`
/// (Phase 4c Task 1) so the worker-side `compact_file_group` action can reuse
/// them without depending on this crate. `delete_heavy_files`,
/// `pack_file_groups_partition_aware`, and `covered_position_deletes` are
/// re-exported at `pub(crate)` visibility (matching their pre-move
/// visibility) because `table_health.rs` and `write_handler.rs` (the #378
/// INSERT OVERWRITE / CoW delete-cleanup path) reach them via
/// `crate::maintenance::`.
///
/// `is_live_delete_entry` / `collect_live_delete_files` were later
/// deduplicated into `sqe-compaction` too (a follow-up cleanup: they were a
/// byte-identical copy shared with `sqe-worker::compaction`). Only
/// `collect_live_delete_files` needs a `pub(crate)` re-export here (used
/// throughout this file and by `write_handler.rs`); `is_live_delete_entry`
/// is imported directly where this file's unit tests exercise it.
use sqe_compaction::{
    group_files_by_partition, plan_delete_aware_read, rewrite_group, SortCtx, SortSpec,
};

/// Callback that returns a snapshot of recent SQL query texts.
///
/// Used by `suggest_bloom_filter_columns` to read the query log without
/// pulling `QueryTracker` into the procedure AST. Returning owned `String`s
/// keeps the closure simple and avoids lifetime gymnastics.
pub type QueryHistoryFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// Data files that either delete trigger marks for rewrite: at least
/// `count` distinct delete files ([`delete_heavy_files`]), or a deleted-row
/// share of at least `ratio` ([`delete_ratio_heavy_files`]). A count of 0
/// and a ratio of 0 (or `None`) each mean that trigger is off, not "every
/// file qualifies". Shared by both rewrite paths and by `table_health`, so
/// the report and the rewrite can never disagree about which files are
/// delete-heavy.
pub(crate) fn delete_triggered_files(
    tasks_by_path: &std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>>,
    count: Option<usize>,
    ratio: Option<f64>,
) -> std::collections::HashSet<String> {
    let mut out = match count {
        Some(threshold) if threshold > 0 => delete_heavy_files(tasks_by_path, threshold),
        _ => std::collections::HashSet::new(),
    };
    if let Some(ratio) = ratio.filter(|r| *r > 0.0) {
        out.extend(delete_ratio_heavy_files(tasks_by_path, ratio));
    }
    out
}

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
    /// Shared DataFusion runtime (FairSpillPool + DiskManager) used by the
    /// sort-compaction path so a large sort spills to disk instead of OOMing.
    runtime: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
    /// The coordinator's worker fleet view (Phase 4c Task 5). `None` means
    /// this handler was never wired to a fleet -- [`Self::healthy_worker_count`]
    /// then always reports `0`, which is exactly what makes `distribution.mode
    /// = "auto"` resolve to `Local` and keeps the single-node/no-fleet path
    /// byte-identical to pre-Task-5 behavior (see [`resolve_execution`]).
    worker_registry: Option<Arc<WorkerRegistry>>,
    /// In-flight group count per worker, used to place `compact_file_group`
    /// dispatches. Deliberately a SEPARATE tracker from the query path's scan
    /// dispatch (`QueryHandler::worker_load`): a maintenance rewrite group and
    /// a query scan fragment are different kinds of work, and conflating
    /// their load counts would let one starve placement decisions for the
    /// other. Always present (cheap to construct) even when `worker_registry`
    /// is `None`, since only the distributed path ever reads it.
    worker_load: Arc<WorkerLoadTracker>,
    /// The coordinator's Ed25519 ticket signer (finding 601), used to sign
    /// `compact_file_group` requests like scan tickets. Must be the same
    /// key workers pin via heartbeat. `None` falls back to the legacy HMAC.
    ticket_signer: Option<Arc<sqe_core::ticket_signing::TicketSigner>>,
    /// Coordinator metrics, for `sqe_puffin_sidecar_total` on the compaction
    /// carry-forward path (architecture review 512). `None` in tests and in
    /// handlers built without a registry; the carry-forward still runs.
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// TEST-ONLY synchronization seam (`#[cfg(all(test, feature = "test-sqlite"))]`, so it compiles only
    /// for this crate's own unit test binary -- never for a release build or
    /// an external integration test). See `RewriteRaceSeam`'s doc comment and
    /// the `#[cfg(all(test, feature = "test-sqlite"))] mod tests` test
    /// `concurrent_position_delete_during_local_rewrite_does_not_resurrect_rows`
    /// for how it is used to land a concurrent commit deterministically
    /// inside `rewrite_data_files_once`'s conflict window, instead of racing
    /// wall-clock timing. Always `None` unless a test opts in via
    /// `with_test_race_seam`; `MaintenanceHandler::new` never sets it.
    #[cfg(all(test, feature = "test-sqlite"))]
    race_seam: Option<Arc<RewriteRaceSeam>>,
}

impl MaintenanceHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self {
            config,
            audit: None,
            table_cache: None,
            query_history: None,
            runtime: None,
            worker_registry: None,
            worker_load: Arc::new(WorkerLoadTracker::new()),
            ticket_signer: None,
            metrics: None,
            #[cfg(all(test, feature = "test-sqlite"))]
            race_seam: None,
        }
    }

    /// Attach the coordinator metrics registry so compaction commits report
    /// `sqe_puffin_sidecar_total{op="rewrite",outcome}`.
    #[must_use = "with_metrics consumes self; bind the returned handler"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// TEST-ONLY: attach a [`RewriteRaceSeam`] so the first attempt of
    /// `rewrite_data_files_once` pauses right before its final commit. See
    /// the field doc comment on `race_seam` above.
    #[cfg(all(test, feature = "test-sqlite"))]
    #[must_use = "with_test_race_seam consumes self; bind the returned handler"]
    fn with_test_race_seam(mut self, seam: Arc<RewriteRaceSeam>) -> Self {
        self.race_seam = Some(seam);
        self
    }

    /// Attach the shared coordinator runtime for spillable sort compaction.
    #[must_use = "with_runtime consumes self; bind the returned handler"]
    pub fn with_runtime(
        mut self,
        runtime: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Self {
        self.runtime = Some(runtime);
        self
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

    /// Attach the coordinator's worker registry (Phase 4c Task 5) so
    /// `rewrite_data_files`'s `distribution.mode` routing can see the live
    /// healthy-worker count instead of always assuming zero workers.
    ///
    /// Never call this in a configuration where `[coordinator]` has no
    /// worker fleet: an empty/never-healthy registry behaves identically to
    /// `None` (see [`Self::healthy_worker_count`]), so wiring it unconditionally
    /// at every construction site is safe -- the single-node/no-fleet path
    /// stays on `Local` either way.
    #[must_use = "with_worker_registry consumes self; bind the returned handler"]
    pub fn with_worker_registry(mut self, registry: Arc<WorkerRegistry>) -> Self {
        self.worker_registry = Some(registry);
        self
    }

    /// Sign `compact_file_group` requests with the coordinator's ticket key.
    #[must_use = "with_ticket_signer consumes self; bind the returned handler"]
    pub fn with_ticket_signer(
        mut self,
        signer: Arc<sqe_core::ticket_signing::TicketSigner>,
    ) -> Self {
        self.ticket_signer = Some(signer);
        self
    }

    /// Healthy worker count as `distribution.mode` resolution sees it: `0`
    /// when no registry was ever attached (see [`Self::with_worker_registry`]),
    /// otherwise the registry's live count. This is the ONLY place
    /// `resolve_execution`'s `healthy_workers` input comes from in production
    /// code; the scheduler's active path reads it through this same method
    /// (via `self.handler`) so the manual `CALL` path and the scheduler can
    /// never disagree about how many workers are healthy right now.
    pub(crate) async fn healthy_worker_count(&self) -> usize {
        match &self.worker_registry {
            Some(registry) => registry.healthy_workers().await.len(),
            None => 0,
        }
    }

    /// Build the S3 connection details a worker needs for `compact_file_group`,
    /// from this coordinator's own storage config. Mirrors the flattened
    /// `s3_*` fields `QueryHandler::try_distribute` already sends workers on
    /// the scan path (`query_handler.rs`'s `ScanTask` construction) field for
    /// field, so a worker's object-store construction behaves identically
    /// whether the bytes arrived via a scan ticket or a compaction group.
    fn s3_conn(&self) -> sqe_compaction::wire::S3Conn {
        let storage = &self.config.storage;
        sqe_compaction::wire::S3Conn {
            endpoint: storage.s3_endpoint.clone(),
            region: storage.s3_region.clone(),
            access_key: storage.s3_access_key.clone(),
            secret_key: storage.s3_secret_key.expose().to_string(),
            session_token: String::new(),
            path_style: storage.s3_path_style,
            allow_http: storage.s3_endpoint.starts_with("http://"),
        }
    }

    /// Distributed rewrite, with every coordinator-owned dependency
    /// (`s3_conn`, `worker_registry`, `worker_load`, `worker_secret`,
    /// `distribution` config) pulled from `self` instead of threaded in by
    /// the caller. This is what Task 5's `distribution.mode` routing calls
    /// from both `handle()`'s manual `CALL` arm and the active-mode
    /// scheduler; [`Self::rewrite_data_files_distributed`] itself keeps its
    /// Task 4 signature (explicit everything) unchanged.
    ///
    /// Callers must only reach this after [`resolve_execution`] has already
    /// returned [`ExecutionPlan::Distributed`] -- that is what guarantees
    /// `self.worker_registry` is `Some` here (a healthy count `>= 1` is only
    /// possible with a registry attached).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rewrite_data_files_distributed_with_defaults(
        &self,
        catalog: &Arc<dyn Catalog>,
        table_ref: &TableRef,
        job_id: &str,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        strategy: Option<String>,
        sort_order: Option<String>,
        delete_file_threshold: Option<usize>,
        delete_ratio_threshold: Option<f64>,
        rewrite_all: bool,
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
    ) -> sqe_core::Result<RewriteOutcome> {
        let registry = self.worker_registry.clone().ok_or_else(|| {
            SqeError::Execution(
                "rewrite_data_files_distributed: resolved to Distributed but no worker \
                 registry is attached to this handler (internal wiring bug)"
                    .into(),
            )
        })?;
        let s3 = self.s3_conn();
        let worker_secret = self.config.coordinator.worker_secret.expose().to_string();
        let dist = self.config.maintenance.distribution.clone();
        self.rewrite_data_files_distributed(
            catalog,
            table_ref,
            job_id,
            &s3,
            &registry,
            &self.worker_load,
            &worker_secret,
            &dist,
            target_file_size_bytes,
            min_input_files,
            strategy,
            sort_order,
            delete_file_threshold,
            delete_ratio_threshold,
            rewrite_all,
            snapshot_properties,
        )
        .await
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
                strategy,
                sort_order,
                delete_file_threshold,
                delete_ratio_threshold,
                distributed,
                rewrite_all,
            } => {
                // Manual `CALL system.rewrite_data_files` always commits with
                // no snapshot-property stamp. Job-identity stamping
                // (`sqe.maintenance.job-id` / `.principal` / `.trigger`) is
                // reserved for the Phase 4b scheduler's internal call path.
                //
                // `rewrite_data_files`/`rewrite_data_files_once` take an
                // already-resolved `catalog` (not a `Session`) so the Phase
                // 4b scheduler can call them directly with the catalog it
                // already built via its own `catalog_factory` seam, without
                // going through `create_catalog_bridge` a second time.
                let catalog = self
                    .create_catalog_bridge(session, table.catalog.as_deref())
                    .await?;

                let rewrite_all = rewrite_all.unwrap_or(false);

                // Phase 4c Task 5: `distribution.mode` routing. `distributed`
                // is the optional per-call override (`distributed =>
                // 'auto'|'local'|'require'`); absent, the configured
                // `[maintenance.distribution] mode` applies. `require` below
                // the healthy-worker floor is an `Err` here -- the manual
                // `CALL` surface is interactive, so a caller who explicitly
                // asked to require the fleet gets a loud failure, never a
                // silent coordinator-local rewrite.
                let mode = match distributed {
                    Some(raw) => parse_distribution_mode_override(raw)?,
                    None => self.config.maintenance.distribution.mode,
                };
                let dist = self.config.maintenance.distribution.clone();
                let healthy = self.healthy_worker_count().await;
                let outcome = match resolve_execution(mode, healthy, dist.min_workers) {
                    ExecutionPlan::SkipInsufficientWorkers => {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                            "CALL system.rewrite_data_files: distribution mode 'require' \
                             needs >= {} healthy workers, only {healthy} are healthy",
                            dist.min_workers
                        )));
                    }
                    ExecutionPlan::Local => {
                        self.rewrite_data_files(
                            &catalog,
                            table,
                            *target_file_size_bytes,
                            *min_input_files,
                            *max_concurrent_file_group_rewrites,
                            strategy.clone(),
                            sort_order.clone(),
                            *delete_file_threshold,
                            *delete_ratio_threshold,
                            rewrite_all,
                            None,
                        )
                        .await?
                    }
                    ExecutionPlan::Distributed => {
                        let job_id = uuid::Uuid::now_v7().to_string();
                        self.rewrite_data_files_distributed_with_defaults(
                            &catalog,
                            table,
                            &job_id,
                            *target_file_size_bytes,
                            *min_input_files,
                            strategy.clone(),
                            sort_order.clone(),
                            *delete_file_threshold,
                            *delete_ratio_threshold,
                            rewrite_all,
                            None,
                        )
                        .await?
                    }
                };
                Ok(vec![rewrite_outcome_batch(
                    &to_table_ident(table),
                    &outcome,
                )?])
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
            ProcedureCall::RemoveOrphanFiles { table, older_than } => {
                let older_than_ms = older_than.map(|t| t.timestamp_millis());
                self.remove_orphan_files(session, table, older_than_ms)
                    .await
            }
            ProcedureCall::RewriteManifests { table } => {
                self.rewrite_manifests(session, table).await
            }
            ProcedureCall::SuggestBloomFilterColumns {
                table,
                history_limit,
            } => self.suggest_bloom_filter_columns(table, *history_limit),
            ProcedureCall::PurgeOrphanLocations { namespace, dry_run } => {
                self.purge_orphan_locations(session, namespace, *dry_run)
                    .await
            }
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
            ProcedureCall::TableHealth { table } => self.table_health(session, table).await,
            // Self-scoped cache refresh is intercepted by the query router
            // (`query_handler`) and never dispatched through the table-scoped
            // maintenance handler. Guard the exhaustiveness check without
            // pulling cache logic into this handler.
            ProcedureCall::RefreshCatalogCache { .. } => Err(SqeError::Execution(
                "refresh_catalog_cache is handled by the query router, not the maintenance handler"
                    .to_string(),
            )),
            ProcedureCall::ReprojectColumnTags { .. } => Err(SqeError::Execution(
                "reproject_column_tags is handled by the query router, not the maintenance handler"
                    .to_string(),
            )),
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("{op_name} apply failed: {e}")))?;
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
            metadata_location = %location_for_log(metadata_location),
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("register_table summary: {e}")))?;
        Ok(vec![batch])
    }

    /// Drop a table from the catalog. `purge => false` removes the catalog
    /// entry only, so the files stay and the table can be re-registered.
    ///
    /// `purge => true` also deletes everything under the table's location,
    /// through the caller's own FileIO, once the catalog drop has succeeded.
    /// It does not send the REST `purgeRequested` flag: Polaris refuses that
    /// unless `DROP_WITH_PURGE_ENABLED` is set, and when it accepts, it
    /// deletes asynchronously with its own credentials, so the CALL could not
    /// say whether anything was deleted (#836).
    ///
    /// Purge is refused before anything changes when the location cannot be
    /// trusted as this table's own directory; see [`purge_prefix`]. If the
    /// prefix delete fails after the catalog drop, the error says the table
    /// is already gone and names the prefix to clean up by hand.
    async fn drop_table(
        &self,
        session: &Session,
        table_ref: &TableRef,
        purge: bool,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);

        // Load before the drop: afterwards there is no location or FileIO
        // left to purge with.
        let purge_target = if purge {
            let table = load_table(&catalog, &ident).await?;
            let prefix = purge_prefix(&table)?;
            refuse_if_location_shared(&catalog, &ident, &prefix).await?;
            Some((table.file_io().clone(), prefix))
        } else {
            None
        };

        info!(
            user = %session.user.username,
            table = %ident,
            purge,
            "drop_table: requesting catalog drop"
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

        let status = match purge_target {
            None => "dropped",
            Some((file_io, prefix)) => {
                file_io.delete_prefix(&prefix).await.map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!(
                            "drop_table: table '{ident}' was dropped from the catalog, but \
                             deleting its files under '{prefix}' failed: {e}. Delete that \
                             prefix by hand."
                        ),
                    )
                })?;
                info!(
                    user = %session.user.username,
                    table = %ident,
                    prefix = %location_for_log(&prefix),
                    "drop_table: purged the table prefix"
                );
                self.audit_purge(session, table_ref, &prefix);
                "purged"
            }
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("table_identifier", DataType::Utf8, false),
            Field::new("purge", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![ident.to_string()])),
                Arc::new(StringArray::from(vec![purge.to_string()])),
                Arc::new(StringArray::from(vec![status])),
            ],
        )
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("drop_table summary: {e}")))?;
        Ok(vec![batch])
    }

    /// Audit a purge as its own `AdminDdl` event. The statement-level event
    /// carries only the CALL text; this one records that files were deleted
    /// and under which prefix, logged as scheme, bucket and a path hash like
    /// the INFO line (#837).
    fn audit_purge(&self, session: &Session, table_ref: &TableRef, prefix: &str) {
        let Some(ref audit) = self.audit else {
            return;
        };
        let actor = sqe_metrics::audit::Actor::from_parts(
            session.user.username.clone(),
            session.user.subject.clone(),
            session.user.email.clone(),
            session.user.roles.clone(),
            session.user.groups.clone(),
        );
        let resource = sqe_metrics::audit::Resource {
            catalog: table_ref.catalog.clone(),
            namespace: vec![table_ref.namespace.clone()],
            name: table_ref.name.clone(),
            object_type: sqe_metrics::audit::ObjectType::Table,
        };
        let text = format!(
            "CALL system.drop_table(table => '{}', purge => true) deleted prefix {}",
            table_ref.as_string(),
            location_for_log(prefix)
        );
        audit.log_event(sqe_metrics::audit::AuditEvent {
            time: chrono::Utc::now(),
            kind: sqe_metrics::audit::AuditKind::AdminDdl,
            actor,
            outcome: sqe_metrics::audit::Outcome::Success,
            resources: vec![resource],
            policy: None,
            timing: None,
            stats: None,
            query: Some(sqe_metrics::audit::QueryInfo {
                query_hash: sqe_metrics::audit::query_hash(&text),
                text: Some(text),
                statement_type: "procedure".to_string(),
            }),
            session_id: Some(session.id.clone()),
            client_ip: None,
            trace_id: None,
            query_id: None,
            transaction: None,
            integrity: sqe_metrics::audit::Integrity::default(),
        });
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
        crate::suggest_bloom::suggest_bloom_filter_columns(table_ref, &queries, history_limit)
    }

    /// Read-only compaction-debt report: `CALL system.table_health`.
    ///
    /// Collects the same file/delete/read-plan data `rewrite_data_files`
    /// would use (`collect_live_data_files`, `collect_live_delete_files`,
    /// `plan_delete_aware_read`), hands it to the pure
    /// [`crate::table_health::analyze_table_health`], and returns the
    /// resulting summary. Never mutates the table and never requires write
    /// privilege (bypassed in [`authorize_or_deny`] like
    /// `suggest_bloom_filter_columns`).
    async fn table_health(
        &self,
        session: &Session,
        table_ref: &TableRef,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = self
            .create_catalog_bridge(session, table_ref.catalog.as_deref())
            .await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

        let (data_files, delete_files, tasks_by_path) = collect_health_inputs(&table).await?;
        let last_compaction_ms = last_compaction_snapshot_ms(&table);

        let health = crate::table_health::analyze_table_health(
            &data_files,
            &delete_files,
            &tasks_by_path,
            &self.config.maintenance.compaction,
            table.metadata().properties(),
            last_compaction_ms,
        );

        info!(
            table = %ident,
            live_data_files = health.live_data_files,
            small_files = health.small_files,
            delete_files = health.delete_files,
            delete_heavy_files = health.delete_heavy_files,
            eligible_groups = health.eligible_groups,
            "table_health: computed compaction-debt report"
        );

        Ok(vec![crate::table_health::table_health_batch(&health)])
    }

    /// Authorization gate for `CALL system.*` (architecture review 510).
    ///
    /// Procedures are tiered by blast radius, see [`procedure_tier`]:
    /// - read-only procedures need nothing beyond the table access Polaris
    ///   already enforced when the metadata loaded;
    /// - table-write procedures (compaction, manifest rewrite) commit through
    ///   the caller's own token, so Polaris is the backstop; the engine only
    ///   refuses sessions whose roles are explicitly read-only, plus grants
    ///   the scheduler's minted maintenance session;
    /// - admin procedures delete data files or history without a catalog
    ///   commit that Polaris could refuse, or move the table's main branch.
    ///   They require a configured admin role; a session with no roles is
    ///   denied. The maintenance-authority marker does NOT reach this tier.
    ///
    /// Denial paths record an audit entry so operators can detect probing.
    async fn authorize_or_deny(
        &self,
        session: &Session,
        call: &ProcedureCall,
    ) -> sqe_core::Result<()> {
        let tier = procedure_tier(call);
        let target = call.target_label();
        let denial = match tier {
            ProcedureTier::ReadOnly => return Ok(()),
            ProcedureTier::TableWrite => {
                if session.has_maintenance_authority() || !session_is_explicitly_read_only(session)
                {
                    return Ok(());
                }
                format!(
                    "user '{}' has read-only roles {:?} and cannot run '{target}'",
                    session.user.username, session.user.roles
                )
            }
            ProcedureTier::Admin => {
                if self.config.auth.has_admin_role(&session.user.roles) {
                    return Ok(());
                }
                format!(
                    "'{target}' requires one of admin roles {:?}; caller '{}' has roles {:?}",
                    self.config.auth.admin_roles, session.user.username, session.user.roles
                )
            }
        };

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
                    message: Some(denial.clone()),
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
                transaction: None,
                integrity: sqe_metrics::audit::Integrity::default(),
            });
        }
        warn!(
            username = %session.user.username,
            procedure = %call.name(),
            target = %target,
            "Maintenance procedure denied: user lacks write privilege"
        );
        // Authenticated, not authorised: gRPC PermissionDenied via the typed code.
        Err(SqeError::with_code(
            SqeErrorCode::AccessDenied,
            format!(
                "CALL system.{}: {denial}",
                call.name()
            ),
        ))
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
    /// Delete-aware bin-pack rewrite with bounded conflict retry, over an
    /// already-resolved `catalog`.
    ///
    /// `catalog` (rather than a `Session`) is the entry point so this method
    /// -- and the retried-per-attempt `rewrite_data_files_once` beneath it --
    /// can be called two ways that must reuse EXACTLY the same code:
    ///
    /// 1. `handle()`'s `RewriteDataFiles` arm builds `catalog` via
    ///    `create_catalog_bridge` from the interactive session, for
    ///    `CALL system.rewrite_data_files`.
    /// 2. The Phase 4b active-mode scheduler (`maintenance_scheduler.rs`)
    ///    builds `catalog` via its own `catalog_factory` seam from a minted
    ///    maintenance `Session`, and passes `snapshot_properties` carrying
    ///    the `sqe.maintenance.*` job-identity stamp -- something the manual
    ///    CALL path never does (see the `handle()` comment above).
    ///
    /// `pub(crate)` (not private): the scheduler lives in a sibling module of
    /// this crate, not a descendant of this one.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rewrite_data_files(
        &self,
        catalog: &Arc<dyn Catalog>,
        table_ref: &TableRef,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        max_concurrent_file_group_rewrites: Option<usize>,
        strategy: Option<String>,
        sort_order: Option<String>,
        delete_file_threshold: Option<usize>,
        delete_ratio_threshold: Option<f64>,
        rewrite_all: bool,
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
    ) -> sqe_core::Result<RewriteOutcome> {
        const MAX_COMMIT_ATTEMPTS: usize = 4;

        let mut attempt: usize = 0;
        loop {
            attempt += 1;
            match self
                .rewrite_data_files_once(
                    catalog,
                    table_ref,
                    target_file_size_bytes,
                    min_input_files,
                    max_concurrent_file_group_rewrites,
                    strategy.clone(),
                    sort_order.clone(),
                    delete_file_threshold,
                    delete_ratio_threshold,
                    rewrite_all,
                    snapshot_properties.clone(),
                )
                .await
            {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // classify_commit_error tags retryable conflicts in the
                    // message ("retryable" / "conflict"). Anything else is a
                    // permanent failure we surface immediately.
                    if is_retryable_commit_error(&e) && attempt < MAX_COMMIT_ATTEMPTS {
                        let backoff = commit_backoff(attempt);
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
        catalog: &Arc<dyn Catalog>,
        table_ref: &TableRef,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        max_concurrent_file_group_rewrites: Option<usize>,
        strategy: Option<String>,
        sort_order: Option<String>,
        delete_file_threshold: Option<usize>,
        delete_ratio_threshold: Option<f64>,
        rewrite_all: bool,
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
    ) -> sqe_core::Result<RewriteOutcome> {
        const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;
        const DEFAULT_MIN_INPUT_FILES: usize = 5;
        const DEFAULT_MAX_CONCURRENT_GROUPS: usize = 4;

        let target_bytes = target_file_size_bytes.unwrap_or(DEFAULT_TARGET_FILE_SIZE_BYTES);
        let min_input = min_input_files.unwrap_or(DEFAULT_MIN_INPUT_FILES);
        let max_concurrent =
            max_concurrent_file_group_rewrites.unwrap_or(DEFAULT_MAX_CONCURRENT_GROUPS);

        let ident = to_table_ident(table_ref);
        let table = load_table(catalog, &ident).await?;

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
        // The plan-baseline snapshot id, captured from this SAME table load.
        // Passed to `set_validate_from_snapshot_id` below (VENDOR PATCH
        // fix/compaction-concurrent-delete-conflict): if a concurrent MoR
        // position delete lands on one of the data files this rewrite
        // replaces after this baseline, the commit is rejected as a
        // retryable conflict instead of silently resurrecting the deleted
        // rows (a dangling position delete that no longer matches any live
        // file path once the rewrite lands its compacted output under a new
        // path). `None` on a brand-new/empty table: there is no snapshot to
        // validate against, and `removed_data_files` is empty anyway in that
        // case (see `validate_no_new_position_deletes`'s empty-set fast path).
        let plan_snapshot_id = table
            .metadata_ref()
            .current_snapshot()
            .map(|s| s.snapshot_id());
        let read_plan = plan_delete_aware_read(&table).await?;
        let live_deletes = collect_live_delete_files(&table).await?;

        // Resolve the sort strategy against the table schema (None = bin-pack).
        // Sort compaction needs the shared spillable runtime; refuse rather than
        // risk the known sort-on-write OOM if it was not wired in.
        let arrow_schema: arrow_schema::SchemaRef = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
                .map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("compaction schema conversion failed: {e}"))
                })?,
        );
        let sort_spec = parse_sort_spec(strategy.as_deref(), sort_order.as_deref(), &arrow_schema)?;
        let sort_ctx: Option<Arc<SortCtx>> = match sort_spec {
            None => None,
            Some(spec) => {
                let runtime = self.runtime.clone().ok_or_else(|| {
                    SqeError::Execution(
                        "rewrite_data_files: sort strategy requires the shared runtime; \
                         not available in this handler"
                            .into(),
                    )
                })?;
                Some(Arc::new(SortCtx { runtime, spec }))
            }
        };

        let old_data_files = collect_live_data_files(&table).await?;
        let input_count = old_data_files.len();
        let total_bytes: i64 = old_data_files
            .iter()
            .map(|f| f.file_size_in_bytes() as i64)
            .sum();
        let total_input_rows: u64 = old_data_files.iter().map(|f| f.record_count()).sum();

        // Delete-heavy data files: those with at least `delete_file_threshold`
        // delete files applying to them. The scan planner attaches every
        // applicable delete file (position AND equality) to each data file's
        // tasks, so this count matches the read cost. These files are worth
        // rewriting even when they are already at or above the target size (to
        // apply the accumulated deletes and shed the delete-file layer), which
        // bin-pack would otherwise leave alone. A file whose deleted-row share
        // reaches `delete_ratio_threshold` qualifies too: on a V3 table each
        // data file has at most one deletion vector, so the count alone never
        // fires there. Empty unless the caller set a threshold. Moot under `strategy => 'sort'`, which already rewrites the
        // whole partition, so it is only computed for the bin-pack path.
        let delete_heavy: std::collections::HashSet<String> = if sort_ctx.is_some() {
            std::collections::HashSet::new()
        } else {
            delete_triggered_files(
                &read_plan.tasks_by_path,
                delete_file_threshold,
                delete_ratio_threshold,
            )
        };

        // Skip a table below `min_input_files` UNLESS a delete-heavy file makes
        // it worth rewriting anyway (apply accumulated deletes) or the caller
        // asked to rewrite everything. Both deliberately override the file-count
        // floor.
        if input_count < min_input && delete_heavy.is_empty() && !rewrite_all {
            info!(
                table = %ident,
                input_count,
                min_input,
                "rewrite_data_files: skipping, below min_input_files"
            );
            return Ok(RewriteOutcome {
                files_in: input_count as i64,
                files_out: 0,
                bytes_in: total_bytes,
                bytes_out: 0,
                rows_removed: 0,
                snapshot_id: None,
                files_rewritten: 0,
                skipped_reason: Some("below min_input_files".to_string()),
                partial: false,
                partial_error: None,
            });
        }

        // Files kept even when at or above target: delete-heavy files always, and
        // every file when `rewrite_all` forces a full re-encode. `rewrite_all` is
        // a superset of `delete_heavy`, so it wins when set. Bin-pack still packs
        // small files under target; a large force-included file anchors its own
        // group (rewritten alone to apply its deletes / re-encode).
        let all_paths: std::collections::HashSet<String>;
        let force_include: &std::collections::HashSet<String> = if rewrite_all {
            all_paths = old_data_files
                .iter()
                .map(|f| f.file_path().to_string())
                .collect();
            &all_paths
        } else {
            &delete_heavy
        };

        // Grouping strategy depends on whether we are sorting.
        //
        // Bin-pack (no sort): greedy-pack small files into groups under
        // `target_bytes`. Files already at or above target are skipped (no win
        // from re-emitting them) unless they are delete-heavy, in which case
        // `force_include` keeps them so their deletes get applied. Partition-
        // aware so a cross-partition group does not fan back out to ~1 file per
        // partition on write.
        //
        // Sort / z-order: pack the WHOLE partition into a single group,
        // including files already at or above target. The group is sorted as
        // one stream and the rolling writer cuts it at `target_bytes`, so the
        // output files carry disjoint key ranges (the property that makes the
        // layout prunable at scale). Per-group sorting would instead leave each
        // output file spanning the full key domain -> no pruning.
        let groups = if sort_ctx.is_some() {
            group_files_by_partition(&old_data_files)
        } else {
            pack_file_groups_partition_aware(&old_data_files, target_bytes, force_include)
        };

        // A group is worth rewriting when it meets `min_input` (real
        // consolidation), the caller forced `rewrite_all`, OR it contains a
        // delete-heavy file (apply accumulated deletes, even if the group is
        // small or the file is large).
        let eligible_groups: Vec<Vec<DataFile>> = groups
            .into_iter()
            .filter(|g| {
                g.len() >= min_input
                    || rewrite_all
                    || g.iter().any(|f| delete_heavy.contains(f.file_path()))
            })
            .collect();

        if eligible_groups.is_empty() {
            info!(
                table = %ident,
                input_count,
                "rewrite_data_files: no groups meet min_input_files after packing"
            );
            return Ok(RewriteOutcome {
                files_in: input_count as i64,
                files_out: 0,
                bytes_in: total_bytes,
                bytes_out: 0,
                rows_removed: 0,
                snapshot_id: None,
                files_rewritten: 0,
                skipped_reason: Some("no eligible groups".to_string()),
                partial: false,
                partial_error: None,
            });
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
        let results: Vec<(Vec<DataFile>, Vec<DataFile>, u64)> = stream::iter(eligible_groups)
            .map(|group| {
                let table_for_group = table_arc.clone();
                let tracker_for_group = tracker.clone();
                let plan_for_group = read_plan_arc.clone();
                let deletes_for_group = live_deletes_arc.clone();
                let schema_for_group = arrow_schema.clone();
                let sort_for_group = sort_ctx.clone();
                async move {
                    rewrite_group(
                        &table_for_group,
                        &plan_for_group,
                        &deletes_for_group,
                        &schema_for_group,
                        sort_for_group.as_deref(),
                        group,
                        compression,
                        tracker_for_group,
                        target_bytes,
                        // The coordinator-local `CALL system.rewrite_data_files`
                        // path has no heartbeat to feed (there is no
                        // dispatch RPC to bound); only the distributed
                        // worker path (`sqe-worker::compaction`) supplies
                        // a progress reporter.
                        None,
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "rewrite_data_files row-count invariant violated: added={added_rows} exceeds \
                 removed={removed_rows} (read_count={rewritten_rows}); a rewrite cannot \
                 increase row count; aborting before commit"
            )));
        }

        let output_count = new_files.len() as i64;
        let output_bytes: i64 = new_files
            .iter()
            .map(|f| f.file_size_in_bytes() as i64)
            .sum();

        // Position delete files whose referenced data file we are removing are
        // now dangling; drop them in the same commit so the delete-file layer
        // shrinks along with the data. Equality deletes are left to age out.
        let removed_data_paths: std::collections::HashSet<String> = old_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();
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
        // set_validate_from_snapshot_id(plan_snapshot_id) is the position-delete
        // conflict-correctness keystone (VENDOR PATCH
        // fix/compaction-concurrent-delete-conflict): see the comment on
        // `plan_snapshot_id`'s capture above and
        // `RewriteFilesAction::set_validate_from_snapshot_id`'s doc comment in
        // `vendor/iceberg-rust/crates/iceberg/src/transaction/rewrite_files.rs`
        // for the full rationale. A hit surfaces as a retryable conflict error
        // from `.commit()` below, which `classify_commit_error` flags and the
        // `rewrite_data_files` retry wrapper (this method's caller) turns into
        // a full re-plan against the fresh snapshot -- so the new delete gets
        // applied to the retry's compacted output instead of being silently
        // dropped.
        let mut action = tx
            .rewrite_files()
            .set_enable_delete_filter_manager(true)
            .set_check_file_existence(true)
            .set_new_data_file_sequence_number(seq_at_start)
            .set_validate_from_snapshot_id(plan_snapshot_id)
            .add_data_files(new_files)
            .delete_files(files_to_remove);
        // Job-identity stamp for autonomous compactions (Phase 4b scheduler).
        // The manual `CALL system.rewrite_data_files` path always passes
        // `None` here, so its committed snapshot summary is byte-identical
        // to before this parameter existed.
        if let Some(props) = snapshot_properties {
            action.set_snapshot_properties(props);
        }
        let tx_applied = action
            .apply(tx)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("rewrite_files apply failed: {e}")))?;

        // TEST-ONLY (see `race_seam`'s doc comment): pauses the FIRST attempt
        // right here -- after the plan baseline was captured from a table
        // load that is now stale, and after this attempt finished computing
        // its compacted output, but BEFORE the commit below -- so a test can
        // deterministically land a concurrent commit inside the conflict
        // window `set_validate_from_snapshot_id` guards. `fired` makes this
        // one-shot: a retry (a fresh call to this method after a rejected
        // first attempt) runs straight through, matching real retry behavior.
        // Compiled only under `#[cfg(all(test, feature = "test-sqlite"))]`; always a no-op in production.
        #[cfg(all(test, feature = "test-sqlite"))]
        if let Some(seam) = self.race_seam.as_ref() {
            if !seam.fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                seam.ready.notify_one();
                seam.go.notified().await;
            }
        }

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "rewrite_data_files"))?;
        cleanup_guard.mark_committed();
        let new_snapshot_id = committed.metadata().current_snapshot_id();

        // Compaction rewrites files, not rows: the previous snapshot's Puffin
        // NDV statistics stay exact for the new one, so re-register them
        // (architecture review 512). Best effort; the data commit is done.
        crate::puffin_carry::carry_forward_statistics(
            catalog.as_ref(),
            &ident,
            &committed,
            crate::puffin_carry::CarryOp::Rewrite,
            None,
            self.metrics.as_deref(),
        )
        .await;

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

        // `rows_removed`: rows physically eliminated by this rewrite via
        // delete application (removed_rows counts every row in the OLD
        // files being replaced; added_rows counts what the writer actually
        // emitted for the NEW files -- the gap is rows a covered delete
        // dropped). The row-count invariant checked above guarantees
        // added_rows <= removed_rows, so this never underflows.
        let rows_removed = (removed_rows - added_rows) as i64;

        Ok(RewriteOutcome {
            files_in: input_count as i64,
            files_out: output_count,
            bytes_in: total_bytes,
            bytes_out: output_bytes,
            rows_removed,
            snapshot_id: new_snapshot_id,
            files_rewritten: old_files.len() as i64,
            skipped_reason: None,
            partial: false,
            partial_error: None,
        })
    }

    /// Distributed counterpart to [`Self::rewrite_data_files`] (Phase 4c
    /// Task 4): bounded conflict retry, same as the local path, but each
    /// attempt fans the bin-packed groups out to the worker fleet
    /// (`compact_file_group`) instead of rewriting them on the coordinator.
    ///
    /// Called by [`Self::rewrite_data_files_distributed_with_defaults`]
    /// (Phase 4c Task 5), which both `handle()`'s manual `CALL` arm and the
    /// active-mode scheduler go through once [`resolve_execution`] has
    /// decided [`ExecutionPlan::Distributed`]. This lower-level method keeps
    /// its Task 4 signature (every dependency explicit) so its own unit
    /// tests are unaffected by the Task 5 wiring.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rewrite_data_files_distributed(
        &self,
        catalog: &Arc<dyn Catalog>,
        table_ref: &TableRef,
        job_id: &str,
        s3: &sqe_compaction::wire::S3Conn,
        registry: &Arc<crate::worker_registry::WorkerRegistry>,
        load_tracker: &crate::worker_registry::WorkerLoadTracker,
        worker_secret: &str,
        dist: &sqe_core::config::MaintenanceDistributionConfig,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        strategy: Option<String>,
        sort_order: Option<String>,
        delete_file_threshold: Option<usize>,
        delete_ratio_threshold: Option<f64>,
        rewrite_all: bool,
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
    ) -> sqe_core::Result<RewriteOutcome> {
        const MAX_COMMIT_ATTEMPTS: usize = 4;
        let mut attempt: usize = 0;
        loop {
            attempt += 1;
            match self
                .rewrite_data_files_distributed_once(
                    catalog,
                    table_ref,
                    job_id,
                    s3,
                    registry,
                    load_tracker,
                    worker_secret,
                    dist,
                    target_file_size_bytes,
                    min_input_files,
                    strategy.clone(),
                    sort_order.clone(),
                    delete_file_threshold,
                    delete_ratio_threshold,
                    rewrite_all,
                    snapshot_properties.clone(),
                )
                .await
            {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // Same retry classification as the local path: a
                    // concurrent writer that commits between our read and
                    // our commit (or a group that has to be re-planned)
                    // surfaces as a retryable conflict; re-plan and
                    // re-dispatch from scratch rather than patch the stale
                    // attempt, for the same correctness reason
                    // `rewrite_data_files` re-reads on retry.
                    if is_retryable_commit_error(&e) && attempt < MAX_COMMIT_ATTEMPTS {
                        let backoff = commit_backoff(attempt);
                        warn!(
                            table = %to_table_ident(table_ref),
                            attempt,
                            backoff_ms = backoff.as_millis() as u64,
                            "rewrite_data_files_distributed: retryable commit conflict; \
                             re-planning and re-dispatching"
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
    async fn rewrite_data_files_distributed_once(
        &self,
        catalog: &Arc<dyn Catalog>,
        table_ref: &TableRef,
        job_id: &str,
        s3: &sqe_compaction::wire::S3Conn,
        registry: &Arc<crate::worker_registry::WorkerRegistry>,
        load_tracker: &crate::worker_registry::WorkerLoadTracker,
        worker_secret: &str,
        dist: &sqe_core::config::MaintenanceDistributionConfig,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        strategy: Option<String>,
        sort_order: Option<String>,
        delete_file_threshold: Option<usize>,
        delete_ratio_threshold: Option<f64>,
        rewrite_all: bool,
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
    ) -> sqe_core::Result<RewriteOutcome> {
        const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;
        const DEFAULT_MIN_INPUT_FILES: usize = 5;

        let target_bytes = target_file_size_bytes.unwrap_or(DEFAULT_TARGET_FILE_SIZE_BYTES);
        let min_input = min_input_files.unwrap_or(DEFAULT_MIN_INPUT_FILES);

        let ident = to_table_ident(table_ref);
        let table = load_table(catalog, &ident).await?;

        let metadata_location = table
            .metadata_location_result()
            .map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "rewrite_data_files_distributed: table has no metadata location: {e}"
                ))
            })?
            .to_string();

        // Same sequence-number pin as the local path (see the extensive
        // comment on `rewrite_data_files_once`): the rewritten data files
        // are pinned to the snapshot we read here so a concurrently
        // committed equality delete at a higher sequence number still
        // applies to the compacted output.
        let seq_at_start = table
            .metadata_ref()
            .current_snapshot()
            .map(|s| s.sequence_number())
            .unwrap_or(0);
        let snapshot_id = table
            .metadata_ref()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0);
        // The plan-baseline snapshot id for `set_validate_from_snapshot_id`
        // (VENDOR PATCH fix/compaction-concurrent-delete-conflict; see the
        // twin comment in `rewrite_data_files_once`). Deliberately a SEPARATE
        // `Option<i64>` from `snapshot_id` above: `snapshot_id` feeds the wire
        // dispatch to workers and defaults to `0` (a real, if unlikely,
        // snapshot id) when the table has no current snapshot yet, whereas the
        // validator needs a true `None` in that case -- `Some(0)` would make
        // it look for a baseline snapshot that was never captured and either
        // false-positive-conflict or match the wrong snapshot by coincidence.
        let plan_snapshot_id = table
            .metadata_ref()
            .current_snapshot()
            .map(|s| s.snapshot_id());

        let read_plan = plan_delete_aware_read(&table).await?;
        let live_deletes = collect_live_delete_files(&table).await?;

        let arrow_schema: arrow_schema::SchemaRef = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
                .map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("compaction schema conversion failed: {e}"))
                })?,
        );
        // The sort is resolved here (same parser the local path uses) but
        // applied on the WORKER, not the coordinator: each request below
        // carries the resolved spec so the worker builds its own SortCtx
        // against its own runtime. The coordinator never needs a spillable
        // runtime for the distributed path.
        let sort_spec = parse_sort_spec(strategy.as_deref(), sort_order.as_deref(), &arrow_schema)?;
        let sort_wire: Option<sqe_compaction::wire::SortSpecWire> =
            sort_spec.as_ref().map(std::convert::Into::into);

        let old_data_files = collect_live_data_files(&table).await?;
        let input_count = old_data_files.len();
        let total_bytes: i64 = old_data_files
            .iter()
            .map(|f| f.file_size_in_bytes() as i64)
            .sum();

        let delete_heavy: std::collections::HashSet<String> = if sort_spec.is_some() {
            std::collections::HashSet::new()
        } else {
            delete_triggered_files(
                &read_plan.tasks_by_path,
                delete_file_threshold,
                delete_ratio_threshold,
            )
        };

        if input_count < min_input && delete_heavy.is_empty() && !rewrite_all {
            info!(
                table = %ident,
                input_count,
                min_input,
                "rewrite_data_files_distributed: skipping, below min_input_files"
            );
            return Ok(RewriteOutcome {
                files_in: input_count as i64,
                files_out: 0,
                bytes_in: total_bytes,
                bytes_out: 0,
                rows_removed: 0,
                snapshot_id: None,
                files_rewritten: 0,
                skipped_reason: Some("below min_input_files".to_string()),
                partial: false,
                partial_error: None,
            });
        }

        // Same force-include semantics as the local path: when `rewrite_all`
        // is set, every file is force-included so bin-pack does not skip
        // files already at or above target.
        let all_paths: std::collections::HashSet<String>;
        let force_include: &std::collections::HashSet<String> = if rewrite_all {
            all_paths = old_data_files
                .iter()
                .map(|f| f.file_path().to_string())
                .collect();
            &all_paths
        } else {
            &delete_heavy
        };

        let groups = if sort_spec.is_some() {
            group_files_by_partition(&old_data_files)
        } else {
            pack_file_groups_partition_aware(&old_data_files, target_bytes, force_include)
        };

        let eligible_groups: Vec<Vec<DataFile>> = groups
            .into_iter()
            .filter(|g| {
                g.len() >= min_input
                    || rewrite_all
                    || g.iter().any(|f| delete_heavy.contains(f.file_path()))
            })
            .collect();

        if eligible_groups.is_empty() {
            info!(
                table = %ident,
                input_count,
                "rewrite_data_files_distributed: no groups meet min_input_files after packing"
            );
            return Ok(RewriteOutcome {
                files_in: input_count as i64,
                files_out: 0,
                bytes_in: total_bytes,
                bytes_out: 0,
                rows_removed: 0,
                snapshot_id: None,
                files_rewritten: 0,
                skipped_reason: Some("no eligible groups".to_string()),
                partial: false,
                partial_error: None,
            });
        }

        info!(
            table = %ident,
            input_count,
            target_bytes,
            group_count = eligible_groups.len(),
            max_inflight_per_worker = dist.max_inflight_groups_per_worker,
            "rewrite_data_files_distributed: dispatching groups to worker fleet"
        );

        let compression = self.config.catalog.parquet_compression.clone();
        let group_timeout = std::time::Duration::from_secs(dist.group_timeout_secs);
        let heartbeat_timeout = std::time::Duration::from_secs(dist.group_heartbeat_timeout_secs);

        // Decode each worker's Avro DataFiles against THIS table load's
        // schema/partition type/spec id/format version -- none of that rides
        // in CompactGroupResponse itself (see its doc comment). Resolved once,
        // up front: every batch below (whether this job ends up as one
        // all-or-nothing commit, the default, or several `partial_progress`
        // batches) decodes against the same planned-against table load.
        let partition_type = table.metadata().default_partition_type().clone();
        let partition_spec_id = table.metadata().default_partition_spec_id();
        let format_version = table.metadata().format_version();
        let schema = table.metadata().current_schema().clone();

        let dispatcher = crate::compaction_dispatch::WorkerFleetDispatcher {
            job_id: job_id.to_string(),
            table_ident: ident.to_string(),
            target_file_size_bytes: target_bytes,
            compression,
            sort: sort_wire,
            s3: s3.clone(),
            registry: Arc::clone(registry),
            load_tracker: load_tracker.clone(),
            worker_secret: worker_secret.to_string(),
            ticket_signer: self.ticket_signer.clone(),
            max_inflight_per_worker: dist.max_inflight_groups_per_worker,
            group_attempts: dist.group_attempts,
            group_timeout,
            heartbeat_timeout,
            schema,
            partition_spec_id,
            partition_type,
            format_version,
        };

        // Dispatch + commit every eligible group.
        //
        // Default (`partial_progress` off, the pre-Task-3 behavior): one
        // all-or-nothing `RewriteFilesAction` over every group -- any group
        // exhausting its retries fails the whole job before any commit is
        // attempted, byte-identical to before this field existed.
        //
        // Opted in: disjoint batches of `partial_progress_batch` groups,
        // each its own commit; a terminal group failure after some batches
        // already committed reports `partial = true` instead of failing the
        // whole job. See [`Self::commit_eligible_groups`]'s doc comment for
        // the seq-pin / `check_file_existence` correctness argument that
        // holds across batches.
        Self::commit_eligible_groups(
            catalog,
            table,
            &ident,
            eligible_groups,
            dist.partial_progress,
            dist.partial_progress_batch,
            seq_at_start,
            plan_snapshot_id,
            &metadata_location,
            snapshot_id,
            &live_deletes,
            snapshot_properties,
            self.table_cache.as_ref(),
            input_count,
            total_bytes,
            &dispatcher,
            self.metrics.as_deref(),
        )
        .await
    }

    /// Commit `eligible_groups`'s rewrite either as one atomic
    /// `RewriteFilesAction` (default, `partial_progress = false`,
    /// byte-identical to the pre-Task-3 behavior) or, when opted in, in
    /// disjoint batches of `partial_progress_batch` groups each getting its
    /// own `RewriteFilesAction` commit (Phase 4d Task 3: opt-in
    /// partial-progress commits).
    ///
    /// `dispatcher` abstracts "dispatch this batch of groups to the worker
    /// fleet and decode the responses" so this function's batching/commit/
    /// partial bookkeeping can be driven by a synthetic
    /// [`crate::compaction_dispatch::GroupBatchDispatcher`] in tests,
    /// without a live worker fleet (see
    /// `crate::compaction_dispatch::WorkerFleetDispatcher` for the
    /// production implementation).
    ///
    /// # Per-batch commit sequence
    ///
    /// Every batch's `RewriteFilesAction` uses the EXACT same sequence as
    /// the pre-Task-3 single commit: `set_enable_delete_filter_manager(true)`,
    /// `set_check_file_existence(true)`, `set_new_data_file_sequence_number`
    /// pinned to `seq_at_start`, the snapshot-property stamp, and
    /// `add_data_files(batch_new).delete_files(batch_old +
    /// batch_covered_deletes)`. The `added <= removed` row invariant
    /// (`aggregate_group_outcomes`) is re-checked per batch, over just that
    /// batch's own files.
    ///
    /// # Why `seq_at_start` never moves
    ///
    /// `seq_at_start` is the sequence number of the snapshot this job
    /// planned against, captured ONCE by the caller before any batch runs.
    /// Every batch's compacted output is pinned to it -- never to the
    /// snapshot that batch actually commits against -- so a concurrent
    /// equality delete committed at any point during this job, including
    /// between two of our own batch commits, still out-ranks the compacted
    /// files and continues to apply to them. Pinning batch K+1 to the
    /// snapshot batch K just created instead would let a delete that landed
    /// between K and K+1 dodge the rows batch K+1 rewrites, silently
    /// resurrecting them. So the pin is fixed at plan time and reused
    /// verbatim for every batch, exactly like the non-batched path reuses it
    /// for its one commit.
    ///
    /// # Why disjoint batches keep `check_file_existence` valid across commits
    ///
    /// `eligible_groups` partitions every file into exactly one group, and
    /// batching only chunks that partition further, so no data file appears
    /// in two batches. After batch K commits, the table's live file set has
    /// batch K's input files removed and its output files added; batch
    /// K+1's input files are a disjoint set neither committed nor touched
    /// by K, so they are still exactly where they were. `check_file_existence
    /// (true)` on batch K+1's commit finds them and validates cleanly,
    /// unless a concurrent EXTERNAL writer removed one -- exactly the same
    /// conflict `check_file_existence` exists to catch on the non-batched
    /// path, just now surfaced once per batch instead of once per job.
    ///
    /// # Position deletes stay disjoint too
    ///
    /// `covered_position_deletes` is computed per batch against the SAME
    /// `live_deletes` snapshot the caller collected once at plan time, but
    /// restricted to that batch's own removed data-file paths. A position
    /// delete file has exactly one `referenced_data_file`, so it can only
    /// ever be "covered" by the one batch (if any) that removes its
    /// referenced file -- batches can never race to claim the same delete
    /// file.
    ///
    /// # Commit-conflict retry layering (Task 3 hardening)
    ///
    /// Two retry layers exist across this job, and they are deliberately
    /// scoped to never both fire for the same commit:
    ///
    /// - The OUTER loop, [`Self::rewrite_data_files_distributed`], retries a
    ///   retryable `Err` by re-planning and re-dispatching the WHOLE job
    ///   from scratch (fresh table load, fresh groups, fresh worker
    ///   dispatch). It owns retrying a failure of the FIRST batch to commit
    ///   in a job (`committed_batches == 0` at failure time) -- exactly the
    ///   only batch that exists when `partial_progress` is off, which is
    ///   why that default path is untouched by this fix.
    /// - This function's INNER retry (inside [`Self::commit_one_batch`])
    ///   owns retrying a retryable commit conflict on any batch AFTER the
    ///   first has already committed (`committed_batches > 0`). Unlike an
    ///   earlier version of this fix, it does NOT reuse the first dispatch's
    ///   worker output: it reloads the table to get a fresh
    ///   `metadata_location`/current `snapshot_id`, RE-DISPATCHES the same
    ///   batch of groups against that fresh snapshot, re-aggregates the new
    ///   outcomes, and only then rebuilds and retries the
    ///   `RewriteFilesAction`, up to `MAX_COMMIT_ATTEMPTS` with the same
    ///   exponential backoff the outer loop uses. Reusing stale worker
    ///   output was unsound: those files were produced by workers reading
    ///   only the PLAN-TIME snapshot, so a concurrent commit that landed a
    ///   POSITION delete on one of this batch's own input files (the thing
    ///   that makes the commit retryable in the first place) would never be
    ///   observed -- the position delete's `(old_file_path, pos)` no longer
    ///   matches anything once the old file is replaced by the compacted
    ///   output, silently resurrecting the deleted rows. Re-dispatching
    ///   against the reloaded snapshot means the worker applies whatever
    ///   position AND equality deletes are current as of that reload, so the
    ///   rows stay deleted. `seq_at_start` itself never changes across a
    ///   retry (see "Why `seq_at_start` never moves" above) -- only the
    ///   snapshot workers are told to READ from does.
    ///
    /// Because the inner layer only ever activates for `committed_batches >
    /// 0`, and the outer layer only ever sees an `Err` bubble out of a
    /// `committed_batches == 0` failure, the two layers partition the
    /// batch space and never stack: there is no 4x4 = 16-attempt blowup.
    /// Before this fix, a retryable conflict on batch 2+ hit neither layer:
    /// it fell straight into the `partial = true` fallback below with no
    /// retry at all, even though the underlying conflict was transient. An
    /// intermediate version of this fix (commit 5d9bebe) DID retry, but by
    /// recommitting the stale worker output rather than re-dispatching --
    /// which fixed the "no retry at all" gap but introduced the row-
    /// resurrection bug described above; this version closes that gap too.
    ///
    /// # Return value
    ///
    /// Returns `Ok` with `partial = true` when `partial_progress` is on, at
    /// least one batch already committed, and a later batch's dispatch/
    /// aggregate/commit TERMINALLY failed (non-retryable, or retryable with
    /// the inner retry's attempts exhausted); the already-committed batches
    /// are never rolled back. Returns `Err` -- no commit at all -- when
    /// `partial_progress` is off, or when it is on but the very first batch
    /// failed before anything committed (there is nothing "partial" about
    /// zero commits) -- that `Err` is what the outer loop retries.
    #[allow(clippy::too_many_arguments)]
    async fn commit_eligible_groups(
        catalog: &Arc<dyn Catalog>,
        mut table: IcebergTable,
        ident: &TableIdent,
        eligible_groups: Vec<Vec<DataFile>>,
        partial_progress: bool,
        partial_progress_batch: usize,
        seq_at_start: i64,
        plan_snapshot_id: Option<i64>,
        metadata_location: &str,
        snapshot_id: i64,
        live_deletes: &[DataFile],
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
        table_cache: Option<&TableMetadataCache>,
        input_count: usize,
        total_bytes: i64,
        dispatcher: &dyn crate::compaction_dispatch::GroupBatchDispatcher,
        metrics: Option<&sqe_metrics::MetricsRegistry>,
    ) -> sqe_core::Result<RewriteOutcome> {
        // `partial_progress` off: one chunk containing every eligible group,
        // i.e. exactly the pre-Task-3 single dispatch-then-commit call.
        let batch_size = if partial_progress {
            partial_progress_batch.max(1)
        } else {
            usize::MAX
        };
        let batches: Vec<&[Vec<DataFile>]> = eligible_groups.chunks(batch_size).collect();
        let total_batches = batches.len();

        let mut files_out_total: i64 = 0;
        let mut bytes_out_total: i64 = 0;
        let mut rows_removed_total: i64 = 0;
        let mut files_rewritten_total: i64 = 0;
        let mut last_snapshot_id: Option<i64> = None;
        let mut committed_batches = 0usize;
        let mut live_expected = input_count as i64;

        for (batch_idx, batch) in batches.into_iter().enumerate() {
            // See the "Commit-conflict retry layering" doc section above:
            // only batches committing AFTER the first success get the
            // inner commit-conflict retry. The first batch's failure keeps
            // bubbling as `Err` for the OUTER loop to retry, unchanged --
            // this is what keeps the `partial_progress = false` path (always
            // exactly one batch, so always `committed_batches == 0` here)
            // byte-identical to before this fix.
            let retry_on_conflict = committed_batches > 0;

            let result = Self::commit_one_batch(
                catalog,
                &table,
                ident,
                batch,
                seq_at_start,
                plan_snapshot_id,
                metadata_location,
                snapshot_id,
                live_deletes,
                snapshot_properties.clone(),
                table_cache,
                live_expected,
                dispatcher,
                retry_on_conflict,
                metrics,
            )
            .await;

            match result {
                Ok(batch_result) => {
                    files_out_total += batch_result.output_count;
                    bytes_out_total += batch_result.output_bytes;
                    rows_removed_total += batch_result.rows_removed;
                    files_rewritten_total += batch_result.old_file_count;
                    last_snapshot_id = batch_result.new_snapshot_id;
                    live_expected = batch_result.live_expected_after;
                    table = batch_result.committed_table;
                    committed_batches += 1;
                }
                Err(e) => {
                    if partial_progress && committed_batches > 0 {
                        warn!(
                            table = %ident,
                            batch_idx,
                            total_batches,
                            committed_batches,
                            error = %e,
                            "rewrite_data_files_distributed: partial_progress terminal group \
                             failure after some batches already committed; reporting partial \
                             success instead of failing the whole job"
                        );
                        return Ok(RewriteOutcome {
                            files_in: input_count as i64,
                            files_out: files_out_total,
                            bytes_in: total_bytes,
                            bytes_out: bytes_out_total,
                            rows_removed: rows_removed_total,
                            snapshot_id: last_snapshot_id,
                            files_rewritten: files_rewritten_total,
                            skipped_reason: None,
                            partial: true,
                            partial_error: Some(e.to_string()),
                        });
                    }
                    return Err(e);
                }
            }
        }

        Ok(RewriteOutcome {
            files_in: input_count as i64,
            files_out: files_out_total,
            bytes_in: total_bytes,
            bytes_out: bytes_out_total,
            rows_removed: rows_removed_total,
            snapshot_id: last_snapshot_id,
            files_rewritten: files_rewritten_total,
            skipped_reason: None,
            partial: false,
            partial_error: None,
        })
    }

    /// Dispatch, aggregate, and commit ONE batch (a `RewriteFilesAction`
    /// over just that batch's groups). See
    /// [`Self::commit_eligible_groups`]'s doc comment for the correctness
    /// argument justifying reusing `seq_at_start` unchanged across batches
    /// and for the "Commit-conflict retry layering" section this function
    /// implements the inner half of.
    ///
    /// For the FIRST attempt of every batch, `dispatcher.dispatch` runs
    /// exactly once, against the caller-supplied `metadata_location`/
    /// `snapshot_id` (the job's plan-time snapshot) -- byte-identical to
    /// before this fix. `retry_on_conflict` is `false` for the first batch
    /// to attempt a commit in the job (see the caller), which skips every
    /// check added below entirely and means the loop never iterates more
    /// than once for that batch, so the whole `partial_progress = false`
    /// default path stays an unretried single dispatch + single commit
    /// attempt.
    ///
    /// When `retry_on_conflict` is set (batch 2+, after at least one earlier
    /// batch has already committed), this function does NOT simply wait for
    /// `commit_aggregated_batch` to return a retryable `Err` before acting:
    /// **it cannot rely on that `Err` ever appearing.**
    /// `iceberg::transaction::Transaction::commit`'s `do_commit`
    /// (`vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs`)
    /// silently reloads the table and REPLAYS every action's `commit()` --
    /// unchanged, over the SAME already-computed `added_data_files`/
    /// `removed_data_files` -- whenever it finds its base stale, and only
    /// gives up (surfacing an `Err` to us) once ITS OWN internal retry
    /// budget (`commit.retry.*` table properties) is exhausted. A single,
    /// one-shot concurrent write is absorbed entirely inside that call: we
    /// never see an `Err` for it. `RewriteFilesAction` has no equivalent of
    /// upstream Iceberg's `validateNoNewDeletesForDataFiles` check, so this
    /// silent reapply can commit a compacted file that does not reflect a
    /// position delete some other writer added to one of this batch's own
    /// input files after our worker read them -- silent resurrection, and
    /// it happens INSIDE the vendored `commit()` call, below the level an
    /// `Err`-catching retry could ever observe.
    ///
    /// So for batch 2+, BEFORE every commit attempt (not just after a
    /// caught `Err`), this function reloads the table itself and checks
    /// whether any position delete in the reload references one of this
    /// batch's own input files AND was not already known to the dispatch
    /// that just ran (i.e. is NOT in `cur_live_deletes`). If so, the
    /// worker output just produced cannot reflect it: re-dispatch against
    /// the reload's `metadata_location`/`snapshot_id` instead of
    /// committing, so the worker gets a chance to read it and exclude those
    /// rows. This closes the realistic single-race window entirely (see
    /// the regression test `batch_commit_conflict_from_own_position_delete_does_not_resurrect_rows`).
    /// A sub-millisecond window remains between this reload and
    /// `do_commit`'s own internal reload (a delete landing in exactly that
    /// gap), which cannot be closed without the missing vendor-side
    /// validation; the commit-`Err` retry below remains as a secondary net
    /// for a conflict this check did not preempt (e.g. an unrelated
    /// concurrent writer, or `commit.retry.*` exhausted by sustained
    /// contention), bounded the same way.
    #[allow(clippy::too_many_arguments)]
    async fn commit_one_batch(
        catalog: &Arc<dyn Catalog>,
        table: &IcebergTable,
        ident: &TableIdent,
        batch: &[Vec<DataFile>],
        seq_at_start: i64,
        plan_snapshot_id: Option<i64>,
        metadata_location: &str,
        snapshot_id: i64,
        live_deletes: &[DataFile],
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
        table_cache: Option<&TableMetadataCache>,
        live_expected_before: i64,
        dispatcher: &dyn crate::compaction_dispatch::GroupBatchDispatcher,
        retry_on_conflict: bool,
        metrics: Option<&sqe_metrics::MetricsRegistry>,
    ) -> sqe_core::Result<BatchCommitResult> {
        let batch_old_files: Vec<DataFile> = batch.iter().flatten().cloned().collect();
        let batch_old_paths: std::collections::HashSet<String> = batch_old_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();

        const MAX_COMMIT_ATTEMPTS: usize = 4;
        let mut attempt: usize = 0;
        let mut reloaded_table: Option<IcebergTable> = None;
        // Attempt 1 dispatches against exactly the caller's
        // `metadata_location`/`snapshot_id`/`live_deletes`; owned copies so
        // a retry (or the pre-commit check below) can swap in fresh ones
        // without touching the caller's borrowed originals.
        let mut cur_metadata_location: String = metadata_location.to_string();
        let mut cur_snapshot_id: i64 = snapshot_id;
        let mut cur_live_deletes: Vec<DataFile> = live_deletes.to_vec();
        // The `set_validate_from_snapshot_id` baseline for THIS attempt's
        // commit. Starts at the caller's job-wide `plan_snapshot_id` (attempt
        // 1, no reload yet -- identical to the non-batched path). Every time
        // this loop reloads the table (either arm below), it must move
        // forward to that reload's snapshot id: the vendored
        // `validate_no_new_position_deletes` check treats ANY live position
        // delete newer than the baseline that references one of this batch's
        // own files as an unconditional conflict, with no way to know this
        // same commit's `cur_live_deletes`/`covered_position_deletes` already
        // accounted for it. Leaving the baseline pinned to the job-wide
        // `plan_snapshot_id` would make that vendor check re-flag the exact
        // delete this loop's own pre-commit re-dispatch just correctly
        // incorporated, permanently failing the batch instead of committing
        // the now-correct output. Moving it to the reload keeps the vendor
        // check in its intended "secondary net" role (only catching a delete
        // that lands in the sub-millisecond gap between this reload and the
        // vendored `Transaction::commit`'s own internal reload).
        let mut cur_plan_snapshot_id: Option<i64> = plan_snapshot_id;

        loop {
            attempt += 1;

            // Re-dispatched on every attempt after the first (only reached
            // via a `continue` below), so the worker reads whatever deletes
            // are current as of `cur_snapshot_id` instead of a stale
            // plan-time view.
            let outcomes = dispatcher
                .dispatch(batch, &cur_metadata_location, cur_snapshot_id)
                .await?;

            // Row invariant, re-run per attempt (the per-group
            // `expected_rows_after_deletes` cross-check already ran on the
            // WORKER before it returned); any violation aborts before this
            // batch's commit, leaving any earlier batches' commits intact.
            // Not retried on conflict (there is nothing to retry: this is a
            // data problem, not a stale-snapshot problem).
            let aggregated =
                sqe_compaction::dispatch::aggregate_group_outcomes(outcomes, &batch_old_files)?;

            if retry_on_conflict {
                // See the doc comment above: check for a concurrent
                // position delete on one of THIS batch's own input files
                // BEFORE committing, because a retryable `Err` out of
                // `commit_aggregated_batch` below is not guaranteed to
                // appear even when one landed.
                let reload = load_table(catalog, ident).await?;
                let reload_live_deletes = collect_live_delete_files(&reload).await?;
                let known_paths: std::collections::HashSet<&str> =
                    cur_live_deletes.iter().map(DataFile::file_path).collect();
                let new_covering_delete = reload_live_deletes.iter().any(|d| {
                    d.content_type() == DataContentType::PositionDeletes
                        && d.referenced_data_file()
                            .is_some_and(|p| batch_old_paths.contains(&p))
                        && !known_paths.contains(d.file_path())
                });

                if new_covering_delete {
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        warn!(
                            table = %ident,
                            attempt,
                            "rewrite_data_files_distributed: detected a concurrent position \
                             delete on a partial_progress batch's own input file; \
                             re-dispatching this batch against the current snapshot instead of \
                             committing worker output that predates it"
                        );
                        cur_metadata_location = reload
                            .metadata_location_result()
                            .map_err(|e| {
                                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                                    "rewrite_data_files_distributed: reloaded table has no \
                                     metadata location: {e}"
                                ))
                            })?
                            .to_string();
                        cur_snapshot_id = reload
                            .metadata_ref()
                            .current_snapshot()
                            .map(|s| s.snapshot_id())
                            .unwrap_or(0);
                        cur_plan_snapshot_id = reload
                            .metadata_ref()
                            .current_snapshot()
                            .map(|s| s.snapshot_id());
                        cur_live_deletes = reload_live_deletes;
                        reloaded_table = Some(reload);
                        continue;
                    }
                    // Retries exhausted and a concurrent delete is still
                    // landing on this batch's own files: never commit
                    // output known to predate it (that would be the exact
                    // resurrection this check exists to prevent). Fail the
                    // batch instead; the caller reports `partial` if any
                    // earlier batch already committed.
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "rewrite_data_files_distributed: a concurrent position delete keeps \
                         landing on this batch's own input files after {attempt} re-dispatch \
                         attempt(s); aborting this batch's commit rather than risk committing \
                         stale (pre-delete) worker output"
                    )));
                }

                // No new covering delete: still adopt this reload as the
                // commit base and delete set, keeping `check_file_existence`
                // and `covered_position_deletes` maximally current.
                cur_plan_snapshot_id = reload
                    .metadata_ref()
                    .current_snapshot()
                    .map(|s| s.snapshot_id());
                cur_live_deletes = reload_live_deletes;
                reloaded_table = Some(reload);
            }

            let current_table: &IcebergTable = reloaded_table.as_ref().unwrap_or(table);
            let result = Self::commit_aggregated_batch(
                catalog,
                current_table,
                ident,
                &batch_old_files,
                aggregated,
                seq_at_start,
                cur_plan_snapshot_id,
                &cur_live_deletes,
                snapshot_properties.clone(),
                table_cache,
                live_expected_before,
                metrics,
            )
            .await;

            match result {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // Secondary net: a conflict this function's own
                    // pre-commit check did not preempt (e.g. an unrelated
                    // concurrent writer, or the vendored `Transaction::
                    // commit`'s own internal retry budget exhausted by
                    // sustained contention). Reload and re-dispatch, same
                    // as above, rather than recommitting stale output.
                    if retry_on_conflict
                        && is_retryable_commit_error(&e)
                        && attempt < MAX_COMMIT_ATTEMPTS
                    {
                        let backoff = commit_backoff(attempt);
                        warn!(
                            table = %ident,
                            attempt,
                            backoff_ms = backoff.as_millis() as u64,
                            "rewrite_data_files_distributed: retryable commit conflict on a \
                             partial_progress batch commit; reloading the table and \
                             re-dispatching this batch against the current snapshot before \
                             retrying the commit"
                        );
                        tokio::time::sleep(backoff).await;
                        let reloaded = load_table(catalog, ident).await?;
                        cur_metadata_location = reloaded
                            .metadata_location_result()
                            .map_err(|e| {
                                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                                    "rewrite_data_files_distributed: reloaded table has no \
                                     metadata location: {e}"
                                ))
                            })?
                            .to_string();
                        cur_snapshot_id = reloaded
                            .metadata_ref()
                            .current_snapshot()
                            .map(|s| s.snapshot_id())
                            .unwrap_or(0);
                        cur_plan_snapshot_id = reloaded
                            .metadata_ref()
                            .current_snapshot()
                            .map(|s| s.snapshot_id());
                        cur_live_deletes = collect_live_delete_files(&reloaded).await?;
                        reloaded_table = Some(reloaded);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Commit one already-aggregated batch (worker outcomes decoded, row
    /// invariant already checked by the caller) against `table`. Split out
    /// of [`Self::commit_one_batch`] so its commit-conflict retry loop can
    /// re-invoke JUST this step -- rebuilding the `Transaction` against a
    /// freshly reloaded `table` -- without re-dispatching to the worker
    /// fleet or re-running the row-invariant check.
    #[allow(clippy::too_many_arguments)]
    async fn commit_aggregated_batch(
        catalog: &Arc<dyn Catalog>,
        table: &IcebergTable,
        ident: &TableIdent,
        batch_old_files: &[DataFile],
        aggregated: sqe_compaction::dispatch::AggregatedRewrite,
        seq_at_start: i64,
        plan_snapshot_id: Option<i64>,
        live_deletes: &[DataFile],
        snapshot_properties: Option<std::collections::HashMap<String, String>>,
        table_cache: Option<&TableMetadataCache>,
        live_expected_before: i64,
        metrics: Option<&sqe_metrics::MetricsRegistry>,
    ) -> sqe_core::Result<BatchCommitResult> {
        let output_count = aggregated.new_files.len() as i64;
        let output_bytes: i64 = aggregated
            .new_files
            .iter()
            .map(|f| f.file_size_in_bytes() as i64)
            .sum();

        // Position delete files fully covered by THIS batch's removed data
        // files (never another batch's -- see the disjointness argument on
        // `commit_eligible_groups`).
        let removed_data_paths: std::collections::HashSet<String> = batch_old_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();
        let covered_deletes = covered_position_deletes(&removed_data_paths, live_deletes);
        let removed_delete_count = covered_deletes.len() as i64;

        info!(
            table = %ident,
            input_count = batch_old_files.len(),
            output_count,
            added_rows = aggregated.added_rows,
            removed_rows = aggregated.removed_rows,
            removed_delete_count,
            "rewrite_data_files_distributed: committing RewriteFilesAction"
        );

        // Commit via RewriteFilesAction: the EXACT same sequence
        // `rewrite_data_files_once` uses (seq pin, check_file_existence,
        // enable_delete_filter_manager, snapshot stamp, covered position
        // deletes dropped in the same atomic swap). Commit authority never
        // leaves the coordinator: workers only produced files in object
        // storage, this is the one and only state change to the table.
        let tx = Transaction::new(table);
        let files_to_remove: Vec<DataFile> = batch_old_files
            .iter()
            .cloned()
            .chain(covered_deletes)
            .collect();
        let mut action = tx
            .rewrite_files()
            .set_enable_delete_filter_manager(true)
            .set_check_file_existence(true)
            .set_new_data_file_sequence_number(seq_at_start)
            .set_validate_from_snapshot_id(plan_snapshot_id)
            .add_data_files(aggregated.new_files)
            .delete_files(files_to_remove);
        if let Some(props) = snapshot_properties {
            action.set_snapshot_properties(props);
        }
        let tx_applied = action
            .apply(tx)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("rewrite_files apply failed: {e}")))?;

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "rewrite_data_files_distributed"))?;
        let new_snapshot_id = committed.metadata().current_snapshot_id();

        // Same as the local path: carry the previous snapshot's Puffin NDV
        // statistics onto the compacted snapshot (architecture review 512).
        crate::puffin_carry::carry_forward_statistics(
            catalog.as_ref(),
            ident,
            &committed,
            crate::puffin_carry::CarryOp::Rewrite,
            None,
            metrics,
        )
        .await;

        let cache_key = format!("{}.{}", ident.namespace(), ident.name());
        if let Some(tc) = table_cache {
            tc.invalidate(&cache_key).await;
        }

        let rows_removed = (aggregated.removed_rows - aggregated.added_rows) as i64;
        let live_expected_after =
            live_expected_before - batch_old_files.len() as i64 + output_count;

        // Same post-commit sanity check as the pre-Task-3 single-commit
        // path, now run once per batch: reload and confirm the live file
        // count matches expectation. Purely observability (a mismatch only
        // warns), but worth keeping identical here since it is the one
        // place that would catch a catalog-state-propagation bug specific
        // to the distributed commit path.
        match catalog.load_table(ident).await {
            Ok(reloaded) => match collect_live_data_files(&reloaded).await {
                Ok(live_files) => {
                    let live_after = live_files.len() as i64;
                    info!(
                        table = %ident,
                        live_after,
                        expected_after = live_expected_after,
                        "rewrite_data_files_distributed: post-commit verification"
                    );
                    if live_after != live_expected_after {
                        warn!(
                            table = %ident,
                            live_after,
                            expected_after = live_expected_after,
                            "rewrite_data_files_distributed: live file count after commit \
                             does not match expectation"
                        );
                    }
                }
                Err(e) => warn!(
                    table = %ident,
                    error = %e,
                    "rewrite_data_files_distributed: post-commit verification: failed to \
                     collect live data files"
                ),
            },
            Err(e) => warn!(
                table = %ident,
                error = %e,
                "rewrite_data_files_distributed: post-commit verification: reload failed"
            ),
        }

        Ok(BatchCommitResult {
            output_count,
            output_bytes,
            rows_removed,
            old_file_count: batch_old_files.len() as i64,
            new_snapshot_id,
            committed_table: committed,
            live_expected_after,
        })
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
        // Branches a crashed coordinator left behind go first, so their
        // snapshots expire in this same call. Best-effort: a lost commit
        // race leaves them for the next call, not this expiry undone.
        let txn_branches = sqe_catalog::txn::drop_stale_branches(
            catalog.as_ref(),
            &ident,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap_or_else(|e| {
            warn!(
                table = %ident,
                error = %e,
                "expire_snapshots: stale transaction branches not dropped"
            );
            Vec::new()
        });
        let table = load_table(&catalog, &ident).await?;

        let before_count = table.metadata().snapshots().count() as i64;

        let before_metadata = table.metadata_ref();
        let tx = Transaction::new(&table);
        let mut action = tx.expire_snapshots();
        if let Some(ts) = older_than_ms {
            action = action.expire_older_than_ms(ts);
        }
        if let Some(keep) = retain_last {
            action = action.retain_last(keep);
        }

        let tx_applied = action
            .apply(tx)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("expire_snapshots apply failed: {e}")))?;

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "expire_snapshots"))?;

        // The old fork's `clear_expire_files(true)` deleted the now-unreachable
        // manifests / data files as part of the action; apache does it as a
        // separate post-commit step. A cleanup failure does not undo the
        // committed expiry, so it is surfaced as an error for the operator.
        committed
            .cleanup_expired_files(&before_metadata)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("expire_snapshots file cleanup failed after commit: {e}")))?;

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
            format!(
                "snapshots_removed={removed} txn_branches_dropped={}",
                txn_branches.len()
            ),
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
            chrono::Utc::now().timestamp_millis() - DEFAULT_OLDER_THAN_DAYS * 24 * 60 * 60 * 1000
        });

        let action =
            iceberg::actions::RemoveOrphanFilesAction::new(table).older_than_ms(threshold_ms);

        let orphans = action
            .execute()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("remove_orphan_files execute failed: {e}")))?;

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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("rewrite_manifests apply failed: {e}")))?;

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "rewrite_manifests"))?;

        // Manifest rewrite touches no data: the previous snapshot's Puffin
        // NDV statistics remain exact for the new one (architecture review 512).
        crate::puffin_carry::carry_forward_statistics(
            catalog.as_ref(),
            &ident,
            &committed,
            crate::puffin_carry::CarryOp::Rewrite,
            None,
            self.metrics.as_deref(),
        )
        .await;

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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "purge_orphan_locations: refusing to run; could not load \
                         registered table '{ident}': {e}. Fix the catalog before \
                         retrying so live tables are not deleted."
                    )));
                }
            }
        }

        let probe = probe_table.ok_or_else(|| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to list prefix '{ns_base}/': {e}")))?;
        let entries: Vec<_> = listing.try_collect().await.map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to collect listing for '{ns_base}/': {e}"))
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build result batch: {e}")))?;

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

/// Where a rewrite job actually runs, once `distribution.mode` has been
/// resolved against the live worker fleet (Phase 4c Task 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionPlan {
    /// Run coordinator-local, via [`MaintenanceHandler::rewrite_data_files`].
    Local,
    /// Fan out to the worker fleet, via
    /// [`MaintenanceHandler::rewrite_data_files_distributed_with_defaults`].
    Distributed,
    /// `distribution.mode = "require"` but the fleet is below `min_workers`
    /// healthy: the job must NOT silently fall back to `Local`. The caller
    /// decides how loud that is -- the scheduler's active path records a
    /// `skipped` job (log row + audit + metric) and moves on; the manual
    /// `CALL system.rewrite_data_files` path returns an `Err` to the caller.
    SkipInsufficientWorkers,
}

/// Pure decision function behind `distribution.mode` routing. Takes no
/// handler/registry/config type so it is trivially unit-testable (see the
/// `resolve_execution_*` tests below) and so both call sites -- the manual
/// `CALL` arm in [`MaintenanceHandler::handle`] and the scheduler's
/// `active_one_table` -- resolve the exact same way from the exact same
/// three inputs, with no risk of the two switch statements drifting apart.
///
/// - `Local`: always coordinator-local, regardless of fleet size. This is
///   what makes an operator's explicit `mode = "local"` an unconditional
///   override, and it is also why a deployment that never attaches a worker
///   registry (or attaches one with zero healthy workers) never even
///   evaluates the other two arms differently -- see `Auto`/`Require` below.
/// - `Auto` (the default): `Distributed` once
///   `healthy_workers >= min_workers.max(1)`, otherwise `Local`. The
///   `.max(1)` floor means distributed execution always needs at least one
///   healthy worker, even when an operator sets `min_workers = 0`. A
///   single-node/no-fleet deployment always has `healthy_workers == 0`,
///   which is never `>= 1`, so `Auto` degrades to `Local` there -- the
///   byte-identical-to-pre-Task-5 guarantee holds regardless of how
///   `min_workers` is configured.
/// - `Require`: `Distributed` once the floor is met, otherwise
///   `SkipInsufficientWorkers` -- never `Local`. This is the whole point of
///   `require`: an operator who set it wants a hard signal (a loud skip or
///   an error) instead of a rewrite quietly running on the coordinator when
///   the fleet it was sized for isn't there. The same `.max(1)` floor
///   applies here too, so `min_workers = 0` with zero healthy workers still
///   skips rather than "distributing" to nothing.
pub(crate) fn resolve_execution(
    mode: sqe_core::config::DistributionMode,
    healthy_workers: usize,
    min_workers: usize,
) -> ExecutionPlan {
    use sqe_core::config::DistributionMode;

    // Distributed execution always requires at least one healthy worker,
    // regardless of how low an operator sets `min_workers` (including 0).
    let floor = min_workers.max(1);

    match mode {
        DistributionMode::Local => ExecutionPlan::Local,
        DistributionMode::Auto => {
            if healthy_workers >= floor {
                ExecutionPlan::Distributed
            } else {
                ExecutionPlan::Local
            }
        }
        DistributionMode::Require => {
            if healthy_workers >= floor {
                ExecutionPlan::Distributed
            } else {
                ExecutionPlan::SkipInsufficientWorkers
            }
        }
    }
}

/// Parse the manual `CALL system.rewrite_data_files(..., distributed => '...')`
/// per-call override into a [`sqe_core::config::DistributionMode`].
/// Case-insensitive, matching the TOML config's `#[serde(rename_all =
/// "lowercase")]` on the same enum. An unrecognized value is a loud parse
/// error rather than a silent fallback to the configured default -- a typo
/// here must not quietly change which fleet-vs-local decision gets made.
fn parse_distribution_mode_override(
    raw: &str,
) -> sqe_core::Result<sqe_core::config::DistributionMode> {
    use sqe_core::config::DistributionMode;
    match raw.to_ascii_lowercase().as_str() {
        "auto" => Ok(DistributionMode::Auto),
        "local" => Ok(DistributionMode::Local),
        "require" => Ok(DistributionMode::Require),
        other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "CALL system.rewrite_data_files: invalid 'distributed' value '{other}'; \
             expected 'auto', 'local', or 'require'"
        ))),
    }
}

fn call_name_rewrite() -> &'static str {
    "rewrite_data_files"
}

/// One batch's committed result, as returned by
/// [`MaintenanceHandler::commit_one_batch`] to
/// [`MaintenanceHandler::commit_eligible_groups`]. `committed_table` is the
/// post-commit `Table` `Transaction::commit` returned, reused as the base
/// for the NEXT batch's `Transaction::new` so it reflects this batch's
/// snapshot without an extra catalog round trip (the separate
/// `catalog.load_table` reload in `commit_one_batch` is a best-effort
/// verification step only, not the source of truth for chaining batches).
struct BatchCommitResult {
    output_count: i64,
    output_bytes: i64,
    rows_removed: i64,
    old_file_count: i64,
    new_snapshot_id: Option<i64>,
    committed_table: IcebergTable,
    /// Running "expected live file count" after this batch, threaded into
    /// the next batch's post-commit sanity check the same way the
    /// pre-Task-3 single-commit path computed it in one shot.
    live_expected_after: i64,
}

/// Structured result of one `rewrite_data_files`/`rewrite_data_files_once`
/// run. Both callers of that method need this:
///
/// - `handle()`'s `RewriteDataFiles` arm renders it into the `CALL`
///   surface's generic `summary_batch` `RecordBatch` (see
///   [`rewrite_outcome_batch`]).
/// - The Phase 4b active-mode scheduler needs the individual counts
///   directly to build a `maintenance_log` job row
///   (`maintenance_log::success_row`), which a formatted summary string
///   cannot losslessly round-trip (no `rows_removed` / `snapshot_id`
///   columns in `summary_batch`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RewriteOutcome {
    /// Live data files in the table before this run.
    pub files_in: i64,
    /// New data files this run wrote. `0` when `skipped_reason` is set.
    pub files_out: i64,
    /// Total bytes of `files_in`.
    pub bytes_in: i64,
    /// Total bytes of `files_out`. `0` when `skipped_reason` is set.
    pub bytes_out: i64,
    /// Rows eliminated by delete application during this rewrite (rows in
    /// the replaced old files minus rows the writer re-emitted for the new
    /// files). `0` when `skipped_reason` is set.
    pub rows_removed: i64,
    /// The snapshot id this run committed, if it committed one. `None` when
    /// `skipped_reason` is set.
    pub snapshot_id: Option<i64>,
    /// Number of OLD input files this run actually replaced (a subset of
    /// `files_in` restricted to eligible groups). `0` when `skipped_reason`
    /// is set.
    pub files_rewritten: i64,
    /// Set (to a short human-readable reason) when the call committed
    /// nothing: below `min_input_files`, or no eligible groups after
    /// packing.
    pub skipped_reason: Option<String>,
    /// `true` when this outcome reflects a `partial_progress` (Phase 4d
    /// Task 3) batched commit that stopped early after a terminal group
    /// failure, having already committed one or more earlier batches.
    /// Always `false` on the coordinator-local path and on the distributed
    /// path when `partial_progress` is off (default): those either commit
    /// everything in one atomic `RewriteFilesAction` or nothing at all, so
    /// there is never a partial outcome to report. `files_in`/`files_out`/
    /// `bytes_in`/`bytes_out`/`rows_removed`/`files_rewritten`/
    /// `snapshot_id` reflect only the batches that actually committed.
    pub partial: bool,
    /// The terminal group failure's error message when `partial` is
    /// `true`. `None` otherwise (including on every non-partial outcome).
    pub partial_error: Option<String>,
}

/// Render a [`RewriteOutcome`] into the generic `summary_batch` shape the
/// `CALL system.rewrite_data_files` surface has always returned. Preserves
/// the pre-Task-3 `status` text exactly for every non-partial outcome:
/// `"skipped: {reason}"` or `"committed rewritten={files_rewritten}"`. A
/// partial outcome (only reachable when `distribution.partial_progress` is
/// opted in) gets its own `"partial: ..."` text so an operator reading the
/// `CALL` surface's summary can tell "fully committed" from "some batches
/// committed, then a group failed" at a glance.
fn rewrite_outcome_batch(
    ident: &TableIdent,
    outcome: &RewriteOutcome,
) -> sqe_core::Result<RecordBatch> {
    let status = match &outcome.skipped_reason {
        Some(reason) => format!("skipped: {reason}"),
        None if outcome.partial => format!(
            "partial: rewritten={} ({})",
            outcome.files_rewritten,
            outcome
                .partial_error
                .as_deref()
                .unwrap_or("terminal group failure")
        ),
        None => format!("committed rewritten={}", outcome.files_rewritten),
    };
    summary_batch(
        call_name_rewrite(),
        ident,
        outcome.files_in,
        outcome.files_out,
        outcome.bytes_in,
        outcome.bytes_out,
        status,
    )
}

/// A caller-supplied location as it may appear in INFO logs (#837): scheme
/// and bucket, then a short hash of the rest. The path can carry a bucket
/// layout the operator keeps out of shared logs, and a query string (a
/// presigned URL) can carry credentials, so neither is logged.
fn location_for_log(location: &str) -> String {
    use sha2::{Digest, Sha256};

    let without_query = location.split(['?', '#']).next().unwrap_or("");
    let hash = hex::encode(&Sha256::digest(without_query.as_bytes())[..6]);
    match without_query.split_once("://") {
        Some((scheme, rest)) => {
            let bucket = rest.split('/').next().unwrap_or("");
            format!("{scheme}://{bucket}/<path sha256:{hash}>")
        }
        None => format!("<path sha256:{hash}>"),
    }
}

/// The prefix `drop_table(purge => true)` deletes: the table location with a
/// trailing `/`, so a sibling such as `t_2` next to `t` is never matched.
/// Refuses, before the catalog drop, when deleting that prefix could take
/// more than this table's files or leave some of them behind:
///
/// - `gc.enabled = false`, the Iceberg marker for a table whose files may be
///   shared (Spark refuses the same purge).
/// - A `write.*.path` property pointing outside the location: files written
///   there would survive, and the path may belong to something else.
/// - A location with no path below the bucket, or a current metadata file
///   that is not under the location, so the location is not this table's
///   own directory.
fn purge_prefix(table: &IcebergTable) -> sqe_core::Result<String> {
    use iceberg::spec::TableProperties;

    let ident = table.identifier();
    let metadata = table.metadata();
    let refuse = |why: String| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("drop_table(purge => true) refused for '{ident}': {why}. Nothing was dropped."),
        )
    };

    let gc_enabled = metadata
        .table_properties()
        .gc_enabled()
        .map_err(|e| refuse(format!("cannot read gc.enabled: {e}")))?;
    if !gc_enabled {
        return Err(refuse("gc.enabled is false on this table".into()));
    }

    let location = strip_trailing_slash(metadata.location());
    let has_table_dir = location.split_once("://").is_some_and(|(_, rest)| {
        rest.split_once('/')
            .is_some_and(|(_, path)| !path.is_empty())
    });
    if !has_table_dir {
        return Err(refuse(format!(
            "location {} is a bucket root",
            location_for_log(&location)
        )));
    }

    for key in [
        TableProperties::PROPERTY_WRITE_DATA_LOCATION,
        TableProperties::PROPERTY_WRITE_METADATA_PATH,
        TableProperties::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION,
        TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_LOCATION,
    ] {
        if let Some(path) = metadata.properties().get(key) {
            if canonicalize_uri(path) != canonicalize_uri(&location)
                && !is_strictly_under(path, &location)
            {
                return Err(refuse(format!(
                    "{key} points outside the table location; delete those files by hand"
                )));
            }
        }
    }

    if let Some(metadata_location) = table.metadata_location() {
        if !is_strictly_under(metadata_location, &location) {
            return Err(refuse(
                "the current metadata file is not under the table location".into(),
            ));
        }
    }

    Ok(format!("{location}/"))
}

/// Refuse a purge when another table in the catalog shares the prefix.
/// `register_table` can point a second identifier at an existing table's
/// files (data-free moves, disaster recovery), so two entries may share one
/// location; deleting it for one would destroy the other's data. Walks every
/// namespace the caller can list and compares locations in both directions
/// (equal, inside, or containing). A table that cannot be loaded refuses the
/// purge: its location is unknown, so it might be the one sharing.
async fn refuse_if_location_shared(
    catalog: &Arc<dyn Catalog>,
    target: &TableIdent,
    prefix: &str,
) -> sqe_core::Result<()> {
    let refuse = |why: String| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "drop_table(purge => true) refused for '{target}': {why}. Nothing was dropped."
            ),
        )
    };

    let mut pending: Vec<NamespaceIdent> = catalog.list_namespaces(None).await.map_err(|e| {
        refuse(format!(
            "cannot list namespaces to check for shared locations: {e}"
        ))
    })?;
    while let Some(ns) = pending.pop() {
        let children = catalog.list_namespaces(Some(&ns)).await.map_err(|e| {
            refuse(format!(
                "cannot list namespaces under '{}': {e}",
                ns.to_url_string()
            ))
        })?;
        pending.extend(children);
        let tables = catalog.list_tables(&ns).await.map_err(|e| {
            refuse(format!(
                "cannot list tables in '{}': {e}",
                ns.to_url_string()
            ))
        })?;
        for ident in tables.iter().filter(|t| *t != target) {
            let other = catalog.load_table(ident).await.map_err(|e| {
                refuse(format!(
                    "cannot load '{ident}' to check whether it shares the location: {e}"
                ))
            })?;
            if locations_overlap(other.metadata().location(), prefix) {
                return Err(refuse(format!(
                    "table '{ident}' uses the same location; drop it first or drop without purge"
                )));
            }
        }
    }
    Ok(())
}

/// True when one location is the other or lies inside it.
fn locations_overlap(a: &str, b: &str) -> bool {
    canonicalize_uri(a) == canonicalize_uri(b) || is_strictly_under(a, b) || is_strictly_under(b, a)
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
        format!(
            "{}://{}",
            scheme.to_ascii_lowercase(),
            authority.to_ascii_lowercase()
        )
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

/// `classify_commit_error` tags retryable conflicts in the message
/// ("retryable" / "conflict"). Anything else is a permanent failure.
fn is_retryable_commit_error(e: &impl std::fmt::Display) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("retryable") || msg.contains("conflict")
}

/// Exponential backoff before retrying commit `attempt` (1-based).
fn commit_backoff(attempt: usize) -> std::time::Duration {
    std::time::Duration::from_millis(50 * (1u64 << (attempt - 1)))
}

/// Translate an iceberg commit error into an SQE error, preserving retryable
/// conflict signalling so callers can distinguish transient from permanent
/// failures.
fn classify_commit_error(err: iceberg::Error, proc: &str) -> SqeError {
    let msg = err.to_string();
    if msg.to_lowercase().contains("conflict") || msg.to_lowercase().contains("retry") {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "{proc}: concurrent writer conflict (retryable): {msg}"
        ))
    } else {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("{proc}: commit failed: {msg}"))
    }
}

fn to_table_ident(table_ref: &TableRef) -> TableIdent {
    let ns = NamespaceIdent::new(table_ref.namespace.clone());
    TableIdent::new(ns, table_ref.name.clone())
}

pub(crate) async fn load_table(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> sqe_core::Result<IcebergTable> {
    catalog
        .load_table(ident)
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to load table '{ident}': {e}")))
}

/// Collect the three inputs [`crate::table_health::analyze_table_health`]
/// needs for one table, in the exact sequence `table_health()` (the `CALL
/// system.table_health` handler, above) uses: live data files, live delete
/// files, and the delete-aware read plan's per-path task map. Consolidated
/// into one seam so the advisory scheduler (`maintenance_scheduler.rs`)
/// promotes a single `pub(crate)` function instead of reaching into three
/// private collectors plus the private `DeleteAwareReadPlan` struct.
pub(crate) async fn collect_health_inputs(
    table: &IcebergTable,
) -> sqe_core::Result<(
    Vec<DataFile>,
    Vec<DataFile>,
    std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>>,
)> {
    let data_files = collect_live_data_files(table).await?;
    let delete_files = collect_live_delete_files(table).await?;
    let read_plan = plan_delete_aware_read(table).await?;
    Ok((data_files, delete_files, read_plan.tasks_by_path))
}

/// Table property prefix a compaction job stamps onto the snapshot it
/// commits (`sqe.maintenance.job-id`, `sqe.maintenance.principal`,
/// `sqe.maintenance.trigger`; see `active_one_table`'s `snapshot_properties`
/// in `maintenance_scheduler.rs`). Shared here so
/// [`last_compaction_snapshot_ms`] and the stamping call site can never
/// disagree about which prefix marks a "this snapshot was a compaction job"
/// snapshot.
pub(crate) const MAINTENANCE_SNAPSHOT_PROPERTY_PREFIX: &str = "sqe.maintenance.";

/// Timestamp (epoch ms) of the most recent snapshot in `table`'s snapshot
/// log whose summary carries any `sqe.maintenance.*` property, or `None` if
/// no snapshot ever did. Feeds
/// [`crate::table_health::TableHealth::last_compaction_snapshot_ms`] (via
/// its `analyze_table_health` parameter): `analyze_table_health` itself
/// stays pure and never touches a live `IcebergTable`, so this is the one
/// place that walks `table.metadata().snapshots()` to answer "when did a
/// compaction job last touch this table."
///
/// Synchronous and I/O-free: `TableMetadata::snapshots()` only reads the
/// already-loaded metadata, not the object store, so callers that already
/// have a loaded `IcebergTable` (the `table_health` handler and the
/// scheduler) can call this directly without an extra catalog round trip.
pub(crate) fn last_compaction_snapshot_ms(table: &IcebergTable) -> Option<i64> {
    table
        .metadata()
        .snapshots()
        .filter(|s| {
            s.summary()
                .additional_properties
                .keys()
                .any(|k| k.starts_with(MAINTENANCE_SNAPSHOT_PROPERTY_PREFIX))
        })
        .map(|s| s.timestamp_ms())
        .max()
}

/// Resolve the `strategy` / `sort_order` procedure args into a `SortSpec`,
/// validated against the table schema. Returns `None` for the default bin-pack
/// path (no sort). Validation lives here (not the parser) because it needs the
/// schema. Mirrors Spark's `rewrite_data_files(strategy, sort_order)`.
///
/// `SortSpec` itself moved to `sqe-compaction` (Phase 4c Task 1) along with
/// the sort-compaction path that consumes it; this parser stays here because
/// it is procedure-argument parsing, not a compaction primitive.
fn parse_sort_spec(
    strategy: Option<&str>,
    sort_order: Option<&str>,
    schema: &arrow_schema::Schema,
) -> sqe_core::Result<Option<SortSpec>> {
    let strat = strategy.map(|s| s.trim().to_ascii_lowercase());
    match strat.as_deref() {
        Some(s) if s != "sort" && s != "binpack" => {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "rewrite_data_files: unknown strategy '{s}'; expected 'binpack' or 'sort'"
            )));
        }
        _ => {}
    }
    // A sort is requested when strategy is 'sort', or when only sort_order is
    // given (strategy omitted). 'binpack' with a sort_order ignores the order.
    let sort_requested =
        matches!(strat.as_deref(), Some("sort")) || (strat.is_none() && sort_order.is_some());
    if !sort_requested {
        return Ok(None);
    }

    let order = sort_order
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            SqeError::Execution("rewrite_data_files: strategy => 'sort' requires a non-empty sort_order".to_string())
        })?;

    let lower = order.to_ascii_lowercase();
    let spec = if lower.starts_with("zorder(") && order.ends_with(')') {
        let inner = &order[order.find('(').unwrap() + 1..order.len() - 1];
        let cols: Vec<String> = inner
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        if cols.is_empty() {
            return Err(SqeError::Execution("rewrite_data_files: zorder(...) needs at least one column".to_string()));
        }
        SortSpec::ZOrder(cols)
    } else {
        let mut cols: Vec<(String, bool)> = Vec::new();
        for part in order.split(',') {
            let toks: Vec<&str> = part.split_whitespace().collect();
            if toks.is_empty() {
                continue;
            }
            let asc = match toks.get(1).map(|s| s.to_ascii_uppercase()).as_deref() {
                None | Some("ASC") => true,
                Some("DESC") => false,
                Some(other) => {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "rewrite_data_files: invalid sort direction '{other}'; expected ASC or DESC"
                    )));
                }
            };
            cols.push((toks[0].to_string(), asc));
        }
        if cols.is_empty() {
            return Err(SqeError::Execution("rewrite_data_files: sort_order is empty".to_string()));
        }
        SortSpec::Columns(cols)
    };

    for name in spec.columns() {
        if schema.field_with_name(name).is_err() {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "rewrite_data_files: sort_order column '{name}' not found in table schema"
            )));
        }
    }
    Ok(Some(spec))
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
    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build summary batch: {e}")))?;
    Ok(batch)
}

/// Blast-radius tier of a maintenance procedure; decides which gate
/// [`MaintenanceHandler::authorize_or_deny`] applies (architecture review 510).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcedureTier {
    /// Reads metadata or history only; no gate beyond table access.
    ReadOnly,
    /// Writes new files and commits a snapshot through the caller's token, so
    /// the catalog is the backstop. Denied only to explicitly read-only roles.
    TableWrite,
    /// Deletes files or history without a refusable commit, moves the main
    /// branch, or registers/drops tables. Requires a configured admin role.
    Admin,
}

/// Classify a procedure call into its authorization tier.
pub(crate) fn procedure_tier(call: &ProcedureCall) -> ProcedureTier {
    match call {
        ProcedureCall::SuggestBloomFilterColumns { .. }
        | ProcedureCall::TableHealth { .. }
        | ProcedureCall::RefreshCatalogCache { .. } => ProcedureTier::ReadOnly,
        ProcedureCall::PurgeOrphanLocations { dry_run: true, .. } => ProcedureTier::ReadOnly,
        ProcedureCall::RewriteDataFiles { .. } | ProcedureCall::RewriteManifests { .. } => {
            ProcedureTier::TableWrite
        }
        ProcedureCall::ExpireSnapshots { .. }
        | ProcedureCall::RemoveOrphanFiles { .. }
        | ProcedureCall::PurgeOrphanLocations { .. }
        | ProcedureCall::DropTable { .. }
        | ProcedureCall::RegisterTable { .. }
        | ProcedureCall::SetCurrentSnapshot { .. }
        | ProcedureCall::RollbackToSnapshot { .. }
        | ProcedureCall::ReprojectColumnTags { .. } => ProcedureTier::Admin,
    }
}

/// A session whose roles say "read-only" and nothing says otherwise. This is
/// the only remaining use of role-name matching: it can refuse a table-write
/// procedure early, it can never grant anything. Sessions with no roles are
/// not read-only here (the catalog decides their commit); they are still
/// denied every admin-tier procedure by `authorize_or_deny`.
pub(crate) fn session_is_explicitly_read_only(session: &Session) -> bool {
    let roles = &session.user.roles;
    let has_write_like = roles.iter().any(|r| {
        let lower = r.to_ascii_lowercase();
        lower.contains("write") || lower.contains("admin") || lower.contains("owner")
    });
    let has_read_only = roles.iter().any(|r| {
        let lower = r.to_ascii_lowercase();
        lower.starts_with("read") || lower.starts_with("select") || lower.contains("readonly")
    });
    has_read_only && !has_write_like
}

/// TEST-ONLY (`#[cfg(all(test, feature = "test-sqlite"))]`) two-way handshake used by
/// `rewrite_data_files_once` (see the `race_seam` field on
/// [`MaintenanceHandler`]) to pause the first attempt right before its final
/// commit and let a test land a concurrent write deterministically inside
/// the conflict window, instead of racing wall-clock timing:
///
/// 1. The rewrite reaches the pause point (after loading the table, capturing
///    the plan baseline, and computing its compacted output -- everything
///    that makes this attempt's view of the table stale) and calls
///    `ready.notify_one()`, then blocks on `go.notified()`.
/// 2. The test, having awaited `ready.notified()`, is guaranteed the rewrite
///    is now paused with its baseline already captured. It commits the
///    concurrent write.
/// 3. The test calls `go.notify_one()`, releasing the rewrite to attempt its
///    commit against the now-advanced table.
///
/// `fired` makes step 1 one-shot: a retried attempt (a fresh call to
/// `rewrite_data_files_once` after this one was rejected) runs straight
/// through without pausing again, matching real retry behavior (only the
/// first attempt races the concurrent writer; the retry re-reads and re-plans
/// against the table the concurrent write already landed on).
#[cfg(all(test, feature = "test-sqlite"))]
struct RewriteRaceSeam {
    ready: tokio::sync::Notify,
    go: tokio::sync::Notify,
    fired: std::sync::atomic::AtomicBool,
}

#[cfg(all(test, feature = "test-sqlite"))]
impl RewriteRaceSeam {
    fn new() -> Self {
        Self {
            ready: tokio::sync::Notify::new(),
            go: tokio::sync::Notify::new(),
            fired: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
mod location_log_tests {
    use super::location_for_log;

    #[test]
    fn keeps_scheme_and_bucket_and_hides_the_path() {
        let logged = location_for_log("s3://lake/wh/sales/orders/metadata/00001-abc.metadata.json");
        assert!(logged.starts_with("s3://lake/<path sha256:"), "{logged}");
        assert!(!logged.contains("sales"), "{logged}");
        assert!(!logged.contains("metadata.json"), "{logged}");
    }

    #[test]
    fn drops_a_presigned_query_string() {
        let logged = location_for_log(
            "https://lake.s3.amazonaws.com/wh/t/m.json?X-Amz-Credential=AKIA123&X-Amz-Signature=deadbeef",
        );
        assert!(
            logged.starts_with("https://lake.s3.amazonaws.com/<path sha256:"),
            "{logged}"
        );
        assert!(!logged.contains("X-Amz"), "{logged}");
        assert!(!logged.contains("AKIA123"), "{logged}");
    }

    #[test]
    fn different_paths_hash_differently_and_the_same_path_stably() {
        let a = location_for_log("s3://lake/wh/a/m.json");
        let b = location_for_log("s3://lake/wh/b/m.json");
        assert_ne!(a, b);
        assert_eq!(a, location_for_log("s3://lake/wh/a/m.json"));
    }

    #[test]
    fn a_location_without_a_scheme_is_fully_hashed() {
        let logged = location_for_log("/local/wh/t/metadata/v1.metadata.json");
        assert!(logged.starts_with("<path sha256:"), "{logged}");
        assert!(!logged.contains("local"), "{logged}");
    }
}

#[cfg(test)]
mod purge_prefix_tests {
    use super::{locations_overlap, purge_prefix};
    use std::sync::Arc;

    #[test]
    fn overlap_is_same_inside_or_containing_but_not_a_sibling() {
        let prefix = "s3://lake/wh/ns/t/";
        assert!(locations_overlap("s3://lake/wh/ns/t", prefix));
        assert!(locations_overlap("S3://LAKE/wh/ns/t//", prefix));
        assert!(locations_overlap("s3://lake/wh/ns/t/nested", prefix));
        assert!(locations_overlap("s3://lake/wh/ns", prefix));
        assert!(!locations_overlap("s3://lake/wh/ns/t_2", prefix));
        assert!(!locations_overlap("s3://lake/wh/other/t", prefix));
    }

    fn table(
        location: &str,
        metadata_location: Option<&str>,
        props: &[(&str, &str)],
    ) -> iceberg::table::Table {
        let properties: serde_json::Map<String, serde_json::Value> = props
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::from(*v)))
            .collect();
        let metadata: iceberg::spec::TableMetadata = serde_json::from_value(serde_json::json!({
            "format-version": 1,
            "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
            "location": location,
            "last-updated-ms": 1602638573874_i64,
            "last-column-id": 1,
            "schema": {"type": "struct", "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}]},
            "partition-spec": [],
            "properties": properties,
            "current-snapshot-id": -1,
            "snapshots": []
        }))
        .unwrap();
        let file_io =
            iceberg::io::FileIOBuilder::new(Arc::new(iceberg::io::LocalFsStorageFactory)).build();
        let mut builder = iceberg::table::Table::builder()
            .runtime(iceberg::Runtime::current())
            .metadata(metadata)
            .identifier(iceberg::TableIdent::from_strs(["ns", "t"]).unwrap())
            .file_io(file_io)
            .disable_cache();
        if let Some(m) = metadata_location {
            builder = builder.metadata_location(m);
        }
        builder.build().unwrap()
    }

    const META: Option<&str> = Some("s3://lake/wh/ns/t/metadata/00001-a.metadata.json");

    /// Polaris refuses overlapping table locations by default, so the
    /// integration suite cannot build a shared location; the in-memory
    /// catalog allows it, like Glue, HMS and the SQL catalogs do.
    #[tokio::test]
    async fn a_table_sharing_the_location_refuses_the_purge() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

        let catalog: Arc<dyn Catalog> = Arc::new(
            iceberg::memory::MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    [("warehouse".to_string(), "memory:///wh".to_string())].into(),
                )
                .await
                .unwrap(),
        );
        let ns = NamespaceIdent::new("ns".into());
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "x",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap();
        let create = |name: &str, location: &str| {
            TableCreation::builder()
                .name(name.to_string())
                .location(location.to_string())
                .schema(schema.clone())
                .build()
        };
        catalog
            .create_table(&ns, create("t", "memory:///wh/ns/t"))
            .await
            .unwrap();
        catalog
            .create_table(&ns, create("t_2", "memory:///wh/ns/t_2"))
            .await
            .unwrap();
        let target = TableIdent::new(ns.clone(), "t".into());

        // A sibling whose name only shares a string prefix does not block.
        super::refuse_if_location_shared(&catalog, &target, "memory:///wh/ns/t/")
            .await
            .expect("no other table shares the location yet");

        catalog
            .create_table(&ns, create("twin", "memory:///wh/ns/t"))
            .await
            .unwrap();
        let err = super::refuse_if_location_shared(&catalog, &target, "memory:///wh/ns/t/")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ns.twin"), "{err}");
        assert!(err.to_string().contains("uses the same location"), "{err}");
    }

    #[tokio::test]
    async fn the_prefix_is_the_location_as_a_directory() {
        assert_eq!(
            purge_prefix(&table("s3://lake/wh/ns/t", META, &[])).unwrap(),
            "s3://lake/wh/ns/t/"
        );
        assert_eq!(
            purge_prefix(&table("s3://lake/wh/ns/t/", META, &[])).unwrap(),
            "s3://lake/wh/ns/t/"
        );
    }

    #[tokio::test]
    async fn gc_disabled_refuses() {
        let err = purge_prefix(&table(
            "s3://lake/wh/ns/t",
            META,
            &[("gc.enabled", "false")],
        ))
        .unwrap_err();
        assert!(err.to_string().contains("gc.enabled"), "{err}");
    }

    #[tokio::test]
    async fn a_bucket_root_refuses() {
        let err = purge_prefix(&table("s3://lake", None, &[])).unwrap_err();
        assert!(err.to_string().contains("bucket root"), "{err}");
        assert!(purge_prefix(&table("s3://lake/", None, &[])).is_err());
    }

    #[tokio::test]
    async fn a_write_path_outside_the_location_refuses() {
        let err = purge_prefix(&table(
            "s3://lake/wh/ns/t",
            META,
            &[("write.data.path", "s3://lake/shared/data")],
        ))
        .unwrap_err();
        assert!(err.to_string().contains("write.data.path"), "{err}");
        // A sibling that only shares a string prefix is outside too.
        assert!(purge_prefix(&table(
            "s3://lake/wh/ns/t",
            META,
            &[("write.metadata.path", "s3://lake/wh/ns/t_2/metadata")],
        ))
        .is_err());
    }

    #[tokio::test]
    async fn a_write_path_inside_the_location_is_fine() {
        assert!(purge_prefix(&table(
            "s3://lake/wh/ns/t",
            META,
            &[("write.data.path", "s3://lake/wh/ns/t/data")],
        ))
        .is_ok());
    }

    #[tokio::test]
    async fn metadata_outside_the_location_refuses() {
        let err = purge_prefix(&table(
            "s3://lake/wh/ns/t",
            Some("s3://lake/elsewhere/00001-a.metadata.json"),
            &[],
        ))
        .unwrap_err();
        assert!(err.to_string().contains("metadata file"), "{err}");
    }
}

#[cfg(all(test, feature = "test-sqlite"))]
mod tests {
    use super::*;
    use chrono::Duration;
    use sqe_compaction::is_live_delete_entry;

    fn session_with_roles(roles: Vec<&str>) -> Session {
        Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("test-token".to_string()),
            None,
            chrono::Utc::now() + Duration::hours(1),
            roles.into_iter().map(String::from).collect(),
        )
    }

    fn parse_call(sql: &str) -> ProcedureCall {
        let stmt =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql)
                .expect("parse")
                .remove(0);
        sqe_sql::procedures::try_parse_call(&stmt)
            .expect("procedure parse")
            .expect("is a procedure")
    }

    fn table_call(name: &str) -> ProcedureCall {
        parse_call(&format!("CALL system.{name}(table => 'ns.t')"))
    }

    async fn gate(config: SqeConfig, session: &Session, call: &ProcedureCall) -> bool {
        MaintenanceHandler::new(config)
            .authorize_or_deny(session, call)
            .await
            .is_ok()
    }

    fn base_config() -> SqeConfig {
        toml::from_str("[coordinator]\n[auth]\n[catalog]\ncatalog_url = \"\"\n")
            .expect("minimal config parses")
    }

    fn config_with_admins(admins: &[&str]) -> SqeConfig {
        let mut config = base_config();
        config.auth.admin_roles = admins.iter().map(|r| r.to_string()).collect();
        config
    }

    #[test]
    fn procedure_tiers_are_assigned_by_blast_radius() {
        assert_eq!(
            procedure_tier(&table_call("table_health")),
            ProcedureTier::ReadOnly
        );
        assert_eq!(
            procedure_tier(&table_call("rewrite_data_files")),
            ProcedureTier::TableWrite
        );
        assert_eq!(
            procedure_tier(&table_call("rewrite_manifests")),
            ProcedureTier::TableWrite
        );
        for admin in [
            "expire_snapshots",
            "remove_orphan_files",
            "drop_table",
            "set_current_snapshot(table => 'ns.t', snapshot_id => 1)",
        ] {
            let call = if admin.contains('(') {
                parse_call(&format!("CALL system.{admin}"))
            } else {
                table_call(admin)
            };
            assert_eq!(procedure_tier(&call), ProcedureTier::Admin, "{admin}");
        }
    }

    #[tokio::test]
    async fn admin_tier_denies_empty_roles_and_non_admins() {
        let config = config_with_admins(&["sqe_admin"]);
        let expire = table_call("expire_snapshots");
        let orphans = table_call("remove_orphan_files");
        assert!(!gate(config.clone(), &session_with_roles(vec![]), &expire).await);
        assert!(
            !gate(
                config.clone(),
                &session_with_roles(vec!["analyst"]),
                &orphans
            )
            .await
        );
        assert!(
            !gate(
                config.clone(),
                &session_with_roles(vec!["co-writer-viewer"]),
                &orphans
            )
            .await
        );
        assert!(
            gate(
                config.clone(),
                &session_with_roles(vec!["sqe_admin"]),
                &expire
            )
            .await
        );
        // The scheduler's minted session is a table writer, not an admin.
        let minted = session_with_roles(vec![]).with_maintenance_authority(true);
        assert!(!gate(config.clone(), &minted, &expire).await);
        // No admin roles configured at all: the admin tier is closed.
        assert!(!gate(base_config(), &session_with_roles(vec!["admin"]), &expire).await);
    }

    #[tokio::test]
    async fn table_write_tier_admits_writers_and_maintenance_authority() {
        let config = config_with_admins(&["sqe_admin"]);
        let rewrite = table_call("rewrite_data_files");
        assert!(
            gate(
                config.clone(),
                &session_with_roles(vec!["analyst"]),
                &rewrite
            )
            .await
        );
        assert!(
            gate(
                config.clone(),
                &session_with_roles(vec!["table_writer"]),
                &rewrite
            )
            .await
        );
        assert!(gate(config.clone(), &session_with_roles(vec![]), &rewrite).await);
        assert!(
            gate(
                config.clone(),
                &session_with_roles(vec!["readonly", "admin"]),
                &rewrite
            )
            .await
        );
        let minted = session_with_roles(vec!["readonly"]).with_maintenance_authority(true);
        assert!(gate(config.clone(), &minted, &rewrite).await);
        assert!(
            !gate(
                config.clone(),
                &session_with_roles(vec!["readonly"]),
                &rewrite
            )
            .await
        );
        assert!(
            !gate(
                config.clone(),
                &session_with_roles(vec!["read_analyst"]),
                &rewrite
            )
            .await
        );
    }

    #[tokio::test]
    async fn read_only_tier_needs_no_roles() {
        let config = config_with_admins(&["sqe_admin"]);
        assert!(
            gate(
                config.clone(),
                &session_with_roles(vec!["readonly"]),
                &table_call("table_health")
            )
            .await
        );
        let dry =
            parse_call("CALL system.purge_orphan_locations(namespace => 'ns', dry_run => true)");
        assert!(gate(config.clone(), &session_with_roles(vec![]), &dry).await);
        let wet =
            parse_call("CALL system.purge_orphan_locations(namespace => 'ns', dry_run => false)");
        assert!(!gate(config, &session_with_roles(vec![]), &wet).await);
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
        assert!(is_strictly_under("s3://b/wh/ns/t/sub", "s3://b/wh/ns"));
    }

    #[test]
    fn is_strictly_under_handles_case_and_slash_variants() {
        assert!(is_strictly_under(
            "S3://MyBucket/wh/ns/t/",
            "s3://mybucket/wh/ns"
        ));
    }

    // ---- resolve_execution (Phase 4c Task 5) -------------------------------
    //
    // 3 modes x 3 floor conditions (above / at / below) = 9 cases, matching
    // the task brief's "9 (mode x above/below floor) combinations". Plus a
    // `min_workers = 0` edge-case group (4c whole-phase review fix): the
    // distributed threshold is `min_workers.max(1)`, so `auto`/`require`
    // must not resolve to `Distributed` with zero healthy workers just
    // because the configured floor is 0.

    use sqe_core::config::DistributionMode;

    #[test]
    fn resolve_execution_local_mode_ignores_fleet_when_above_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Local, 5, 2),
            ExecutionPlan::Local
        );
    }

    #[test]
    fn resolve_execution_local_mode_ignores_fleet_at_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Local, 2, 2),
            ExecutionPlan::Local
        );
    }

    #[test]
    fn resolve_execution_local_mode_ignores_fleet_below_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Local, 0, 2),
            ExecutionPlan::Local
        );
    }

    #[test]
    fn resolve_execution_auto_mode_distributes_above_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Auto, 5, 2),
            ExecutionPlan::Distributed
        );
    }

    #[test]
    fn resolve_execution_auto_mode_distributes_at_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Auto, 2, 2),
            ExecutionPlan::Distributed
        );
    }

    #[test]
    fn resolve_execution_auto_mode_falls_back_to_local_below_floor() {
        // The single-node/no-fleet case: `healthy_workers == 0` is always
        // `< min_workers` (min_workers defaults to 2, and 0 workers is
        // never `>= 1` even with the floor lowered), so `Auto` degrades to
        // `Local` -- the byte-identical-to-pre-Task-5 guarantee.
        assert_eq!(
            resolve_execution(DistributionMode::Auto, 0, 2),
            ExecutionPlan::Local
        );
    }

    #[test]
    fn resolve_execution_require_mode_distributes_above_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Require, 5, 2),
            ExecutionPlan::Distributed
        );
    }

    #[test]
    fn resolve_execution_require_mode_distributes_at_floor() {
        assert_eq!(
            resolve_execution(DistributionMode::Require, 2, 2),
            ExecutionPlan::Distributed
        );
    }

    #[test]
    fn resolve_execution_require_mode_skips_below_floor_never_falls_back_to_local() {
        assert_eq!(
            resolve_execution(DistributionMode::Require, 0, 2),
            ExecutionPlan::SkipInsufficientWorkers
        );
    }

    // ---- resolve_execution: min_workers = 0 edge cases (4c review fix) -----
    //
    // `min_workers = 0` must not let `auto`/`require` treat "zero healthy
    // workers" as satisfying the floor. The distributed threshold is
    // `min_workers.max(1)`, so these degrade the same way the `min_workers =
    // 2` cases above do at 0 healthy workers.

    #[test]
    fn resolve_execution_auto_mode_min_workers_zero_falls_back_to_local_at_zero_workers() {
        assert_eq!(
            resolve_execution(DistributionMode::Auto, 0, 0),
            ExecutionPlan::Local
        );
    }

    #[test]
    fn resolve_execution_require_mode_min_workers_zero_skips_at_zero_workers() {
        assert_eq!(
            resolve_execution(DistributionMode::Require, 0, 0),
            ExecutionPlan::SkipInsufficientWorkers
        );
    }

    #[test]
    fn resolve_execution_auto_mode_min_workers_zero_distributes_with_one_worker() {
        assert_eq!(
            resolve_execution(DistributionMode::Auto, 1, 0),
            ExecutionPlan::Distributed
        );
    }

    #[test]
    fn resolve_execution_require_mode_min_workers_zero_distributes_with_one_worker() {
        assert_eq!(
            resolve_execution(DistributionMode::Require, 1, 0),
            ExecutionPlan::Distributed
        );
    }

    // ---- parse_distribution_mode_override ----------------------------------

    #[test]
    fn parse_distribution_mode_override_accepts_known_values_case_insensitively() {
        assert_eq!(
            parse_distribution_mode_override("auto").unwrap(),
            DistributionMode::Auto
        );
        assert_eq!(
            parse_distribution_mode_override("LOCAL").unwrap(),
            DistributionMode::Local
        );
        assert_eq!(
            parse_distribution_mode_override("Require").unwrap(),
            DistributionMode::Require
        );
    }

    #[test]
    fn parse_distribution_mode_override_rejects_unknown_value() {
        assert!(parse_distribution_mode_override("yolo").is_err());
    }

    // -------------------------------------------------------------------
    // Concurrent-position-delete-during-compaction guard (VENDOR PATCH
    // fix/compaction-concurrent-delete-conflict): proves the coordinator-
    // local `rewrite_data_files` path does not resurrect a row deleted by a
    // concurrent MoR DELETE that lands mid-compaction.
    //
    // `rewrite_files_new_delete_conflict_test.rs` already proves the
    // vendored `RewriteFilesAction::set_validate_from_snapshot_id` primitive
    // in isolation. This test instead drives the REAL call path
    // (`rewrite_data_files_once`, the same method the public `handle()` /
    // `rewrite_data_files` retry wrapper calls), through a real SQLite-
    // backed catalog with real Parquet data and a real position delete
    // file, to prove the wiring in THIS file (not just the vendored
    // primitive) closes the resurrection hole end to end.
    //
    // The race is forced deterministically via `RewriteRaceSeam` rather than
    // wall-clock timing: without it, the concurrent DELETE would have to
    // land inside the narrow window between this attempt's stale table load
    // and its commit, which cannot be guaranteed by sleeping.
    //
    // Run with `cargo test -p sqe-coordinator --lib --features test-sqlite
    // maintenance::tests::concurrent_position_delete`.
    #[cfg(feature = "test-sqlite")]
    mod concurrent_delete_race {
        use super::*;
        use crate::test_support::sqlite_rig::SqliteRig;
        use arrow_array::Array;
        use iceberg::spec::Schema as IcebergSchema;
        use iceberg::TableCreation;

        fn race_one_col_arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }

        /// Reload the table and read every live data file's `id` column back
        /// directly (bypassing any delete-application scan machinery, since
        /// the whole point is to confirm the COMMITTED data files themselves
        /// no longer contain the deleted row).
        async fn race_live_ids(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<i64> {
            let table = catalog.load_table(ident).await.expect("reload table");
            let files = collect_live_data_files(&table)
                .await
                .expect("collect live data files");
            let mut ids = Vec::new();
            for f in files {
                let input = table.file_io().new_input(f.file_path()).expect("new_input");
                let bytes = input.read().await.expect("read file bytes");
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                        .expect("open parquet")
                        .build()
                        .expect("build reader");
                for batch in reader {
                    let batch = batch.expect("read batch");
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("id column is Int64Array");
                    for i in 0..col.len() {
                        ids.push(col.value(i));
                    }
                }
            }
            ids.sort_unstable();
            ids
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn concurrent_position_delete_during_local_rewrite_does_not_resurrect_rows() {
            let rig = SqliteRig::new().await;
            let catalog = rig.catalog.clone();
            let ns = NamespaceIdent::new("default".to_string());

            let iceberg_schema: IcebergSchema =
                iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&race_one_col_arrow_schema())
                    .expect("arrow schema converts to iceberg schema");
            let creation = TableCreation::builder()
                .name("race_test".to_string())
                .schema(iceberg_schema)
                .build();
            catalog
                .create_table(&ns, creation)
                .await
                .expect("create table");
            let ident = TableIdent::new(ns.clone(), "race_test".to_string());

            // Seed ONE data file with three rows: id 0, 1, 2.
            let seed_table = catalog
                .load_table(&ident)
                .await
                .expect("load table for seeding");
            let batch = RecordBatch::try_new(
                race_one_col_arrow_schema(),
                vec![Arc::new(Int64Array::from(vec![0i64, 1, 2]))],
            )
            .expect("build seed batch");
            let compression = parse_parquet_compression("zstd");
            let tracker = new_upload_tracker();
            let seeded = crate::writer::write_data_files(
                &seed_table,
                vec![batch],
                "seed",
                compression,
                tracker,
            )
            .await
            .expect("write seed data file");
            assert_eq!(seeded.len(), 1, "setup: expected a single seed data file");
            let old_path = seeded[0].file_path().to_string();

            let tx = Transaction::new(&seed_table);
            let action = tx.fast_append().add_data_files(seeded);
            let tx = action.apply(tx).expect("apply fast_append");
            tx.commit(catalog.as_ref())
                .await
                .expect("commit seed data file");

            // Race machinery: attempt 1 pauses right before its commit.
            let seam = Arc::new(RewriteRaceSeam::new());
            let handler =
                MaintenanceHandler::new(rig.config.clone()).with_test_race_seam(seam.clone());

            let table_ref = TableRef::parse("default.race_test").expect("parse table ref");
            let catalog_for_task = catalog.clone();

            let rewrite_task = tokio::spawn(async move {
                handler
                    .rewrite_data_files_once(
                        &catalog_for_task,
                        &table_ref,
                        None,
                        Some(1),
                        None,
                        None,
                        None,
                        None,
                        None,
                        true, // rewrite_all: force the lone file into the rewrite
                        None,
                    )
                    .await
            });

            // Attempt 1 has now loaded the table, captured its (soon-to-be
            // stale) plan baseline, and finished computing its compacted
            // output; only the commit remains.
            seam.ready.notified().await;

            // Concurrent writer: a MoR position DELETE lands on the very
            // file being rewritten, marking the row at position 1 (id=1) as
            // deleted -- the row-resurrection hazard this patch closes.
            let table_at_s0 = catalog.load_table(&ident).await.expect("reload at s0");
            let delete_files = crate::writer::write_position_delete_files(
                &table_at_s0,
                vec![(old_path.clone(), 1)],
                compression,
            )
            .await
            .expect("write position delete file");
            let tx2 = Transaction::new(&table_at_s0);
            let action2 = tx2.row_delta().add_delete_files(delete_files);
            let tx2 = action2.apply(tx2).expect("apply row_delta");
            tx2.commit(catalog.as_ref())
                .await
                .expect("commit concurrent position delete");

            // Release attempt 1: its stale in-memory table has no idea about
            // the delete above, so `set_validate_from_snapshot_id` must
            // reject its commit as a retryable conflict instead of
            // committing the 3-row compacted output over the new delete.
            seam.go.notify_one();

            let attempt1 = rewrite_task.await.expect("task join");
            let err = attempt1.expect_err(
                "attempt 1 must be rejected: a new position delete landed on the file being \
                 rewritten after its plan baseline; committing anyway would resurrect id=1",
            );
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("conflict") || msg.contains("retry"),
                "rejection must be classified retryable so the outer `rewrite_data_files` \
                 retry wrapper retries instead of surfacing a hard failure, got: {msg}"
            );

            // Retry (mirrors `rewrite_data_files`'s retry wrapper): a fresh
            // handler with no seam re-runs `rewrite_data_files_once`,
            // reloading the table -- now at S1, with the position delete
            // live -- and re-planning against it.
            let retry_handler = MaintenanceHandler::new(rig.config.clone());
            let outcome = retry_handler
                .rewrite_data_files_once(
                    &catalog,
                    &TableRef::parse("default.race_test").expect("parse table ref"),
                    None,
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    None,
                )
                .await
                .expect("retry attempt must succeed against the now-current snapshot");
            assert!(
                outcome.skipped_reason.is_none(),
                "retry must actually run, not skip"
            );

            // Correctness: id=1 must stay deleted, not resurrected by
            // attempt 1's stale (pre-delete) compacted output.
            let ids = race_live_ids(&catalog, &ident).await;
            assert_eq!(
                ids,
                vec![0, 2],
                "concurrently-deleted row (id=1) must not resurrect; got {ids:?}"
            );
        }
    }

    // ---------------------------------------------------------------------
    // Architecture review 512: compaction must not drop Puffin NDV statistics.
    //
    // Run with `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --lib
    // --features test-sqlite maintenance::tests::puffin_carry_forward`.
    // ---------------------------------------------------------------------
    #[cfg(feature = "test-sqlite")]
    mod puffin_carry_forward {
        use super::*;
        use crate::test_support::sqlite_rig::SqliteRig;
        use sqe_catalog::puffin_stats::{
            build_theta_sketches, parse_ndv_properties, read_ndv_blobs,
            write_puffin_sidecar_from_sketches, PROP_CARRIED_FROM, PROP_NDV_UPPER_BOUND,
            PROP_PUFFIN_STATS,
        };

        fn id_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }

        fn id_batch(range: std::ops::Range<i64>) -> RecordBatch {
            RecordBatch::try_new(
                id_schema(),
                vec![Arc::new(Int64Array::from(range.collect::<Vec<i64>>()))],
            )
            .expect("build batch")
        }

        /// Create an opted-in table and commit `batches` as one data file
        /// each in a single fast-append. Returns the ident and the table as
        /// committed.
        async fn seeded_table(
            rig: &SqliteRig,
            name: &str,
            batches: Vec<RecordBatch>,
        ) -> (TableIdent, IcebergTable) {
            let table = rig
                .local_fs_table_with_properties(
                    name,
                    &id_schema(),
                    None,
                    std::collections::HashMap::from([(
                        PROP_PUFFIN_STATS.to_string(),
                        "true".to_string(),
                    )]),
                )
                .await;
            let committed = rig.commit_rows(&table, batches).await;
            (rig.ident(name), committed)
        }

        async fn rewrite_all(rig: &SqliteRig, name: &str) -> RewriteOutcome {
            MaintenanceHandler::new(rig.config.clone())
                .rewrite_data_files_once(
                    &rig.catalog,
                    &TableRef::parse(&format!("default.{name}")).expect("parse table ref"),
                    None,
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    None,
                )
                .await
                .expect("rewrite_data_files_once succeeds")
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn rewrite_data_files_carries_statistics_onto_the_new_snapshot() {
            let rig = SqliteRig::new().await;
            let catalog = rig.catalog.clone();
            let batch_a = id_batch(0..100);
            let batch_b = id_batch(100..150);
            let (ident, seeded) = seeded_table(
                &rig,
                "stats_carry",
                vec![batch_a.clone(), batch_b.clone()],
            )
            .await;
            let seed_snapshot = seeded
                .metadata()
                .current_snapshot_id()
                .expect("seed snapshot");

            // Register statistics for the seed snapshot the way an INSERT
            // would: sketch the rows, write the sidecar, commit with retry.
            let sketches =
                build_theta_sketches(seeded.metadata().current_schema(), &[batch_a, batch_b]);
            let base_dir = format!(
                "{}/metadata",
                seeded.metadata().location().trim_end_matches('/')
            );
            let stats = write_puffin_sidecar_from_sketches(
                seeded.file_io(),
                &base_dir,
                &sketches,
                seed_snapshot,
                seeded.metadata().last_sequence_number(),
            )
            .await
            .expect("write seed sidecar");
            let seed_stats_path = stats.statistics_path.clone();
            crate::puffin_carry::commit_statistics_with_retry(catalog.as_ref(), &ident, stats)
                .await
                .expect("commit seed statistics");
            let before = catalog.load_table(&ident).await.expect("reload");
            let seed_ndv = parse_ndv_properties(
                before
                    .metadata()
                    .statistics_for_snapshot(seed_snapshot)
                    .expect("seed snapshot has statistics"),
            );
            assert_eq!(seed_ndv.get(&1), Some(&150), "setup: 150 distinct ids");

            let outcome = rewrite_all(&rig, "stats_carry").await;
            assert!(
                outcome.skipped_reason.is_none(),
                "compaction must run: {outcome:?}"
            );

            let after = catalog
                .load_table(&ident)
                .await
                .expect("reload after rewrite");
            let new_snapshot = after
                .metadata()
                .current_snapshot_id()
                .expect("rewrite produced a snapshot");
            assert_ne!(new_snapshot, seed_snapshot);
            let carried = after
                .metadata()
                .statistics_for_snapshot(new_snapshot)
                .expect("the compacted snapshot must have its own statistics file");
            assert_ne!(
                carried.statistics_path, seed_stats_path,
                "carried statistics get their own object so expiring the old snapshot cannot orphan them"
            );
            assert_eq!(carried.snapshot_id, new_snapshot);
            assert_eq!(
                parse_ndv_properties(carried).get(&1),
                Some(&150),
                "compaction does not change row identity: ndv is unchanged"
            );
            let blob = &carried.blob_metadata[0];
            assert_eq!(blob.snapshot_id, new_snapshot);
            assert_eq!(
                blob.properties.get(PROP_CARRIED_FROM),
                Some(&seed_snapshot.to_string())
            );
            assert!(
                !blob.properties.contains_key(PROP_NDV_UPPER_BOUND),
                "rewrite keeps the estimate exact, not an upper bound"
            );
            assert!(
                after
                    .metadata()
                    .statistics_for_snapshot(seed_snapshot)
                    .is_some(),
                "the previous snapshot keeps its own statistics"
            );

            // The carried body is a real sketch, so a later INSERT can union it.
            let blobs = read_ndv_blobs(after.file_io(), carried)
                .await
                .expect("carried sidecar is readable");
            assert_eq!(blobs.len(), 1);
            let decoded = crate::puffin_carry::RawThetaSketch::decode(&blobs[0])
                .expect("carried body decodes");
            assert_eq!(decoded.estimate(), 150);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn rewrite_without_previous_statistics_registers_nothing() {
            let rig = SqliteRig::new().await;
            let catalog = rig.catalog.clone();
            let (ident, seeded) = seeded_table(
                &rig,
                "no_stats",
                vec![id_batch(0..10), id_batch(10..20)],
            )
            .await;
            assert_eq!(seeded.metadata().statistics_iter().len(), 0);

            let outcome = rewrite_all(&rig, "no_stats").await;
            assert!(outcome.skipped_reason.is_none());

            let after = catalog.load_table(&ident).await.expect("reload");
            let new_snapshot = after.metadata().current_snapshot_id().expect("snapshot");
            assert!(
                after
                    .metadata()
                    .statistics_for_snapshot(new_snapshot)
                    .is_none(),
                "nothing to carry: no statistics are invented"
            );
            let stats_dir = std::fs::read_dir(format!(
                "{}/metadata",
                after.metadata().location().trim_start_matches("file://")
            ))
            .expect("metadata dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "stats"))
            .count();
            assert_eq!(stats_dir, 0, "no orphan .stats object is staged");
        }
    }

    // ---------------------------------------------------------------------
    // Phase 4d Task 3: opt-in partial-progress batched commits
    //
    // `MaintenanceHandler::commit_eligible_groups` is the code the
    // distributed rewrite job actually runs; `dispatch_and_collect_groups`
    // (the network-facing half, `compaction_dispatch.rs`) is bypassed here
    // via a synthetic `GroupBatchDispatcher` so this exercises the
    // batching/commit/partial-status logic deterministically against a
    // real SQLite-backed Iceberg catalog, without a live worker fleet.
    // Run with `cargo test -p sqe-coordinator --features test-sqlite`.
    // ---------------------------------------------------------------------
    #[cfg(feature = "test-sqlite")]
    mod partial_progress_batching {
        use super::*;
        use crate::test_support::sqlite_rig::SqliteRig;
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};
        use iceberg::TableCreation;

        fn minimal_schema() -> iceberg::spec::Schema {
            iceberg::spec::Schema::builder()
                .with_fields(vec![iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )
                .into()])
                .build()
                .expect("build schema")
        }

        /// A fabricated data file. Physical bytes are never read:
        /// `check_file_existence` (see `validate_data_file_changes` in the
        /// vendored `transaction/snapshot.rs`) validates removed files
        /// against the current snapshot's MANIFESTS, not the object store,
        /// so a file only needs to be registered via a prior commit (this
        /// test's `fast_append` below), never actually written to disk.
        fn fabricated_data_file(
            path: &str,
            record_count: u64,
            bytes: u64,
        ) -> iceberg::spec::DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(bytes)
                .record_count(record_count)
                .partition(Struct::from_iter(std::iter::empty()))
                .partition_spec_id(0)
                .build()
                .expect("build data file")
        }

        /// Build a fresh table with 6 committed data files (`record_count`
        /// 10 each, so total rows = 60), grouped into 3 eligible groups of 2
        /// files. Returns `(catalog, ident, table, eligible_groups,
        /// seq_at_start)`. Each test gets its own tempdir/catalog/table so
        /// the two scenarios (partial_progress on vs off) can never bleed
        /// state into each other.
        async fn table_with_six_files(
            table_name: &str,
        ) -> (
            Arc<dyn Catalog>,
            TableIdent,
            IcebergTable,
            Vec<Vec<DataFile>>,
            i64,
            String,
            i64,
        ) {
            // Leak the rig so its tempdir outlives this function (the sqlite
            // catalog's backing file must stay on disk for the rest of the
            // test); acceptable in a test process that exits promptly.
            let rig: &'static SqliteRig = Box::leak(Box::new(SqliteRig::new().await));
            let catalog = rig.catalog.clone();

            let ns = NamespaceIdent::new("default".to_string());
            let creation = TableCreation::builder()
                .name(table_name.to_string())
                .schema(minimal_schema())
                .build();
            let table = catalog
                .create_table(&ns, creation)
                .await
                .expect("create table");

            let paths: Vec<String> = (1..=6)
                .map(|i| format!("mem://{table_name}/data/f{i}.parquet"))
                .collect();
            let initial_files: Vec<DataFile> = paths
                .iter()
                .map(|p| fabricated_data_file(p, 10, 100))
                .collect();

            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(initial_files.clone());
            let tx_applied = action.apply(tx).expect("apply fast_append");
            tx_applied
                .commit(catalog.as_ref())
                .await
                .expect("commit fast_append");

            let ident = TableIdent::new(ns, table_name.to_string());
            let table = catalog.load_table(&ident).await.expect("reload table");
            let seq_at_start = table
                .metadata()
                .current_snapshot()
                .expect("initial snapshot exists")
                .sequence_number();
            let metadata_location = table
                .metadata_location_result()
                .expect("initial metadata location exists")
                .to_string();
            let snapshot_id = table
                .metadata()
                .current_snapshot()
                .expect("initial snapshot exists")
                .snapshot_id();

            // 3 groups of 2 files each, in the same order as `paths`.
            let eligible_groups: Vec<Vec<DataFile>> =
                initial_files.chunks(2).map(<[DataFile]>::to_vec).collect();

            (
                catalog,
                ident,
                table,
                eligible_groups,
                seq_at_start,
                metadata_location,
                snapshot_id,
            )
        }

        /// Synthetic [`crate::compaction_dispatch::GroupBatchDispatcher`]:
        /// fails any batch containing `poison_path`, otherwise "compacts"
        /// each group in the batch into one consolidated file carrying the
        /// group's summed row count and byte size (preserving the
        /// `added <= removed` row invariant exactly, satisfying it with
        /// equality).
        struct PoisonPathDispatcher {
            poison_path: String,
        }

        #[async_trait::async_trait]
        impl crate::compaction_dispatch::GroupBatchDispatcher for PoisonPathDispatcher {
            async fn dispatch(
                &self,
                batch: &[Vec<DataFile>],
                _metadata_location: &str,
                _snapshot_id: i64,
            ) -> sqe_core::Result<Vec<sqe_compaction::dispatch::GroupOutcome>> {
                if batch
                    .iter()
                    .flatten()
                    .any(|f| f.file_path() == self.poison_path)
                {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "synthetic dispatch failure: group containing poisoned file {}",
                        self.poison_path
                    )));
                }
                Ok(batch
                    .iter()
                    .enumerate()
                    .map(|(i, group)| {
                        let rows: u64 = group
                            .iter()
                            .map(iceberg::spec::DataFile::record_count)
                            .sum();
                        let bytes: u64 = group
                            .iter()
                            .map(iceberg::spec::DataFile::file_size_in_bytes)
                            .sum();
                        let new_path = format!(
                            "mem://partial_progress_test/data/consolidated-{}-{}.parquet",
                            i,
                            uuid::Uuid::now_v7()
                        );
                        let new_file = fabricated_data_file(&new_path, rows, bytes.max(1));
                        sqe_compaction::dispatch::GroupOutcome {
                            group_id: i as u32,
                            new_files: vec![new_file],
                            rows_written: rows,
                            bytes_written: bytes,
                            uploaded_paths: vec![],
                        }
                    })
                    .collect())
            }
        }

        /// Synthetic dispatcher covering the Task 3 hardening gap: like
        /// [`PoisonPathDispatcher`]'s happy path, every batch "compacts"
        /// cleanly into one consolidated file per group. But the FIRST time
        /// it is asked to dispatch a batch containing `conflict_after_path`,
        /// it first lands an unrelated, ordinary `fast_append` directly on
        /// the same table through the same catalog -- simulating a
        /// concurrent external writer -- which advances the table's
        /// snapshot out from under the (now-stale) `table` handle that
        /// `commit_eligible_groups` is about to build that batch's
        /// `RewriteFilesAction` against. The vendored SQL catalog's
        /// `update_table` (`vendor/iceberg-rust/crates/catalog/sql/src/
        /// catalog.rs`) re-validates the transaction's requirements against
        /// a FRESH reload before applying, so this reliably produces a real
        /// `ErrorKind::CatalogCommitConflicts` ("commit conflict",
        /// retryable) on that batch's first commit attempt -- not a
        /// simulated/injected error string, an actual optimistic-concurrency
        /// conflict from the underlying catalog. `triggered` ensures the
        /// injection fires exactly once, modeling a one-shot transient race
        /// rather than a permanently wedged table, so the fix's
        /// reload-and-redispatch retry is expected to succeed on its next
        /// attempt. This dispatcher's "compacted" output only depends on
        /// `batch` (deterministic row/byte sums), so being re-dispatched on
        /// retry (the fix's behavior) reproduces the same aggregate counts
        /// as the old reuse-the-first-output behavior for THIS scenario
        /// (the injected concurrent write is a benign append unrelated to
        /// the batch's own files, not a resurrection-triggering position
        /// delete on one of them -- see `DeleteInjectingDispatcher` below
        /// for that case).
        struct ConflictOnceDispatcher {
            conflict_after_path: String,
            catalog: Arc<dyn Catalog>,
            ident: TableIdent,
            triggered: std::sync::atomic::AtomicBool,
        }

        #[async_trait::async_trait]
        impl crate::compaction_dispatch::GroupBatchDispatcher for ConflictOnceDispatcher {
            async fn dispatch(
                &self,
                batch: &[Vec<DataFile>],
                _metadata_location: &str,
                _snapshot_id: i64,
            ) -> sqe_core::Result<Vec<sqe_compaction::dispatch::GroupOutcome>> {
                let should_inject = batch
                    .iter()
                    .flatten()
                    .any(|f| f.file_path() == self.conflict_after_path)
                    && !self
                        .triggered
                        .swap(true, std::sync::atomic::Ordering::SeqCst);

                if should_inject {
                    let fresh = self
                        .catalog
                        .load_table(&self.ident)
                        .await
                        .expect("reload table for injected concurrent commit");
                    let extra_path = format!(
                        "mem://conflict_retry_test/data/concurrent-{}.parquet",
                        uuid::Uuid::now_v7()
                    );
                    let extra_file = fabricated_data_file(&extra_path, 5, 50);
                    let tx = Transaction::new(&fresh);
                    let action = tx.fast_append().add_data_files(vec![extra_file]);
                    let tx_applied = action.apply(tx).expect("apply concurrent fast_append");
                    tx_applied
                        .commit(self.catalog.as_ref())
                        .await
                        .expect("commit concurrent fast_append");
                }

                Ok(batch
                    .iter()
                    .enumerate()
                    .map(|(i, group)| {
                        let rows: u64 = group
                            .iter()
                            .map(iceberg::spec::DataFile::record_count)
                            .sum();
                        let bytes: u64 = group
                            .iter()
                            .map(iceberg::spec::DataFile::file_size_in_bytes)
                            .sum();
                        let new_path = format!(
                            "mem://conflict_retry_test/data/consolidated-{}-{}.parquet",
                            i,
                            uuid::Uuid::now_v7()
                        );
                        let new_file = fabricated_data_file(&new_path, rows, bytes.max(1));
                        sqe_compaction::dispatch::GroupOutcome {
                            group_id: i as u32,
                            new_files: vec![new_file],
                            rows_written: rows,
                            bytes_written: bytes,
                            uploaded_paths: vec![],
                        }
                    })
                    .collect())
            }
        }

        async fn total_rows(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> u64 {
            let reloaded = catalog.load_table(ident).await.expect("reload table");
            collect_live_data_files(&reloaded)
                .await
                .expect("collect live data files")
                .iter()
                .map(iceberg::spec::DataFile::record_count)
                .sum()
        }

        #[tokio::test]
        async fn partial_progress_on_commits_earlier_batches_and_reports_partial() {
            let (
                catalog,
                ident,
                table,
                eligible_groups,
                seq_at_start,
                metadata_location,
                snapshot_id,
            ) = table_with_six_files("partial_on_test").await;
            // Group 3 (files f5, f6) is the LAST group -- poison its first
            // file so the terminal failure lands on the last batch, exactly
            // like the brief's "forced failure on the last group" scenario.
            let poison_path = eligible_groups[2][0].file_path().to_string();
            let dispatcher = PoisonPathDispatcher {
                poison_path: poison_path.clone(),
            };

            let outcome = MaintenanceHandler::commit_eligible_groups(
                &catalog,
                table,
                &ident,
                eligible_groups,
                /* partial_progress */ true,
                /* partial_progress_batch */ 1,
                seq_at_start,
                Some(snapshot_id),
                &metadata_location,
                snapshot_id,
                &[],
                None,
                None,
                6,
                600,
                &dispatcher,
                None,
            )
            .await
            .expect("partial_progress must report Ok, not fail the whole job");

            assert!(outcome.partial, "outcome must be marked partial");
            assert!(
                outcome.partial_error.is_some(),
                "partial outcome must carry the failure reason"
            );
            assert_eq!(
                outcome.files_rewritten, 4,
                "2 committed batches x 2 files each"
            );
            assert_eq!(
                outcome.files_out, 2,
                "one consolidated file per committed batch"
            );
            assert!(
                outcome.snapshot_id.is_some(),
                "the 2nd batch's commit must be reported"
            );

            // Table reflects exactly the 2 successful batches: 2 new
            // consolidated files + the last group's 2 untouched files (f5,
            // f6, since batch 3 never committed) = 4 live files.
            let reloaded = catalog.load_table(&ident).await.expect("reload table");
            let live_files = collect_live_data_files(&reloaded)
                .await
                .expect("collect live files");
            assert_eq!(
                live_files.len(),
                4,
                "2 consolidated + 2 untouched from the failed batch"
            );

            // 3 snapshots total: the initial fast_append + 2 successful
            // batch commits. The 3rd (failed) batch never commits, so it
            // must not add a snapshot.
            assert_eq!(reloaded.metadata().snapshots().count(), 3);

            // Row count is preserved exactly: nothing was lost or
            // manufactured across the 2 real commits plus the 2 untouched
            // files.
            assert_eq!(total_rows(&catalog, &ident).await, 60);
        }

        #[tokio::test]
        async fn partial_progress_off_leaves_table_unchanged_on_the_same_forced_failure() {
            let (
                catalog,
                ident,
                table,
                eligible_groups,
                seq_at_start,
                metadata_location,
                snapshot_id,
            ) = table_with_six_files("partial_off_test").await;
            let poison_path = eligible_groups[2][0].file_path().to_string();
            let dispatcher = PoisonPathDispatcher { poison_path };

            let result = MaintenanceHandler::commit_eligible_groups(
                &catalog,
                table,
                &ident,
                eligible_groups,
                /* partial_progress */ false,
                /* partial_progress_batch */
                1, // ignored when partial_progress is false
                seq_at_start,
                Some(snapshot_id),
                &metadata_location,
                snapshot_id,
                &[],
                None,
                None,
                6,
                600,
                &dispatcher,
                None,
            )
            .await;

            assert!(
                result.is_err(),
                "partial_progress off must fail the whole job on any group failure, like today"
            );

            // Table is byte-identical to before the call: still 6 live
            // files, still exactly the one snapshot from the initial
            // fast_append -- no batch ever committed.
            let reloaded = catalog.load_table(&ident).await.expect("reload table");
            let live_files = collect_live_data_files(&reloaded)
                .await
                .expect("collect live files");
            assert_eq!(live_files.len(), 6, "no commit at all must have happened");
            assert_eq!(reloaded.metadata().snapshots().count(), 1);
            assert_eq!(total_rows(&catalog, &ident).await, 60);
        }

        // ---------------------------------------------------------------
        // Task 3 hardening: batch commit-conflict retry
        //
        // Covers the review gap directly: a retryable commit conflict on a
        // batch AFTER the first one has already committed must be retried
        // in place (reload + recommit, no re-dispatch) instead of being
        // swallowed straight into `partial = true` with zero retries.
        // ---------------------------------------------------------------

        #[tokio::test]
        async fn batch_commit_conflict_after_first_batch_is_retried_and_job_completes() {
            let (
                catalog,
                ident,
                table,
                eligible_groups,
                seq_at_start,
                metadata_location,
                snapshot_id,
            ) = table_with_six_files("conflict_retry_test").await;
            // Group index 1 (files f3, f4) is the 2nd batch when
            // partial_progress_batch = 1, i.e. `committed_batches == 1`
            // (> 0) at the time its commit is attempted -- exactly the case
            // this fix adds retry for.
            let conflict_after_path = eligible_groups[1][0].file_path().to_string();
            let dispatcher = ConflictOnceDispatcher {
                conflict_after_path,
                catalog: Arc::clone(&catalog),
                ident: ident.clone(),
                triggered: std::sync::atomic::AtomicBool::new(false),
            };

            let outcome = MaintenanceHandler::commit_eligible_groups(
                &catalog,
                table,
                &ident,
                eligible_groups,
                /* partial_progress */ true,
                /* partial_progress_batch */ 1,
                seq_at_start,
                Some(snapshot_id),
                &metadata_location,
                snapshot_id,
                &[],
                None,
                None,
                6,
                600,
                &dispatcher,
                None,
            )
            .await
            .expect("the injected conflict is retryable and must not fail the job");

            assert!(
                !outcome.partial,
                "a retryable commit conflict on batch 2 must be fully absorbed by the inner \
                 per-batch retry, not reported as a partial-progress fallback"
            );
            assert!(
                outcome.partial_error.is_none(),
                "no terminal failure occurred, so there is nothing to report as a partial error"
            );
            assert_eq!(
                outcome.files_rewritten, 6,
                "all 3 batches x 2 files each eventually commit"
            );
            assert_eq!(outcome.files_out, 3, "one consolidated file per batch");

            let reloaded = catalog.load_table(&ident).await.expect("reload table");
            let live_files = collect_live_data_files(&reloaded)
                .await
                .expect("collect live files");
            // 3 consolidated files (one per batch) + 1 extra file from the
            // injected concurrent commit that triggered the conflict.
            assert_eq!(live_files.len(), 4);

            // Snapshots: initial fast_append + the injected concurrent
            // fast_append + 3 successful batch commits. The batch-2 commit
            // attempt that hit the conflict never produces a snapshot of
            // its own (the SQL catalog's CAS rejects it before writing new
            // metadata), so retrying does not inflate this count.
            assert_eq!(reloaded.metadata().snapshots().count(), 5);

            // All 60 original rows are preserved across the 3 real batch
            // commits, plus the 5 rows the injected concurrent commit added.
            assert_eq!(total_rows(&catalog, &ident).await, 65);
        }

        // ---------------------------------------------------------------
        // CRITICAL regression: the inner retry must not resurrect rows
        //
        // Unlike `ConflictOnceDispatcher` above (whose injected concurrent
        // write is a benign, unrelated `fast_append`), this dispatcher
        // injects a concurrent POSITION delete that references one of the
        // SAME batch's own input files -- exactly the scenario the review
        // flagged: reusing stale worker output on the inner retry recommits
        // rows the concurrent delete already removed, because the
        // compacted output's new file path no longer matches the position
        // delete's `(old_file_path, pos)`. This test fails against the
        // pre-fix code (which recommits the first, naive dispatch's output
        // unchanged) and passes only once the inner retry re-dispatches
        // against the reloaded snapshot.
        // ---------------------------------------------------------------

        /// Synthetic dispatcher reproducing the resurrection bug. The FIRST
        /// time it is asked to dispatch a batch containing `victim_path`,
        /// it commits a concurrent POSITION delete directly through the
        /// catalog, referencing `victim_path` itself -- one of THAT BATCH'S
        /// OWN input files -- simulating a concurrent `DELETE FROM` landing
        /// mid-job (not an unrelated file, unlike `ConflictOnceDispatcher`'s
        /// benign append). It then returns a "naive" consolidated output
        /// that does NOT subtract the deleted rows, modeling a worker that
        /// only ever read the job's plan-time snapshot.
        ///
        /// Every SUBSEQUENT call for the same batch -- only reachable if
        /// the caller re-dispatches on the resulting commit conflict, i.e.
        /// only after the fix -- returns a CORRECTED output with the
        /// concurrently-deleted rows subtracted, modeling a worker that
        /// re-read the current snapshot and applied the now-visible
        /// position delete. Before the fix, `commit_one_batch` never calls
        /// `dispatch` a second time for a retried commit (it just recommits
        /// the first, naive output against the reloaded table), so running
        /// this test against pre-fix code recommits the naive (un-deleted)
        /// row count -- the resurrection bug. After the fix, the
        /// re-dispatch call produces the corrected output and the deleted
        /// rows stay deleted.
        struct DeleteInjectingDispatcher {
            victim_path: String,
            deleted_rows: u64,
            catalog: Arc<dyn Catalog>,
            ident: TableIdent,
            triggered: std::sync::atomic::AtomicBool,
            victim_dispatch_count: std::sync::atomic::AtomicUsize,
        }

        #[async_trait::async_trait]
        impl crate::compaction_dispatch::GroupBatchDispatcher for DeleteInjectingDispatcher {
            async fn dispatch(
                &self,
                batch: &[Vec<DataFile>],
                _metadata_location: &str,
                _snapshot_id: i64,
            ) -> sqe_core::Result<Vec<sqe_compaction::dispatch::GroupOutcome>> {
                let is_victim_batch = batch
                    .iter()
                    .flatten()
                    .any(|f| f.file_path() == self.victim_path);

                if is_victim_batch
                    && !self
                        .triggered
                        .swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    // Simulate a concurrent `DELETE FROM ... WHERE ...`
                    // landing a position delete on one of THIS batch's own
                    // input files, after the previous batch committed but
                    // before this one does -- the exact trigger for the
                    // resurrection bug.
                    let fresh = self
                        .catalog
                        .load_table(&self.ident)
                        .await
                        .expect("reload table for injected concurrent position delete");
                    let pos_delete = iceberg::spec::DataFileBuilder::default()
                        .content(DataContentType::PositionDeletes)
                        .file_path(format!(
                            "mem://resurrection_test/deletes/pos-{}.parquet",
                            uuid::Uuid::now_v7()
                        ))
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(64)
                        .record_count(self.deleted_rows)
                        .partition(Struct::from_iter(std::iter::empty()))
                        .partition_spec_id(0)
                        .referenced_data_file(Some(self.victim_path.clone()))
                        .build()
                        .expect("build position delete file");
                    let tx = Transaction::new(&fresh);
                    let action = tx.row_delta().add_delete_files(vec![pos_delete]);
                    let tx_applied = action.apply(tx).expect("apply concurrent row_delta");
                    tx_applied
                        .commit(self.catalog.as_ref())
                        .await
                        .expect("commit concurrent position delete");
                }

                // Whether THIS call is the naive first dispatch or a
                // corrected re-dispatch is decided purely by call count for
                // the victim batch: the first call always injects the
                // delete above and returns naive output (a real worker
                // reading the plan-time snapshot would have no way to see a
                // delete that did not exist yet); any later call for the
                // same batch only happens if the caller re-dispatches after
                // a commit conflict, so it returns the corrected output.
                let victim_call_index = if is_victim_batch {
                    self.victim_dispatch_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                } else {
                    0
                };

                Ok(batch
                    .iter()
                    .enumerate()
                    .map(|(i, group)| {
                        let rows: u64 = group
                            .iter()
                            .map(iceberg::spec::DataFile::record_count)
                            .sum();
                        let bytes: u64 = group
                            .iter()
                            .map(iceberg::spec::DataFile::file_size_in_bytes)
                            .sum();
                        let is_victim_group =
                            group.iter().any(|f| f.file_path() == self.victim_path);
                        let effective_rows = if is_victim_group && victim_call_index > 0 {
                            // Corrected: the re-dispatch "sees" the
                            // concurrently-committed position delete and
                            // excludes its rows from the compacted output.
                            rows.saturating_sub(self.deleted_rows)
                        } else {
                            // Naive: full input row count, no delete
                            // applied -- exactly what a stale, reused
                            // worker output looks like.
                            rows
                        };
                        let new_path = format!(
                            "mem://resurrection_test/data/consolidated-{}-{}.parquet",
                            i,
                            uuid::Uuid::now_v7()
                        );
                        let new_file =
                            fabricated_data_file(&new_path, effective_rows, bytes.max(1));
                        sqe_compaction::dispatch::GroupOutcome {
                            group_id: i as u32,
                            new_files: vec![new_file],
                            rows_written: effective_rows,
                            bytes_written: bytes,
                            uploaded_paths: vec![],
                        }
                    })
                    .collect())
            }
        }

        #[tokio::test]
        async fn batch_commit_conflict_from_own_position_delete_does_not_resurrect_rows() {
            let (
                catalog,
                ident,
                table,
                eligible_groups,
                seq_at_start,
                metadata_location,
                snapshot_id,
            ) = table_with_six_files("resurrection_test").await;
            // Group index 1 (files f3, f4) is the 2nd batch when
            // partial_progress_batch = 1, i.e. `committed_batches == 1`
            // (> 0) at the time its commit is attempted -- the same
            // retry-eligible position as `ConflictOnceDispatcher`'s
            // scenario, but this time the concurrent write targets one of
            // THIS batch's own files (f3) with a position delete instead of
            // an unrelated append.
            let victim_path = eligible_groups[1][0].file_path().to_string();
            let deleted_rows = 4u64;
            let dispatcher = DeleteInjectingDispatcher {
                victim_path,
                deleted_rows,
                catalog: Arc::clone(&catalog),
                ident: ident.clone(),
                triggered: std::sync::atomic::AtomicBool::new(false),
                victim_dispatch_count: std::sync::atomic::AtomicUsize::new(0),
            };

            let outcome = MaintenanceHandler::commit_eligible_groups(
                &catalog,
                table,
                &ident,
                eligible_groups,
                /* partial_progress */ true,
                /* partial_progress_batch */ 1,
                seq_at_start,
                Some(snapshot_id),
                &metadata_location,
                snapshot_id,
                &[],
                None,
                None,
                6,
                600,
                &dispatcher,
                None,
            )
            .await
            .expect("the injected conflict is retryable and must not fail the job");

            assert!(
                !outcome.partial,
                "a retryable commit conflict on batch 2 must be fully absorbed by the inner \
                 per-batch retry, not reported as a partial-progress fallback"
            );
            assert!(
                outcome.partial_error.is_none(),
                "no terminal failure occurred, so there is nothing to report as a partial error"
            );
            assert_eq!(
                outcome.files_rewritten, 6,
                "all 3 batches x 2 files each eventually commit"
            );
            assert_eq!(outcome.files_out, 3, "one consolidated file per batch");

            // The critical assertion: total live rows must reflect the
            // concurrently-deleted rows STAYING deleted (60 - 4 = 56), not
            // resurrected back to 60. Before the fix, the inner retry
            // recommits the first (naive, pre-delete) dispatch's output
            // unchanged, so this would read 60.
            assert_eq!(
                total_rows(&catalog, &ident).await,
                56,
                "the 4 concurrently-deleted rows must stay deleted, not resurrect in the \
                 compacted output"
            );

            // And no rows were incorrectly dropped either: exactly 3
            // consolidated files (one per batch), no orphaned extras.
            let reloaded = catalog.load_table(&ident).await.expect("reload table");
            let live_files = collect_live_data_files(&reloaded)
                .await
                .expect("collect live files");
            assert_eq!(
                live_files.len(),
                3,
                "one consolidated data file per batch, nothing extra"
            );
        }
    }
}
