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

use crate::worker_registry::{WorkerLoadTracker, WorkerRegistry};
use crate::writer::{new_upload_tracker, parse_parquet_compression, WriteCleanupGuard};

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
use sqe_compaction::{group_files_by_partition, plan_delete_aware_read, rewrite_group, SortCtx, SortSpec};
pub(crate) use sqe_compaction::{
    collect_live_delete_files, covered_position_deletes, delete_heavy_files,
    pack_file_groups_partition_aware,
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
    /// TEST-ONLY synchronization seam (`#[cfg(test)]`, so it compiles only
    /// for this crate's own unit test binary -- never for a release build or
    /// an external integration test). See `RewriteRaceSeam`'s doc comment and
    /// the `#[cfg(test)] mod tests` test
    /// `concurrent_position_delete_during_local_rewrite_does_not_resurrect_rows`
    /// for how it is used to land a concurrent commit deterministically
    /// inside `rewrite_data_files_once`'s conflict window, instead of racing
    /// wall-clock timing. Always `None` unless a test opts in via
    /// `with_test_race_seam`; `MaintenanceHandler::new` never sets it.
    #[cfg(test)]
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
            #[cfg(test)]
            race_seam: None,
        }
    }

    /// TEST-ONLY: attach a [`RewriteRaceSeam`] so the first attempt of
    /// `rewrite_data_files_once` pauses right before its final commit. See
    /// the field doc comment on `race_seam` above.
    #[cfg(test)]
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
                        return Err(SqeError::Execution(format!(
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
                            rewrite_all,
                            None,
                        )
                        .await?
                    }
                };
                Ok(vec![rewrite_outcome_batch(&to_table_ident(table), &outcome)?])
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
            ProcedureCall::TableHealth { table } => self.table_health(session, table).await,
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
        if matches!(
            call,
            ProcedureCall::SuggestBloomFilterColumns { .. } | ProcedureCall::TableHealth { .. }
        ) {
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
        catalog: &Arc<dyn Catalog>,
        table_ref: &TableRef,
        target_file_size_bytes: Option<u64>,
        min_input_files: Option<usize>,
        max_concurrent_file_group_rewrites: Option<usize>,
        strategy: Option<String>,
        sort_order: Option<String>,
        delete_file_threshold: Option<usize>,
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
        let plan_snapshot_id = table.metadata_ref().current_snapshot().map(|s| s.snapshot_id());
        let read_plan = plan_delete_aware_read(&table).await?;
        let live_deletes = collect_live_delete_files(&table).await?;

        // Resolve the sort strategy against the table schema (None = bin-pack).
        // Sort compaction needs the shared spillable runtime; refuse rather than
        // risk the known sort-on-write OOM if it was not wired in.
        let arrow_schema: arrow_schema::SchemaRef = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
                .map_err(|e| {
                    SqeError::Execution(format!("compaction schema conversion failed: {e}"))
                })?,
        );
        let sort_spec =
            parse_sort_spec(strategy.as_deref(), sort_order.as_deref(), &arrow_schema)?;
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
        // bin-pack would otherwise leave alone. Empty unless the caller set a
        // threshold. Moot under `strategy => 'sort'`, which already rewrites the
        // whole partition, so it is only computed for the bin-pack path.
        let delete_heavy: std::collections::HashSet<String> =
            match (sort_ctx.is_some(), delete_file_threshold) {
                (false, Some(threshold)) if threshold > 0 => {
                    delete_heavy_files(&read_plan.tasks_by_path, threshold)
                }
                _ => std::collections::HashSet::new(),
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
            });
        }

        // Files kept even when at or above target: delete-heavy files always, and
        // every file when `rewrite_all` forces a full re-encode. `rewrite_all` is
        // a superset of `delete_heavy`, so it wins when set. Bin-pack still packs
        // small files under target; a large force-included file anchors its own
        // group (rewritten alone to apply its deletes / re-encode).
        let all_paths: std::collections::HashSet<String>;
        let force_include: &std::collections::HashSet<String> = if rewrite_all {
            all_paths = old_data_files.iter().map(|f| f.file_path().to_string()).collect();
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
        let results: Vec<(Vec<DataFile>, Vec<DataFile>, u64)> =
            stream::iter(eligible_groups.into_iter())
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
            .map_err(|e| SqeError::Execution(format!("rewrite_files apply failed: {e}")))?;

        // TEST-ONLY (see `race_seam`'s doc comment): pauses the FIRST attempt
        // right here -- after the plan baseline was captured from a table
        // load that is now stale, and after this attempt finished computing
        // its compacted output, but BEFORE the commit below -- so a test can
        // deterministically land a concurrent commit inside the conflict
        // window `set_validate_from_snapshot_id` guards. `fired` makes this
        // one-shot: a retry (a fresh call to this method after a rejected
        // first attempt) runs straight through, matching real retry behavior.
        // Compiled only under `#[cfg(test)]`; always a no-op in production.
        #[cfg(test)]
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
                    let msg = e.to_string().to_lowercase();
                    let retryable = msg.contains("retryable") || msg.contains("conflict");
                    if retryable && attempt < MAX_COMMIT_ATTEMPTS {
                        let backoff = std::time::Duration::from_millis(50 * (1u64 << (attempt - 1)));
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
                SqeError::Execution(format!(
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
        let plan_snapshot_id = table.metadata_ref().current_snapshot().map(|s| s.snapshot_id());

        let read_plan = plan_delete_aware_read(&table).await?;
        let live_deletes = collect_live_delete_files(&table).await?;

        let arrow_schema: arrow_schema::SchemaRef = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
                .map_err(|e| {
                    SqeError::Execution(format!("compaction schema conversion failed: {e}"))
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

        let delete_heavy: std::collections::HashSet<String> =
            match (sort_spec.is_some(), delete_file_threshold) {
                (false, Some(threshold)) if threshold > 0 => {
                    delete_heavy_files(&read_plan.tasks_by_path, threshold)
                }
                _ => std::collections::HashSet::new(),
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
            });
        }

        // Same force-include semantics as the local path: when `rewrite_all`
        // is set, every file is force-included so bin-pack does not skip
        // files already at or above target.
        let all_paths: std::collections::HashSet<String>;
        let force_include: &std::collections::HashSet<String> = if rewrite_all {
            all_paths = old_data_files.iter().map(|f| f.file_path().to_string()).collect();
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

        // Dispatch every group; on success this holds one CompactGroupResponse
        // per group (order not significant, each carries its own group_id).
        // A group that exhausts `group_attempts` fails the whole job before
        // any commit is attempted (no partial commit).
        let responses = crate::compaction_dispatch::dispatch_and_collect_groups(
            job_id,
            &ident.to_string(),
            &metadata_location,
            snapshot_id,
            &eligible_groups,
            target_bytes,
            &compression,
            sort_wire.as_ref(),
            s3,
            registry,
            load_tracker,
            worker_secret,
            dist.max_inflight_groups_per_worker,
            dist.group_attempts,
            group_timeout,
            heartbeat_timeout,
        )
        .await?;

        // Decode each worker's Avro DataFiles against THIS table load's
        // schema/partition type/spec id/format version -- none of that rides
        // in CompactGroupResponse itself (see its doc comment).
        let partition_type = table.metadata().default_partition_type().clone();
        let partition_spec_id = table.metadata().default_partition_spec_id();
        let format_version = table.metadata().format_version();
        let schema = table.metadata().current_schema().clone();

        let outcomes: Vec<sqe_compaction::dispatch::GroupOutcome> = responses
            .iter()
            .map(|r| {
                sqe_compaction::dispatch::decode_group_response(
                    r,
                    schema.as_ref(),
                    partition_spec_id,
                    &partition_type,
                    format_version,
                )
            })
            .collect::<sqe_core::Result<Vec<_>>>()?;

        let old_files: Vec<DataFile> = eligible_groups.into_iter().flatten().collect();

        // Global added <= removed invariant, re-run over the FULL job (the
        // per-group `expected_rows_after_deletes` cross-check already ran on
        // each worker before it returned); any violation aborts before the
        // commit below.
        let aggregated = sqe_compaction::dispatch::aggregate_group_outcomes(outcomes, &old_files)?;

        let output_count = aggregated.new_files.len() as i64;
        let output_bytes: i64 = aggregated
            .new_files
            .iter()
            .map(|f| f.file_size_in_bytes() as i64)
            .sum();

        // Position delete files fully covered by the removed data files are
        // dropped in the same commit, exactly like the local path.
        let removed_data_paths: std::collections::HashSet<String> =
            old_files.iter().map(|f| f.file_path().to_string()).collect();
        let covered_deletes = covered_position_deletes(&removed_data_paths, &live_deletes);
        let removed_delete_count = covered_deletes.len() as i64;

        info!(
            table = %ident,
            input_count = old_files.len(),
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
        let tx = Transaction::new(&table);
        let files_to_remove: Vec<DataFile> =
            old_files.iter().cloned().chain(covered_deletes).collect();
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
            .map_err(|e| SqeError::Execution(format!("rewrite_files apply failed: {e}")))?;

        let committed = tx_applied
            .commit(catalog.as_ref())
            .await
            .map_err(|e| classify_commit_error(e, "rewrite_data_files_distributed"))?;
        let new_snapshot_id = committed.metadata().current_snapshot_id();

        let cache_key = format!("{}.{}", ident.namespace(), ident.name());
        if let Some(tc) = &self.table_cache {
            tc.invalidate(&cache_key).await;
        }

        // Same post-commit sanity check as the local path: reload and
        // confirm the live file count matches expectation. Purely
        // observability (a mismatch only warns, matching
        // `rewrite_data_files_once`), but worth keeping identical here since
        // it is the one place that would catch a catalog-state-propagation
        // bug specific to the distributed commit path.
        match catalog.load_table(&ident).await {
            Ok(reloaded) => match collect_live_data_files(&reloaded).await {
                Ok(live_files) => {
                    let live_after = live_files.len();
                    let expected_after =
                        output_count + (input_count as i64 - old_files.len() as i64);
                    info!(
                        table = %ident,
                        live_after,
                        expected_after,
                        "rewrite_data_files_distributed: post-commit verification"
                    );
                    if live_after as i64 != expected_after {
                        warn!(
                            table = %ident,
                            live_after,
                            expected_after,
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

        let rows_removed = (aggregated.removed_rows - aggregated.added_rows) as i64;

        Ok(RewriteOutcome {
            files_in: input_count as i64,
            files_out: output_count,
            bytes_in: total_bytes,
            bytes_out: output_bytes,
            rows_removed,
            snapshot_id: new_snapshot_id,
            files_rewritten: old_files.len() as i64,
            skipped_reason: None,
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
fn parse_distribution_mode_override(raw: &str) -> sqe_core::Result<sqe_core::config::DistributionMode> {
    use sqe_core::config::DistributionMode;
    match raw.to_ascii_lowercase().as_str() {
        "auto" => Ok(DistributionMode::Auto),
        "local" => Ok(DistributionMode::Local),
        "require" => Ok(DistributionMode::Require),
        other => Err(SqeError::Execution(format!(
            "CALL system.rewrite_data_files: invalid 'distributed' value '{other}'; \
             expected 'auto', 'local', or 'require'"
        ))),
    }
}

fn call_name_rewrite() -> &'static str {
    "rewrite_data_files"
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
}

/// Render a [`RewriteOutcome`] into the generic `summary_batch` shape the
/// `CALL system.rewrite_data_files` surface has always returned. Preserves
/// the pre-refactor `status` text exactly: `"skipped: {reason}"` or
/// `"committed rewritten={files_rewritten}"`.
fn rewrite_outcome_batch(ident: &TableIdent, outcome: &RewriteOutcome) -> sqe_core::Result<RecordBatch> {
    let status = match &outcome.skipped_reason {
        Some(reason) => format!("skipped: {reason}"),
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

pub(crate) async fn load_table(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> sqe_core::Result<IcebergTable> {
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
            return Err(SqeError::Execution(format!(
                "rewrite_data_files: unknown strategy '{s}'; expected 'binpack' or 'sort'"
            )));
        }
        _ => {}
    }
    // A sort is requested when strategy is 'sort', or when only sort_order is
    // given (strategy omitted). 'binpack' with a sort_order ignores the order.
    let sort_requested = matches!(strat.as_deref(), Some("sort"))
        || (strat.is_none() && sort_order.is_some());
    if !sort_requested {
        return Ok(None);
    }

    let order = sort_order
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            SqeError::Execution(
                "rewrite_data_files: strategy => 'sort' requires a non-empty sort_order".into(),
            )
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
            return Err(SqeError::Execution(
                "rewrite_data_files: zorder(...) needs at least one column".into(),
            ));
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
                    return Err(SqeError::Execution(format!(
                        "rewrite_data_files: invalid sort direction '{other}'; expected ASC or DESC"
                    )));
                }
            };
            cols.push((toks[0].to_string(), asc));
        }
        if cols.is_empty() {
            return Err(SqeError::Execution(
                "rewrite_data_files: sort_order is empty".into(),
            ));
        }
        SortSpec::Columns(cols)
    };

    for name in spec.columns() {
        if schema.field_with_name(name).is_err() {
            return Err(SqeError::Execution(format!(
                "rewrite_data_files: sort_order column '{name}' not found in table schema"
            )));
        }
    }
    Ok(Some(spec))
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

/// Treat the session as write-capable when it carries the explicit
/// maintenance-authority marker, or when no explicit read-only role is set.
///
/// Rules, applied in order:
/// 0. If `session.has_maintenance_authority()` is true, the session is
///    write-capable, full stop. This is the explicit Phase 4b authorization
///    path for the in-process maintenance principal (set only by
///    `MaintenancePrincipal::session_from_identity`); it is checked before
///    and independent of the role-name heuristic below, so the maintenance
///    session's write authority never depends on how its Polaris roles
///    happen to be named.
/// 1. If any role name matches `^read` or `^select` (case-insensitive),
///    AND no role contains "write" or "admin", the session is read-only.
/// 2. Otherwise the session is write-capable.
///
/// The Polaris/Cedar backends will override this with richer decisions once
/// the policy enforcement wiring lands; this function is the engine-level
/// fallback and is the source of truth for the `#[ignore]` integration tests.
pub(crate) fn session_has_write_privilege(session: &Session) -> bool {
    if session.has_maintenance_authority() {
        return true;
    }

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

/// TEST-ONLY (`#[cfg(test)]`) two-way handshake used by
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
#[cfg(test)]
struct RewriteRaceSeam {
    ready: tokio::sync::Notify,
    go: tokio::sync::Notify,
    fired: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
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

    #[test]
    fn write_privilege_maintenance_authority_overrides_read_only_roles() {
        // A session carrying the explicit maintenance-authority marker must
        // pass the write gate even if its roles would otherwise mark it
        // read-only. This is the Phase 4b explicit authorization path: the
        // maintenance session must not depend on how its Polaris roles
        // happen to be spelled.
        let session = session_with_roles(vec!["readonly"]).with_maintenance_authority(true);
        assert!(session_has_write_privilege(&session));
    }

    #[test]
    fn write_privilege_normal_read_only_session_without_marker_still_denied() {
        // Without the explicit marker, a normal read-only session is denied
        // exactly as before: the new marker must not weaken the existing
        // role-name heuristic for non-maintenance sessions.
        let session = session_with_roles(vec!["readonly"]);
        assert!(!session_has_write_privilege(&session));
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
        use arrow_array::Array;
        use iceberg::spec::Schema as IcebergSchema;
        use iceberg::TableCreation;

        async fn race_sqlite_catalog(dir: &tempfile::TempDir) -> Arc<dyn Catalog> {
            let location = dir.path().to_str().expect("tempdir path is UTF-8");
            sqe_catalog::mount::build_catalog(
                location,
                sqe_sql::CatalogKind::Sqlite,
                &std::collections::BTreeMap::new(),
                &sqe_core::SecretStore::new(),
            )
            .await
            .expect("sqlite catalog builds")
        }

        fn race_one_col_arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }

        fn race_test_config() -> SqeConfig {
            toml::from_str("[coordinator]\n[auth]\n[catalog]\ncatalog_url = \"\"\n")
                .expect("minimal SqeConfig parses")
        }

        /// Reload the table and read every live data file's `id` column back
        /// directly (bypassing any delete-application scan machinery, since
        /// the whole point is to confirm the COMMITTED data files themselves
        /// no longer contain the deleted row).
        async fn race_live_ids(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<i64> {
            let table = catalog.load_table(ident).await.expect("reload table");
            let files = collect_live_data_files(&table).await.expect("collect live data files");
            let mut ids = Vec::new();
            for f in files {
                let input = table.file_io().new_input(f.file_path()).expect("new_input");
                let bytes = input.read().await.expect("read file bytes");
                let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
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
            let dir = tempfile::tempdir().expect("tempdir");
            let catalog = race_sqlite_catalog(&dir).await;
            let ns = NamespaceIdent::new("default".to_string());
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .expect("create namespace");

            let iceberg_schema: IcebergSchema =
                iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&race_one_col_arrow_schema())
                    .expect("arrow schema converts to iceberg schema");
            let creation = TableCreation::builder()
                .name("race_test".to_string())
                .schema(iceberg_schema)
                .build();
            catalog.create_table(&ns, creation).await.expect("create table");
            let ident = TableIdent::new(ns.clone(), "race_test".to_string());

            // Seed ONE data file with three rows: id 0, 1, 2.
            let seed_table = catalog.load_table(&ident).await.expect("load table for seeding");
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
            tx.commit(catalog.as_ref()).await.expect("commit seed data file");

            // Race machinery: attempt 1 pauses right before its commit.
            let seam = Arc::new(RewriteRaceSeam::new());
            let handler = MaintenanceHandler::new(race_test_config())
                .with_test_race_seam(seam.clone());

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
            let retry_handler = MaintenanceHandler::new(race_test_config());
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
                    true,
                    None,
                )
                .await
                .expect("retry attempt must succeed against the now-current snapshot");
            assert!(outcome.skipped_reason.is_none(), "retry must actually run, not skip");

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
}
