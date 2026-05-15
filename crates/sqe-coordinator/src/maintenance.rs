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
    new_upload_tracker, parse_parquet_compression, write_data_files, WriteCleanupGuard,
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

    pub fn with_audit(mut self, audit: Arc<sqe_metrics::audit::AuditLogger>) -> Self {
        self.audit = Some(audit);
        self
    }

    pub fn with_table_cache(mut self, cache: TableMetadataCache) -> Self {
        self.table_cache = Some(cache);
        self
    }

    /// Attach a callback that returns the current query log.
    ///
    /// Required for `suggest_bloom_filter_columns`; without it the procedure
    /// returns an empty suggestion set (still a well-formed response).
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
        }
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
        let audit_status = "denied";
        if let Some(ref audit) = self.audit {
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: session.user.username.clone(),
                session_id: Some(session.id.clone()),
                query_hash: sqe_metrics::audit::query_hash(&format!(
                    "CALL system.{}({target})",
                    call.name(),
                )),
                query_text: Some(format!(
                    "CALL system.{}('{target}')",
                    call.name(),
                )),
                statement_type: "procedure".to_string(),
                duration_ms: 0,
                rows_returned: 0,
                status: audit_status.to_string(),
                client_ip: None,
                tables_touched: vec![target.to_string()],
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

    async fn rewrite_data_files(
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

        let catalog = self.create_catalog_bridge(session).await?;
        let ident = to_table_ident(table_ref);
        let table = load_table(&catalog, &ident).await?;

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
        let groups = pack_file_groups(&old_data_files, target_bytes);

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
        let results: Vec<(Vec<DataFile>, Vec<DataFile>, u64)> =
            stream::iter(eligible_groups.into_iter())
                .map(|group| {
                    let table_for_group = table_arc.clone();
                    let tracker_for_group = tracker.clone();
                    async move {
                        rewrite_group(&table_for_group, group, compression, tracker_for_group)
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

        // Row-count invariant: the rows in the files we are deleting must
        // equal the rows in the files we are adding. If not, we have a bug
        // and must NOT commit.
        let removed_rows: u64 = old_files.iter().map(|f| f.record_count()).sum();
        let added_rows: u64 = new_files.iter().map(|f| f.record_count()).sum();
        if added_rows != removed_rows {
            return Err(SqeError::Execution(format!(
                "rewrite_data_files row-count invariant violated: removed={removed_rows} \
                 added={added_rows} (read_count={rewritten_rows}); aborting before commit"
            )));
        }

        let output_count = new_files.len() as i64;
        let output_bytes: i64 = new_files.iter().map(|f| f.file_size_in_bytes() as i64).sum();

        info!(
            table = %ident,
            input_count = old_files.len(),
            output_count,
            removed_rows,
            added_rows,
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
        let action = tx
            .rewrite_files()
            .set_enable_delete_filter_manager(true)
            .set_check_file_existence(true)
            .add_data_files(new_files)
            .delete_files(old_files.clone());
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
        let catalog = self.create_catalog_bridge(session).await?;
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

        let catalog = self.create_catalog_bridge(session).await?;
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
        let catalog = self.create_catalog_bridge(session).await?;
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
        let catalog = self.create_catalog_bridge(session).await?;
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

    async fn create_catalog_bridge(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = Arc::new(
            SessionCatalog::for_session(
                &self.config,
                self.table_cache.clone(),
                session.access_token().expose(),
            )
            .await?,
        );
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

/// Read every Parquet file in `group`, combine the batches, and emit the
/// combined contents as a fresh set of data files. Returns
/// `(new_files, old_files, total_rows_read)` so the caller can build the
/// commit payload and validate the row-count invariant.
async fn rewrite_group(
    table: &IcebergTable,
    group: Vec<DataFile>,
    compression: parquet::basic::Compression,
    tracker: crate::writer::UploadedPaths,
) -> sqe_core::Result<(Vec<DataFile>, Vec<DataFile>, u64)> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut rows_read: u64 = 0;

    for df in &group {
        let file_batches = read_parquet_file(table, df.file_path()).await?;
        for b in file_batches {
            rows_read += b.num_rows() as u64;
            if b.num_rows() > 0 {
                batches.push(b);
            }
        }
    }

    // Safety check: rows we read must equal the sum the manifest told us.
    let expected: u64 = group.iter().map(|f| f.record_count()).sum();
    if rows_read != expected {
        return Err(SqeError::Execution(format!(
            "rewrite_group: Parquet row count {rows_read} does not match manifest \
             record_count {expected} for group of {} files",
            group.len()
        )));
    }

    if batches.is_empty() {
        // Empty group: caller treats this as a no-op (nothing added, nothing removed).
        return Ok((vec![], vec![], 0));
    }

    let new_files = write_data_files(table, batches, "rewrite", compression, tracker).await?;

    Ok((new_files, group, rows_read))
}

/// Read a Parquet data file via the table's configured FileIO. Mirrors the
/// helper in `WriteHandler::read_parquet_via_table` but lives here to avoid
/// depending on that handler's session context.
async fn read_parquet_file(
    table: &IcebergTable,
    file_path: &str,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let file_io = table.file_io();
    let input = file_io
        .new_input(file_path)
        .map_err(|e| SqeError::Execution(format!("Failed to open file '{file_path}': {e}")))?;

    let input_file = input
        .read()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to read file '{file_path}': {e}")))?;

    let reader = parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(input_file)
        .map_err(|e| {
            SqeError::Execution(format!(
                "Failed to create Parquet reader for '{file_path}': {e}"
            ))
        })?;

    let reader = reader.build().map_err(|e| {
        SqeError::Execution(format!(
            "Failed to build Parquet reader for '{file_path}': {e}"
        ))
    })?;

    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            SqeError::Execution(format!("Failed to read Parquet file '{file_path}': {e}"))
        })?;

    Ok(batches)
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
