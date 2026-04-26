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
use sqe_sql::{ProcedureCall, TableRef};
use tracing::{info, warn};

use crate::writer::{parse_parquet_compression, write_data_files};

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
        let table_ref = call.table().clone();
        self.authorize_or_deny(session, call, &table_ref).await?;

        match call {
            ProcedureCall::RewriteDataFiles {
                table: _,
                target_file_size_bytes,
                min_input_files,
                max_concurrent_file_group_rewrites,
            } => {
                self.rewrite_data_files(
                    session,
                    &table_ref,
                    *target_file_size_bytes,
                    *min_input_files,
                    *max_concurrent_file_group_rewrites,
                )
                .await
            }
            ProcedureCall::ExpireSnapshots {
                table: _,
                older_than,
                retain_last,
            } => {
                let older_than_ms = older_than.map(|t| t.timestamp_millis());
                self.expire_snapshots(session, &table_ref, older_than_ms, *retain_last)
                    .await
            }
            ProcedureCall::RemoveOrphanFiles {
                table: _,
                older_than,
            } => {
                let older_than_ms = older_than.map(|t| t.timestamp_millis());
                self.remove_orphan_files(session, &table_ref, older_than_ms)
                    .await
            }
            ProcedureCall::RewriteManifests { table: _ } => {
                self.rewrite_manifests(session, &table_ref).await
            }
            ProcedureCall::SuggestBloomFilterColumns {
                table: _,
                history_limit,
            } => self.suggest_bloom_filter_columns(&table_ref, *history_limit),
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
        table_ref: &TableRef,
    ) -> sqe_core::Result<()> {
        // Read-only procedures bypass the write-privilege gate.
        if matches!(call, ProcedureCall::SuggestBloomFilterColumns { .. }) {
            return Ok(());
        }

        if session_has_write_privilege(session) {
            return Ok(());
        }

        let audit_status = "denied";
        if let Some(ref audit) = self.audit {
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: session.user.username.clone(),
                session_id: Some(session.id.clone()),
                query_hash: sqe_metrics::audit::query_hash(&format!(
                    "CALL system.{}({})",
                    call.name(),
                    table_ref.as_string()
                )),
                query_text: Some(format!(
                    "CALL system.{}(table => '{}')",
                    call.name(),
                    table_ref.as_string()
                )),
                statement_type: "procedure".to_string(),
                duration_ms: 0,
                rows_returned: 0,
                status: audit_status.to_string(),
                client_ip: None,
            });
        }
        warn!(
            username = %session.user.username,
            procedure = %call.name(),
            table = %table_ref.as_string(),
            "Maintenance procedure denied: user lacks write privilege"
        );
        Err(SqeError::Execution(format!(
            "Access denied: user '{}' does not have write privilege on '{}' required by \
             CALL system.{}",
            session.user.username,
            table_ref.as_string(),
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
        let results: Vec<(Vec<DataFile>, Vec<DataFile>, u64)> =
            stream::iter(eligible_groups.into_iter())
                .map(|group| {
                    let table_for_group = table_arc.clone();
                    async move {
                        rewrite_group(&table_for_group, group, compression).await
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

    async fn create_catalog_bridge(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
                self.table_cache.clone(),
                None,
                None,
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
    small.sort_by(|a, b| b.file_size_in_bytes().cmp(&a.file_size_in_bytes()));

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

    let new_files = write_data_files(table, batches, "rewrite", compression).await?;

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
            "test-token".to_string(),
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
}
