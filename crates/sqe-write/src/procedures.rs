//! The catalog-only `CALL system.*` procedures, shared by the cluster's
//! `MaintenanceHandler` (`sqe-coordinator`) and the embedded CLI.
//!
//! Each core takes an already-built catalog and a table identifier, so the
//! caller owns what differs between them: authorization, the user-scoped
//! catalog bridge, audit entries and log lines that name the user. The
//! helpers the cores share (`summary_batch`, `load_table`,
//! `reload_committed`, ...) live here too; `maintenance.rs` imports them
//! back so its other procedures and the scheduler keep calling them.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use iceberg::spec::DataFile;
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use sqe_catalog::TableMetadataCache;
use sqe_core::config::MaintenanceCompactionConfig;
use sqe_core::{SqeError, SqeErrorCode};
use sqe_sql::TableRef;
use tracing::{info, warn};

/// Build a single-row `RecordBatch` describing the procedure's effect.
/// Columns: procedure, table, input_count, output_count, input_bytes,
/// output_bytes, status.
pub fn summary_batch(
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
    .map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("Failed to build summary batch: {e}"),
        )
    })?;
    Ok(batch)
}

/// Translate an iceberg commit error into an SQE error, preserving retryable
/// conflict signalling so callers can distinguish transient from permanent
/// failures.
pub fn classify_commit_error(err: iceberg::Error, proc: &str) -> SqeError {
    let msg = err.to_string();
    if msg.to_lowercase().contains("conflict") || msg.to_lowercase().contains("retry") {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("{proc}: concurrent writer conflict (retryable): {msg}"),
        )
    } else {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("{proc}: commit failed: {msg}"),
        )
    }
}

pub fn to_table_ident(table_ref: &TableRef) -> TableIdent {
    let ns = nested_namespace(table_ref.namespace_parts());
    TableIdent::new(ns, table_ref.name.clone())
}

/// `NamespaceIdent` from a CALL reference's levels (`a.b` -> `["a", "b"]`).
pub fn nested_namespace(parts: Vec<String>) -> NamespaceIdent {
    NamespaceIdent::from_vec(parts).expect("a dotted name splits into at least one part")
}

pub async fn load_table(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> sqe_core::Result<IcebergTable> {
    catalog
        .load_table(ident)
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to load table '{ident}': {e}")))
}

/// The table a commit returned, reloaded before anything reads or deletes
/// its files.
///
/// A REST commit's response carries metadata but no storage `config`, so
/// the table the catalog builds from it has a `FileIO` with the catalog's
/// own properties only: on a credential-vending catalog with no static
/// warehouse key, its first file access fails with "failed to load signing
/// credential" (portal request §2). Only `loadTable` vends the caller's
/// credential. The reload must show the metadata this commit wrote; if
/// another writer committed in between, the caller's file step is refused
/// rather than run against metadata it did not produce.
pub async fn reload_committed(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    committed: &IcebergTable,
) -> sqe_core::Result<IcebergTable> {
    let reloaded = load_table(catalog, ident).await?;
    if reloaded.metadata_location() != committed.metadata_location() {
        return Err(SqeError::with_code(
            SqeErrorCode::CommitConflict,
            format!(
                "table '{ident}' changed after this call's commit; the commit stands but no \
                 file was deleted; system.remove_orphan_files removes what it left"
            ),
        ));
    }
    Ok(reloaded)
}

/// Carry the previous snapshot's Puffin statistics onto the snapshot
/// `committed` made (a rewrite: the NDVs stay exact). Best effort, like the
/// carry itself.
///
/// The sidecars are read and written through a fresh `loadTable`'s
/// `FileIO`: the table a REST commit returns has no vended credential, and
/// `loaded`'s may have expired during a long rewrite. `loaded` is the
/// fallback when the reload fails.
pub async fn carry_rewrite_statistics(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    loaded: &IcebergTable,
    committed: &IcebergTable,
    metrics: Option<&sqe_metrics::MetricsRegistry>,
) {
    let reloaded = load_table(catalog, ident).await.ok();
    let io = reloaded.as_ref().unwrap_or(loaded).file_io();
    crate::puffin_carry::carry_forward_statistics(
        catalog.as_ref(),
        ident,
        committed,
        io,
        crate::puffin_carry::CarryOp::Rewrite,
        None,
        metrics,
    )
    .await;
}

/// Collect the three inputs [`crate::table_health::analyze_table_health`]
/// needs for one table, in the exact sequence `table_health()` (the `CALL
/// system.table_health` handler, above) uses: live data files, live delete
/// files, and the delete-aware read plan's per-path task map. Consolidated
/// into one seam so the advisory scheduler (`maintenance_scheduler.rs`)
/// promotes a single `pub(crate)` function instead of reaching into three
/// private collectors plus the private `DeleteAwareReadPlan` struct.
pub async fn collect_health_inputs(
    table: &IcebergTable,
) -> sqe_core::Result<(
    Vec<DataFile>,
    Vec<DataFile>,
    std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>>,
)> {
    let data_files = crate::writer::collect_live_data_files(table).await?;
    let delete_files = sqe_compaction::collect_live_delete_files(table).await?;
    let read_plan = sqe_compaction::plan_delete_aware_read(table).await?;
    Ok((data_files, delete_files, read_plan.tasks_by_path))
}

/// Manifests in `table`'s current snapshot, data and delete manifests
/// together; 0 for a table with no snapshot. Reads the manifest list
/// (through the table's object cache), not the snapshot summary, which
/// carries no manifest count.
pub async fn current_manifest_count(table: &IcebergTable) -> sqe_core::Result<i64> {
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else {
        return Ok(0);
    };
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("failed to load manifest list: {e}")))?;
    Ok(manifest_list.entries().len() as i64)
}

/// Table property prefix a compaction job stamps onto the snapshot it
/// commits (`sqe.maintenance.job-id`, `sqe.maintenance.principal`,
/// `sqe.maintenance.trigger`; see `active_one_table`'s `snapshot_properties`
/// in `maintenance_scheduler.rs`). Shared here so
/// [`last_compaction_snapshot_ms`] and the stamping call site can never
/// disagree about which prefix marks a "this snapshot was a compaction job"
/// snapshot.
pub const MAINTENANCE_SNAPSHOT_PROPERTY_PREFIX: &str = "sqe.maintenance.";

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
pub fn last_compaction_snapshot_ms(table: &IcebergTable) -> Option<i64> {
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
pub async fn move_main_ref(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    snapshot_id: i64,
    require_ancestor: bool,
    op_name: &'static str,
) -> sqe_core::Result<Vec<RecordBatch>> {
    use iceberg::spec::MAIN_BRANCH;

    let table = load_table(catalog, ident).await?;

    let before = table.metadata().current_snapshot_id().unwrap_or(-1);

    // Target must exist in the table's snapshot history.
    if table.metadata().snapshot_by_id(snapshot_id).is_none() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
            "CALL system.{op_name}: snapshot id {snapshot_id} not found in table '{ident}' history"
        ),
        ));
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
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!(
                "CALL system.rollback_to_snapshot: snapshot id {snapshot_id} is not an ancestor \
                 of the current snapshot of '{ident}'; use set_current_snapshot to move to an \
                 arbitrary snapshot"
            ),
            ));
        }
    }

    // Repoint `main` at the target. create_branch with the default
    // if_not_exists=false updates the existing ref (if_not_exists=true
    // would silently no-op on an existing branch). This resets main's
    // retention to Branch{None,None,None}, which is the intended default
    // for these operations.
    let tx = Transaction::new(&table);
    let action = tx.create_branch(MAIN_BRANCH).with_snapshot_id(snapshot_id);
    let tx = action.apply(tx).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("{op_name} apply failed: {e}"),
        )
    })?;
    let committed = tx
        .commit(catalog.as_ref())
        .await
        .map_err(|e| classify_commit_error(e, op_name))?;

    // Invalidate the table cache so subsequent SELECTs see the moved
    // pointer immediately instead of a stale pre-move snapshot.
    sqe_catalog::invalidate_rest_catalog_cache_all().await;

    let after = committed.metadata().current_snapshot_id().unwrap_or(-1);

    info!(
        table = %ident,
        before_snapshot = before,
        after_snapshot = after,
        "{op_name}: committed main ref move"
    );

    Ok(vec![summary_batch(
        op_name,
        ident,
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
pub async fn register_table(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    metadata_location: &str,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let table = catalog
        .register_table(ident, metadata_location.to_string())
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
    .map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("register_table summary: {e}"),
        )
    })?;
    Ok(vec![batch])
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
pub async fn table_health(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    cfg: &MaintenanceCompactionConfig,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let table = load_table(catalog, ident).await?;

    let (data_files, delete_files, tasks_by_path) = collect_health_inputs(&table).await?;
    let last_compaction_ms = last_compaction_snapshot_ms(&table);

    let health = crate::table_health::analyze_table_health(
        &data_files,
        &delete_files,
        &tasks_by_path,
        cfg,
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

/// Expire snapshots older than `older_than_ms` (keeping at least
/// `retain_last`), then delete the files only they referenced.
///
/// `table_cache` is invalidated between the commit and the file cleanup, so
/// no cached handle lists snapshots whose files are being deleted.
pub async fn expire_snapshots(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    older_than_ms: Option<i64>,
    retain_last: Option<usize>,
    table_cache: Option<&TableMetadataCache>,
) -> sqe_core::Result<Vec<RecordBatch>> {
    // Branches a crashed coordinator left behind go first, so their
    // snapshots expire in this same call. Best-effort: a lost commit
    // race leaves them for the next call, not this expiry undone.
    let txn_branches = sqe_catalog::txn::drop_stale_branches(
        catalog.as_ref(),
        ident,
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
    let table = load_table(catalog, ident).await?;

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

    let tx_applied = action.apply(tx).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("expire_snapshots apply failed: {e}"),
        )
    })?;

    let committed = tx_applied
        .commit(catalog.as_ref())
        .await
        .map_err(|e| classify_commit_error(e, "expire_snapshots"))?;
    // Before the cleanup: a cached handle would still list the snapshots
    // whose files are about to go.
    if let Some(tc) = table_cache {
        tc.invalidate(&format!("{}.{}", ident.namespace(), ident.name()))
            .await;
    }

    // The old fork's `clear_expire_files(true)` deleted the now-unreachable
    // manifests / data files as part of the action; apache does it as a
    // separate post-commit step. A cleanup failure does not undo the
    // committed expiry, so it is surfaced as an error for the operator.
    // The cleanup deletes through a reload of the committed table: the
    // table a commit returns has no vended credential.
    let committed = reload_committed(catalog, ident, &committed).await?;
    committed
        .cleanup_expired_files(&before_metadata)
        .await
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("expire_snapshots file cleanup failed after commit: {e}"),
            )
        })?;

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
        ident,
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

/// Rewrite the current snapshot's data manifests into as few as the target
/// manifest size allows. Delete manifests are kept.
pub async fn rewrite_manifests(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    metrics: Option<&sqe_metrics::MetricsRegistry>,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let table = load_table(catalog, ident).await?;
    let manifests_before = current_manifest_count(&table).await?;

    let tx = Transaction::new(&table);
    // Without a cluster function the action only applies manual
    // add/delete calls, so the rewrite committed an unchanged manifest
    // list (#815). One key merges every data manifest of a partition
    // spec; the action still seals a manifest at
    // `commit.manifest.target-size-bytes`. Delete manifests are kept.
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| String::new()));
    let tx_applied = action.apply(tx).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("rewrite_manifests apply failed: {e}"),
        )
    })?;

    let committed = tx_applied
        .commit(catalog.as_ref())
        .await
        .map_err(|e| classify_commit_error(e, "rewrite_manifests"))?;

    // Manifest rewrite touches no data: the previous snapshot's Puffin
    // NDV statistics remain exact for the new one (architecture review 512).
    carry_rewrite_statistics(catalog, ident, &table, &committed, metrics).await;

    let manifests_after = current_manifest_count(&committed).await?;

    info!(
        table = %ident,
        manifests_before,
        manifests_after,
        "rewrite_manifests: committed"
    );

    Ok(vec![summary_batch(
        "rewrite_manifests",
        ident,
        manifests_before,
        manifests_after,
        0,
        0,
        "committed".to_string(),
    )?])
}
