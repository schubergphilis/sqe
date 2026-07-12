use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{dml::InsertOp, LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext as DFSessionContext;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestStatus, NestedField, Schema as IcebergSchema,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqlparser::ast::{ObjectName, Statement};
use tracing::{info, warn};
use uuid::Uuid;

use sqe_catalog::puffin_stats::{puffin_stats_enabled, write_puffin_sidecar};
use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::table_properties::{
    resolve_delete_mode, resolve_merge_mode, resolve_update_mode, WriteMode,
};
use sqe_core::{Session, SqeConfig, SqeError};
use tracing::instrument;

use crate::catalog_ops::{catalog_qualifier, fold_unquoted_ident, parse_table_ref};
use crate::write_memory::{TrackedBatchBuffer, WriteReservation};
use crate::writer::{
    new_upload_tracker, resolve_write_compression, write_data_files_streaming_with_metrics,
    write_data_files_with_metrics, write_equality_delete_files, write_position_delete_files,
    FanoutLimits, WriteCleanupGuard,
};

/// Rewrap the source SELECT of an `INSERT INTO t (c1, c2, ...) SELECT ...` so
/// the resulting projection matches the target table's column order.
///
/// The user-supplied column list maps SELECT-output position `i` to target
/// column `columns[i]`. Without this rewrite the writer stamps Iceberg field
/// IDs positionally over the source's Arrow schema, silently swapping
/// matching-type columns. We wrap the SELECT in a subquery, alias each
/// projected column by the explicit target name, then re-project in the
/// target schema's declared order. Columns absent from `columns` are written
/// as NULL.
fn reorder_insert_select(
    select_sql: &str,
    // sqlparser 0.62: INSERT column list is Vec<ObjectName> (was Vec<Ident>).
    // Each entry is a bare column name; use its last identifier part.
    columns: &[sqlparser::ast::ObjectName],
    target_schema: &IcebergSchema,
) -> sqe_core::Result<String> {
    use std::collections::HashSet;

    let column_value = |c: &sqlparser::ast::ObjectName| -> String {
        c.0.last()
            .and_then(|p| p.as_ident())
            .map(|id| id.value.clone())
            .unwrap_or_default()
    };

    let target_fields = target_schema.as_struct().fields();
    let target_names: Vec<&str> = target_fields.iter().map(|f| f.name.as_str()).collect();

    if columns.len() > target_names.len() {
        return Err(SqeError::Execution(format!(
            "INSERT column list has {} entries but target table has {} columns",
            columns.len(),
            target_names.len(),
        )));
    }

    let mut seen: HashSet<String> = HashSet::new();
    let provided_lower: Vec<String> = columns
        .iter()
        .map(|c| column_value(c).to_ascii_lowercase())
        .collect();
    for name in &provided_lower {
        if !seen.insert(name.clone()) {
            return Err(SqeError::Execution(format!(
                "INSERT column list contains duplicate column '{name}'"
            )));
        }
        if !target_names.iter().any(|t| t.eq_ignore_ascii_case(name)) {
            return Err(SqeError::Execution(format!(
                "INSERT column '{name}' does not exist in target table"
            )));
        }
    }

    // The source SELECT yields N columns. Position i in that result is
    // the value destined for `columns[i]`. Alias each output by that target
    // name in a CTE, then project the full target schema with NULLs for
    // unprovided columns.
    let alias_list: Vec<String> = columns
        .iter()
        .map(|c| quote_ident(&column_value(c)))
        .collect();
    let alias_csv = alias_list.join(", ");

    let projection: Vec<String> = target_names
        .iter()
        .map(|target| {
            let lower = target.to_ascii_lowercase();
            if provided_lower.iter().any(|p| p == &lower) {
                quote_ident(target)
            } else {
                format!("NULL AS {}", quote_ident(target))
            }
        })
        .collect();

    Ok(format!(
        "SELECT {} FROM ({}) AS __sqe_insert_src({})",
        projection.join(", "),
        select_sql,
        alias_csv,
    ))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a single-row RecordBatch reporting affected row count.
/// Matches Trino's DML response which returns the update count.
fn affected_rows_batch(count: usize) -> Vec<RecordBatch> {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "rows_affected",
        DataType::Int64,
        false,
    )]));
    match RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![count as i64]))]) {
        Ok(batch) => vec![batch],
        Err(_) => vec![],
    }
}

/// Run the MERGE cardinality check against the registered target/source
/// scratch tables. Errors when a single target row is matched by more than one
/// source row (Spark / Trino / Athena all reject this rather than silently
/// duplicating the target row). Shared by the copy-on-write and merge-on-read
/// merge paths; the caller gates on `merge_cardinality_check` and on the
/// statement actually having a matched clause (source-only / by-source rows
/// cannot violate cardinality).
async fn check_merge_cardinality(
    ctx: &DFSessionContext,
    pair_count_sql: &str,
    matched_count_sql: &str,
) -> sqe_core::Result<()> {
    async fn scalar_count(ctx: &DFSessionContext, sql: &str) -> sqe_core::Result<i64> {
        let batches = ctx
            .sql(sql)
            .await
            .map_err(|e| {
                SqeError::Execution(format!("MERGE cardinality check failed to plan: {e}"))
            })?
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("MERGE cardinality check failed: {e}")))?;
        Ok(batches
            .first()
            .and_then(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
            })
            .map(|a| a.value(0))
            .unwrap_or(0))
    }

    let pair_count = scalar_count(ctx, pair_count_sql).await?;
    let matched_count = scalar_count(ctx, matched_count_sql).await?;
    if pair_count > matched_count {
        return Err(SqeError::Execution(
            "One MERGE target table row matched more than one source row \
             (cardinality violation). Deduplicate the source rows or narrow the \
             ON condition; set query.merge_cardinality_check = false to disable \
             this check."
                .to_string(),
        ));
    }
    Ok(())
}

/// Wrap a source SELECT `LogicalPlan` in a synthetic `LogicalPlan::Dml::Insert`
/// targeting the given (catalog, namespace, table) tuple, so the OpenLineage
/// extractor can produce both inputs (via the source's TableScans) and outputs
/// (via the wrapper's table_name) plus column lineage.
///
/// The write path does not normally plan the full INSERT statement through
/// DataFusion: it parses the SELECT, plans that, then streams batches to a
/// Parquet writer and commits via Iceberg. The lineage extractor needs a
/// write-shaped plan (`LogicalPlan::Dml`) to recognise an output dataset and
/// fan out column lineage. We synthesise that wrapper here purely for lineage
/// capture; the wrapper plan is never executed.
///
/// The target schema is derived from `source.schema()` so column-name
/// alignment matches what the writer will persist (DataFusion's planner has
/// already projected source columns to the target shape by ordinal — see
/// design spec §5.3).
fn build_lineage_insert_plan(
    source: LogicalPlan,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> sqe_core::Result<LogicalPlan> {
    let arrow_schema: ArrowSchema = source.schema().as_arrow().clone();
    let target_mem = MemTable::try_new(Arc::new(arrow_schema), vec![vec![]])
        .map_err(|e| SqeError::Execution(format!("Failed to build lineage target stub: {e}")))?;
    let target_provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(target_mem);
    let target_ref = TableReference::full(
        catalog.to_string(),
        namespace.to_string(),
        table.to_string(),
    );
    LogicalPlanBuilder::insert_into(
        source,
        target_ref,
        provider_as_source(target_provider),
        InsertOp::Append,
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build lineage INSERT wrapper: {e}")))?
    .build()
    .map_err(|e| SqeError::Execution(format!("Failed to build lineage plan: {e}")))
}

/// Extract the target table `ObjectName` from a `Statement::Delete`. Centralises
/// the FromTable + TableFactor matching so a future sqlparser variant change only
/// needs to be patched once.
fn delete_target_object_name(stmt: &Statement) -> sqe_core::Result<&ObjectName> {
    let delete = match stmt {
        Statement::Delete(d) => d,
        other => {
            return Err(SqeError::Execution(format!(
                "Expected DELETE statement, got: {other}"
            )));
        }
    };
    let tables = match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables) => tables,
        sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };
    let first = tables
        .first()
        .ok_or_else(|| SqeError::Execution("DELETE statement has no target table".to_string()))?;
    match &first.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name),
        other => Err(SqeError::Execution(format!(
            "Expected table name in DELETE, got: {other}"
        ))),
    }
}

/// Resolve the target `TableIdent` for a `Statement::Delete`. Combines the
/// `ObjectName` extraction with `parse_table_ref`; five DELETE handlers used
/// to inline this prologue.
fn resolve_delete_target_ident(
    stmt: &Statement,
    session: &Session,
) -> sqe_core::Result<TableIdent> {
    resolve_table_ident(delete_target_object_name(stmt)?, session)
}

/// Best-effort variant for the dispatcher: returns `None` instead of an error
/// so the dispatcher can fall through to a slower path that surfaces the same
/// error in context.
fn try_resolve_delete_target_ident(stmt: &Statement, session: &Session) -> Option<TableIdent> {
    delete_target_object_name(stmt)
        .ok()
        .and_then(|name| resolve_table_ident(name, session).ok())
}

/// Extract the target table `ObjectName` from a `Statement::Update`.
fn update_target_object_name(stmt: &Statement) -> sqe_core::Result<&ObjectName> {
    let table_factor = match stmt {
        Statement::Update(update) => &update.table,
        other => {
            return Err(SqeError::Execution(format!(
                "Expected UPDATE statement, got: {other}"
            )));
        }
    };
    match &table_factor.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name),
        other => Err(SqeError::Execution(format!(
            "Expected table name in UPDATE, got: {other}"
        ))),
    }
}

/// Resolve the target `TableIdent` for a `Statement::Update`. Mirrors
/// `resolve_delete_target_ident` so callers stay symmetric.
fn resolve_update_target_ident(
    stmt: &Statement,
    session: &Session,
) -> sqe_core::Result<TableIdent> {
    resolve_table_ident(update_target_object_name(stmt)?, session)
}

/// Best-effort variant for the dispatcher.
fn try_resolve_update_target_ident(stmt: &Statement, session: &Session) -> Option<TableIdent> {
    update_target_object_name(stmt)
        .ok()
        .and_then(|name| resolve_table_ident(name, session).ok())
}

/// Resolve the (catalog, namespace, table) tuple used to label a write target
/// in OL events. SQE's catalog name is whatever `resolve_default_catalog()`
/// returns; the OL `catalog_lookup` falls back to `sqe://<name>` when the
/// operator hasn't configured an explicit URL for it.
fn lineage_target_parts(config: &SqeConfig, table_ident: &TableIdent) -> (String, String, String) {
    let catalog = config.resolve_default_catalog();
    // `NamespaceIdent::Display` joins parts with a literal dot, which matches
    // how the OL extractor parses `<catalog>.<schema>.<table>` (it splits on
    // dots in `parse_table_ref`). `to_url_string` uses the unit separator
    // and would be misread as a single-segment name.
    let namespace = table_ident.namespace().to_string();
    let table = table_ident.name().to_string();
    (catalog, namespace, table)
}

/// Build an Arrow schema from the Iceberg table's current schema. Used
/// to drive the policy probe that resolves row filters and column masks
/// for the DELETE / UPDATE WHERE evaluator.
fn build_arrow_schema_for_table(table: &IcebergTable) -> sqe_core::Result<Arc<ArrowSchema>> {
    let iceberg_schema = table.metadata().current_schema();
    let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
        .map_err(|e| SqeError::Execution(format!("Iceberg -> Arrow schema: {e}")))?;
    Ok(Arc::new(arrow_schema))
}

/// Resolve a table reference to a `TableIdent`, honoring the session schema
/// (X-Trino-Schema) for an unqualified 1-part name.
///
/// `parse_table_ref` defaults an unqualified name to the `default` namespace.
/// The read path and Trino instead resolve `t` against the session's default
/// schema, so a `CREATE TABLE new (LIKE nation)` issued with
/// `X-Trino-Schema: tpch_demo` must look for `tpch_demo.nation`, not
/// `default.nation`. A 2- or 3-part name already names its namespace and is
/// returned unchanged.
fn resolve_table_ident(name: &ObjectName, session: &Session) -> sqe_core::Result<TableIdent> {
    let ident = parse_table_ref(name)?;
    let is_unqualified = name.0.iter().filter_map(|p| p.as_ident()).count() == 1;
    if is_unqualified {
        if let Some(schema) = session.default_schema.as_deref().filter(|s| !s.is_empty()) {
            return Ok(TableIdent::new(
                NamespaceIdent::new(schema.to_string()),
                ident.name().to_string(),
            ));
        }
    }
    Ok(ident)
}

/// Alias used inside the policy subquery to expose the unmasked value of a
/// masked column. UPDATE's CASE-ELSE branch must reference this so unchanged
/// rows persist their pre-mask values instead of writing the masked value
/// to disk.
fn policy_raw_alias(col: &str) -> String {
    format!("__pol_raw_{col}")
}

/// Combine the user's WHERE clause with the policy row filter. ANDing it
/// here ensures DELETE and UPDATE only act on rows the policy admits,
/// matching PostgreSQL RLS semantics on the SELECT path.
fn augment_where_with_row_filter(
    where_sql: &str,
    predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
) -> String {
    match &predicates.row_filter_sql {
        Some(rf) => format!("({where_sql}) AND ({rf})"),
        None => where_sql.to_string(),
    }
}

/// Build the FROM clause for the per-batch evaluator. With no policy this
/// is the scratch table aliased to the target name. With column masks it
/// wraps the scratch table in a subquery that projects each column either
/// raw or masked, plus a `__pol_raw_<col>` raw alias for masked columns
/// when `expose_raw` is set (the UPDATE path needs this for the CASE-ELSE
/// branch).
fn build_policy_from_clause(
    scratch_table: &str,
    orig_alias: &str,
    schema: &ArrowSchema,
    predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    expose_raw: bool,
) -> String {
    if predicates.column_mask_sqls.is_empty() {
        return format!("{scratch_table} AS \"{orig_alias}\"");
    }

    let mut projections: Vec<String> = Vec::with_capacity(schema.fields().len() + 1);
    for f in schema.fields() {
        let name = f.name();
        match predicates.column_mask_sqls.get(name) {
            Some(mask_sql) => {
                projections.push(format!("({mask_sql}) AS \"{name}\""));
                if expose_raw {
                    let raw = policy_raw_alias(name);
                    projections.push(format!("\"{name}\" AS \"{raw}\""));
                }
            }
            None => {
                projections.push(format!("\"{name}\""));
            }
        }
    }
    let projection_sql = projections.join(", ");
    format!("(SELECT {projection_sql} FROM {scratch_table}) AS \"{orig_alias}\"")
}

/// Best-effort cleanup of a partially-created CTAS table.
///
/// Both CTAS paths (`handle_ctas`, `handle_ctas_streaming`) follow the same
/// three-step pattern: (1) `catalog.create_table()` registers in Polaris and
/// writes `metadata/00000-*.json`, (2) the write loop streams parquet to the
/// table location, (3) a fast-append transaction commits the new snapshot. If
/// step 2 or step 3 fails the catalog still has the registered (snapshot-less)
/// entry and the S3 prefix is populated with one or both of:
///
///   - `metadata/00000-*.json` from step 1
///   - `data/*.parquet` from step 2
///
/// Without rollback, the next CTAS targeting the same logical name hits
/// Polaris's location-conflict check (the `metadata.json` already there) and
/// returns 403 forever — until an operator runs `rm -rf` on S3 or
/// `system.purge_orphan_locations` (when that lands). The morning of
/// 2026-05-11 we observed this loop three times in a row across one dbt
/// session.
///
/// This helper undoes step 1 + step 2: delete the S3 prefix, then drop the
/// catalog entry. Both calls are best-effort and only warn on failure — the
/// original error from the CTAS is what the caller propagates.
async fn rollback_ctas_partial_create(
    catalog: &Arc<dyn Catalog>,
    table: &IcebergTable,
    table_ident: &TableIdent,
) {
    let location = table.metadata().location();
    let file_io = table.file_io();
    match file_io.delete_prefix(location).await {
        Ok(_) => info!(
            table = %table_ident,
            location,
            "CTAS rollback: deleted S3 prefix"
        ),
        Err(e) => warn!(
            table = %table_ident,
            location,
            error = %e,
            "CTAS rollback: failed to delete S3 prefix — orphan data may remain"
        ),
    }
    match catalog.drop_table(table_ident).await {
        Ok(_) => info!(
            table = %table_ident,
            "CTAS rollback: dropped catalog entry"
        ),
        Err(e) => warn!(
            table = %table_ident,
            error = %e,
            "CTAS rollback: failed to drop catalog entry — orphan registration may remain"
        ),
    }
}

/// Bounded retry around an Iceberg commit that may lose an optimistic-concurrency
/// race against another writer.
///
/// iceberg-rust's vendored catalogs (SQL, in-memory, our REST bridge) tag conflict
/// errors with `retryable = true`. Polaris and HMS surface the same condition as
/// `CatalogCommitConflicts`. The fix for #47 wraps every commit site that builds
/// a Transaction so a conflict reloads the table and rebuilds the action against
/// the new base snapshot. Without the reload the next attempt commits over the
/// same stale metadata-location and the conflict repeats.
///
/// `build_and_commit` runs once per attempt; it receives a freshly-loaded
/// IcebergTable and is expected to build a `Transaction`, apply its action, and
/// call `tx.commit(catalog).await`. The closure clones data files / delete files
/// it captures so each attempt operates on its own action graph.
///
/// Backoff: exponential with jitter, capped at ~1s base. After `max_attempts`
/// the last error propagates unchanged so the caller's error-mapping still runs.
async fn commit_with_retry<F, Fut>(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    op: &str,
    mut build_and_commit: F,
) -> std::result::Result<IcebergTable, iceberg::Error>
where
    F: FnMut(IcebergTable) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<IcebergTable, iceberg::Error>>,
{
    let _commit_span = tracing::info_span!("sqe.write_commit", op = %op, table = %table_ident.name);

    const MAX_ATTEMPTS: u32 = 4;
    let mut last_err: Option<iceberg::Error> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let table = catalog.load_table(table_ident).await?;
        match build_and_commit(table).await {
            Ok(t) => {
                if attempt > 1 {
                    info!(
                        table = %table_ident,
                        op,
                        attempts = attempt,
                        "commit succeeded after retry"
                    );
                }
                return Ok(t);
            }
            Err(e) => {
                let retryable = e.retryable() || is_conflict_message(&e.to_string());
                if !retryable || attempt == MAX_ATTEMPTS {
                    last_err = Some(e);
                    break;
                }
                let base_ms: u64 = 50_u64.saturating_mul(1_u64 << (attempt - 1));
                let jitter_ms: u64 = (Uuid::new_v4().as_u128() as u64) % 50;
                let sleep_ms = base_ms.saturating_add(jitter_ms).min(1000);
                warn!(
                    table = %table_ident,
                    op,
                    attempt,
                    backoff_ms = sleep_ms,
                    error = %e,
                    "commit conflict; reloading table and retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "commit_with_retry exhausted attempts without capturing an error",
        )
    }))
}

/// Backoff (exponential + jitter, capped ~1s) for retrying a CoW UPDATE/DELETE
/// after an optimistic-concurrency conflict. Mirrors `commit_with_retry`'s
/// schedule. CoW cannot use `commit_with_retry` directly because the rewritten
/// data files are snapshot-specific: on conflict the whole rewrite must re-run
/// against the reloaded snapshot, not just re-commit stale files.
fn cow_conflict_backoff_ms(attempt: u32) -> u64 {
    let base = 50_u64.saturating_mul(1_u64 << attempt.saturating_sub(1));
    let jitter = (Uuid::new_v4().as_u128() as u64) % 50;
    base.saturating_add(jitter).min(1000)
}

/// Max attempts for a CoW UPDATE/DELETE that loses an optimistic-concurrency race.
const COW_MAX_ATTEMPTS: u32 = 4;

fn is_conflict_message(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("commitconflict")
        || lower.contains("commit conflict")
        || lower.contains("stale snapshot")
        || lower.contains("rowdelta conflict")
        || lower.contains("retryable")
}

/// Build a unique table location under the namespace base by appending a UUID
/// suffix to the table name.
///
/// Why: Polaris (and the Iceberg REST spec generally) refuse to create a table
/// at a location that already belongs to another registered table or namespace.
/// Without a UUID, every CTAS of `foo__dbt_tmp` lands at the deterministic
/// `<warehouse>/<ns>/foo__dbt_tmp/`. dbt-trino's swap pattern then renames
/// `foo__dbt_tmp` to `foo` — Iceberg renames are metadata-only, so the data
/// stays at the `__dbt_tmp/` prefix and the slot is permanently claimed. The
/// next `dbt run` hits 403. With a UUID, each CTAS gets its own slot
/// (`foo__dbt_tmp-<uuid>/`) and the collision can't happen.
///
/// Returns `None` (so the catalog falls back to its default location) when:
///   - the namespace lookup fails, or
///   - the namespace has no `location` property set (e.g. in-memory catalogs
///     used by unit tests). The collision-on-rename problem only bites against
///     Polaris and other REST catalogs that enforce location-uniqueness; the
///     in-memory paths don't care.
async fn unique_table_location(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> Option<String> {
    let ns = catalog.get_namespace(namespace).await.ok()?;
    let base = ns.properties().get("location")?.trim_end_matches('/');
    Some(format!("{base}/{table_name}-{}", Uuid::new_v4()))
}

/// Handles write operations: CTAS (CREATE TABLE AS SELECT) and INSERT INTO SELECT.
///
/// Write handlers receive already-executed RecordBatches from the query pipeline
/// and persist them as Iceberg data files via Parquet, then commit the changes
/// through the Iceberg REST catalog.
/// Snapshot-pinned scan plan for delete-aware DML rewrite reads (#371).
/// Built by [`WriteHandler::plan_delete_aware_read`]; consumed per file by
/// [`WriteHandler::read_data_file_applying_deletes`].
struct DeleteAwareReadPlan {
    scan: iceberg::scan::TableScan,
    tasks_by_path: std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>>,
}

impl DeleteAwareReadPlan {
    /// True when any planned data file carries delete files. The streaming
    /// MERGE target path reads raw parquet and must downgrade to the
    /// buffered (delete-applying) path when deletes exist.
    fn has_deletes(&self) -> bool {
        self.tasks_by_path
            .values()
            .flatten()
            .any(|t| !t.deletes.is_empty())
    }
}

pub struct WriteHandler {
    config: SqeConfig,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// Shared global table metadata cache. Used so write-path SessionCatalog
    /// instances hit the warm cache and invalidate the right entry on commit.
    table_cache: Option<TableMetadataCache>,
    /// Policy enforcer applied to every source SELECT of an INSERT, CTAS,
    /// DELETE, or UPDATE. Without this the SELECT path would enforce row
    /// filters and column masks but the write path would let masked or
    /// filtered-out rows be copied verbatim into a sink table.
    policy_enforcer: Option<Arc<dyn sqe_policy::PolicyEnforcer>>,
}

impl WriteHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self {
            config,
            metrics: None,
            table_cache: None,
            policy_enforcer: None,
        }
    }

    #[must_use = "with_metrics consumes self; bind the returned handler"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach a global table metadata cache shared across all sessions.
    #[must_use = "with_table_cache consumes self; bind the returned handler"]
    pub fn with_table_cache(mut self, cache: TableMetadataCache) -> Self {
        self.table_cache = Some(cache);
        self
    }

    /// Attach the policy enforcer used for write-path source SELECTs.
    #[must_use = "with_policy_enforcer consumes self; bind the returned handler"]
    pub fn with_policy_enforcer(mut self, enforcer: Arc<dyn sqe_policy::PolicyEnforcer>) -> Self {
        self.policy_enforcer = Some(enforcer);
        self
    }

    /// Run the source plan through the configured policy enforcer, if any.
    /// Used by INSERT / CTAS source SELECTs so row filters, column masks,
    /// and column restrictions apply on writes too.
    async fn enforce_source_plan(
        &self,
        session: &Session,
        plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, sqe_policy::PolicySummary)> {
        match &self.policy_enforcer {
            Some(enf) => enf.evaluate(&session.user, plan).await,
            None => Ok((plan, sqe_policy::PolicySummary::default())),
        }
    }

    /// Resolve write-path policy predicates for the target of a DELETE / UPDATE.
    /// Returns row-filter SQL and per-column mask SQL fragments derived from
    /// the configured PolicyEnforcer. With no enforcer the result is empty
    /// and the DML path is unchanged.
    async fn compute_write_predicates(
        &self,
        session: &Session,
        table_ident: &TableIdent,
        schema: Arc<ArrowSchema>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<sqe_policy::write_predicates::WritePolicyPredicates> {
        let Some(enf) = &self.policy_enforcer else {
            return Ok(sqe_policy::write_predicates::WritePolicyPredicates::default());
        };
        // The rewriter keys policy lookups by the last namespace component
        // and the table name. Flatten the multi-level namespace to match.
        let namespace_key = table_ident
            .namespace()
            .as_ref()
            .last()
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        let (predicates, summary) = sqe_policy::write_predicates::extract_write_predicates(
            enf.as_ref(),
            &session.user,
            &namespace_key,
            table_ident.name(),
            schema,
        )
        .await?;
        *policy_summary_out = Some(summary);
        Ok(predicates)
    }

    /// Return the Parquet compression codec for this write.
    ///
    /// A per-session `iceberg.compression_codec` (Trino session property, #353)
    /// takes precedence; otherwise the static `catalog.parquet_compression`
    /// config default is used. Unknown values fall back to ZSTD (with a warning)
    /// via `parse_parquet_compression`, matching the config path's leniency.
    fn compression(&self, session: &Session) -> parquet::basic::Compression {
        resolve_write_compression(
            session.compression_codec.as_deref(),
            &self.config.catalog.parquet_compression,
        )
    }

    /// Resolve fanout limits for a partitioned streaming write from config.
    ///
    /// Both knobs default to 0 → [`FanoutLimits::unbounded`], the vendored
    /// `TaskWriter` path (byte-for-byte unchanged). Setting
    /// `fanout_max_open_writers` or `fanout_buffer_budget` opts into the bounded
    /// writer; once bounded mode is active, a knob left at 0 is auto-derived
    /// from the coordinator memory pool ([`auto_fanout_caps`]) so the operator
    /// need only set one. `write_buffer_tracking = false` forces unbounded. A
    /// malformed `fanout_buffer_budget` string warns and disables just the
    /// byte budget rather than failing the write.
    ///
    /// Note: both-0 stays unbounded on purpose. Making auto-derivation apply
    /// with no knob set (spec open decision #4 "both-0 = bounded") would route
    /// every partitioned write through the bounded writer; that default flip is
    /// held for a stack-validated follow-up.
    fn fanout_limits(&self) -> FanoutLimits {
        if !self.config.query.write_buffer_tracking {
            return FanoutLimits::unbounded();
        }
        let cfg_max = self.config.query.fanout_max_open_writers;
        let cfg_budget =
            match sqe_core::config::parse_memory_limit(&self.config.query.fanout_buffer_budget) {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        error = %e,
                        value = %self.config.query.fanout_buffer_budget,
                        "invalid fanout_buffer_budget; disabling fanout byte budget"
                    );
                    0
                }
            };

        // Both unset → unbounded, unchanged default path.
        if cfg_max == 0 && cfg_budget == 0 {
            return FanoutLimits::unbounded();
        }

        // Bounded mode is active; auto-derive whichever knob is still 0 from the
        // coordinator pool size so the operator only has to set one.
        let pool_bytes =
            sqe_core::config::parse_memory_limit(&self.config.coordinator.memory_limit)
                .unwrap_or(8 * 1024 * 1024 * 1024);
        let (auto_max, auto_budget) = crate::writer::auto_fanout_caps(pool_bytes);
        FanoutLimits {
            max_open: if cfg_max > 0 { cfg_max } else { auto_max },
            byte_budget: if cfg_budget > 0 {
                cfg_budget
            } else {
                auto_budget
            },
        }
    }

    /// Best-effort capture of a self-referential write plan (DELETE / UPDATE
    /// against a single target). Plans `SELECT * FROM <target>`, wraps it in a
    /// synthetic INSERT to the same target, and stores the wrapper in
    /// `plan_out`. The wrapper is not executed; it exists purely so the OL
    /// extractor can recognise inputs (the target's TableScan), the output
    /// dataset, and identity column lineage between them.
    ///
    /// On any planning error this becomes a no-op: lineage capture must not
    /// fail the write.
    async fn capture_self_lineage(
        &self,
        ctx: &DFSessionContext,
        table_ident: &TableIdent,
        table_factor_name: &sqlparser::ast::ObjectName,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
    ) {
        let select_sql = format!("SELECT * FROM {table_factor_name}");
        let df = match ctx.sql(&select_sql).await {
            Ok(df) => df,
            Err(e) => {
                tracing::debug!(
                    table = %table_ident,
                    error = %e,
                    "lineage capture: failed to plan self-scan; skipping"
                );
                return;
            }
        };
        let source_plan = df.logical_plan().clone();
        let (lin_catalog, lin_namespace, lin_table) =
            lineage_target_parts(&self.config, table_ident);
        match build_lineage_insert_plan(source_plan, &lin_catalog, &lin_namespace, &lin_table) {
            Ok(wrapper) => {
                *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
            }
            Err(e) => {
                tracing::debug!(
                    table = %table_ident,
                    error = %e,
                    "lineage capture: failed to build INSERT wrapper; skipping"
                );
            }
        }
    }

    /// Emit a Puffin NDV sidecar for the most recent snapshot, if opted in.
    ///
    /// Runs after a successful append-style commit. The caller reloads the
    /// table to get the new snapshot; we then build theta sketches from the
    /// just-written batches and register the sidecar via
    /// [`UpdateStatisticsAction`]. A failure here is logged and swallowed:
    /// the data commit has already succeeded, so losing statistics is not
    /// worth rolling back for.
    async fn maybe_emit_puffin_sidecar(
        &self,
        catalog: &Arc<dyn Catalog>,
        table_ident: &TableIdent,
        batches_for_stats: &[RecordBatch],
    ) {
        // Reload the table post-commit so we see the new snapshot id and
        // sequence number.
        let table = match catalog.load_table(table_ident).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "puffin: could not reload table for sidecar");
                return;
            }
        };
        if !puffin_stats_enabled(table.metadata().properties()) {
            return;
        }
        let Some(snapshot_id) = table.metadata().current_snapshot_id() else {
            // No snapshot yet — nothing to attach the sidecar to.
            return;
        };
        let sequence_number = table.metadata().last_sequence_number();

        let metadata_location = match table.metadata_location() {
            Some(p) => p,
            None => {
                tracing::warn!("puffin: no metadata_location on loaded table");
                return;
            }
        };
        let base_dir = metadata_location
            .rsplit_once('/')
            .map(|(head, _tail)| head)
            .unwrap_or(metadata_location)
            .to_string();

        let stats_file = match write_puffin_sidecar(
            table.file_io(),
            &base_dir,
            table.metadata().current_schema(),
            batches_for_stats,
            snapshot_id,
            sequence_number,
        )
        .await
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "puffin: sidecar write failed");
                return;
            }
        };

        let staged_path = stats_file.statistics_path.clone();
        let file_io = table.file_io().clone();
        let cleanup_staged = |reason: &str, err: &dyn std::fmt::Display| {
            let path = staged_path.clone();
            let io = file_io.clone();
            tracing::warn!(error = %err, path = %path, reason = reason, "puffin: deleting orphan sidecar");
            tokio::spawn(async move {
                if let Err(del_err) = io.delete(&path).await {
                    tracing::warn!(
                        error = %del_err,
                        path = %path,
                        "puffin: orphan sidecar delete failed; will require manual cleanup"
                    );
                }
            });
        };

        let table = match catalog.load_table(table_ident).await {
            Ok(t) => t,
            Err(e) => {
                cleanup_staged("reload_for_stats_tx_failed", &e);
                return;
            }
        };
        let tx = Transaction::new(&table);
        let action = tx.update_statistics().set_statistics(stats_file);
        let tx = match action.apply(tx) {
            Ok(t) => t,
            Err(e) => {
                cleanup_staged("apply_update_statistics_failed", &e);
                return;
            }
        };
        if let Err(e) = tx.commit(catalog.as_ref()).await {
            cleanup_staged("commit_update_statistics_failed", &e);
        }
    }

    /// Handle CREATE TABLE [OR REPLACE] ns.table AS SELECT ...
    ///
    /// The caller has already executed the inner SELECT and provides the result
    /// batches. This method:
    /// 1. Extracts the table name from the CTAS statement
    /// 2. Converts the Arrow schema to an Iceberg schema
    /// 3. Creates the table in the catalog
    /// 4. Writes RecordBatches as Parquet data files
    /// 5. Commits the data files via a fast-append transaction
    #[instrument(skip(self, session, stmt, batches), fields(username = %session.user.username))]
    pub async fn handle_ctas(
        &self,
        session: &Session,
        stmt: &Statement,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (table_name, _or_replace, arrow_schema, partition_by, user_options) = match stmt {
            Statement::CreateTable(ct) => {
                if ct.query.is_none() {
                    return Err(SqeError::Execution(
                        "CTAS statement has no SELECT query".into(),
                    ));
                }

                // Get the Arrow schema from the first batch. The caller guarantees
                // at least one batch is present (possibly with 0 rows).
                let schema = if let Some(batch) = batches.first() {
                    batch.schema()
                } else {
                    return Err(SqeError::Execution(
                        "CTAS query returned no batches — cannot infer schema".into(),
                    ));
                };

                (
                    &ct.name,
                    ct.or_replace,
                    schema,
                    ct.partition_by.as_deref(),
                    create_table_options_as_slice(&ct.table_options),
                )
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;
        let namespace = table_ident.namespace().clone();
        let name = table_ident.name().to_string();

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Executing CTAS"
        );

        // Convert Arrow schema to Iceberg schema
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;

        // Create the catalog bridge for this session, resolving the target
        // catalog qualifier (`catalog.ns.table`) rather than the default warehouse.
        let target_catalog = resolve_target_catalog(catalog_qualifier(table_name), session);
        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

        // Create the table in the catalog. Honor PARTITIONED BY (see the
        // streaming CTAS path for why).
        let partition_spec = build_partition_spec(partition_by, &iceberg_schema)?;
        let create_format_version = self.format_version();
        let location = unique_table_location(catalog.as_ref(), &namespace, &name).await;
        // Merge user-specified TBLPROPERTIES / WITH options so a CTAS keeps
        // the properties the user typed (e.g. write.parquet.bloom-filter-*,
        // write.*.mode). Without this a CTAS silently drops every property,
        // unlike the plain CREATE TABLE path. The properties must be set at
        // creation time because they drive the data write that follows.
        let mut props = self.format_version_props(create_format_version);
        merge_user_table_properties(&mut props, user_options);
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .location_opt(location)
            .format_version(create_format_version)
            .partition_spec_opt(partition_spec)
            .properties(props)
            .build();

        let _created_table = catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        // Load the table back (needed for the writer infrastructure which reads
        // table metadata for location generation, file IO, etc.)
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load created table: {e}")))?;

        // Everything after `catalog.create_table` is wrapped so a failure in
        // the write or commit triggers a rollback of the partially-created
        // table (drop catalog entry + delete S3 prefix). Without this the
        // next CTAS at the same target gets a 403 from Polaris's
        // location-uniqueness check.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "ctas")
                .with_metrics(self.metrics.clone());
        let post_create: sqe_core::Result<()> = async {
            if total_rows > 0 {
                // Clone batches cheaply for the Puffin sidecar when the table
                // has opted in. RecordBatch clones are Arc bumps, not data
                // copies.
                let stats_snapshot: Option<Vec<RecordBatch>> =
                    puffin_stats_enabled(table.metadata().properties()).then(|| batches.clone());

                let data_files = write_data_files_with_metrics(
                    &table,
                    batches,
                    "ctas",
                    self.metrics.as_ref(),
                    self.compression(session),
                    tracker,
                )
                .await?;

                if !data_files.is_empty() {
                    let files_for_retry = data_files.clone();
                    let catalog_for_commit = catalog.clone();
                    commit_with_retry(catalog.as_ref(), &table_ident, "ctas", move |fresh_table| {
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
                    .map_err(|e| {
                        SqeError::Execution(format!("Failed to commit CTAS transaction: {e}"))
                    })?;
                    cleanup_guard.mark_committed();

                    if let Some(stats_batches) = stats_snapshot {
                        self.maybe_emit_puffin_sidecar(&catalog, &table_ident, &stats_batches)
                            .await;
                    }
                } else {
                    cleanup_guard.mark_committed();
                }

                info!(
                    table = %table_ident,
                    total_rows,
                    "CTAS committed successfully"
                );
            } else {
                cleanup_guard.mark_committed();
                info!(
                    table = %table_ident,
                    "CTAS created empty table (no data to write)"
                );
            }
            Ok(())
        }
        .await;

        if let Err(ref err) = post_create {
            warn!(
                table = %table_ident,
                error = %err,
                "CTAS failed after create_table; rolling back to prevent orphan"
            );
            rollback_ctas_partial_create(&catalog, &table, &table_ident).await;
        }
        post_create?;

        Ok(vec![]) // DDL success, no result rows
    }

    /// Handle CREATE TABLE [OR REPLACE] ns.table AS SELECT ... — streaming variant.
    ///
    /// Instead of buffering the full SELECT result in memory, this method:
    /// 1. Plans the SELECT via DataFusion and derives the Iceberg schema from the
    ///    DataFrame schema (before execution — no data buffered yet).
    /// 2. Creates the table in the catalog.
    /// 3. Streams batches directly to the Parquet writer via `df.execute_stream()`.
    /// 4. Commits the data files via a fast-append transaction.
    ///
    /// Peak memory is O(batch_size) instead of O(total_rows). Critical for large
    /// CTAS loads (SF1 lineorder, store_sales, etc.) that OOM with `df.collect()`.
    #[instrument(skip(self, session, stmt, ctx, select_sql, plan_out), fields(username = %session.user.username))]
    pub async fn handle_ctas_streaming(
        &self,
        session: &Session,
        stmt: &Statement,
        ctx: &DFSessionContext,
        select_sql: &str,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (table_name, _or_replace, partition_by, user_options) = match stmt {
            Statement::CreateTable(ct) => {
                if ct.query.is_none() {
                    return Err(SqeError::Execution(
                        "CTAS statement has no SELECT query".into(),
                    ));
                }
                (
                    &ct.name,
                    ct.or_replace,
                    ct.partition_by.as_deref(),
                    create_table_options_as_slice(&ct.table_options),
                )
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;
        let namespace = table_ident.namespace().clone();
        let name = table_ident.name().to_string();

        // Plan the SELECT without executing it; gives us the output schema cheaply.
        let df = ctx
            .sql(select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        // Enforce policy on the source SELECT so masks and restrictions
        // shape the target table. Without this, CTAS over a masked table
        // creates a sink with plaintext columns.
        let (enforced_source, policy_summary) = self
            .enforce_source_plan(session, df.logical_plan().clone())
            .await?;
        *policy_summary_out = Some(policy_summary);
        let df = ctx
            .execute_logical_plan(enforced_source)
            .await
            .map_err(|e| SqeError::Execution(format!("Plan execution failed: {e}")))?;

        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            "Executing CTAS (streaming)"
        );

        let target_catalog = resolve_target_catalog(catalog_qualifier(table_name), session);
        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

        // Defense-in-depth: ensure the target namespace exists in this catalog
        // (dbt's CREATE SCHEMA may not have landed here); idempotent.
        ensure_namespace(catalog.as_ref(), &namespace).await?;

        // Honor PARTITIONED BY on CTAS (the plain CREATE TABLE path already
        // does this). Without it, `CREATE TABLE ... PARTITIONED BY (month(d))
        // AS SELECT ...` silently produced an unpartitioned table.
        let partition_spec = build_partition_spec(partition_by, &iceberg_schema)?;

        let create_format_version = self.format_version();
        let location = unique_table_location(catalog.as_ref(), &namespace, &name).await;
        // Merge user TBLPROPERTIES / WITH options (see handle_ctas for why):
        // a CTAS must keep properties like write.parquet.bloom-filter-* that
        // drive the data write, matching the plain CREATE TABLE path.
        let mut props = self.format_version_props(create_format_version);
        merge_user_table_properties(&mut props, user_options);
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .location_opt(location)
            .format_version(create_format_version)
            .partition_spec_opt(partition_spec)
            .properties(props)
            .build();

        let _created_table = catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load created table: {e}")))?;

        // Capture the SELECT plan as a synthetic INSERT-into-target wrapper for
        // OpenLineage extraction. The wrapper is never executed; it lets the
        // extractor walk the source plan for inputs + column lineage and the
        // wrapper for the output dataset.
        let source_plan = df.logical_plan().clone();
        let (lin_catalog, lin_namespace, lin_table) =
            lineage_target_parts(&self.config, &table_ident);
        if let Ok(wrapper) =
            build_lineage_insert_plan(source_plan, &lin_catalog, &lin_namespace, &lin_table)
        {
            *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
        }

        // Stream + commit wrapped so any failure after `catalog.create_table`
        // rolls back the partially-created table. Same rationale as the
        // non-streaming `handle_ctas` path: a failed write or commit leaves
        // the catalog entry plus S3 prefix in place, which Polaris's
        // location-uniqueness check rejects on every retry.
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "ctas-streaming")
                .with_metrics(self.metrics.clone());
        let post_create: sqe_core::Result<()> = async {
            let stream = df.execute_stream().await.map_err(|e| {
                SqeError::Execution(format!("Failed to start execution stream: {e}"))
            })?;

            let (data_files, total_rows) = write_data_files_streaming_with_metrics(
                &table,
                stream,
                "ctas",
                self.metrics.as_ref(),
                self.compression(session),
                tracker,
                self.fanout_limits(),
            )
            .await?;

            if !data_files.is_empty() {
                let files_for_retry = data_files.clone();
                let catalog_for_commit = catalog.clone();
                commit_with_retry(
                    catalog.as_ref(),
                    &table_ident,
                    "ctas-streaming",
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
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to commit CTAS transaction: {e}"))
                })?;
                cleanup_guard.mark_committed();
                info!(
                    table = %table_ident,
                    total_rows,
                    "CTAS committed successfully (streaming)"
                );
            } else {
                cleanup_guard.mark_committed();
                info!(
                    table = %table_ident,
                    "CTAS created empty table (no data to write)"
                );
            }
            Ok(())
        }
        .await;

        if let Err(ref err) = post_create {
            warn!(
                table = %table_ident,
                error = %err,
                "CTAS (streaming) failed after create_table; rolling back to prevent orphan"
            );
            rollback_ctas_partial_create(&catalog, &table, &table_ident).await;
        }
        post_create?;

        Ok(vec![])
    }

    /// Handle INSERT INTO ns.table SELECT ... — streaming variant.
    ///
    /// Streams batches from the SELECT directly to the Parquet writer without
    /// buffering the full result set. Peak memory is O(batch_size).
    #[instrument(skip(self, session, stmt, ctx, select_sql, plan_out), fields(username = %session.user.username))]
    pub async fn handle_insert_streaming(
        &self,
        session: &Session,
        stmt: &Statement,
        ctx: &DFSessionContext,
        select_sql: &str,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (table_name, explicit_columns) = match stmt {
            Statement::Insert(ins) => match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => (name, &ins.columns),
                other => {
                    return Err(SqeError::Execution(format!(
                        "INSERT INTO table functions not supported: {other}"
                    )));
                }
            },
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected Insert statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;

        info!(
            username = %session.user.username,
            table = %table_ident,
            "Executing INSERT INTO SELECT (streaming)"
        );

        let target_catalog = resolve_target_catalog(catalog_qualifier(table_name), session);
        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        let effective_sql = if explicit_columns.is_empty() {
            select_sql.to_string()
        } else {
            reorder_insert_select(
                select_sql,
                explicit_columns,
                table.metadata().current_schema().as_ref(),
            )?
        };

        let df = ctx
            .sql(&effective_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        // Enforce row filters, column masks, and restricted columns on the
        // source SELECT. Without this, INSERT INTO sink SELECT FROM masked
        // would copy plaintext into sink, bypassing every policy.
        let source_plan = df.logical_plan().clone();
        let (enforced_plan, policy_summary) =
            self.enforce_source_plan(session, source_plan).await?;
        *policy_summary_out = Some(policy_summary);

        let (lin_catalog, lin_namespace, lin_table) =
            lineage_target_parts(&self.config, &table_ident);
        if let Ok(wrapper) = build_lineage_insert_plan(
            enforced_plan.clone(),
            &lin_catalog,
            &lin_namespace,
            &lin_table,
        ) {
            *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
        }

        let df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Plan execution failed: {e}")))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to start execution stream: {e}")))?;

        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "insert-streaming")
                .with_metrics(self.metrics.clone());
        let (data_files, total_rows) = write_data_files_streaming_with_metrics(
            &table,
            stream,
            "insert",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
            self.fanout_limits(),
        )
        .await?;

        if total_rows == 0 {
            info!(table = %table_ident, "INSERT SELECT returned no rows — nothing to write");
            cleanup_guard.mark_committed();
            return Ok(vec![]);
        }

        if !data_files.is_empty() {
            let files_for_retry = data_files.clone();
            let catalog_for_commit = catalog.clone();
            commit_with_retry(
                catalog.as_ref(),
                &table_ident,
                "insert",
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
            .map_err(|e| {
                SqeError::Execution(format!("Failed to commit INSERT transaction: {e}"))
            })?;
            cleanup_guard.mark_committed();

            info!(
                table = %table_ident,
                total_rows,
                "INSERT INTO committed successfully (streaming)"
            );
        } else {
            cleanup_guard.mark_committed();
        }

        Ok(vec![])
    }

    /// Handle CREATE TABLE [IF NOT EXISTS] ns.table (column definitions)
    ///
    /// Creates an empty Iceberg table from explicit column definitions.
    /// Honours V3 features (nanosecond timestamps, column defaults) by
    /// auto-bumping the format version.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn handle_create_table(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let ct = match stmt {
            Statement::CreateTable(ct) => ct,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        // Trino's `CREATE TABLE new (LIKE src)` copies the source table's
        // schema. The classifier rewrites the parenthesized form to the plain
        // `LIKE src` sqlparser records in `CreateTable.like`; copy the source
        // schema here (#343).
        if let Some(like_kind) = &ct.like {
            return self.handle_create_table_like(session, ct, like_kind).await;
        }

        let table_ident = resolve_table_ident(&ct.name, session)?;
        let namespace = table_ident.namespace().clone();
        let name = table_ident.name().to_string();

        if ct.columns.is_empty() {
            return Err(SqeError::Execution(
                "CREATE TABLE requires at least one column definition".into(),
            ));
        }

        // Convert SQL column definitions to Arrow schema (folds unquoted names, #337).
        let arrow_schema = ArrowSchema::new(create_table_arrow_fields(&ct.columns)?);
        let iceberg_schema = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns)?;

        // Decide the format version. V3 features auto-upgrade the table;
        // otherwise fall back to the configured default (normally V2).
        let needs_v3 = requires_v3_features(&ct.columns, &iceberg_schema);
        let format_version = if needs_v3 {
            FormatVersion::V3
        } else {
            self.format_version()
        };

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            columns = arrow_schema.fields().len(),
            format_version = ?format_version,
            "Creating empty table"
        );

        // Resolve the target catalog: an explicit 3-part qualifier wins, else the
        // session's connection catalog (Trino `catalog=...` header / Flight
        // default), else the configured default warehouse. Without the session
        // fallback a dbt/Trino client connected with catalog=ws_team_a issuing an
        // unqualified statement resolves against the default warehouse.
        let target_catalog = resolve_target_catalog(catalog_qualifier(&ct.name), session);
        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

        // Defense-in-depth: make sure the namespace exists in THIS catalog before
        // creating the table. dbt issues CREATE SCHEMA IF NOT EXISTS first; if that
        // did not land in this catalog the create would fail opaquely with
        // "namespace does not exist". Idempotent.
        ensure_namespace(catalog.as_ref(), &namespace).await?;

        if ct.if_not_exists && catalog.load_table(&table_ident).await.is_ok() {
            info!(table = %table_ident, "Table already exists, skipping (IF NOT EXISTS)");
            return Ok(vec![]);
        }

        // Merge in user-specified TBLPROPERTIES / WITH options so Polaris
        // stores them alongside the format-version directive. Without this
        // step CREATE TABLE silently drops every property the user typed.
        let mut props = self.format_version_props(format_version);
        // sqlparser 0.62 folds the old `table_properties` (TBLPROPERTIES) and
        // `with_options` (WITH (...)) fields into a single `table_options`
        // enum. Extract the user options regardless of which syntax produced
        // them so CREATE TABLE keeps every property the user typed.
        merge_user_table_properties(&mut props, create_table_options_as_slice(&ct.table_options));

        // Translate any PARTITIONED BY (...) clause into an Iceberg
        // UnboundPartitionSpec. Identity transforms cover bare column
        // refs; year/month/day/hour/bucket/truncate/void cover the
        // standard hidden-partitioning transforms.
        let partition_spec = build_partition_spec(ct.partition_by.as_deref(), &iceberg_schema)?;

        let location = unique_table_location(catalog.as_ref(), &namespace, &name).await;
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .location_opt(location)
            .format_version(format_version)
            .properties(props)
            .partition_spec_opt(partition_spec)
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        info!(
            namespace = %namespace,
            table = %name,
            "Table created successfully"
        );

        Ok(vec![])
    }

    /// Handle `CREATE TABLE new (LIKE src)` by copying the source table's
    /// current schema (#343).
    ///
    /// The new table is created empty (no data), with a freshly assigned set of
    /// Iceberg field IDs derived from the source's Arrow schema. Table
    /// properties, partitioning, and sort order are NOT copied: Trino gates
    /// those behind explicit `INCLUDING PROPERTIES` clauses that SQE does not
    /// yet parse, so the safe default is a plain schema copy. The source is
    /// resolved against its own catalog qualifier, so `CREATE TABLE a.b.new
    /// (LIKE c.d.src)` copies across catalogs.
    async fn handle_create_table_like(
        &self,
        session: &Session,
        ct: &sqlparser::ast::CreateTable,
        like_kind: &sqlparser::ast::CreateTableLikeKind,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::CreateTableLikeKind;

        let src_name = match like_kind {
            CreateTableLikeKind::Plain(l) | CreateTableLikeKind::Parenthesized(l) => &l.name,
        };
        // Resolve unqualified names against the session schema (X-Trino-Schema),
        // matching Trino and the read path. `parse_table_ref` alone defaults a
        // 1-part name to the `default` namespace, which would miss a source
        // table that lives in the session schema.
        let src_ident = resolve_table_ident(src_name, session)?;
        let table_ident = resolve_table_ident(&ct.name, session)?;
        let namespace = table_ident.namespace().clone();
        let name = table_ident.name().to_string();

        // Load the source table's current schema (resolved against the source's
        // own catalog qualifier) and re-derive a clean Iceberg schema from it.
        let src_catalog = self
            .create_catalog_bridge(session, catalog_qualifier(src_name).as_deref())
            .await?;
        let src_table = src_catalog.load_table(&src_ident).await.map_err(|e| {
            SqeError::Catalog(format!(
                "CREATE TABLE LIKE: source table '{src_ident}' not found: {e}"
            ))
        })?;
        let src_arrow = iceberg::arrow::schema_to_arrow_schema(
            src_table.metadata().current_schema(),
        )
        .map_err(|e| {
            SqeError::Execution(format!("CREATE TABLE LIKE: source schema conversion: {e}"))
        })?;
        let iceberg_schema = arrow_schema_to_iceberg(&src_arrow)?;

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            source = %src_ident,
            columns = src_arrow.fields().len(),
            "Creating table from LIKE source"
        );

        let target_catalog = resolve_target_catalog(catalog_qualifier(&ct.name), session);
        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

        ensure_namespace(catalog.as_ref(), &namespace).await?;

        if ct.if_not_exists && catalog.load_table(&table_ident).await.is_ok() {
            info!(table = %table_ident, "Table already exists, skipping (IF NOT EXISTS)");
            return Ok(vec![]);
        }

        let format_version = self.format_version();
        let props = self.format_version_props(format_version);
        let location = unique_table_location(catalog.as_ref(), &namespace, &name).await;
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .location_opt(location)
            .format_version(format_version)
            .properties(props)
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        info!(namespace = %namespace, table = %name, "Table created successfully (LIKE)");
        Ok(vec![])
    }

    /// Handle INSERT INTO ns.table SELECT ...
    ///
    /// The caller has already executed the SELECT and provides the result
    /// batches. This method:
    /// 1. Extracts the target table name from the INSERT statement
    /// 2. Loads the existing table from the catalog
    /// 3. Writes RecordBatches as Parquet data files
    /// 4. Commits the data files via a fast-append transaction
    #[instrument(skip(self, session, stmt, batches), fields(username = %session.user.username))]
    pub async fn handle_insert(
        &self,
        session: &Session,
        stmt: &Statement,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_name = match stmt {
            Statement::Insert(ins) => match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => name,
                other => {
                    return Err(SqeError::Execution(format!(
                        "INSERT INTO table functions not supported: {other}"
                    )));
                }
            },
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected Insert statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(
            username = %session.user.username,
            table = %table_ident,
            total_rows,
            "Executing INSERT INTO SELECT"
        );

        if total_rows == 0 {
            info!(table = %table_ident, "INSERT SELECT returned no rows — nothing to write");
            return Ok(vec![]);
        }

        // Create the catalog bridge and load the existing table, resolving the
        // target catalog qualifier rather than the default warehouse.
        let target_catalog = resolve_target_catalog(catalog_qualifier(table_name), session);
        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        // Clone batches for a Puffin sidecar only when the table opted in.
        let stats_snapshot: Option<Vec<RecordBatch>> =
            puffin_stats_enabled(table.metadata().properties()).then(|| batches.clone());

        // Write data files
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "insert")
                .with_metrics(self.metrics.clone());
        let data_files = write_data_files_with_metrics(
            &table,
            batches,
            "insert",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
        )
        .await?;

        if !data_files.is_empty() {
            let files_for_retry = data_files.clone();
            let catalog_for_commit = catalog.clone();
            commit_with_retry(
                catalog.as_ref(),
                &table_ident,
                "insert",
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
            .map_err(|e| {
                SqeError::Execution(format!("Failed to commit INSERT transaction: {e}"))
            })?;
            cleanup_guard.mark_committed();

            if let Some(stats_batches) = stats_snapshot {
                self.maybe_emit_puffin_sidecar(&catalog, &table_ident, &stats_batches)
                    .await;
            }

            info!(
                table = %table_ident,
                total_rows,
                "INSERT INTO committed successfully"
            );
        } else {
            cleanup_guard.mark_committed();
        }

        Ok(affected_rows_batch(total_rows)) // DML success with affected row count
    }

    /// Handle a Flight SQL DoPut ingest — write streamed Arrow batches to an Iceberg table.
    pub async fn handle_ingest(
        &self,
        session: &Session,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<usize> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        if total_rows == 0 {
            return Ok(0);
        }

        // Parse "catalog.schema.table" or "schema.table"
        let parts: Vec<&str> = table_name.split('.').collect();
        let (target_catalog, namespace_str, name) = match parts.as_slice() {
            [ns, tbl] => (None, *ns, (*tbl).to_string()),
            [cat, ns, tbl] => (Some(*cat), *ns, (*tbl).to_string()),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Invalid table name for ingest: {table_name}"
                )));
            }
        };

        let namespace = iceberg::NamespaceIdent::new(namespace_str.to_string());
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            total_rows,
            "Executing DoPut ingest"
        );

        let catalog = self.create_catalog_bridge(session, target_catalog).await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "ingest")
                .with_metrics(self.metrics.clone());
        let data_files = write_data_files_with_metrics(
            &table,
            batches,
            "ingest",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
        )
        .await?;

        if !data_files.is_empty() {
            let files_for_retry = data_files.clone();
            let catalog_for_commit = catalog.clone();
            commit_with_retry(
                catalog.as_ref(),
                &table_ident,
                "ingest",
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
            .map_err(|e| {
                SqeError::Execution(format!("Failed to commit ingest transaction: {e}"))
            })?;
            cleanup_guard.mark_committed();

            info!(table = %table_ident, total_rows, "DoPut ingest committed successfully");
        } else {
            cleanup_guard.mark_committed();
        }

        Ok(total_rows)
    }

    /// Streaming variant of [`handle_ingest`] for the Flight `DoPut` path.
    ///
    /// The batched [`handle_ingest`] `try_collect`s the entire uploaded Arrow
    /// stream into a `Vec<RecordBatch>` on the coordinator before writing it.
    /// That buffer is unbounded and invisible to the memory pool: a client can
    /// upload an arbitrarily large stream and balloon the coordinator past its
    /// cgroup limit. This variant feeds each decoded batch straight into
    /// `write_data_files_streaming`, the same O(batch_size) sink CTAS and INSERT
    /// already use, so resident memory stays at one batch plus one row group.
    ///
    /// The target schema comes from table metadata, so the stream is never
    /// peeked. Per-batch field-id stamping and type casts are applied by the
    /// streaming sink exactly as they are for INSERT (see `apply_stamped_schema`).
    ///
    /// `handle_ingest` (the batched signature) is retained for internal callers
    /// and the name-parsing tests.
    pub async fn handle_ingest_streaming<S, E>(
        &self,
        session: &Session,
        table_name: &str,
        batch_stream: S,
    ) -> sqe_core::Result<usize>
    where
        S: futures::Stream<Item = std::result::Result<RecordBatch, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Parse "catalog.schema.table" or "schema.table" (mirrors handle_ingest).
        let parts: Vec<&str> = table_name.split('.').collect();
        let (target_catalog, namespace_str, name) = match parts.as_slice() {
            [ns, tbl] => (None, *ns, (*tbl).to_string()),
            [cat, ns, tbl] => (Some(*cat), *ns, (*tbl).to_string()),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Invalid table name for ingest: {table_name}"
                )));
            }
        };

        let namespace = iceberg::NamespaceIdent::new(namespace_str.to_string());
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            "Executing streaming DoPut ingest"
        );

        let catalog = self.create_catalog_bridge(session, target_catalog).await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        // The streaming sink re-stamps every batch onto the table schema, so the
        // adapter's declared schema is only the stream contract, not the write
        // schema. Derive it from the table's current Iceberg schema.
        let arrow_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                |e| SqeError::Execution(format!("Failed to derive ingest Arrow schema: {e}")),
            )?,
        );

        let mapped =
            batch_stream.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)));
        let stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
            RecordBatchStreamAdapter::new(arrow_schema, Box::pin(mapped)),
        );

        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "ingest")
                .with_metrics(self.metrics.clone());
        let (data_files, total_rows) = write_data_files_streaming_with_metrics(
            &table,
            stream,
            "ingest",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
            self.fanout_limits(),
        )
        .await?;

        if !data_files.is_empty() {
            let files_for_retry = data_files.clone();
            let catalog_for_commit = catalog.clone();
            commit_with_retry(
                catalog.as_ref(),
                &table_ident,
                "ingest",
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
            .map_err(|e| {
                SqeError::Execution(format!("Failed to commit ingest transaction: {e}"))
            })?;
            cleanup_guard.mark_committed();

            info!(
                table = %table_ident,
                total_rows,
                "Streaming DoPut ingest committed successfully"
            );
        } else {
            cleanup_guard.mark_committed();
        }

        Ok(total_rows)
    }

    /// Handle DELETE FROM ns.table [WHERE ...]
    ///
    /// Uses Copy-on-Write: reads all data files, filters out rows matching
    /// the WHERE predicate, writes new files with surviving rows, and
    /// atomically swaps via rewrite_files().
    ///
    /// Without a WHERE clause, this is a truncate: commits a rewrite that
    /// removes all data files.
    #[instrument(skip(self, session, stmt, catalog, ctx, policy_summary_out), fields(username = %session.user.username))]
    pub async fn handle_delete(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_ident = resolve_delete_target_ident(stmt, session)?;
        let delete = match stmt {
            Statement::Delete(d) => d,
            // unreachable: resolve_delete_target_ident above already filtered.
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DELETE statement, got: {other}"
                )))
            }
        };

        let mut table = catalog.load_table(&table_ident).await?;

        let where_clause = &delete.selection;
        // Snapshot-independent setup (schema + policy predicates) runs once;
        // the data-file changes are retried against a reloaded snapshot on an
        // optimistic-concurrency conflict.
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(session, &table_ident, target_schema, policy_summary_out)
            .await?;

        // No WHERE = truncate. With no policy row filter we keep the fast
        // path: drop every file. With a row filter we must preserve rows
        // outside the user's view, so the truncate degrades into a CoW
        // rewrite under the row filter (handled by the main path below).
        if where_clause.is_none() && predicates.row_filter_sql.is_none() {
            let mut attempt = 1u32;
            loop {
                let old_data_files = self.collect_data_files(&table).await?;
                if old_data_files.is_empty() {
                    info!(table = %table_ident, "DELETE: table has no data files, nothing to delete");
                    return Ok(vec![]);
                }
                info!(table = %table_ident, file_count = old_data_files.len(), attempt, "DELETE: truncating table (no WHERE clause)");
                let tx = Transaction::new(&table);
                let action = tx.rewrite_files().delete_files(old_data_files);
                let commit_result = match action.apply(tx) {
                    Ok(tx) => tx.commit(catalog.as_catalog().as_ref()).await,
                    Err(e) => Err(e),
                };
                match commit_result {
                    Ok(_) => {
                        info!(table = %table_ident, "DELETE: table truncated successfully");
                        return Ok(vec![]);
                    }
                    Err(e)
                        if (e.retryable() || is_conflict_message(&e.to_string()))
                            && attempt < COW_MAX_ATTEMPTS =>
                    {
                        let sleep_ms = cow_conflict_backoff_ms(attempt);
                        warn!(table = %table_ident, op = "truncate", attempt, backoff_ms = sleep_ms, error = %e, "commit conflict; reloading table and retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                        table = catalog.load_table(&table_ident).await?;
                        attempt += 1;
                    }
                    Err(e) => {
                        return Err(SqeError::Execution(format!(
                            "Failed to commit truncate: {e}"
                        )));
                    }
                }
            }
        }

        // Lift any `IN (subquery)` expressions out of the WHERE into materialised
        // scratch MemTables joined via LEFT JOIN. The guard must outlive every
        // rewrite below; its Drop deregisters the scratch tables.
        let raw_where = where_clause
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;

        // Partition-pruned CoW under optimistic-concurrency retry: each attempt
        // re-reads the snapshot, rewrites only data files whose partition could
        // match the WHERE (rest untouched, #263), and commits. A commit conflict
        // re-runs the whole rewrite against the reloaded snapshot (#47).
        let mut attempt = 1u32;
        let total_deleted = loop {
            let old_data_files = self.collect_data_files(&table).await?;
            if old_data_files.is_empty() {
                info!(table = %table_ident, "DELETE: table has no data files, nothing to delete");
                return Ok(vec![]);
            }
            let (to_rewrite, pruned_files) =
                partition_prune_cow_files(old_data_files, &table, where_clause.as_ref());
            if to_rewrite.is_empty() {
                info!(table = %table_ident, "DELETE: no partitions match the WHERE clause; nothing to rewrite");
                return Ok(affected_rows_batch(0));
            }
            if pruned_files > 0 {
                info!(table = %table_ident, pruned_files, rewrite_files = to_rewrite.len(), "DELETE: partition-pruned CoW rewrite");
            }
            info!(
                table = %table_ident,
                file_count = to_rewrite.len(),
                where_clause = %where_sql,
                policy_row_filter = predicates.row_filter_sql.is_some(),
                policy_masks = predicates.column_mask_sqls.len(),
                attempt,
                "DELETE: CoW rewrite"
            );

            let mut new_data_files = Vec::new();
            let mut deleted_this_attempt = 0usize;
            let tracker = new_upload_tracker();
            let cleanup_guard =
                WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "delete-cow")
                    .with_metrics(self.metrics.clone());
            let pool = ctx.runtime_env().memory_pool.clone();
            let read_plan = self.plan_delete_aware_read(&table).await?;

            for data_file in &to_rewrite {
                let file_path = data_file.file_path();
                let batches = self
                    .read_data_file_applying_deletes(&read_plan, file_path, ctx, true)
                    .await?;
                if batches.is_empty() {
                    continue;
                }
                // Pool-track the surviving-rows accumulation (Layer A,
                // cow-keep-buffer). Per-file and a subset of the file, but the
                // decode reservation has already released by now, so this is
                // its own tracked buffer, not a double-count.
                let mut surviving = TrackedBatchBuffer::gated(
                    &pool,
                    "cow-keep-buffer",
                    self.config.query.write_buffer_tracking,
                );
                for batch in &batches {
                    let filtered = self
                        .filter_batch_negate(
                            ctx,
                            batch,
                            &where_sql,
                            &joins_sql,
                            &table_ident,
                            &predicates,
                        )
                        .await?;
                    deleted_this_attempt += batch.num_rows() - filtered.num_rows();
                    if filtered.num_rows() > 0 {
                        surviving.push(filtered)?;
                    }
                }
                if !surviving.is_empty() {
                    let new_files = write_data_files_with_metrics(
                        &table,
                        surviving.into_inner(),
                        "delete",
                        self.metrics.as_ref(),
                        self.compression(session),
                        tracker.clone(),
                    )
                    .await?;
                    new_data_files.extend(new_files);
                }
            }

            info!(
                table = %table_ident,
                deleted_rows = deleted_this_attempt,
                rewrite_files = to_rewrite.len(),
                new_files = new_data_files.len(),
                "DELETE: committing CoW rewrite"
            );

            let tx = Transaction::new(&table);
            let action = tx
                .rewrite_files()
                .add_data_files(new_data_files)
                .delete_files(to_rewrite);
            let commit_result = match action.apply(tx) {
                Ok(tx) => tx.commit(catalog.as_catalog().as_ref()).await,
                Err(e) => Err(e),
            };
            match commit_result {
                Ok(_) => {
                    cleanup_guard.mark_committed();
                    break deleted_this_attempt;
                }
                Err(e)
                    if (e.retryable() || is_conflict_message(&e.to_string()))
                        && attempt < COW_MAX_ATTEMPTS =>
                {
                    drop(cleanup_guard);
                    let sleep_ms = cow_conflict_backoff_ms(attempt);
                    warn!(
                        table = %table_ident,
                        op = "delete-cow",
                        attempt,
                        backoff_ms = sleep_ms,
                        error = %e,
                        "commit conflict; reloading table and retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    table = catalog.load_table(&table_ident).await?;
                    attempt += 1;
                    continue;
                }
                Err(e) => {
                    return Err(SqeError::Execution(format!("Failed to commit DELETE: {e}")));
                }
            }
        };

        info!(table = %table_ident, deleted_rows = total_deleted, "DELETE committed successfully");
        Ok(affected_rows_batch(total_deleted))
    }

    /// Handle DELETE FROM using Merge-on-Read (position deletes).
    ///
    /// Instead of rewriting data files (CoW), this method writes position delete files
    /// that mark specific row positions for deletion. This is more efficient for small
    /// deletes on large tables — the cost is O(deleted rows) vs O(total rows) for CoW —
    /// but increases read amplification until the table is compacted.
    ///
    /// The position delete files are committed via `FastAppendAction`, which auto-routes
    /// `DataFile`s with `content_type = PositionDeletes` into the delete manifest.
    ///
    /// Without a WHERE clause this falls back to the CoW truncate path (same as
    /// `handle_delete`), since there is no efficiency benefit to writing delete files
    /// for every row.
    #[instrument(skip(self, session, stmt, catalog, ctx, policy_summary_out), fields(username = %session.user.username))]
    pub async fn handle_delete_mor(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_ident = resolve_delete_target_ident(stmt, session)?;
        let delete = match stmt {
            Statement::Delete(d) => d,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DELETE statement, got: {other}"
                )))
            }
        };

        let table = catalog.load_table(&table_ident).await?;

        let old_data_files = self.collect_data_files(&table).await?;

        if old_data_files.is_empty() {
            info!(table = %table_ident, "MoR DELETE: table has no data files, nothing to delete");
            return Ok(vec![]);
        }

        let where_clause = &delete.selection;
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(session, &table_ident, target_schema, policy_summary_out)
            .await?;

        // No WHERE clause and no policy row filter: keep the fast truncate
        // path. With a row filter we cannot wipe every file because rows
        // outside the user's view must survive; the position-delete loop
        // below handles that case by emitting deletes only where the row
        // filter admits a row.
        if where_clause.is_none() && predicates.row_filter_sql.is_none() {
            info!(
                table = %table_ident,
                file_count = old_data_files.len(),
                "MoR DELETE: no WHERE clause, truncating table via CoW"
            );
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_files(old_data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply truncate transaction: {e}"))
            })?;
            tx.commit(catalog.as_catalog().as_ref())
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to commit truncate: {e}")))?;
            info!(table = %table_ident, "MoR DELETE: table truncated successfully");
            return Ok(vec![]);
        }

        let raw_where = where_clause
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        // Lift any `IN (subquery)` expressions; see `handle_delete` for details.
        // The guard must outlive the per-batch loop below.
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;
        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            where_clause = %where_sql,
            policy_row_filter = predicates.row_filter_sql.is_some(),
            policy_masks = predicates.column_mask_sqls.len(),
            "MoR DELETE: collecting row positions to delete"
        );

        // Scan each data file and collect (file_path, row_position) pairs for matching rows.
        //
        // This read is deliberately RAW (`read_parquet_via_table`, no delete
        // application): position deletes address physical row offsets in the
        // file, so applying existing delete files here would shift positions
        // and delete the wrong rows. Re-marking an already-deleted position
        // is harmless.
        let mut position_deletes: Vec<(String, i64)> = Vec::new();

        for data_file in &old_data_files {
            let file_path = data_file.file_path().to_string();
            let batches = self
                .read_parquet_via_table(&table, &file_path, ctx, true)
                .await?;

            if batches.is_empty() {
                continue;
            }

            // Row positions are 0-based and contiguous across all batches in the file.
            let mut row_offset: i64 = 0;
            for batch in &batches {
                let match_mask = self
                    .filter_batch_match(
                        ctx,
                        batch,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;

                for row_idx in 0..batch.num_rows() {
                    if match_mask.value(row_idx) {
                        position_deletes.push((file_path.clone(), row_offset + row_idx as i64));
                    }
                }
                row_offset += batch.num_rows() as i64;
            }
        }

        if position_deletes.is_empty() {
            info!(table = %table_ident, "MoR DELETE: no matching rows, nothing to commit");
            return Ok(vec![]);
        }

        let deleted_count = position_deletes.len();
        info!(
            table = %table_ident,
            delete_count = deleted_count,
            "MoR DELETE: writing position delete files"
        );

        // Write position delete files (sorted by (file_path, pos) inside the helper).
        let delete_files =
            write_position_delete_files(&table, position_deletes, self.compression(session))
                .await?;

        // Commit: append position delete files. FastAppendAction auto-routes DataFiles
        // with content_type=PositionDeletes into the delete manifest entry.
        let files_for_retry = delete_files.clone();
        let catalog_for_commit = catalog.clone();
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "mor-delete",
            move |fresh_table| {
                let files = files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                async move {
                    let tx = Transaction::new(&fresh_table);
                    let action = tx.fast_append().add_data_files(files);
                    let tx = action.apply(tx)?;
                    tx.commit(cat.as_catalog().as_ref()).await
                }
            },
        )
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to commit MoR DELETE: {e}")))?;

        info!(table = %table_ident, deleted_rows = deleted_count, "MoR DELETE committed successfully");
        Ok(affected_rows_batch(deleted_count))
    }

    /// Handle DELETE FROM using Merge-on-Read with equality deletes.
    ///
    /// Phase E, tasks 6.7 and 6.8. Commits an equality-delete file that names
    /// the table's declared identifier fields (primary key). Downstream readers
    /// exclude any row where those fields match one of the emitted values.
    ///
    /// Advantages over position deletes:
    ///
    /// - Snapshot-stable: rows added later that match the equality keys are
    ///   also excluded without writing new deletes.
    /// - Compact: one delete file per batch of keys, regardless of how many
    ///   data files those keys span.
    ///
    /// The file is committed via `RowDeltaAction` so the operation classifies
    /// as `Overwrite` with `added-delete-files=1` in the snapshot summary,
    /// matching Java Iceberg and Spark semantics.
    #[instrument(skip(self, session, stmt, catalog, ctx, policy_summary_out), fields(username = %session.user.username))]
    pub async fn handle_delete_equality(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_ident = resolve_delete_target_ident(stmt, session)?;
        let delete = match stmt {
            Statement::Delete(d) => d,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DELETE statement, got: {other}"
                )))
            }
        };
        let table = catalog.load_table(&table_ident).await?;

        // Equality deletes require declared identifier-field-ids (primary key).
        let identifier_field_ids: Vec<i32> = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect();
        if identifier_field_ids.is_empty() {
            return Err(SqeError::Execution(format!(
                "table {table_ident} has no identifier-field-ids; equality-delete path requires a primary key"
            )));
        }

        let old_data_files = self.collect_data_files(&table).await?;
        if old_data_files.is_empty() {
            info!(table = %table_ident, "equality DELETE: table has no data files");
            return Ok(vec![]);
        }

        let where_clause = &delete.selection;
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(session, &table_ident, target_schema, policy_summary_out)
            .await?;

        // DELETE without WHERE and without a policy row filter: fall back
        // to CoW truncate. Emitting an empty equality-delete file serves
        // no purpose. With a policy row filter we scan instead so rows
        // outside the user's view survive.
        if where_clause.is_none() && predicates.row_filter_sql.is_none() {
            info!(
                table = %table_ident,
                file_count = old_data_files.len(),
                "equality DELETE: no WHERE clause, falling back to CoW truncate"
            );
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_files(old_data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply truncate transaction: {e}"))
            })?;
            tx.commit(catalog.as_catalog().as_ref())
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to commit truncate: {e}")))?;
            return Ok(vec![]);
        }

        let raw_where = where_clause
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;
        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            where_clause = %where_sql,
            equality_ids = ?identifier_field_ids,
            policy_row_filter = predicates.row_filter_sql.is_some(),
            policy_masks = predicates.column_mask_sqls.len(),
            "equality DELETE: scanning for matching rows"
        );

        // Scan every data file and collect rows where WHERE matches. Equality
        // deletes need only the identifier columns, so we keep the full batch
        // for now and let the writer project downstream. Deletes are applied
        // during the read (#371) so already-deleted rows neither inflate the
        // affected-row count nor emit redundant delete keys.
        let mut key_batches: Vec<RecordBatch> = Vec::new();
        let mut total_matched: usize = 0;
        let read_plan = self.plan_delete_aware_read(&table).await?;

        for data_file in &old_data_files {
            let file_path = data_file.file_path().to_string();
            let batches = self
                .read_data_file_applying_deletes(&read_plan, &file_path, ctx, true)
                .await?;
            if batches.is_empty() {
                continue;
            }
            for batch in batches {
                let match_mask = self
                    .filter_batch_match(
                        ctx,
                        &batch,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;
                let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
                    SqeError::Execution(format!("failed to filter match rows: {e}"))
                })?;
                if filtered.num_rows() == 0 {
                    continue;
                }
                total_matched += filtered.num_rows();
                key_batches.push(filtered);
            }
        }

        if total_matched == 0 {
            info!(table = %table_ident, "equality DELETE: no matching rows, nothing to commit");
            return Ok(vec![]);
        }

        let delete_files = write_equality_delete_files(
            &table,
            key_batches,
            identifier_field_ids,
            self.compression(session),
        )
        .await?;

        // Commit via RowDeltaAction: this emits Operation::Overwrite with
        // added-delete-files > 0 and no removed/added data files. The
        // SnapshotProducer's added-delete-files summary key mirrors Spark.
        let files_for_retry = delete_files.clone();
        let catalog_for_commit = catalog.clone();
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "equality-delete",
            move |fresh_table| {
                let files = files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                async move {
                    let snapshot_id = fresh_table.metadata().current_snapshot_id();
                    let tx = Transaction::new(&fresh_table);
                    let mut action = tx.row_delta().add_delete_files(files);
                    if let Some(snap) = snapshot_id {
                        action = action.validate_from_snapshot(snap);
                    }
                    let tx = action.apply(tx)?;
                    tx.commit(cat.as_catalog().as_ref()).await
                }
            },
        )
        .await
        .map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("stale snapshot") || msg.contains("rowdelta conflict") {
                SqeError::Catalog(format!("commit conflict: {e}"))
            } else {
                SqeError::Execution(format!("Failed to commit equality DELETE: {e}"))
            }
        })?;

        info!(
            table = %table_ident,
            deleted_rows = total_matched,
            "equality DELETE committed successfully"
        );
        Ok(affected_rows_batch(total_matched))
    }

    /// Dispatch a DELETE statement to CoW, MoR position deletes, or MoR
    /// equality deletes based on the target table's `write.delete.mode`
    /// property (Phase E, task 6.8).
    ///
    /// Semantics:
    ///
    /// - `copy-on-write` (default): rewrite data files. Backward compatible.
    /// - `merge-on-read`: pick equality deletes when the table declares a
    ///   primary key (identifier-field-ids), otherwise fall back to position
    ///   deletes.
    pub async fn handle_delete_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Resolve the target catalog from the table reference, mirroring INSERT,
        // so a DELETE against a discovered/non-default catalog loads and commits
        // against the right warehouse instead of the session-default one (#334).
        // On name-extraction failure keep the passed catalog; the slow path
        // surfaces the original error in context.
        let catalog = match delete_target_object_name(stmt) {
            Ok(name) => {
                let target = resolve_target_catalog(catalog_qualifier(name), session);
                self.resolve_session_catalog(session, target.as_deref())
                    .await?
            }
            Err(_) => catalog,
        };

        // Peek at the target table to read its properties. Any parse or
        // load error falls through to the default CoW path, which surfaces
        // the error at that point.
        let Ok(table_factor_name) = delete_target_object_name(stmt) else {
            return self
                .handle_delete(session, stmt, catalog, ctx, policy_summary_out)
                .await;
        };
        let Some(table_ident) = try_resolve_delete_target_ident(stmt, session) else {
            return self
                .handle_delete(session, stmt, catalog, ctx, policy_summary_out)
                .await;
        };
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self
                .handle_delete(session, stmt, catalog, ctx, policy_summary_out)
                .await;
        };

        // Capture lineage: a DELETE rewrites the target in place, so the
        // OL plan is `target -> target` with identity column lineage. We
        // synthesise this by planning `SELECT * FROM target` and wrapping
        // it in a synthetic INSERT against the same target. Errors are
        // swallowed because lineage capture is best-effort.
        self.capture_self_lineage(ctx, &table_ident, table_factor_name, plan_out)
            .await;

        let mode = resolve_delete_mode(table.metadata().properties())?;

        match mode {
            WriteMode::MergeOnRead => {
                // Prefer equality deletes when the table declares a PK.
                let has_ids = table
                    .metadata()
                    .current_schema()
                    .identifier_field_ids()
                    .next()
                    .is_some();
                if has_ids {
                    info!(
                        table = %table_ident,
                        "DELETE dispatch: MoR + equality deletes"
                    );
                    self.handle_delete_equality(session, stmt, catalog, ctx, policy_summary_out)
                        .await
                } else {
                    info!(
                        table = %table_ident,
                        "DELETE dispatch: MoR + position deletes (no PK declared)"
                    );
                    self.handle_delete_mor(session, stmt, catalog, ctx, policy_summary_out)
                        .await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "DELETE dispatch: CoW");
                self.handle_delete(session, stmt, catalog, ctx, policy_summary_out)
                    .await
            }
        }
    }

    /// Handle UPDATE ns.table SET col = expr [WHERE ...]
    ///
    /// Uses Copy-on-Write: reads all data files, applies SET assignments to
    /// rows matching WHERE, writes new files, atomically swaps.
    #[instrument(skip(self, session, stmt, catalog, ctx, policy_summary_out), fields(username = %session.user.username))]
    pub async fn handle_update(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_ident = resolve_update_target_ident(stmt, session)?;
        let (assignments, selection) = match stmt {
            Statement::Update(update) => (&update.assignments, &update.selection),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected UPDATE statement, got: {other}"
                )))
            }
        };

        let mut table = catalog.load_table(&table_ident).await?;

        // Snapshot-independent setup runs once. Schema, policy predicates, and
        // lifted IN-subqueries do not depend on the data snapshot, so they are
        // computed before the retry loop. Subquery values are evaluated against
        // the initial snapshot, consistent with UPDATE's snapshot isolation.
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(session, &table_ident, target_schema, policy_summary_out)
            .await?;
        // Build the SET clause as SQL CASE expressions for a SELECT rewrite:
        // UPDATE t SET c1=e1 WHERE cond  ->  SELECT CASE WHEN cond THEN e1 ELSE c1 END, ...
        let raw_where = selection
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        // Lift any `IN (subquery)` expressions; see `handle_delete` for details.
        // The guard must outlive every rewrite below.
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;

        // Partition-pruned CoW under optimistic-concurrency retry. Each attempt
        // re-reads the current snapshot, rewrites only the data files whose
        // partition could match the WHERE (the rest stay untouched, #263), and
        // commits. On a commit conflict the whole rewrite re-runs against the
        // reloaded snapshot -- re-committing snapshot-specific rewritten files
        // would drop a concurrent writer's changes (#47, write-path race).
        let mut attempt = 1u32;
        let total_updated = loop {
            let old_data_files = self.collect_data_files(&table).await?;
            if old_data_files.is_empty() {
                info!(table = %table_ident, "UPDATE: table has no data files");
                return Ok(vec![]);
            }
            let (to_rewrite, pruned_files) =
                partition_prune_cow_files(old_data_files, &table, selection.as_ref());
            if to_rewrite.is_empty() {
                info!(table = %table_ident, "UPDATE: no partitions match the WHERE clause; nothing to rewrite");
                return Ok(affected_rows_batch(0));
            }
            if pruned_files > 0 {
                info!(table = %table_ident, pruned_files, rewrite_files = to_rewrite.len(), "UPDATE: partition-pruned CoW rewrite");
            }
            info!(
                table = %table_ident,
                file_count = to_rewrite.len(),
                assignments = assignments.len(),
                where_clause = %where_sql,
                policy_row_filter = predicates.row_filter_sql.is_some(),
                policy_masks = predicates.column_mask_sqls.len(),
                attempt,
                "UPDATE: CoW rewrite"
            );

            let mut new_data_files = Vec::new();
            let mut updated_this_attempt = 0usize;
            let tracker = new_upload_tracker();
            let cleanup_guard =
                WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "update-cow")
                    .with_metrics(self.metrics.clone());
            let pool = ctx.runtime_env().memory_pool.clone();
            let read_plan = self.plan_delete_aware_read(&table).await?;

            for data_file in &to_rewrite {
                let file_path = data_file.file_path();
                let batches = self
                    .read_data_file_applying_deletes(&read_plan, file_path, ctx, true)
                    .await?;
                if batches.is_empty() {
                    continue;
                }
                // Pool-track the per-file rewritten accumulation (Layer A,
                // cow-keep-buffer). The decode reservation has released by now,
                // so this is its own tracked buffer.
                let mut rewritten = TrackedBatchBuffer::gated(
                    &pool,
                    "cow-keep-buffer",
                    self.config.query.write_buffer_tracking,
                );
                for batch in &batches {
                    let out = self
                        .apply_update(
                            ctx,
                            batch,
                            assignments,
                            &where_sql,
                            &joins_sql,
                            &table_ident,
                            &predicates,
                        )
                        .await?;
                    rewritten.push(out)?;
                }
                for batch in &batches {
                    let count = self
                        .count_matching_rows(
                            ctx,
                            batch,
                            &where_sql,
                            &joins_sql,
                            &table_ident,
                            &predicates,
                        )
                        .await?;
                    updated_this_attempt += count;
                }
                let new_files = write_data_files_with_metrics(
                    &table,
                    rewritten.into_inner(),
                    "update",
                    self.metrics.as_ref(),
                    self.compression(session),
                    tracker.clone(),
                )
                .await?;
                new_data_files.extend(new_files);
            }

            info!(
                table = %table_ident,
                updated_rows = updated_this_attempt,
                rewrite_files = to_rewrite.len(),
                new_files = new_data_files.len(),
                "UPDATE: committing CoW rewrite"
            );

            let tx = Transaction::new(&table);
            let action = tx
                .rewrite_files()
                .add_data_files(new_data_files)
                .delete_files(to_rewrite);
            let commit_result = match action.apply(tx) {
                Ok(tx) => tx.commit(catalog.as_catalog().as_ref()).await,
                Err(e) => Err(e),
            };
            match commit_result {
                Ok(_) => {
                    cleanup_guard.mark_committed();
                    break updated_this_attempt;
                }
                Err(e)
                    if (e.retryable() || is_conflict_message(&e.to_string()))
                        && attempt < COW_MAX_ATTEMPTS =>
                {
                    // Drop the guard so this attempt's orphan parquet files are
                    // deleted before we reload and rewrite again.
                    drop(cleanup_guard);
                    let sleep_ms = cow_conflict_backoff_ms(attempt);
                    warn!(
                        table = %table_ident,
                        op = "update-cow",
                        attempt,
                        backoff_ms = sleep_ms,
                        error = %e,
                        "commit conflict; reloading table and retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    table = catalog.load_table(&table_ident).await?;
                    attempt += 1;
                    continue;
                }
                Err(e) => {
                    return Err(SqeError::Execution(format!("Failed to commit UPDATE: {e}")));
                }
            }
        };

        info!(table = %table_ident, updated_rows = total_updated, "UPDATE committed successfully");
        Ok(affected_rows_batch(total_updated))
    }

    /// Dispatch an UPDATE statement to CoW or MoR based on
    /// `write.update.mode` (Phase H, task 9.4).
    ///
    /// - `copy-on-write` (default): rewrite affected data files in place.
    /// - `merge-on-read`: fall through to MoR only when the table declares
    ///   identifier-field-ids (primary key). Without a PK we cannot emit an
    ///   equality delete for the old row, so we fall back to CoW with a log
    ///   entry rather than fail.
    pub async fn handle_update_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Resolve the target catalog from the table reference, mirroring INSERT,
        // so an UPDATE against a discovered/non-default catalog loads and commits
        // against the right warehouse instead of the session-default one (#334).
        let catalog = match update_target_object_name(stmt) {
            Ok(name) => {
                let target = resolve_target_catalog(catalog_qualifier(name), session);
                self.resolve_session_catalog(session, target.as_deref())
                    .await?
            }
            Err(_) => catalog,
        };

        // Peek at the target table to read its properties.
        let Ok(table_factor_name) = update_target_object_name(stmt) else {
            return self
                .handle_update(session, stmt, catalog, ctx, policy_summary_out)
                .await;
        };
        let Some(table_ident) = try_resolve_update_target_ident(stmt, session) else {
            return self
                .handle_update(session, stmt, catalog, ctx, policy_summary_out)
                .await;
        };
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self
                .handle_update(session, stmt, catalog, ctx, policy_summary_out)
                .await;
        };

        // Capture lineage: UPDATE rewrites target rows in place. v1 emits
        // identity column lineage; richer per-column SET-expression tracing
        // is deferred. See `capture_self_lineage` for details.
        self.capture_self_lineage(ctx, &table_ident, table_factor_name, plan_out)
            .await;

        let mode = resolve_update_mode(table.metadata().properties())?;

        match mode {
            WriteMode::MergeOnRead => {
                let has_ids = table
                    .metadata()
                    .current_schema()
                    .identifier_field_ids()
                    .next()
                    .is_some();
                if has_ids {
                    info!(
                        table = %table_ident,
                        "UPDATE dispatch: MoR + equality deletes"
                    );
                    self.handle_update_equality(session, stmt, catalog, ctx, policy_summary_out)
                        .await
                } else {
                    info!(
                        table = %table_ident,
                        "UPDATE dispatch: MoR requested but no PK; falling back to CoW"
                    );
                    self.handle_update(session, stmt, catalog, ctx, policy_summary_out)
                        .await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "UPDATE dispatch: CoW");
                self.handle_update(session, stmt, catalog, ctx, policy_summary_out)
                    .await
            }
        }
    }

    /// Handle UPDATE in Merge-on-Read mode.
    ///
    /// For each matched row we emit two records:
    ///
    /// 1. A row in a new data file carrying the UPDATE'd values.
    /// 2. A row in an equality-delete file carrying the old primary-key
    ///    values so the pre-update row is hidden at scan time.
    ///
    /// Both are committed atomically via `RowDeltaAction`. Unmatched rows
    /// in existing data files are left alone: no file rewrite. The SF100
    /// `trade_result_update_holding` pattern benefits here because the
    /// working set is the small set of matched rows, not every file in
    /// the partition.
    #[instrument(skip(self, session, stmt, catalog, ctx, policy_summary_out), fields(username = %session.user.username))]
    pub async fn handle_update_equality(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_ident = resolve_update_target_ident(stmt, session)?;
        let (assignments, selection) = match stmt {
            Statement::Update(update) => (&update.assignments, &update.selection),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected UPDATE statement, got: {other}"
                )))
            }
        };
        let table = catalog.load_table(&table_ident).await?;

        // MoR UPDATE requires declared identifier-field-ids (primary key)
        // so we can emit an equality delete for the old row. Without a PK
        // the dispatcher falls back to CoW; reaching this function without
        // a PK is a caller bug.
        let identifier_field_ids: Vec<i32> = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect();
        if identifier_field_ids.is_empty() {
            return Err(SqeError::Execution(format!(
                "MoR UPDATE on {table_ident} requires identifier-field-ids (primary key)"
            )));
        }

        let old_data_files = self.collect_data_files(&table).await?;
        if old_data_files.is_empty() {
            info!(table = %table_ident, "MoR UPDATE: table has no data files");
            return Ok(vec![]);
        }

        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(session, &table_ident, target_schema, policy_summary_out)
            .await?;

        let raw_where = selection
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;

        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            assignments = assignments.len(),
            where_clause = %where_sql,
            equality_ids = ?identifier_field_ids,
            policy_row_filter = predicates.row_filter_sql.is_some(),
            policy_masks = predicates.column_mask_sqls.len(),
            "MoR UPDATE: scanning for matching rows"
        );

        // For each data file, find the matched rows twice:
        //   - once with the UPDATE applied, projected into a new data file
        //   - once as the raw matched rows, projected into an equality
        //     delete file keyed on identifier-field-ids
        //
        // The CoW `apply_update` helper returns a per-batch full rewrite
        // (matched rows get new values, others pass through). For MoR we
        // only want the matched rows, so we filter after apply_update.
        let mut new_row_batches: Vec<RecordBatch> = Vec::new();
        let mut key_batches: Vec<RecordBatch> = Vec::new();
        let mut total_updated: usize = 0;
        // Deletes applied during the read (#371): a row already removed by a
        // delete file must not be matched and re-emitted as an updated row.
        let read_plan = self.plan_delete_aware_read(&table).await?;

        for data_file in &old_data_files {
            let file_path = data_file.file_path().to_string();
            let batches = self
                .read_data_file_applying_deletes(&read_plan, &file_path, ctx, true)
                .await?;
            if batches.is_empty() {
                continue;
            }
            for batch in batches {
                let match_mask = self
                    .filter_batch_match(
                        ctx,
                        &batch,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;
                // Skip files with zero matches: no new data rows, no
                // equality deletes. Leaving them alone is the point of MoR.
                let matched_count = match_mask.true_count();
                if matched_count == 0 {
                    continue;
                }
                total_updated += matched_count;

                // Old PKs for the equality delete. Filter the original
                // batch by the match mask; the equality-delete writer
                // projects identifier columns from the Iceberg schema.
                let old_keys = filter_record_batch(&batch, &match_mask).map_err(|e| {
                    SqeError::Execution(format!("failed to filter match rows: {e}"))
                })?;
                if old_keys.num_rows() > 0 {
                    key_batches.push(old_keys);
                }

                // New values for the data file. `apply_update` produces a
                // full-batch rewrite with CASE WHEN where THEN new ELSE
                // old END, then we filter to only the matched rows so we
                // do not re-write the unchanged ones.
                let full_rewrite = self
                    .apply_update(
                        ctx,
                        &batch,
                        assignments,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;
                let new_rows = filter_record_batch(&full_rewrite, &match_mask).map_err(|e| {
                    SqeError::Execution(format!("failed to filter updated rows: {e}"))
                })?;
                if new_rows.num_rows() > 0 {
                    new_row_batches.push(new_rows);
                }
            }
        }

        if total_updated == 0 {
            info!(table = %table_ident, "MoR UPDATE: no matching rows, nothing to commit");
            return Ok(vec![]);
        }

        // Write the data file with the new values and the equality delete
        // file with the old keys. Both go into one RowDelta commit.
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "update-mor");
        let new_data_files = write_data_files_with_metrics(
            &table,
            new_row_batches,
            "update-mor",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
        )
        .await?;

        let delete_files = write_equality_delete_files(
            &table,
            key_batches,
            identifier_field_ids,
            self.compression(session),
        )
        .await?;

        info!(
            table = %table_ident,
            updated_rows = total_updated,
            new_data_files = new_data_files.len(),
            equality_delete_files = delete_files.len(),
            "MoR UPDATE: committing row delta"
        );

        let data_files_for_retry = new_data_files.clone();
        let delete_files_for_retry = delete_files.clone();
        let catalog_for_commit = catalog.clone();
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "mor-update",
            move |fresh_table| {
                let dfs = data_files_for_retry.clone();
                let dels = delete_files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                async move {
                    let snapshot_id = fresh_table.metadata().current_snapshot_id();
                    let tx = Transaction::new(&fresh_table);
                    let mut action = tx.row_delta().add_data_files(dfs).add_delete_files(dels);
                    if let Some(snap) = snapshot_id {
                        action = action.validate_from_snapshot(snap);
                    }
                    let tx = action.apply(tx)?;
                    tx.commit(cat.as_catalog().as_ref()).await
                }
            },
        )
        .await
        .map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("stale snapshot") || msg.contains("rowdelta conflict") {
                SqeError::Catalog(format!("commit conflict: {e}"))
            } else {
                SqeError::Execution(format!("Failed to commit MoR UPDATE: {e}"))
            }
        })?;
        cleanup_guard.mark_committed();

        info!(
            table = %table_ident,
            updated_rows = total_updated,
            "MoR UPDATE committed successfully"
        );
        Ok(affected_rows_batch(total_updated))
    }

    /// Handle MERGE INTO target USING source ON condition WHEN ...
    ///
    /// Uses Copy-on-Write: reads all target data files, performs a FULL OUTER
    /// JOIN with the provided source batches to classify rows as matched /
    /// not-matched / target-only, applies the appropriate MERGE actions via
    /// CASE WHEN SQL expressions, writes new data files, and atomically swaps
    /// via rewrite_files().
    ///
    /// The caller is responsible for executing the source query and providing
    /// the result batches. This follows the same pattern as `handle_ctas` and
    /// `handle_insert`.
    #[instrument(skip(self, session, stmt, source_batches, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_merge(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::TableFactor;

        let (table_factor, source_factor, on_expr, clauses) = match stmt {
            Statement::Merge(merge) => (&merge.table, &merge.source, &merge.on, &merge.clauses),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected MERGE statement, got: {other}"
                )));
            }
        };

        // Extract target table name and optional alias
        let (target_table_name, target_alias) = match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                (name, alias_str)
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected table name in MERGE target, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(target_table_name, session)?;

        // Extract source alias (needed for column references in the JOIN)
        let source_alias = match source_factor {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };

        let on_sql = format!("{on_expr}");

        info!(
            username = %session.user.username,
            table = %table_ident,
            on_condition = %on_sql,
            clause_count = clauses.len(),
            "Executing MERGE INTO"
        );

        // Load target table and read all data files
        let table = catalog.load_table(&table_ident).await?;

        let old_data_files = self.collect_data_files(&table).await?;
        let read_plan = self.plan_delete_aware_read(&table).await?;

        // Build the target relation for the merge SELECT. Two paths:
        //  - Buffered (default): read the whole target into a pool-tracked Vec
        //    (Layer A `merge-target-buffer`) and register it as a MemTable.
        //  - Streaming (B2, opt-in via `merge_target_streaming`): scan the
        //    pinned `old_data_files` lazily so the target flows through the
        //    merge join as governed operator memory instead of a MemTable Vec.
        // See docs/internal/specs/2026-07-02-write-path-memory-safety-design.md.
        //
        // The streaming provider decodes raw parquet, so it must stand down
        // when the snapshot carries delete files (#371): rewriting a target
        // read without its position/equality deletes applied resurrects
        // MoR-deleted rows. The buffered path below applies them.
        let stream_target = self.config.query.merge_target_streaming
            && self.config.query.write_buffer_tracking
            && !read_plan.has_deletes();
        if self.config.query.merge_target_streaming
            && self.config.query.write_buffer_tracking
            && read_plan.has_deletes()
        {
            info!(
                table = %table_ident,
                "MERGE: target carries delete files; using the buffered \
                 (delete-applying) path instead of the streaming provider"
            );
        }

        // Holds the Layer A target reservation alive to the end of the merge in
        // the buffered path (the MemTable clones its Arc-backed batches). None
        // in the streaming path.
        let mut _target_buf_hold: Option<TrackedBatchBuffer> = None;

        let (target_schema, target_provider): (
            arrow_schema::SchemaRef,
            Arc<dyn datafusion::datasource::TableProvider>,
        ) = if stream_target {
            let schema = Arc::new(
                iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                    |e| {
                        SqeError::Execution(format!(
                            "Failed to convert Iceberg schema to Arrow: {e}"
                        ))
                    },
                )?,
            );
            let provider = crate::merge_target_provider::merge_target_table(
                &table,
                &old_data_files,
                schema.clone(),
            )?;
            info!(
                table = %table_ident,
                files = old_data_files.len(),
                "MERGE: streaming target from pinned data files (B2)"
            );
            (schema, Arc::new(provider))
        } else {
            let pool = ctx.runtime_env().memory_pool.clone();
            let mut target_buf = TrackedBatchBuffer::gated(
                &pool,
                "merge-target-buffer",
                self.config.query.write_buffer_tracking,
            );
            for data_file in &old_data_files {
                let file_path = data_file.file_path();
                // track = false: the returned batches are re-registered into
                // `merge-target-buffer` just below, so tracking the decode here
                // too would double-count the same bytes.
                let batches = self
                    .read_data_file_applying_deletes(&read_plan, file_path, ctx, false)
                    .await?;
                for batch in batches {
                    target_buf.push(batch)?;
                }
            }

            // Target schema from existing data (or table metadata if empty).
            let target_schema = if let Some(first) = target_buf.as_slice().first() {
                first.schema()
            } else {
                let iceberg_schema = table.metadata().current_schema();
                Arc::new(
                    iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(|e| {
                        SqeError::Execution(format!(
                            "Failed to convert Iceberg schema to Arrow: {e}"
                        ))
                    })?,
                )
            };

            let target_mem = if target_buf.is_empty() {
                datafusion::datasource::MemTable::try_new(target_schema.clone(), vec![])
            } else {
                datafusion::datasource::MemTable::try_new(
                    target_schema.clone(),
                    vec![target_buf.as_slice().to_vec()],
                )
            }
            .map_err(|e| SqeError::Execution(format!("Failed to create target MemTable: {e}")))?;
            _target_buf_hold = Some(target_buf);
            (target_schema, Arc::new(target_mem))
        };

        let target_columns: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        if target_columns.is_empty() {
            return Err(SqeError::Execution(format!(
                "MERGE target table {table_ident} has no columns for match detection"
            )));
        }

        // Use the target alias (or a default) for the merge scratch table names.
        // The scratch table names embed a per-invocation uuid so concurrent
        // MERGEs running in the same session do not clobber each other's
        // registrations.
        let t_alias = target_alias.clone().unwrap_or_else(|| "t".to_string());
        let s_alias = source_alias.clone().unwrap_or_else(|| "s".to_string());
        let merge_id = Uuid::now_v7().simple().to_string();
        let target_table_ref = format!("__merge_target_{merge_id}");
        let source_table_ref = format!("__merge_source_{merge_id}");
        let qualified_target_ref = format!("datafusion.public.{target_table_ref}");
        let qualified_source_ref = format!("datafusion.public.{source_table_ref}");
        let _merge_scratch_guard = MergeScratchCleanup::new(
            ctx,
            vec![qualified_target_ref.clone(), qualified_source_ref.clone()],
        );

        // Register the target relation in the full session context (which has
        // all catalog tables registered for cross-table subqueries).
        ctx.register_table(&qualified_target_ref, target_provider)
            .map_err(|e| SqeError::Execution(format!("Failed to register target provider: {e}")))?;

        // Use the pre-executed source batches (caller handles source query execution)
        if source_batches.is_empty() {
            info!(table = %table_ident, "MERGE: source returned no data, nothing to merge");
            return Ok(vec![]);
        }

        let source_schema = source_batches[0].schema();

        // Register source data as a MemTable
        let source_mem =
            datafusion::datasource::MemTable::try_new(source_schema.clone(), vec![source_batches])
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to create source MemTable: {e}"))
                })?;
        ctx.register_table(&qualified_source_ref, Arc::new(source_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register source MemTable: {e}")))?;

        // Rewrite the ON condition to use our MemTable names instead of
        // aliases, using the same identifier-aware rewriter as the clause
        // predicates so an alias cannot fire inside a substring, dotted path,
        // or string literal.
        let on_rewritten = crate::merge_sql::replace_alias_qualifier(
            &crate::merge_sql::replace_alias_qualifier(&on_sql, &t_alias, &target_table_ref),
            &s_alias,
            &source_table_ref,
        );

        let source_columns: Vec<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        if source_columns.is_empty() {
            return Err(SqeError::Execution(
                "MERGE source has no columns for match detection".to_string(),
            ));
        }

        // Classify the clauses (honoring statement order and per-clause
        // predicates) and compile them into one SELECT over a FULL OUTER
        // JOIN. Row classes are detected through NULL sentinels on the
        // first column of each side; rows a DELETE clause claims (or
        // source-only rows no INSERT clause claims) are dropped by the
        // keep-column WHERE inside the generated SQL.
        let merge_names = crate::merge_sql::MergeNames {
            target_ref: &target_table_ref,
            source_ref: &source_table_ref,
            t_alias: &t_alias,
            s_alias: &s_alias,
            target_columns: &target_columns,
            source_columns: &source_columns,
        };
        let classified = crate::merge_sql::classify_merge_clauses(clauses, &merge_names)?;
        let (matched_clauses, not_matched_clauses, by_source_clauses) = classified.counts();

        // Cardinality check: a target row matched by >1 source row would be
        // silently duplicated by the FULL OUTER JOIN. Only matched clauses can
        // trigger it, so skip the extra count pass for insert-only / by-source
        // merges.
        if self.config.query.merge_cardinality_check && matched_clauses > 0 {
            let (pair_sql, matched_sql) = crate::merge_sql::build_cardinality_check_sql(
                &merge_names,
                &qualified_target_ref,
                &qualified_source_ref,
                &on_rewritten,
            );
            check_merge_cardinality(ctx, &pair_sql, &matched_sql).await?;
        }

        let select_sql = crate::merge_sql::build_merge_select(
            &classified,
            &merge_names,
            &qualified_target_ref,
            &qualified_source_ref,
            &on_rewritten,
        );

        info!(
            table = %table_ident,
            merge_sql = %select_sql,
            matched_clauses,
            not_matched_clauses,
            by_source_clauses,
            "MERGE: executing merge query"
        );

        let df = ctx
            .sql(&select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to plan MERGE query: {e}")))?;

        // Layer B (output side): stream the merged output straight into the
        // streaming sink instead of `df.collect()`-ing the whole merged result
        // into a Vec and buffering it through `write_data_files_with_metrics`.
        // The merged output is at least as large as the target table, so this
        // removes the single biggest MERGE buffer. The join/CASE state stays
        // inside DataFusion operators, which are pool-tracked (and spillable
        // under subsystem A) rather than an invisible Vec.
        //
        // The stream must be fully consumed by the awaited write below before
        // the `target_buf` reservation and the `MergeScratchCleanup` guard
        // (both declared above) drop: the MemTableExec feeding the join holds
        // Arc refs into the tracked target batches for the duration of the read.
        // Rows removed by the MERGE (DELETE clauses, unclaimed source-only
        // rows) are filtered by the keep-column WHERE inside `select_sql`, so
        // the stream carries exactly the surviving rows.
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to execute MERGE query: {e}")))?;

        // Write new data files from the merged results (streaming).
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "merge-cow");
        let (new_data_files, total_rows) = write_data_files_streaming_with_metrics(
            &table,
            stream,
            "merge",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
            self.fanout_limits(),
        )
        .await?;

        info!(
            table = %table_ident,
            old_files = old_data_files.len(),
            new_files = new_data_files.len(),
            total_rows,
            "MERGE: committing CoW rewrite"
        );

        // Atomic commit: remove all old files, add new merged files
        if old_data_files.is_empty() && new_data_files.is_empty() {
            cleanup_guard.mark_committed();
            info!(table = %table_ident, "MERGE: no changes to commit");
            return Ok(vec![]);
        }

        let tx = Transaction::new(&table);
        let mut action = tx.rewrite_files();
        if !new_data_files.is_empty() {
            action = action.add_data_files(new_data_files);
        }
        if !old_data_files.is_empty() {
            action = action.delete_files(old_data_files);
        }
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("Failed to apply MERGE rewrite: {e}")))?;
        tx.commit(catalog.as_catalog().as_ref())
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to commit MERGE: {e}")))?;
        cleanup_guard.mark_committed();

        info!(table = %table_ident, total_rows, "MERGE committed successfully");
        Ok(affected_rows_batch(total_rows))
    }

    /// Plan + collect one MoR MERGE sub-query, mapping errors to a labelled
    /// `SqeError::Execution`. Shared by the matched / not-matched / by-source
    /// clause loops.
    async fn run_merge_subquery(
        &self,
        ctx: &DFSessionContext,
        sql: &str,
        label: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("MoR MERGE {label} plan failed: {e}")))?;
        df.collect()
            .await
            .map_err(|e| SqeError::Execution(format!("MoR MERGE {label} execution failed: {e}")))
    }

    /// Dispatch MERGE to CoW or MoR based on `write.merge.mode`
    /// (Phase H, task 9.7).
    ///
    /// - `copy-on-write` (default): rewrite all target files via
    ///   `handle_merge` (pre-existing behaviour).
    /// - `merge-on-read`: route to `handle_merge_equality` when the table
    ///   declares a primary key. Without a PK we fall back to CoW because
    ///   the MATCHED clauses need old-row keys for the equality delete.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_merge_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        source_plan: Option<LogicalPlan>,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Resolve the target catalog from the table reference, mirroring INSERT,
        // so a MERGE against a discovered/non-default catalog loads and commits
        // against the right warehouse instead of the session-default one (#334).
        let catalog = match stmt {
            Statement::Merge(merge) => match &merge.table {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    let target = resolve_target_catalog(catalog_qualifier(name), session);
                    self.resolve_session_catalog(session, target.as_deref())
                        .await?
                }
                _ => catalog,
            },
            _ => catalog,
        };

        // Peek at the target table to read its properties.
        let merge_table = match stmt {
            Statement::Merge(merge) => &merge.table,
            _ => {
                return self
                    .handle_merge(session, stmt, source_batches, catalog, ctx)
                    .await;
            }
        };
        let target_name = match merge_table {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            _ => {
                return self
                    .handle_merge(session, stmt, source_batches, catalog, ctx)
                    .await;
            }
        };
        let Ok(table_ident) = resolve_table_ident(target_name, session) else {
            return self
                .handle_merge(session, stmt, source_batches, catalog, ctx)
                .await;
        };
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self
                .handle_merge(session, stmt, source_batches, catalog, ctx)
                .await;
        };

        // Capture lineage. The MERGE source SELECT was already planned and
        // executed by the caller. We re-wrap that source plan as a synthetic
        // INSERT into the target so OL events carry inputs (the source's
        // TableScans), output (the target), and identity column lineage.
        // v1 emits identity transformations; per-clause MERGE_INSERT /
        // MERGE_UPDATE annotation is deferred (spec §5.3 v1 limitation).
        if let Some(src_plan) = source_plan {
            let (lin_catalog, lin_namespace, lin_table) =
                lineage_target_parts(&self.config, &table_ident);
            match build_lineage_insert_plan(src_plan, &lin_catalog, &lin_namespace, &lin_table) {
                Ok(wrapper) => {
                    *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
                }
                Err(e) => {
                    tracing::debug!(
                        table = %table_ident,
                        error = %e,
                        "lineage capture: failed to build MERGE INSERT wrapper; skipping"
                    );
                }
            }
        }

        let mode = resolve_merge_mode(table.metadata().properties())?;
        match mode {
            WriteMode::MergeOnRead => {
                // The equality path now expresses the full clause surface
                // (predicates, multiple clauses per class, and NOT MATCHED BY
                // SOURCE) via first-match-wins guards, so every shape stays on
                // MoR as long as the table declares a primary key. Without a PK
                // there are no equality-delete keys, so fall back to CoW.
                let has_ids = table
                    .metadata()
                    .current_schema()
                    .identifier_field_ids()
                    .next()
                    .is_some();
                if has_ids {
                    info!(
                        table = %table_ident,
                        "MERGE dispatch: MoR + equality deletes"
                    );
                    self.handle_merge_equality(session, stmt, source_batches, catalog, ctx)
                        .await
                } else {
                    info!(
                        table = %table_ident,
                        "MERGE dispatch: MoR requested but no PK; falling back to CoW"
                    );
                    self.handle_merge(session, stmt, source_batches, catalog, ctx)
                        .await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "MERGE dispatch: CoW");
                self.handle_merge(session, stmt, source_batches, catalog, ctx)
                    .await
            }
        }
    }

    /// Handle MERGE INTO in Merge-on-Read mode.
    ///
    /// Supports the full clause surface via first-match-wins guards (one
    /// guarded query per clause), so each MERGE clause maps onto RowDelta
    /// inputs:
    ///
    /// - `WHEN MATCHED THEN UPDATE`: emit a data file row with the new values
    ///   and an equality-delete row with the matched target's PK.
    /// - `WHEN MATCHED THEN DELETE`: emit an equality-delete row only.
    /// - `WHEN NOT MATCHED THEN INSERT`: emit a data file row only.
    /// - `WHEN NOT MATCHED BY SOURCE THEN DELETE / UPDATE`: act on target-only
    ///   rows (no source match) via an anti-join; DELETE emits an
    ///   equality-delete, UPDATE emits the old PK as a delete plus the
    ///   reassigned row as new data.
    ///
    /// Multiple clauses per class are honored in statement order: a target row
    /// belongs to the first clause in its class whose predicate passes, so the
    /// per-clause guards are mutually exclusive and no row is emitted twice.
    /// All outputs commit in one `RowDeltaAction`. Target rows claimed by no
    /// clause pass through untouched: no rewrite, no delete.
    #[instrument(
        skip(self, session, stmt, source_batches, catalog, ctx),
        fields(username = %session.user.username)
    )]
    pub async fn handle_merge_equality(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::TableFactor;

        let (table_factor, source_factor, on_expr, clauses) = match stmt {
            Statement::Merge(merge) => (&merge.table, &merge.source, &merge.on, &merge.clauses),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected MERGE statement, got: {other}"
                )));
            }
        };

        let (target_table_name, target_alias) = match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                (name, alias_str)
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected table name in MERGE target, got: {other}"
                )));
            }
        };
        let table_ident = resolve_table_ident(target_table_name, session)?;

        let source_alias = match source_factor {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };
        let on_sql = format!("{on_expr}");

        let table = catalog.load_table(&table_ident).await?;
        let identifier_field_ids: Vec<i32> = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect();
        if identifier_field_ids.is_empty() {
            return Err(SqeError::Execution(format!(
                "MoR MERGE on {table_ident} requires identifier-field-ids (primary key)"
            )));
        }

        // Collect target batches for the JOIN. Unlike CoW we do not need
        // to rewrite them; the RowDelta only touches matched rows.
        let old_data_files = self.collect_data_files(&table).await?;
        // Pool-track the accumulated target read (Layer A), mirroring the CoW
        // MERGE path. track = false on the per-file read so the decode is not
        // double-counted against this buffer.
        let pool = ctx.runtime_env().memory_pool.clone();
        let mut target_buf = TrackedBatchBuffer::gated(
            &pool,
            "merge-eq-target-buffer",
            self.config.query.write_buffer_tracking,
        );
        // Deletes applied during the read (#371): an already-deleted target
        // row must not join as MATCHED and be re-emitted by the RowDelta.
        let read_plan = self.plan_delete_aware_read(&table).await?;
        for data_file in &old_data_files {
            let file_path = data_file.file_path();
            let batches = self
                .read_data_file_applying_deletes(&read_plan, file_path, ctx, false)
                .await?;
            for batch in batches {
                target_buf.push(batch)?;
            }
        }

        // Resolve schema from the existing data or the Iceberg metadata.
        let target_schema = if let Some(first) = target_buf.as_slice().first() {
            first.schema()
        } else {
            let iceberg_schema = table.metadata().current_schema();
            let arrow_schema =
                iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(|e| {
                    SqeError::Execution(format!("Failed to convert Iceberg schema to Arrow: {e}"))
                })?;
            Arc::new(arrow_schema)
        };
        let target_columns: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let t_alias = target_alias.clone().unwrap_or_else(|| "t".to_string());
        let s_alias = source_alias.clone().unwrap_or_else(|| "s".to_string());
        let merge_id = Uuid::now_v7().simple().to_string();
        let target_ref = format!("__merge_mor_target_{merge_id}");
        let source_ref = format!("__merge_mor_source_{merge_id}");
        let q_target = format!("datafusion.public.{target_ref}");
        let q_source = format!("datafusion.public.{source_ref}");
        let _merge_scratch_guard =
            MergeScratchCleanup::new(ctx, vec![q_target.clone(), q_source.clone()]);

        let target_mem = if target_buf.is_empty() {
            datafusion::datasource::MemTable::try_new(target_schema.clone(), vec![])
        } else {
            datafusion::datasource::MemTable::try_new(
                target_schema.clone(),
                vec![target_buf.as_slice().to_vec()],
            )
        }
        .map_err(|e| SqeError::Execution(format!("Failed to create target MemTable: {e}")))?;
        ctx.register_table(&q_target, Arc::new(target_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register target MemTable: {e}")))?;

        if source_batches.is_empty() {
            info!(table = %table_ident, "MoR MERGE: source returned no data, nothing to merge");
            return Ok(vec![]);
        }
        let source_schema = source_batches[0].schema();
        let source_mem =
            datafusion::datasource::MemTable::try_new(source_schema.clone(), vec![source_batches])
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to create source MemTable: {e}"))
                })?;
        ctx.register_table(&q_source, Arc::new(source_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register source MemTable: {e}")))?;

        let source_columns: Vec<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Identifier-aware ON rewrite (see the CoW path for rationale).
        let on_rewritten = crate::merge_sql::replace_alias_qualifier(
            &crate::merge_sql::replace_alias_qualifier(&on_sql, &t_alias, &target_ref),
            &s_alias,
            &source_ref,
        );

        // Classify clauses with the same classifier the CoW path uses: ordered
        // per row class, predicates rewritten to scratch refs, Oracle
        // sub-predicates rejected, NOT MATCHED BY SOURCE supported.
        let merge_names = crate::merge_sql::MergeNames {
            target_ref: &target_ref,
            source_ref: &source_ref,
            t_alias: &t_alias,
            s_alias: &s_alias,
            target_columns: &target_columns,
            source_columns: &source_columns,
        };
        let classified = crate::merge_sql::classify_merge_clauses(clauses, &merge_names)?;
        let (matched_count, not_matched_count, by_source_count) = classified.counts();

        // Cardinality check (shared with CoW): a matched target row hit by more
        // than one source row would be emitted multiple times. Only matched
        // clauses can trigger it.
        if self.config.query.merge_cardinality_check && matched_count > 0 {
            let (pair_sql, matched_sql) = crate::merge_sql::build_cardinality_check_sql(
                &merge_names,
                &q_target,
                &q_source,
                &on_rewritten,
            );
            check_merge_cardinality(ctx, &pair_sql, &matched_sql).await?;
        }

        info!(
            table = %table_ident,
            on_condition = %on_sql,
            matched_count,
            not_matched_count,
            by_source_count,
            "MoR MERGE: planning row delta"
        );

        let mut new_data_batches: Vec<RecordBatch> = Vec::new();
        let mut equality_delete_batches: Vec<RecordBatch> = Vec::new();
        let mut updated_rows: usize = 0;
        let mut deleted_rows: usize = 0;
        let mut inserted_rows: usize = 0;

        // Old-key projection for an equality delete: the writer projects the
        // identifier fields, so selecting all target columns is sufficient.
        let old_key_cols = || -> String {
            target_columns
                .iter()
                .map(|col| format!("{target_ref}.\"{col}\" AS \"{col}\""))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // MATCHED clauses (INNER JOIN target+source). First-match-wins guards
        // make each matched target row belong to at most one clause, so no row
        // is emitted twice across clauses.
        for (i, clause) in classified.matched.iter().enumerate() {
            let guard = crate::merge_sql::first_match_guard(&classified.matched, i);
            match &clause.op {
                crate::merge_sql::MergeOp::Update(assignments) => {
                    let update_cols: Vec<String> = target_columns
                        .iter()
                        .map(|col| {
                            let expr = crate::merge_sql::resolve_update_expr(
                                col,
                                assignments,
                                &target_ref,
                                &source_ref,
                                &t_alias,
                                &s_alias,
                            );
                            format!("{expr} AS \"{col}\"")
                        })
                        .collect();
                    let new_sql = format!(
                        "SELECT {} FROM {q_target} INNER JOIN {q_source} ON {on_rewritten} WHERE {guard}",
                        update_cols.join(", ")
                    );
                    for batch in self
                        .run_merge_subquery(ctx, &new_sql, "MATCHED UPDATE new-row")
                        .await?
                    {
                        updated_rows += batch.num_rows();
                        if batch.num_rows() > 0 {
                            new_data_batches.push(batch);
                        }
                    }
                    let old_sql = format!(
                        "SELECT {} FROM {q_target} INNER JOIN {q_source} ON {on_rewritten} WHERE {guard}",
                        old_key_cols()
                    );
                    for batch in self
                        .run_merge_subquery(ctx, &old_sql, "MATCHED UPDATE old-key")
                        .await?
                    {
                        if batch.num_rows() > 0 {
                            equality_delete_batches.push(batch);
                        }
                    }
                }
                crate::merge_sql::MergeOp::Delete => {
                    let del_sql = format!(
                        "SELECT {} FROM {q_target} INNER JOIN {q_source} ON {on_rewritten} WHERE {guard}",
                        old_key_cols()
                    );
                    for batch in self
                        .run_merge_subquery(ctx, &del_sql, "MATCHED DELETE")
                        .await?
                    {
                        deleted_rows += batch.num_rows();
                        if batch.num_rows() > 0 {
                            equality_delete_batches.push(batch);
                        }
                    }
                }
                crate::merge_sql::MergeOp::Insert(..) => {
                    return Err(SqeError::Execution(
                        "internal error: INSERT classified as a matched MERGE clause".to_string(),
                    ));
                }
            }
        }

        // NOT MATCHED (source-only): INSERT via anti-join from source to target.
        for (i, clause) in classified.not_matched.iter().enumerate() {
            let guard = crate::merge_sql::first_match_guard(&classified.not_matched, i);
            if let crate::merge_sql::MergeOp::Insert(insert_cols, insert_kind) = &clause.op {
                let insert_exprs: Vec<String> = target_columns
                    .iter()
                    .map(|col| {
                        let expr = crate::merge_sql::resolve_insert_expr(
                            col,
                            insert_cols,
                            insert_kind,
                            &source_ref,
                            &source_columns,
                            &s_alias,
                            &t_alias,
                            &target_ref,
                        );
                        format!("{expr} AS \"{col}\"")
                    })
                    .collect();
                let insert_sql = format!(
                    "SELECT {} FROM {q_source} WHERE NOT EXISTS \
                     (SELECT 1 FROM {q_target} WHERE {on_rewritten}) AND ({guard})",
                    insert_exprs.join(", ")
                );
                for batch in self
                    .run_merge_subquery(ctx, &insert_sql, "NOT MATCHED INSERT")
                    .await?
                {
                    inserted_rows += batch.num_rows();
                    if batch.num_rows() > 0 {
                        new_data_batches.push(batch);
                    }
                }
            }
        }

        // NOT MATCHED BY SOURCE (target-only): UPDATE / DELETE target rows with
        // no source match. Source columns are absent for this class, so a BY
        // SOURCE clause references target columns only (SQL standard); a source
        // reference would fail to plan here.
        for (i, clause) in classified.by_source.iter().enumerate() {
            let guard = crate::merge_sql::first_match_guard(&classified.by_source, i);
            let anti = format!(
                "FROM {q_target} WHERE NOT EXISTS \
                 (SELECT 1 FROM {q_source} WHERE {on_rewritten}) AND ({guard})"
            );
            match &clause.op {
                crate::merge_sql::MergeOp::Delete => {
                    let del_sql = format!("SELECT {} {anti}", old_key_cols());
                    for batch in self
                        .run_merge_subquery(ctx, &del_sql, "BY SOURCE DELETE")
                        .await?
                    {
                        deleted_rows += batch.num_rows();
                        if batch.num_rows() > 0 {
                            equality_delete_batches.push(batch);
                        }
                    }
                }
                crate::merge_sql::MergeOp::Update(assignments) => {
                    let new_cols: Vec<String> = target_columns
                        .iter()
                        .map(|col| {
                            let expr = crate::merge_sql::resolve_update_expr(
                                col,
                                assignments,
                                &target_ref,
                                &source_ref,
                                &t_alias,
                                &s_alias,
                            );
                            format!("{expr} AS \"{col}\"")
                        })
                        .collect();
                    let new_sql = format!("SELECT {} {anti}", new_cols.join(", "));
                    for batch in self
                        .run_merge_subquery(ctx, &new_sql, "BY SOURCE UPDATE new-row")
                        .await?
                    {
                        updated_rows += batch.num_rows();
                        if batch.num_rows() > 0 {
                            new_data_batches.push(batch);
                        }
                    }
                    let old_sql = format!("SELECT {} {anti}", old_key_cols());
                    for batch in self
                        .run_merge_subquery(ctx, &old_sql, "BY SOURCE UPDATE old-key")
                        .await?
                    {
                        if batch.num_rows() > 0 {
                            equality_delete_batches.push(batch);
                        }
                    }
                }
                crate::merge_sql::MergeOp::Insert(..) => {
                    return Err(SqeError::Execution(
                        "internal error: INSERT classified as a BY SOURCE MERGE clause".to_string(),
                    ));
                }
            }
        }

        // Scratch MemTables are deregistered by the `MergeScratchCleanup`
        // drop guard declared above.

        let total_touched = updated_rows + deleted_rows + inserted_rows;
        if total_touched == 0 {
            info!(table = %table_ident, "MoR MERGE: no matched or not-matched rows");
            return Ok(vec![]);
        }

        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "merge-mor");
        let new_data_files = if !new_data_batches.is_empty() {
            write_data_files_with_metrics(
                &table,
                new_data_batches,
                "merge-mor",
                self.metrics.as_ref(),
                self.compression(session),
                tracker,
            )
            .await?
        } else {
            vec![]
        };

        let delete_files = if !equality_delete_batches.is_empty() {
            write_equality_delete_files(
                &table,
                equality_delete_batches,
                identifier_field_ids,
                self.compression(session),
            )
            .await?
        } else {
            vec![]
        };

        info!(
            table = %table_ident,
            updated_rows,
            deleted_rows,
            inserted_rows,
            new_data_files = new_data_files.len(),
            equality_delete_files = delete_files.len(),
            "MoR MERGE: committing row delta"
        );

        let data_files_for_retry = new_data_files.clone();
        let delete_files_for_retry = delete_files.clone();
        let catalog_for_commit = catalog.clone();
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "mor-merge",
            move |fresh_table| {
                let dfs = data_files_for_retry.clone();
                let dels = delete_files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                async move {
                    let snapshot_id = fresh_table.metadata().current_snapshot_id();
                    let tx = Transaction::new(&fresh_table);
                    let mut action = tx.row_delta();
                    if !dfs.is_empty() {
                        action = action.add_data_files(dfs);
                    }
                    if !dels.is_empty() {
                        action = action.add_delete_files(dels);
                    }
                    if let Some(snap) = snapshot_id {
                        action = action.validate_from_snapshot(snap);
                    }
                    let tx = action.apply(tx)?;
                    tx.commit(cat.as_catalog().as_ref()).await
                }
            },
        )
        .await
        .map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("stale snapshot") || msg.contains("rowdelta conflict") {
                SqeError::Catalog(format!("commit conflict: {e}"))
            } else {
                SqeError::Execution(format!("Failed to commit MoR MERGE: {e}"))
            }
        })?;
        cleanup_guard.mark_committed();

        info!(
            table = %table_ident,
            updated_rows,
            deleted_rows,
            inserted_rows,
            "MoR MERGE committed successfully"
        );
        Ok(affected_rows_batch(total_touched))
    }

    // -------------------------------------------------------------------------
    // CoW helper methods
    // -------------------------------------------------------------------------

    /// Collect all current DataFile objects from the table's manifest entries.
    ///
    /// Reads the current snapshot's manifest list, loads each manifest, and
    /// collects all data file entries that are Added or Existing (not Deleted).
    ///
    /// Routes reads through `Table::object_cache()` so warm CoW operations
    /// avoid redundant S3 GETs. Cold reads are parallelised with
    /// `buffer_unordered` at `config.catalog.manifest_concurrency`.
    async fn collect_data_files(&self, table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
        let metadata_ref = table.metadata_ref();
        let snapshot = match metadata_ref.current_snapshot() {
            Some(s) => s,
            None => return Ok(vec![]), // no snapshot = empty table
        };

        let cache = table.object_cache();
        let manifest_list = cache
            .get_manifest_list(snapshot, &metadata_ref)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

        let concurrency = self.config.catalog.manifest_concurrency.max(1);
        let manifests: Vec<Arc<iceberg::spec::Manifest>> =
            futures::stream::iter(manifest_list.entries().iter().cloned())
                .map(|mf| {
                    let cache = cache.clone();
                    async move { cache.get_manifest(&mf).await }
                })
                .buffer_unordered(concurrency)
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
                        // Only include live data files (Added or Existing), skip Deleted
                        entry.status() != ManifestStatus::Deleted
                            && entry.data_file().content_type() == DataContentType::Data
                    })
                    .map(|entry| entry.data_file().clone())
                    .collect::<Vec<_>>()
            })
            .collect();

        Ok(data_files)
    }

    /// Read all RecordBatches from a Parquet data file using the table's FileIO.
    ///
    /// Uses iceberg-rust's scan infrastructure to read a single file via the
    /// table's already-configured FileIO (which handles S3 credentials, region, etc.).
    /// Read one Iceberg data file into memory as `RecordBatch`es.
    ///
    /// When `track` is true the copy-on-write decode is registered against the
    /// query memory pool (Layer A, write-path memory safety): the compressed
    /// file bytes and the decoded Arrow batches are both reserved, so a wide
    /// file decompressing to several GB of Arrow fails this one query with a
    /// typed `ResourceExhausted` instead of OOM-killing the coordinator. The
    /// peak residency is here, during decode, where both reservations are live;
    /// they release when this function returns. Callers that then accumulate
    /// the returned batches keep their own tracked buffer alive for that.
    ///
    /// `handle_merge` passes `track = false` because it pushes every returned
    /// batch into its own `merge-target-buffer`, and double-registering the
    /// same bytes would overstate the reservation.
    async fn read_parquet_via_table(
        &self,
        table: &IcebergTable,
        file_path: &str,
        ctx: &DFSessionContext,
        track: bool,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let pool = if track && self.config.query.write_buffer_tracking {
            Some(ctx.runtime_env().memory_pool.clone())
        } else {
            None
        };

        let file_io = table.file_io();
        let input = file_io
            .new_input(file_path)
            .map_err(|e| SqeError::Execution(format!("Failed to open file '{file_path}': {e}")))?;

        let input_file = input
            .read()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to read file '{file_path}': {e}")))?;

        // Reserve the compressed bytes for the lifetime of the decode below.
        // Dropped when this function returns (on success or via `?`).
        let _file_bytes_res = if let Some(p) = pool.as_ref() {
            let mut r = WriteReservation::new(p, "cow-file-bytes");
            r.try_grow(input_file.len())?;
            Some(r)
        } else {
            None
        };

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

        let batches: Vec<RecordBatch> = if let Some(p) = pool.as_ref() {
            // Reserve each decoded batch as it is produced so an oversized file
            // is denied mid-decode rather than fully materialised first.
            let mut buf = TrackedBatchBuffer::new(p, "cow-decode-buffer");
            for item in reader {
                let batch = item.map_err(|e| {
                    SqeError::Execution(format!("Failed to read Parquet file '{file_path}': {e}"))
                })?;
                buf.push(batch)?;
            }
            buf.into_inner()
        } else {
            reader
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to read Parquet file '{file_path}': {e}"))
                })?
        };

        Ok(batches)
    }

    /// Plan a snapshot-pinned, delete-aware read for a DML rewrite (#371).
    ///
    /// `read_parquet_via_table` decodes a data file's raw bytes, which
    /// silently resurrects rows already removed by position/equality delete
    /// files: a MoR DELETE followed by a CoW UPDATE/DELETE/MERGE rewrote the
    /// deleted rows back into the table. This plan routes per-file rewrite
    /// reads through the Iceberg scan machinery instead, so every file's
    /// applicable delete files are applied during the read.
    ///
    /// Plan once per commit attempt, right after each `collect_data_files`
    /// on the same loaded table, so the task set matches the snapshot whose
    /// files the rewrite deletes.
    async fn plan_delete_aware_read(
        &self,
        table: &IcebergTable,
    ) -> sqe_core::Result<DeleteAwareReadPlan> {
        let scan =
            table.scan().select_all().build().map_err(|e| {
                SqeError::Execution(format!("Failed to build rewrite read scan: {e}"))
            })?;
        let tasks: Vec<iceberg::scan::FileScanTask> = scan
            .plan_files()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to plan rewrite read: {e}")))?
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to plan rewrite read: {e}")))?;
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

    /// Read one data file with its delete files applied, for CoW/MoR
    /// rewrites. Memory-tracking contract matches `read_parquet_via_table`:
    /// with `track` the decoded batches are reserved against the query pool
    /// while buffered here; callers that re-accumulate pass `track = false`.
    async fn read_data_file_applying_deletes(
        &self,
        plan: &DeleteAwareReadPlan,
        file_path: &str,
        ctx: &DFSessionContext,
        track: bool,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let Some(tasks) = plan.tasks_by_path.get(file_path) else {
            // The file came from `collect_data_files` but is absent from the
            // scan plan's `tasks_by_path`. That should be unreachable when both
            // are derived from the same table load, but if a storage backend
            // normalises `data_file_path` differently between the manifest
            // entry and the scan task, silently raw-reading here would drop the
            // file's delete files and resurrect deleted rows. Fail loud instead
            // (see !576 review): a wrong DML result is worse than an error.
            return Err(SqeError::Execution(format!(
                "delete-aware read: data file '{file_path}' is missing from the \
                 scan plan; refusing to read it without its delete files (this \
                 indicates a data-file path mismatch between the manifest and \
                 the scan planner)"
            )));
        };
        let stream: iceberg::scan::FileScanTaskStream =
            Box::pin(futures::stream::iter(tasks.clone().into_iter().map(Ok)));
        let result = plan
            .scan
            .read_tasks_to_arrow_with_metrics(stream)
            .map_err(|e| {
                SqeError::Execution(format!("Failed to read data file '{file_path}': {e}"))
            })?;
        let mut batch_stream = result.stream();

        let pool = if track && self.config.query.write_buffer_tracking {
            Some(ctx.runtime_env().memory_pool.clone())
        } else {
            None
        };
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut buf = pool
            .as_ref()
            .map(|p| TrackedBatchBuffer::new(p, "cow-decode-buffer"));
        while let Some(item) = batch_stream.next().await {
            let batch = item.map_err(|e| {
                SqeError::Execution(format!("Failed to read data file '{file_path}': {e}"))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            match buf.as_mut() {
                Some(b) => b.push(batch)?,
                None => batches.push(batch),
            }
        }
        Ok(match buf {
            Some(b) => b.into_inner(),
            None => batches,
        })
    }

    /// Evaluate a WHERE clause against a RecordBatch and return rows that do NOT match.
    /// Used for DELETE: we keep the rows that don't match the WHERE predicate.
    ///
    /// `joins_sql` is a concatenation of `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`] and is spliced into the outer SELECT's
    /// FROM clause immediately after the aliased target. Pass an empty string
    /// when no lifted joins are needed.
    ///
    /// `predicates` carries the row-filter and column-mask SQL derived from
    /// the configured PolicyEnforcer. The row filter is ANDed onto the user's
    /// WHERE so DELETE only removes rows the policy admits, and masked
    /// columns are exposed via a wrapping subquery so any WHERE reference
    /// to a masked column sees the masked value.
    #[allow(clippy::too_many_arguments)]
    async fn filter_batch_negate(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<RecordBatch> {
        use arrow::compute::not;
        use datafusion::arrow::array::BooleanArray;

        // Register the batch as a temporary table so DataFusion can evaluate the predicate
        let table_name = format!("__delete_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        let augmented_where = augment_where_with_row_filter(where_sql, predicates);
        let from_clause = build_policy_from_clause(
            &format!("datafusion.public.{table_name}"),
            orig_name,
            &batch.schema(),
            predicates,
            false,
        );

        // Execute: SELECT <where_clause> AS __match FROM __delete_<table>
        // Alias the scratch table to the original target name (see apply_update
        // for rationale) so correlated subqueries inside the WHERE clause can
        // reference `tablename.col`.
        let eval_sql = format!(
            "SELECT CAST(({augmented_where}) AS BOOLEAN) AS __match \
             FROM {from_clause}{joins_sql}"
        );
        let df = ctx
            .sql(&eval_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}")))?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}")))?;

        // Deregister temp table
        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        if result_batches.is_empty() || result_batches[0].num_rows() == 0 {
            return Ok(batch.clone());
        }

        // Build a boolean mask: NOT <predicate> (rows to keep)
        let mask_batch = &result_batches[0];
        let match_col = mask_batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                SqeError::Execution("WHERE evaluation did not produce a boolean column".into())
            })?;
        let negated = not(match_col)
            .map_err(|e| SqeError::Execution(format!("Failed to negate WHERE mask: {e}")))?;

        // Apply the mask to the original batch
        filter_record_batch(batch, &negated)
            .map_err(|e| SqeError::Execution(format!("Failed to filter batch: {e}")))
    }

    /// Evaluate a WHERE clause against a RecordBatch and return a BooleanArray indicating
    /// which rows MATCH the predicate (i.e., rows to be deleted in a MoR DELETE).
    ///
    /// Unlike `filter_batch_negate`, this returns the raw match mask rather than the
    /// filtered batch, so the caller can record which row positions matched.
    ///
    /// `joins_sql` carries the `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`]; see `filter_batch_negate` for details.
    ///
    /// `predicates` augments the user's WHERE with the policy row filter and
    /// wraps masked columns so the predicate evaluates against masked values.
    #[allow(clippy::too_many_arguments)]
    async fn filter_batch_match(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<arrow_array::BooleanArray> {
        use arrow_array::BooleanArray;

        let table_name = format!("__mor_delete_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        let augmented_where = augment_where_with_row_filter(where_sql, predicates);
        let from_clause = build_policy_from_clause(
            &format!("datafusion.public.{table_name}"),
            orig_name,
            &batch.schema(),
            predicates,
            false,
        );

        // Alias the scratch table to the original target name so correlated
        // subqueries inside WHERE can reference `tablename.col`.
        let eval_sql = format!(
            "SELECT CAST(({augmented_where}) AS BOOLEAN) AS __match \
             FROM {from_clause}{joins_sql}"
        );
        let df = ctx
            .sql(&eval_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}")))?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}")))?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        if result_batches.is_empty() || result_batches[0].num_rows() == 0 {
            // No rows matched
            return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
        }

        let mask_batch = &result_batches[0];
        let match_col = mask_batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                SqeError::Execution("WHERE evaluation did not produce a boolean column".into())
            })?
            .clone();

        Ok(match_col)
    }

    /// Apply UPDATE SET assignments to a RecordBatch using DataFusion SQL evaluation.
    ///
    /// For each column, generates CASE WHEN <where> THEN <new_value> ELSE <old_value> END.
    /// Unchanged columns pass through directly.
    ///
    /// `in_subquery_joins` carries the `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`] and is appended to the outer SELECT's FROM
    /// clause after any decorrelator-generated joins. Pass an empty string when
    /// no lifted joins are needed.
    #[allow(clippy::too_many_arguments)]
    async fn apply_update(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        assignments: &[sqlparser::ast::Assignment],
        where_sql: &str,
        in_subquery_joins: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<RecordBatch> {
        let table_name = format!("__update_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Best-effort decorrelation of any `ScalarSubquery` nodes in the SET
        // expressions. DataFusion's physical planner cannot compile scalar
        // subqueries that survive inside a CASE WHEN ... THEN (subquery) ELSE
        // col END projection, so we rewrite recognised correlated-equality
        // shapes into LEFT JOINs at the outer FROM. Shapes we don't recognise
        // are left alone and will surface DataFusion's original error: no
        // change in behaviour for them.
        let (decorrelated, extra_joins) = decorrelate_scalar_subqueries(assignments, orig_name);
        let mut assignment_map = std::collections::HashMap::new();
        for d in &decorrelated {
            assignment_map.insert(d.col_name.clone(), d.expr_sql.clone());
        }

        let augmented_where = augment_where_with_row_filter(where_sql, predicates);

        // Build SELECT with CASE expressions for assigned columns. Masked
        // columns require special handling: the ELSE branch must reference
        // the raw alias so unchanged rows keep their pre-mask values on
        // disk. The aliased subquery exposes `__pol_raw_<col>` for every
        // masked column in addition to the masked projection.
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let col = f.name().clone();
                let is_masked = predicates.column_mask_sqls.contains_key(&col);
                let else_ref = if is_masked {
                    format!("\"{}\"", policy_raw_alias(&col))
                } else {
                    format!("\"{col}\"")
                };
                if let Some(expr) = assignment_map.get(&col) {
                    format!(
                        "CASE WHEN ({augmented_where}) THEN ({expr}) ELSE {else_ref} END AS \"{col}\""
                    )
                } else if is_masked {
                    format!("{else_ref} AS \"{col}\"")
                } else {
                    format!("\"{col}\"")
                }
            })
            .collect();

        // Alias the scratch table back to the UPDATE target's original name so
        // correlated subqueries inside the SET expression can reference it.
        // e.g. `SET x = (SELECT ... WHERE ... = holding_summary.hs_ca_id)` needs
        // `holding_summary` to be in scope; without the alias DataFusion only
        // sees `__update_holding_summary` and fails to resolve the correlation.
        //
        // Two join sources get appended to the outer FROM clause:
        //   1. `extra_joins` from the decorrelator above: these provide the
        //      `__corrN.__val` columns substituted into the SET expressions.
        //   2. `in_subquery_joins` from `lift_in_subqueries`: these provide the
        //      `__sqN.__matched` flags referenced from the rewritten WHERE.
        // Decorrelator joins come first so any columns they introduce are in
        // scope for the IN-subquery join ON clauses (not currently exercised,
        // but preserves a consistent ordering).
        let joins_sql = if extra_joins.is_empty() {
            in_subquery_joins.to_string()
        } else {
            format!(" {}{}", extra_joins.join(" "), in_subquery_joins)
        };
        let from_clause = build_policy_from_clause(
            &format!("datafusion.public.{table_name}"),
            orig_name,
            &batch.schema(),
            predicates,
            true,
        );
        let select_sql = format!(
            "SELECT {cols} FROM {from_clause}{joins}",
            cols = columns.join(", "),
            joins = joins_sql,
        );
        let df = ctx
            .sql(&select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to evaluate UPDATE: {e}")))?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to collect UPDATE results: {e}")))?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        // Return the first (and only) result batch
        result_batches
            .into_iter()
            .next()
            .ok_or_else(|| SqeError::Execution("UPDATE produced no output batches".to_string()))
    }

    /// Count rows matching a WHERE clause in a batch.
    ///
    /// `joins_sql` carries the `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`]; see `filter_batch_negate` for details.
    /// `predicates` augments the count with the policy row filter and
    /// column masks so the affected-row count matches what was actually
    /// modified.
    #[allow(clippy::too_many_arguments)]
    async fn count_matching_rows(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<usize> {
        let table_name = format!("__count_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("MemTable error: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Register error: {e}")))?;

        let augmented_where = augment_where_with_row_filter(where_sql, predicates);
        let from_clause = build_policy_from_clause(
            &format!("datafusion.public.{table_name}"),
            orig_name,
            &batch.schema(),
            predicates,
            false,
        );

        // Alias the scratch table to the original target name (see apply_update
        // for rationale): allows `tablename.col` references in WHERE subqueries
        // to resolve correctly.
        let sql = format!(
            "SELECT COUNT(*) AS cnt \
             FROM {from_clause}{joins_sql} \
             WHERE {augmented_where}"
        );
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Count query failed: {e}")))?;
        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Count collect failed: {e}")))?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        let count = batches
            .first()
            .and_then(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
            })
            .map(|a| a.value(0) as usize)
            .unwrap_or(0);
        Ok(count)
    }

    /// Lift every `IN (subquery)` in `where_sql` into a LEFT JOIN over a
    /// pre-materialised DISTINCT keyset.
    ///
    /// Replaces the old literal-inlining rewriter. The old rewriter materialised
    /// each subquery into an `IN (v1, v2, ...)` or OR-of-ANDs list, producing
    /// O(N) plan text for N matching rows. TPC-E SF10 at 34,496 tuples crashed
    /// the coordinator with a stack overflow (see the regression test at
    /// `tests/in_subquery_or_stack_overflow.rs` for the failure mode).
    ///
    /// The new approach:
    ///
    /// 1. Execute each subquery once, projecting its columns as `__col0..__colK`
    ///    plus a constant `TRUE AS __matched` column, with NULL rows dropped and
    ///    DISTINCT applied.
    /// 2. Register the result as a scratch `MemTable` named `__sqe_in_subq_<id>`
    ///    where `<id>` is a process-global monotonic counter.
    /// 3. Emit a `LEFT JOIN` clause against the scratch table on the LHS columns.
    /// 4. Replace the original `IN (subquery)` node with
    ///    `COALESCE("__sq<alias_id>"."__matched", FALSE)`, wrapped in `NOT` for
    ///    `NOT IN`.
    ///
    /// Returns:
    /// - The rewritten WHERE string (O(1) in subquery cardinality).
    /// - A concatenated JOIN clause string to append to the outer SELECT's FROM.
    /// - An RAII guard that deregisters every scratch table on drop.
    ///
    /// Fast path: if `where_sql` contains no `SELECT` token, returns
    /// `(where_sql.to_string(), "", empty_guard)` with zero overhead. Matches
    /// the fast-path behaviour of the old rewriter.
    ///
    /// NULL semantics: rows where any matcher column is NULL are dropped from
    /// the scratch keyset, and outer rows with NULL in matcher columns do not
    /// match. This preserves the behaviour of the old rewriter (which skipped
    /// NULL subquery rows at the Rust level) and is a deliberate deviation from
    /// strict SQL `IN`/`NOT IN` semantics in the presence of NULLs.
    async fn lift_in_subqueries(
        &self,
        where_sql: &str,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<(String, String, InSubqueryCleanup)> {
        lift_in_subqueries(where_sql, ctx).await
    }
}

// ---------------------------------------------------------------------------
// Free-function form of the IN-subquery lifter.
//
// `WriteHandler::lift_in_subqueries` is a thin shim that delegates here. The
// method has no `self` dependencies; keeping the implementation free-standing
// lets integration tests drive it directly without constructing a full
// `SqeConfig` + `WriteHandler`.
// ---------------------------------------------------------------------------

/// See [`WriteHandler::lift_in_subqueries`] for semantics. Exposed for the
/// integration test in `tests/in_subquery_view_rewrite.rs`.
#[doc(hidden)]
pub async fn lift_in_subqueries(
    where_sql: &str,
    ctx: &DFSessionContext,
) -> sqe_core::Result<(String, String, InSubqueryCleanup)> {
    use datafusion::datasource::MemTable;
    use sqlparser::ast::SetExpr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    // Fast path: most WHERE clauses don't contain subqueries.
    if !where_sql.to_uppercase().contains("SELECT") {
        return Ok((
            where_sql.to_string(),
            String::new(),
            InSubqueryCleanup::empty(ctx),
        ));
    }

    // Parse the WHERE expression by wrapping it in a dummy SELECT.
    let dummy_sql = format!("SELECT * FROM __dummy WHERE {where_sql}");
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &dummy_sql)
        .map_err(|e| SqeError::Execution(format!("IN-subquery lift parse error: {e}")))?;

    let where_expr_opt = match stmts.first_mut() {
        Some(Statement::Query(q)) => match q.body.as_mut() {
            SetExpr::Select(sel) => sel.selection.take(),
            _ => {
                return Ok((
                    where_sql.to_string(),
                    String::new(),
                    InSubqueryCleanup::empty(ctx),
                ));
            }
        },
        _ => {
            return Ok((
                where_sql.to_string(),
                String::new(),
                InSubqueryCleanup::empty(ctx),
            ));
        }
    };

    let mut expr = match where_expr_opt {
        Some(e) => e,
        None => {
            return Ok((
                where_sql.to_string(),
                String::new(),
                InSubqueryCleanup::empty(ctx),
            ));
        }
    };

    // Walk the AST and replace every `InSubquery` node with a sentinel
    // identifier. The sentinel is a plain `Expr::Identifier` containing a
    // unique token; text-substitution at the end swaps each token for the
    // real COALESCE expression. Using a sentinel keeps the rewritten AST
    // depth O(1) in subquery cardinality and avoids the sqlparser-Display
    // trick that caused the old rewriter's stack overflow.
    let mut found: Vec<LiftedSubquery> = Vec::new();
    collect_and_sentinel_in_subqueries(&mut expr, &mut found);

    if found.is_empty() {
        return Ok((
            where_sql.to_string(),
            String::new(),
            InSubqueryCleanup::empty(ctx),
        ));
    }

    let mut joins: Vec<String> = Vec::with_capacity(found.len());
    let mut scratch_names: Vec<String> = Vec::with_capacity(found.len());
    let mut replacements: Vec<(String, String)> = Vec::with_capacity(found.len());

    for (alias_idx, lifted) in found.into_iter().enumerate() {
        let LiftedSubquery {
            lhs_cols,
            subquery_text,
            negated,
            sentinel,
        } = lifted;
        let num_cols = lhs_cols.len();

        // Preflight: get the subquery's output column names so we can alias
        // them positionally as __col0..__colN. Column names in the
        // subquery's projection may be arbitrary (e.g. `t.t_ca_id`); we do
        // not depend on them beyond this preflight.
        let preflight_sql = format!("SELECT * FROM ({subquery_text}) AS __sq");
        let pre_df = ctx.sql(&preflight_sql).await.map_err(|e| {
            SqeError::Execution(format!(
                "IN-subquery preflight failed for `{preflight_sql}`: {e}"
            ))
        })?;
        let subq_cols: Vec<String> = pre_df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        drop(pre_df);

        if subq_cols.len() != num_cols {
            return Err(SqeError::Execution(format!(
                "IN-subquery arity mismatch: LHS has {num_cols} column(s), \
                     subquery returns {} column(s)",
                subq_cols.len()
            )));
        }

        // Build the scratch-materialiser SQL. Using `__sq."<name>"` avoids
        // ambiguity when the subquery contains joins whose output column
        // names collide; wrapping in `FROM ({subquery}) AS __sq` flattens
        // the projection to a single relation.
        let projections: Vec<String> = subq_cols
            .iter()
            .enumerate()
            .map(|(i, name)| {
                format!(
                    "\"__sq\".\"{esc_name}\" AS \"__col{i}\"",
                    esc_name = name.replace('"', "\"\""),
                )
            })
            .collect();
        let null_filters: Vec<String> = subq_cols
            .iter()
            .map(|name| {
                format!(
                    "\"__sq\".\"{esc_name}\" IS NOT NULL",
                    esc_name = name.replace('"', "\"\""),
                )
            })
            .collect();
        let materialiser_sql = format!(
            "SELECT DISTINCT {projs}, TRUE AS \"__matched\" \
                 FROM ({sub}) AS __sq \
                 WHERE {filters}",
            projs = projections.join(", "),
            sub = subquery_text,
            filters = null_filters.join(" AND "),
        );

        // Execute the materialiser and wrap its output into a MemTable.
        // The DataFrame's schema is taken before `collect()` consumes it so
        // we can build the MemTable even when the subquery returns zero
        // rows (in which case the scratch table has the right shape but no
        // data; every LEFT JOIN lookup misses and `COALESCE(..., FALSE)`
        // evaluates to FALSE — i.e. `IN (empty)` is FALSE and `NOT IN
        // (empty)` is TRUE, matching the old rewriter's behaviour).
        let mat_df = ctx.sql(&materialiser_sql).await.map_err(|e| {
            SqeError::Execution(format!("IN-subquery materialiser plan failed: {e}"))
        })?;
        let schema: Arc<ArrowSchema> = Arc::new(mat_df.schema().as_arrow().clone());
        let batches = mat_df.collect().await.map_err(|e| {
            SqeError::Execution(format!("IN-subquery materialiser execution failed: {e}"))
        })?;

        let mem = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            SqeError::Execution(format!(
                "Failed to build MemTable for IN-subquery keyset: {e}"
            ))
        })?;

        // Register the scratch MemTable under the built-in `datafusion.public`
        // catalog/schema rather than the session's default. In production the
        // DML handler hands us a `SessionContext` whose default catalog is the
        // Iceberg catalog, whose `SchemaProvider` rejects `register_table` with
        // "schema provider does not support registering tables". The other
        // scratch-table call sites in this file (`filter_batch_negate`,
        // `filter_batch_match`, `apply_update`, `count_matching_rows`) already
        // use this qualified path for the same reason — see commit 725a47c
        // "fix: register DML temp tables in datafusion catalog, not Iceberg
        // catalog". Keep register, JOIN reference, and deregister in lockstep
        // so the Drop impl on `InSubqueryCleanup` releases the same table it
        // registered.
        let counter_id = IN_SUBQUERY_COUNTER.fetch_add(1, Ordering::Relaxed);
        let scratch_name = format!("__sqe_in_subq_{counter_id}");
        let qualified_scratch = format!("datafusion.public.{scratch_name}");
        ctx.register_table(qualified_scratch.as_str(), Arc::new(mem))
            .map_err(|e| {
                SqeError::Execution(format!(
                    "Failed to register IN-subquery scratch MemTable: {e}"
                ))
            })?;
        scratch_names.push(qualified_scratch.clone());

        // Build the LEFT JOIN clause and the replacement expression text.
        // The per-statement alias `__sqN` is bounded by `found.len()` and
        // keeps the JOIN's ON clause readable in debug logs. The scratch
        // table is referenced through its fully-qualified catalog path to
        // match the registration above; a bare name would resolve against
        // the session's default catalog (Iceberg) and fail at plan time.
        let alias = format!("__sq{alias_idx}");
        let on_clauses: Vec<String> = (0..num_cols)
            .map(|i| format!("\"{alias}\".\"__col{i}\" = {lhs}", lhs = lhs_cols[i],))
            .collect();
        let join_clause = format!(
            " LEFT JOIN datafusion.public.\"{scratch_name}\" AS \"{alias}\" ON {on}",
            on = on_clauses.join(" AND "),
        );
        joins.push(join_clause);

        let coalesce_sql = format!("COALESCE(\"{alias}\".\"__matched\", FALSE)");
        let replacement = if negated {
            format!("(NOT {coalesce_sql})")
        } else {
            coalesce_sql
        };
        replacements.push((sentinel, replacement));
    }

    // Stringify the modified WHERE AST. Depth is O(1) in subquery
    // cardinality because every `InSubquery` node is now a single
    // `Expr::Identifier` sentinel.
    let mut rewritten = format!("{expr}");
    for (sentinel, replacement) in &replacements {
        rewritten = rewritten.replace(sentinel, replacement);
    }

    tracing::info!(
        original = %&where_sql[..where_sql.len().min(200)],
        rewritten = %&rewritten[..rewritten.len().min(200)],
        subquery_count = replacements.len(),
        "Lifted IN (subquery) into LEFT JOIN(s) for DML WHERE clause"
    );

    Ok((
        rewritten,
        joins.join(""),
        InSubqueryCleanup {
            ctx: ctx.clone(),
            scratch_tables: scratch_names,
        },
    ))
}

impl WriteHandler {
    fn format_version(&self) -> FormatVersion {
        match self.config.catalog.default_table_format_version {
            3 => FormatVersion::V3,
            1 => FormatVersion::V1,
            _ => FormatVersion::V2,
        }
    }

    /// Table-creation properties carrying the desired Iceberg format version.
    ///
    /// The reserved `format-version` property is a REST-catalog workaround:
    /// iceberg-rust's REST `create_table` does not translate
    /// `TableCreation.format_version` into the request, so the version is
    /// communicated via the property instead. Every non-REST catalog (Glue,
    /// S3 Tables, HMS, JDBC) reads `TableCreation.format_version` directly and
    /// REJECTS `format-version` as a reserved property
    /// (`Table properties should not contain reserved properties`). So we only
    /// emit the property for the REST backend; other backends rely solely on
    /// the `.format_version(...)` builder field.
    fn format_version_props(
        &self,
        format_version: FormatVersion,
    ) -> std::collections::HashMap<String, String> {
        format_version_props_for_backend(&self.config.catalog.backend, format_version)
    }

    /// Create a `SessionCatalogBridge` (which implements `iceberg::Catalog`)
    /// for the given session's write target.
    ///
    /// `target_warehouse` is the catalog qualifier of a 3-part target
    /// (`catalog.namespace.table`), or `None` for an unqualified target. When it
    /// names a non-default catalog and `catalog_discovery = polaris-auto` is on,
    /// the bridge is built against THAT catalog -- discovered via Polaris with the
    /// caller's bearer, mirroring the read path's `discover_catalog_provider` --
    /// instead of the configured default warehouse. Symmetric with the read
    /// path: an explicit catalog Polaris cannot resolve is an "unknown catalog"
    /// error, not a silent fall-through to the default warehouse.
    /// Resolve the typed [`SessionCatalog`] for a write target.
    ///
    /// `target_warehouse` is the catalog qualifier resolved from the table
    /// reference via [`resolve_target_catalog`]: `Some(w)` selects a
    /// discovered/non-default catalog; `None` (or the configured default)
    /// uses the session's default warehouse. Returns the concrete
    /// `Arc<SessionCatalog>` so the UPDATE/DELETE/MERGE handlers (which thread
    /// `Arc<SessionCatalog>`, not `dyn Catalog`) can load and commit against
    /// the right warehouse; [`Self::create_catalog_bridge`] wraps the same
    /// value as `dyn Catalog` for the INSERT/CTAS paths.
    async fn resolve_session_catalog(
        &self,
        session: &Session,
        target_warehouse: Option<&str>,
    ) -> sqe_core::Result<Arc<SessionCatalog>> {
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

        // Warm up the REST catalog by listing namespaces. The RisingWave fork's
        // RestCatalog requires this initial API call to bootstrap its internal
        // session state before load_table works correctly.
        let _ = session_catalog.list_namespaces().await;

        Ok(session_catalog)
    }

    async fn create_catalog_bridge(
        &self,
        session: &Session,
        target_warehouse: Option<&str>,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        Ok(self
            .resolve_session_catalog(session, target_warehouse)
            .await?
            .as_catalog())
    }
}

// ---------------------------------------------------------------------------
// IN-subquery view-lift: RAII cleanup + AST sentinelisation
// ---------------------------------------------------------------------------

/// Process-global monotonic counter for naming scratch MemTables registered
/// by [`WriteHandler::lift_in_subqueries`]. A plain atomic is sufficient:
/// DML statements across sessions never share scratch tables, and the counter
/// only has to guarantee distinctness, not ordering.
static IN_SUBQUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII handle that deregisters a set of scratch MemTables on drop.
///
/// Returned by [`WriteHandler::lift_in_subqueries`] and bound by the DML
/// handler so it outlives the per-batch SELECT loop. On drop, every
/// registered scratch table is deregistered from the session context.
///
/// `scratch_tables` stores the fully-qualified `datafusion.public.<name>`
/// path used at registration, so `deregister_table` resolves to the same
/// slot regardless of what the session's default catalog is (in production
/// that default is the Iceberg catalog).
///
/// Deregister errors are logged at `warn!` and swallowed: matches the
/// existing scratch-table cleanup behaviour inside `filter_batch_negate`,
/// `filter_batch_match`, `apply_update`, and `count_matching_rows` (see the
/// `let _ = ctx.deregister_table(...)` calls in this file).
#[doc(hidden)]
pub struct InSubqueryCleanup {
    ctx: DFSessionContext,
    /// Fully-qualified `datafusion.public.<scratch_name>` paths that Drop
    /// will deregister. Storing the qualified form (not the bare name)
    /// mirrors the registration path in `lift_in_subqueries`.
    scratch_tables: Vec<String>,
}

impl InSubqueryCleanup {
    /// Build a cleanup guard that holds no scratch tables. Used on fast-path
    /// returns from `lift_in_subqueries` where no subqueries were found.
    fn empty(ctx: &DFSessionContext) -> Self {
        Self {
            ctx: ctx.clone(),
            scratch_tables: Vec::new(),
        }
    }
}

impl Drop for InSubqueryCleanup {
    fn drop(&mut self) {
        for name in &self.scratch_tables {
            if let Err(e) = self.ctx.deregister_table(name.as_str()) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "in-subquery scratch deregister failed"
                );
            }
        }
    }
}

/// RAII guard that deregisters MERGE scratch MemTables on drop.
///
/// MERGE writes register target and source RecordBatches as MemTables in
/// the shared session context. Two concurrent MERGEs in the same session
/// would clobber each other's registrations if the names were not unique
/// per invocation. The handler embeds a `Uuid::now_v7().simple()` suffix
/// into the scratch names; this guard ensures the registrations are
/// removed even if a `?` short-circuits the happy path.
struct MergeScratchCleanup {
    ctx: DFSessionContext,
    scratch_tables: Vec<String>,
}

impl MergeScratchCleanup {
    fn new(ctx: &DFSessionContext, scratch_tables: Vec<String>) -> Self {
        Self {
            ctx: ctx.clone(),
            scratch_tables,
        }
    }
}

impl Drop for MergeScratchCleanup {
    fn drop(&mut self) {
        for name in &self.scratch_tables {
            if let Err(e) = self.ctx.deregister_table(name.as_str()) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "merge scratch deregister failed"
                );
            }
        }
    }
}

/// One `IN (subquery)` occurrence that the lifter needs to materialise.
struct LiftedSubquery {
    /// LHS columns as SQL text. For `col IN (...)` this has length 1; for
    /// `(c1, c2) IN (...)` it has length 2. The strings are used verbatim in
    /// the JOIN ON clause, so qualified references like `target.col` are
    /// preserved.
    lhs_cols: Vec<String>,
    /// Text of the parenthesised subquery (without outer `IN (` wrapper).
    subquery_text: String,
    /// Whether the original expression was `NOT IN`.
    negated: bool,
    /// Unique sentinel token substituted into the WHERE string. The lifter
    /// replaces the `Expr::InSubquery` AST node with `Expr::Identifier(sentinel)`
    /// before stringifying; after stringification the sentinel is swapped for
    /// the real `COALESCE(...)` expression text.
    sentinel: String,
}

/// Walk `expr` recursively. For every `InSubquery { expr, subquery, negated }`
/// found, collect its LHS column list, subquery text, and a unique sentinel
/// token, then replace the node with `Expr::Identifier(sentinel)` (no wrap:
/// the `negated` flag is encoded in the replacement text that gets
/// substituted later, not in the AST).
///
/// The sentinel token uses a `__SQE_IN_SUBQ_SENTINEL_<idx>__` form that does
/// not appear in user SQL and is stable across `Display` impls.
fn collect_and_sentinel_in_subqueries(
    expr: &mut sqlparser::ast::Expr,
    out: &mut Vec<LiftedSubquery>,
) {
    use sqlparser::ast::{Expr, Ident};

    match expr {
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let neg = *negated;
            let subquery_text = format!("{subquery}");
            let lhs_cols: Vec<String> = match inner.as_ref() {
                Expr::Tuple(items) => items.iter().map(|e| format!("{e}")).collect(),
                other => vec![format!("{other}")],
            };
            let idx = out.len();
            let sentinel = format!("__SQE_IN_SUBQ_SENTINEL_{idx}__");
            out.push(LiftedSubquery {
                lhs_cols,
                subquery_text,
                negated: neg,
                sentinel: sentinel.clone(),
            });
            *expr = Expr::Identifier(Ident::new(sentinel));
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_and_sentinel_in_subqueries(left, out);
            collect_and_sentinel_in_subqueries(right, out);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_and_sentinel_in_subqueries(inner, out);
        }
        Expr::Nested(inner) => {
            collect_and_sentinel_in_subqueries(inner, out);
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_and_sentinel_in_subqueries(e, out);
            collect_and_sentinel_in_subqueries(low, out);
            collect_and_sentinel_in_subqueries(high, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_and_sentinel_in_subqueries(op, out);
            }
            // sqlparser 0.62: each WHEN/THEN pair is a CaseWhen { condition, result }
            // (previously parallel `conditions` and `results` Vecs).
            for c in conditions {
                collect_and_sentinel_in_subqueries(&mut c.condition, out);
                collect_and_sentinel_in_subqueries(&mut c.result, out);
            }
            if let Some(e) = else_result {
                collect_and_sentinel_in_subqueries(e, out);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Correlated ScalarSubquery decorrelator (UPDATE SET)
// ---------------------------------------------------------------------------
//
// DataFusion's physical planner does not compile `Expr::ScalarSubquery` that
// survives into DML. The `ScalarSubqueryToJoin` optimizer rule rewrites most
// projection-scoped scalar subqueries, but it does not reach subqueries buried
// inside a `CASE WHEN cond THEN expr ELSE col END` — which is the exact shape
// `apply_update` generates from an `UPDATE ... SET col = <expr>` statement.
//
// We decorrelate here at the sqlparser-AST level for a narrow shape:
//
//     SET col = <expr_with>(SELECT <scalar>
//                           FROM <tables>
//                           WHERE <target>.k1 = <sub_alias>.k1'
//                             AND <target>.k2 = <sub_alias>.k2'
//                             AND <local_preds>
//                           [LIMIT 1])
//
// Rewritten to:
//
//     SET col = <expr_with> "__corrN"."__val"
//     (+ LEFT JOIN (
//          SELECT <sub_alias>.k1' AS __k0, <sub_alias>.k2' AS __k1,
//                 MAX(<scalar>)    AS __val
//          FROM <tables>
//          WHERE <local_preds>
//          GROUP BY <sub_alias>.k1', <sub_alias>.k2'
//        ) AS "__corrN"
//        ON __corrN.__k0 = <target>.k1 AND __corrN.__k1 = <target>.k2)
//
// The `MAX` aggregate is an approximation of `LIMIT 1`: any one value per
// correlation group. For UPDATE statements that expect a specific row
// (ORDER BY + LIMIT 1), MAX may pick a different row, so behaviour differs
// from e.g. PostgreSQL's UPDATE FROM JOIN semantics. Use only when the
// subquery is a simple correlated lookup.
//
// Any shape not matching the above (non-equality correlation, non-scalar
// projection, nested subqueries, no correlation at all) leaves the
// assignment unchanged — current behaviour then surfaces DataFusion's clear
// "ScalarSubquery not implemented" error.

/// Returned by [`decorrelate_scalar_subqueries`] for each SET assignment.
pub(crate) struct DecorrelatedAssignment {
    /// Target column name.
    pub col_name: String,
    /// Assignment RHS expression (with correlated scalar subqueries replaced
    /// by `"__corrN"."__val"` column references if decorrelation succeeded).
    pub expr_sql: String,
}

/// Walks the `assignments`, attempts to decorrelate every correlated
/// ScalarSubquery it finds, and returns the rewritten assignments plus the
/// LEFT JOIN clauses to append to the outer SELECT's FROM.
pub(crate) fn decorrelate_scalar_subqueries(
    assignments: &[sqlparser::ast::Assignment],
    target_name: &str,
) -> (Vec<DecorrelatedAssignment>, Vec<String>) {
    use sqlparser::ast::AssignmentTarget;

    let mut out_assignments: Vec<DecorrelatedAssignment> = Vec::with_capacity(assignments.len());
    let mut joins: Vec<String> = Vec::new();
    let mut next_idx = 0usize;

    for a in assignments {
        let col_name = match &a.target {
            AssignmentTarget::ColumnName(name) => format!("{name}"),
            AssignmentTarget::Tuple(names) => {
                names.first().map(|n| format!("{n}")).unwrap_or_default()
            }
        };
        let mut expr = a.value.clone();
        rewrite_subqueries_in_expr(&mut expr, target_name, &mut next_idx, &mut joins);
        out_assignments.push(DecorrelatedAssignment {
            col_name,
            expr_sql: format!("{expr}"),
        });
    }

    (out_assignments, joins)
}

/// Recursively walk an Expr looking for `Expr::Subquery(Box<Query>)` nodes and
/// try to decorrelate each. On success, the node is replaced with a compound
/// identifier pointing at the joined lookup column; on failure, the node is
/// left untouched so the caller's current error path still fires.
fn rewrite_subqueries_in_expr(
    expr: &mut sqlparser::ast::Expr,
    target_name: &str,
    next_idx: &mut usize,
    joins: &mut Vec<String>,
) {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Subquery(q) => {
            if let Some((join_sql, alias)) = try_decorrelate_query(q, target_name, *next_idx) {
                joins.push(join_sql);
                *expr = Expr::CompoundIdentifier(vec![
                    sqlparser::ast::Ident::with_quote('"', alias),
                    sqlparser::ast::Ident::with_quote('"', "__val".to_string()),
                ]);
                *next_idx += 1;
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_subqueries_in_expr(left, target_name, next_idx, joins);
            rewrite_subqueries_in_expr(right, target_name, next_idx, joins);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            rewrite_subqueries_in_expr(inner, target_name, next_idx, joins);
        }
        Expr::Nested(inner) => {
            rewrite_subqueries_in_expr(inner, target_name, next_idx, joins);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                rewrite_subqueries_in_expr(op, target_name, next_idx, joins);
            }
            // sqlparser 0.62: WHEN/THEN pairs live in CaseWhen { condition, result }.
            for c in conditions {
                rewrite_subqueries_in_expr(&mut c.condition, target_name, next_idx, joins);
                rewrite_subqueries_in_expr(&mut c.result, target_name, next_idx, joins);
            }
            if let Some(e) = else_result {
                rewrite_subqueries_in_expr(e, target_name, next_idx, joins);
            }
        }
        Expr::Function(_) => {
            // Most function arguments are expressions; the full sqlparser
            // walker is complex. For now we skip — correlated subqueries
            // buried inside function calls remain un-decorrelated and surface
            // DataFusion's original error, which is an acceptable fallback.
        }
        _ => {}
    }
}

/// Attempt to decorrelate a single scalar subquery against `target_name`.
/// Returns `(join_sql, alias)` when the shape matches, or `None` to skip.
fn try_decorrelate_query(
    q: &sqlparser::ast::Query,
    target_name: &str,
    idx: usize,
) -> Option<(String, String)> {
    use sqlparser::ast::{Expr, SelectItem, SetExpr};

    // Only plain SELECT (no UNION, no VALUES).
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => return None,
    };

    // Require a single unnamed scalar projection.
    if select.projection.len() != 1 {
        return None;
    }
    let scalar_expr_sql = match &select.projection[0] {
        SelectItem::UnnamedExpr(e) => format!("{e}"),
        SelectItem::ExprWithAlias { expr, .. } => format!("{expr}"),
        _ => return None,
    };

    // Need a WHERE clause — correlation has to live somewhere.
    let where_expr = select.selection.as_ref()?;

    // Partition the WHERE conjuncts into correlation predicates (equality
    // between a target-table column and a subquery-side column) and local
    // predicates (everything else).
    let mut conjuncts: Vec<&Expr> = Vec::new();
    collect_and_conjuncts(where_expr, &mut conjuncts);

    let mut correlation: Vec<(String, String)> = Vec::new();
    let mut local_preds: Vec<String> = Vec::new();
    for c in conjuncts {
        if let Some((t_col, s_col)) = extract_correlation_eq(c, target_name) {
            correlation.push((t_col, s_col));
        } else {
            local_preds.push(format!("{c}"));
        }
    }
    if correlation.is_empty() {
        return None;
    }

    // Rebuild FROM clause verbatim (sqlparser's Display handles joins).
    let from_sql = select
        .from
        .iter()
        .map(|t| format!("{t}"))
        .collect::<Vec<_>>()
        .join(", ");
    if from_sql.is_empty() {
        return None;
    }

    let alias = format!("__corr{idx}");
    let select_cols: Vec<String> = correlation
        .iter()
        .enumerate()
        .map(|(i, (_, s))| format!("({s}) AS __k{i}"))
        .collect();
    let group_by: Vec<String> = correlation.iter().map(|(_, s)| s.clone()).collect();

    let where_sql = if local_preds.is_empty() {
        "TRUE".to_string()
    } else {
        local_preds.join(" AND ")
    };

    let decorr_sql = format!(
        "SELECT {cols}, MAX({scalar}) AS __val FROM {from} WHERE {where_} GROUP BY {group_by}",
        cols = select_cols.join(", "),
        scalar = scalar_expr_sql,
        from = from_sql,
        where_ = where_sql,
        group_by = group_by.join(", "),
    );

    let on_clauses: Vec<String> = correlation
        .iter()
        .enumerate()
        .map(|(i, (t_col, _))| format!("\"{alias}\".__k{i} = \"{target_name}\".{t_col}"))
        .collect();

    let join_sql = format!(
        "LEFT JOIN ({sub}) AS \"{alias}\" ON {on}",
        sub = decorr_sql,
        on = on_clauses.join(" AND "),
    );

    Some((join_sql, alias))
}

/// Flatten `a AND b AND c ...` into `[a, b, c]`.
fn collect_and_conjuncts<'a>(
    expr: &'a sqlparser::ast::Expr,
    out: &mut Vec<&'a sqlparser::ast::Expr>,
) {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_conjuncts(left, out);
            collect_and_conjuncts(right, out);
        }
        Expr::Nested(inner) => collect_and_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// If `pred` is `<target>.col = <alias>.col'` (or the reversed form), return
/// `Some((<target>.col, <alias>.col'))`. The `<target>.col` is returned as
/// just the column name (outer target alias is the UPDATE target itself and
/// applied by the caller when building the ON clause).
fn extract_correlation_eq(
    pred: &sqlparser::ast::Expr,
    target_name: &str,
) -> Option<(String, String)> {
    use sqlparser::ast::{BinaryOperator, Expr};

    let (left, right) = match pred {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        Expr::Nested(inner) => return extract_correlation_eq(inner, target_name),
        _ => return None,
    };

    let left_ref = compound_ident_parts(left);
    let right_ref = compound_ident_parts(right);
    match (left_ref, right_ref) {
        (Some((lq, lc)), Some((rq, rc))) if lq == target_name && rq != target_name => {
            Some((lc, format!("{rq}.{rc}")))
        }
        (Some((lq, lc)), Some((rq, rc))) if rq == target_name && lq != target_name => {
            Some((rc, format!("{lq}.{lc}")))
        }
        _ => None,
    }
}

/// If `expr` is a two-part compound identifier `a.b`, return `(a, b)`.
fn compound_ident_parts(expr: &sqlparser::ast::Expr) -> Option<(String, String)> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((parts[0].value.clone(), parts[1].value.clone()))
        }
        _ => None,
    }
}

/// Convert a sqlparser SQL data type to an Arrow DataType.
pub(crate) fn sql_type_to_arrow(
    sql_type: &sqlparser::ast::DataType,
) -> sqe_core::Result<arrow_schema::DataType> {
    use arrow_schema::DataType;
    use sqe_sql::{detect_ns_timestamp, NsTimestamp};
    use sqlparser::ast::DataType as SqlType;

    // V3 nanosecond timestamps arrive as `DataType::Custom` from sqlparser.
    // Route them through the sqe-sql helper so the mapping stays in one place.
    if let Some(kind) = detect_ns_timestamp(sql_type) {
        return Ok(match kind {
            NsTimestamp::WithoutTz => DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            NsTimestamp::WithTz => {
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()))
            }
        });
    }

    match sql_type {
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::TinyInt(_) | SqlType::Int8(_) => Ok(DataType::Int8),
        SqlType::SmallInt(_) | SqlType::Int16 => Ok(DataType::Int16),
        SqlType::Int(_) | SqlType::Integer(_) | SqlType::Int32 => Ok(DataType::Int32),
        SqlType::BigInt(_) | SqlType::Int64 => Ok(DataType::Int64),
        SqlType::Float(_) | SqlType::Real => Ok(DataType::Float32),
        SqlType::Double(_) | SqlType::DoublePrecision => Ok(DataType::Float64),
        SqlType::Varchar(_) | SqlType::CharVarying(_) | SqlType::Text | SqlType::String(_) => {
            Ok(DataType::Utf8)
        }
        SqlType::Char(_) | SqlType::Character(_) => Ok(DataType::Utf8),
        // UUID has no native Arrow logical type; alias it to Utf8 (the
        // canonical string form). Equality, regex, and CAST(... AS UUID)
        // all work transparently against the string representation.
        // Matches the same pattern as JSON above.
        SqlType::Uuid => Ok(DataType::Utf8),
        SqlType::Binary(_) | SqlType::Varbinary(_) | SqlType::Bytea => Ok(DataType::Binary),
        SqlType::Date => Ok(DataType::Date32),
        SqlType::Timestamp(precision, tz_info) => {
            let p = precision.unwrap_or(6);
            let tz = if sqe_sql::is_tz_variant(tz_info) {
                Some("UTC".into())
            } else {
                None
            };
            match p {
                0..=3 => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz)),
                4..=6 => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz)),
                _ => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz)),
            }
        }
        SqlType::Decimal(info) | SqlType::Numeric(info) => {
            let (precision, scale) = match info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p, *s),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p, 0),
                sqlparser::ast::ExactNumberInfo::None => (38, 10),
            };
            Ok(DataType::Decimal128(precision as u8, scale as i8))
        }
        // JSON has no native Arrow logical type; alias it to Utf8.
        // CAST(json_col AS BIGINT|DOUBLE|...) goes through DataFusion's
        // built-in Utf8 -> target coercion. JSON-shaped extraction stays
        // available via json_extract / json_get_str / json_get_int /
        // json_get_float / json_get_bool registered at session start.
        SqlType::JSON => Ok(DataType::Utf8),
        // TIME (without timezone) maps to Arrow's Time64. Iceberg's
        // `time` primitive is microseconds-since-midnight, which lines up
        // with Time64(Microsecond). The same Time64 type already flows
        // through the engine: localtime() / current_time() return it,
        // and iceberg-rust's arrow bridge maps Iceberg time <-> Time64.
        // TIME WITH TIME ZONE has no Arrow equivalent and is rejected.
        SqlType::Time(precision, tz_info) => {
            if sqe_sql::is_tz_variant(tz_info) {
                return Err(SqeError::NotImplemented(
                    "TIME WITH TIME ZONE has no Arrow equivalent. \
                     Use TIMESTAMP WITH TIME ZONE instead."
                        .to_string(),
                ));
            }
            // Iceberg's `time` primitive is microseconds-since-midnight
            // (no `time_ns` exists in the spec, V2 or V3). Iceberg-rust's
            // arrow bridge therefore only round-trips `Time64(Microsecond)`.
            // We accept any precision <= 6 and store at microsecond
            // resolution. Higher precision is rejected with a clear message
            // so the user picks `TIMESTAMP(9)` if they really need
            // sub-microsecond resolution.
            let p = precision.unwrap_or(6);
            if p > 6 {
                return Err(SqeError::NotImplemented(format!(
                    "TIME({p}) precision exceeds Iceberg's microsecond `time` \
                     primitive. Use TIMESTAMP(9) for sub-microsecond resolution."
                )));
            }
            Ok(DataType::Time64(arrow_schema::TimeUnit::Microsecond))
        }
        // STRUCT<a INT, b VARCHAR> -> Arrow Struct (#321). sqlparser parses the
        // native `STRUCT<...>` form into structured fields, so each field type
        // (including nested structs and parameterized types) recurses cleanly.
        // Fields are nullable, matching how CTAS-derived struct schemas land.
        SqlType::Struct(fields, _) => {
            let arrow_fields = fields
                .iter()
                .enumerate()
                .map(|(i, sf)| {
                    let name = sf
                        .field_name
                        .as_ref()
                        .map(|id| id.value.clone())
                        .unwrap_or_else(|| format!("col{i}"));
                    Ok(arrow_schema::Field::new(
                        name,
                        sql_type_to_arrow(&sf.field_type)?,
                        true,
                    ))
                })
                .collect::<sqe_core::Result<Vec<_>>>()?;
            Ok(DataType::Struct(arrow_fields.into()))
        }
        // Trino's `ROW(a INT, b VARCHAR)` (#321). sqlparser does not parse this
        // into `Struct`; it lands as a `Custom("ROW", [a, INT, b, VARCHAR])`
        // type with a flattened name/type modifier list. Rebuild the equivalent
        // `STRUCT<...>` text and reparse so field types go through sqlparser's
        // grammar, then map via the Struct arm above. Parameterized field types
        // (e.g. `decimal(10,2)`) do not survive sqlparser's modifier flattening
        // -- the same limitation as the `CAST(.. AS ROW(..))` rewrite -- so a
        // ROW field that needs parameters should use `STRUCT<...>` directly.
        SqlType::Custom(name, modifiers)
            if name.0.len() == 1
                && name.0[0]
                    .as_ident()
                    .map(|i| i.value.eq_ignore_ascii_case("row"))
                    .unwrap_or(false)
                && !modifiers.is_empty()
                && modifiers.len() % 2 == 0 =>
        {
            let fields_str = modifiers
                .chunks(2)
                .map(|pair| format!("{} {}", pair[0], pair[1]))
                .collect::<Vec<_>>()
                .join(", ");
            let struct_type = parse_struct_type(&fields_str).ok_or_else(|| {
                SqeError::NotImplemented(format!(
                    "ROW type not supported for CREATE TABLE: ROW({fields_str}). \
                     Use STRUCT<...> for parameterized field types."
                ))
            })?;
            sql_type_to_arrow(&struct_type)
        }
        other => Err(SqeError::NotImplemented(format!(
            "SQL type not supported for CREATE TABLE: {other}"
        ))),
    }
}

/// Parse a `STRUCT<...>` field list (e.g. `a INT, b VARCHAR`) into the
/// sqlparser `Struct` data type by reconstructing a one-column `CREATE TABLE`
/// and pulling the column's type. Returns `None` if the reconstructed text does
/// not parse (e.g. a field type sqlparser cannot recover from a `ROW` modifier
/// list). Used to bridge Trino's `ROW(...)` syntax to the native struct mapping.
fn parse_struct_type(fields_str: &str) -> Option<sqlparser::ast::DataType> {
    use sqlparser::ast::{ColumnDef, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let sql = format!("CREATE TABLE __sqe_row (__c STRUCT<{fields_str}>)");
    let stmts = Parser::parse_sql(&GenericDialect {}, &sql).ok()?;
    let cols = match stmts.into_iter().next()? {
        Statement::CreateTable(ct) => ct.columns,
        _ => return None,
    };
    let ColumnDef { data_type, .. } = cols.into_iter().next()?;
    matches!(data_type, sqlparser::ast::DataType::Struct(..)).then_some(data_type)
}

/// A literal an identity-partition column is constrained to equal, lifted from
/// a WHERE clause. Used to skip data files whose partition provably cannot hold
/// a matching row, so Copy-on-Write UPDATE/DELETE rewrite only affected
/// partitions instead of the whole table. See issue #263.
#[derive(Debug, Clone, PartialEq)]
enum PartitionConstraintValue {
    Int(i64),
    Str(String),
    Bool(bool),
}

/// Lift top-level `col = <literal>` equality conjuncts from a WHERE clause.
/// Only AND-connected equalities qualify; anything under OR/NOT is ignored so
/// pruning stays conservative (we must never skip a file that could match).
/// Column names are lowercased for case-insensitive matching.
fn extract_partition_eq_constraints(
    where_expr: &sqlparser::ast::Expr,
) -> std::collections::HashMap<String, PartitionConstraintValue> {
    use sqlparser::ast::{BinaryOperator, Expr};
    let mut out = std::collections::HashMap::new();
    fn lit(e: &Expr) -> Option<PartitionConstraintValue> {
        use sqlparser::ast::Value;
        match e {
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => n.parse::<i64>().ok().map(PartitionConstraintValue::Int),
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    Some(PartitionConstraintValue::Str(s.clone()))
                }
                Value::Boolean(b) => Some(PartitionConstraintValue::Bool(*b)),
                _ => None,
            },
            _ => None,
        }
    }
    fn walk(e: &Expr, out: &mut std::collections::HashMap<String, PartitionConstraintValue>) {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                walk(left, out);
                walk(right, out);
            }
            Expr::Nested(inner) => walk(inner, out),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let pair = match (left.as_ref(), right.as_ref()) {
                    (Expr::Identifier(id), v) => Some((id, v)),
                    (v, Expr::Identifier(id)) => Some((id, v)),
                    _ => None,
                };
                if let Some((id, v)) = pair {
                    if let Some(cv) = lit(v) {
                        out.insert(id.value.to_ascii_lowercase(), cv);
                    }
                }
            }
            _ => {}
        }
    }
    walk(where_expr, &mut out);
    out
}

/// True when an iceberg partition literal is provably different from the
/// constrained value. Returns false for any pair we cannot compare exactly, so
/// uncertainty never prunes a file.
fn partition_literal_definitely_ne(
    lit: &iceberg::spec::Literal,
    want: &PartitionConstraintValue,
) -> bool {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match (lit, want) {
        (Literal::Primitive(PrimitiveLiteral::Int(v)), PartitionConstraintValue::Int(w)) => {
            (*v as i64) != *w
        }
        (Literal::Primitive(PrimitiveLiteral::Long(v)), PartitionConstraintValue::Int(w)) => v != w,
        (Literal::Primitive(PrimitiveLiteral::String(v)), PartitionConstraintValue::Str(w)) => {
            v != w
        }
        (Literal::Primitive(PrimitiveLiteral::Boolean(v)), PartitionConstraintValue::Bool(w)) => {
            v != w
        }
        _ => false,
    }
}

/// Decide whether a data file's partition could contain a row matching the
/// constraints. Returns `false` only when an identity-partition column is
/// constrained to a value the file's partition provably differs from; any
/// uncertainty (non-identity transform, null partition value, type mismatch,
/// unconstrained column) keeps the file (`true`). Conservative by design:
/// false negatives would silently lose data.
fn partition_could_match(
    partition: &iceberg::spec::Struct,
    spec: &iceberg::spec::PartitionSpec,
    schema: &IcebergSchema,
    constraints: &std::collections::HashMap<String, PartitionConstraintValue>,
) -> bool {
    if constraints.is_empty() {
        return true;
    }
    for (idx, pf) in spec.fields().iter().enumerate() {
        if !matches!(pf.transform, iceberg::spec::Transform::Identity) {
            continue;
        }
        let Some(src) = schema.field_by_id(pf.source_id) else {
            continue;
        };
        let Some(want) = constraints.get(&src.name.to_ascii_lowercase()) else {
            continue;
        };
        if let Some(Some(lit)) = partition.fields().get(idx) {
            if partition_literal_definitely_ne(lit, want) {
                return false;
            }
        }
    }
    true
}

/// Partition-prune the data files a CoW UPDATE/DELETE must rewrite. Returns the
/// subset of `files` whose partition could match `where_expr` (these get
/// rewritten); the rest are left untouched in the new snapshot. Falls back to
/// all files when the table is unpartitioned, the WHERE is absent, or no
/// identity-partition equality can be lifted. Returns `(to_rewrite, pruned_count)`.
fn partition_prune_cow_files(
    files: Vec<DataFile>,
    table: &IcebergTable,
    where_expr: Option<&sqlparser::ast::Expr>,
) -> (Vec<DataFile>, usize) {
    let Some(where_expr) = where_expr else {
        return (files, 0);
    };
    let constraints = extract_partition_eq_constraints(where_expr);
    if constraints.is_empty() {
        return (files, 0);
    }
    let spec = table.metadata().default_partition_spec();
    if spec.fields().is_empty() {
        return (files, 0);
    }
    let schema = table.metadata().current_schema();
    let total = files.len();
    let to_rewrite: Vec<DataFile> = files
        .into_iter()
        .filter(|df| {
            partition_could_match(df.partition(), spec.as_ref(), schema.as_ref(), &constraints)
        })
        .collect();
    let pruned = total - to_rewrite.len();
    (to_rewrite, pruned)
}

/// Convert an Arrow schema to an Iceberg schema, assigning field IDs.
///
/// Arrow schemas from DataFusion queries carry no Parquet field-id metadata, so
/// `iceberg::arrow::arrow_schema_to_schema` (which requires `PARQUET_FIELD_ID`)
/// cannot be used. `arrow_schema_to_schema_auto_assign_ids` walks the schema and
/// assigns fresh Iceberg field IDs recursively, including nested struct, list,
/// and map fields. A manual top-level-only loop (the previous implementation)
/// could not: nested struct fields then hit iceberg-rust's "Field id not found
/// in metadata", which is the ROW/struct write failure in #321.
fn arrow_schema_to_iceberg(arrow_schema: &ArrowSchema) -> sqe_core::Result<IcebergSchema> {
    arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .map_err(|e| SqeError::Execution(format!("Failed to build Iceberg schema: {e}")))
}

/// Build an Iceberg schema from an Arrow schema, applying column DEFAULTs.
///
/// For each column def with a `DEFAULT` option, extracts the literal and
/// sets both `initial_default` and `write_default` on the `NestedField`.
/// `initial_default` fills existing rows in case of retroactive `ADD COLUMN`;
/// `write_default` applies to new rows when no value is provided.
/// Build the Arrow fields for a `CREATE TABLE (cols)` statement, folding
/// unquoted column names to lowercase (quoted names preserved) so stored names
/// match Trino / DataFusion query-side identifier normalization (#337).
pub(crate) fn create_table_arrow_fields(
    column_defs: &[sqlparser::ast::ColumnDef],
) -> sqe_core::Result<Vec<arrow_schema::Field>> {
    column_defs
        .iter()
        .map(|col| {
            let arrow_type = sql_type_to_arrow(&col.data_type)?;
            let nullable = !col
                .options
                .iter()
                .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
            Ok(arrow_schema::Field::new(
                fold_unquoted_ident(&col.name),
                arrow_type,
                nullable,
            ))
        })
        .collect()
}

pub(crate) fn arrow_schema_to_iceberg_with_defaults(
    arrow_schema: &ArrowSchema,
    column_defs: &[sqlparser::ast::ColumnDef],
) -> sqe_core::Result<IcebergSchema> {
    use sqe_sql::{extract_default_literal, DefaultLiteral};

    if arrow_schema.fields().len() != column_defs.len() {
        return Err(SqeError::Execution(format!(
            "Schema field count ({}) does not match column definition count ({})",
            arrow_schema.fields().len(),
            column_defs.len()
        )));
    }

    // Assign field IDs recursively first (handles nested struct/list/map, which
    // a top-level-only loop cannot -- see arrow_schema_to_iceberg / #321), then
    // attach column DEFAULTs to the top-level fields. DEFAULTs apply only to
    // top-level columns, so the nested IDs from the auto-assignment are
    // preserved unchanged.
    let base = arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .map_err(|e| SqeError::Execution(format!("Failed to build Iceberg schema: {e}")))?;

    let mut fields = Vec::with_capacity(column_defs.len());

    for (base_field, col_def) in base.as_struct().fields().iter().zip(column_defs.iter()) {
        let mut field: NestedField = (**base_field).clone();
        let iceberg_type = field.field_type.as_ref().clone();

        // Pull the DEFAULT from the column def (if any) and lift it into
        // an iceberg Literal compatible with the target type.
        let default_expr = col_def.options.iter().find_map(|o| match &o.option {
            sqlparser::ast::ColumnOption::Default(expr) => Some(expr),
            _ => None,
        });

        if let Some(expr) = default_expr {
            let sql_literal = extract_default_literal(expr).map_err(|e| {
                SqeError::Execution(format!(
                    "Invalid DEFAULT for column '{}': {e}",
                    col_def.name.value
                ))
            })?;

            match default_to_iceberg_literal(&sql_literal, &iceberg_type) {
                Ok(Some(iceberg_literal)) => {
                    field = field
                        .with_initial_default(iceberg_literal.clone())
                        .with_write_default(iceberg_literal);
                }
                Ok(None) if matches!(sql_literal, DefaultLiteral::Null) => {
                    // DefaultLiteral::Null is a no-op: NULL is already the absent default.
                }
                Ok(None) => {
                    return Err(SqeError::Execution(format!(
                        "DEFAULT literal for column '{}' is not compatible with type {:?}",
                        col_def.name.value, iceberg_type
                    )));
                }
                Err(msg) => {
                    return Err(SqeError::Execution(format!(
                        "DEFAULT literal for column '{}': {msg}",
                        col_def.name.value
                    )));
                }
            }
        }

        fields.push(Arc::new(field));
    }

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| SqeError::Execution(format!("Failed to build Iceberg schema: {e}")))
}

/// Convert a SQL-surface default literal into an Iceberg `Literal`.
///
/// Returns `Ok(None)` if the combination of SQL literal and target Iceberg
/// type is not representable but is not an error (the caller decides whether
/// that is a hard error or a silent NULL). Returns `Err` when the literal
/// is structurally compatible but out of range for the target type, so the
/// user sees the failure instead of a silently wrapped value. Issue #77.
pub(crate) fn default_to_iceberg_literal(
    sql_literal: &sqe_sql::DefaultLiteral,
    target: &iceberg::spec::Type,
) -> Result<Option<iceberg::spec::Literal>, String> {
    use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Type};
    use sqe_sql::DefaultLiteral;

    let prim = match target {
        Type::Primitive(p) => p,
        // Struct/list/map defaults are not in scope.
        _ => return Ok(None),
    };

    let lit = match (sql_literal, prim) {
        (DefaultLiteral::Null, _) => None,
        (DefaultLiteral::Int(i), PrimitiveType::Int) => {
            let narrowed = i32::try_from(*i).map_err(|_| {
                format!("DEFAULT literal {i} does not fit in INT (range -2147483648 to 2147483647)")
            })?;
            Some(Literal::int(narrowed))
        }
        (DefaultLiteral::Int(i), PrimitiveType::Long) => Some(Literal::long(*i)),
        (DefaultLiteral::Int(i), PrimitiveType::Float) => {
            // f32 has ~24 bits of mantissa; reject literals that lose precision
            // so the on-disk value matches what the user wrote.
            if (*i as f32 as i64) != *i {
                return Err(format!(
                    "DEFAULT literal {i} cannot be represented exactly as FLOAT (24-bit mantissa)"
                ));
            }
            Some(Literal::float(*i as f32))
        }
        (DefaultLiteral::Int(i), PrimitiveType::Double) => {
            // f64 has 53 bits of mantissa; outside that range integer precision is lost.
            if (*i as f64 as i64) != *i {
                return Err(format!(
                    "DEFAULT literal {i} cannot be represented exactly as DOUBLE (53-bit mantissa)"
                ));
            }
            Some(Literal::double(*i as f64))
        }
        (DefaultLiteral::Float(f), PrimitiveType::Float) => Some(Literal::float(*f as f32)),
        (DefaultLiteral::Float(f), PrimitiveType::Double) => Some(Literal::double(*f)),
        (DefaultLiteral::Bool(b), PrimitiveType::Boolean) => Some(Literal::bool(*b)),
        (DefaultLiteral::String(s), PrimitiveType::String) => Some(Literal::string(s)),
        // Fall back: wrap string-like literals as strings.
        (DefaultLiteral::String(s), _) => {
            Some(Literal::Primitive(PrimitiveLiteral::String(s.clone())))
        }
        _ => None,
    };
    Ok(lit)
}

/// Decide whether a CREATE TABLE definition requires Iceberg format-version 3.
///
/// Triggers V3 when any column uses a V3-only SQL type (nanosec timestamps)
/// or when any column carries a write-default through the Iceberg schema.
pub(crate) fn requires_v3_features(
    column_defs: &[sqlparser::ast::ColumnDef],
    iceberg_schema: &IcebergSchema,
) -> bool {
    use sqe_sql::is_v3_only_type;

    // Nanosecond timestamps and other V3-only SQL types trigger V3.
    if column_defs.iter().any(|c| is_v3_only_type(&c.data_type)) {
        return true;
    }

    // A write_default or initial_default means V3 too.
    iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .any(|f| f.write_default.is_some() || f.initial_default.is_some())
}

/// Build the table-properties map that signals the desired Iceberg format
/// version to a REST catalog (Polaris, Tabular, etc).
///
/// The Iceberg REST `CreateTableRequest` has no dedicated `format-version`
/// field; the spec uses the reserved `format-version` table property to
/// communicate the version at create time. iceberg-rust currently does not
/// auto-translate `TableCreation.format_version` into this property, so we
/// set it explicitly.
pub(crate) fn format_version_properties(
    format_version: FormatVersion,
) -> std::collections::HashMap<String, String> {
    let mut props = std::collections::HashMap::new();
    let value = match format_version {
        FormatVersion::V1 => "1",
        FormatVersion::V2 => "2",
        FormatVersion::V3 => "3",
    };
    props.insert("format-version".to_string(), value.to_string());
    props
}

/// Format-version table properties, gated by catalog backend.
///
/// The reserved `format-version` property is a REST-only workaround (see
/// [`format_version_properties`]). Non-REST catalogs (Glue, S3 Tables, HMS,
/// JDBC) read `TableCreation.format_version` directly and reject the reserved
/// property at `create_table` with "Table properties should not contain
/// reserved properties, but got: [format-version]". So we emit it only for the
/// REST backend; other backends get an empty map and rely on the builder's
/// `.format_version(...)` field.
pub(crate) fn format_version_props_for_backend(
    backend: &sqe_core::config::CatalogBackend,
    format_version: FormatVersion,
) -> std::collections::HashMap<String, String> {
    if matches!(backend, sqe_core::config::CatalogBackend::Rest) {
        format_version_properties(format_version)
    } else {
        std::collections::HashMap::new()
    }
}

/// Resolve the catalog a write statement targets: the explicit 3-part qualifier
/// from the statement if present, else the session's connection catalog
/// (`default_catalog`, set from the Trino `catalog=` header or a Flight default),
/// else `None` (the configured default warehouse). This lets unqualified
/// DDL/writes from a client connected to a non-default (polaris-auto workspace)
/// catalog land in that catalog -- the case a dbt/Trino connection hits, since
/// it sets the catalog once on the connection rather than in every statement.
pub(crate) fn resolve_target_catalog(
    explicit: Option<String>,
    session: &Session,
) -> Option<String> {
    explicit.or_else(|| session.default_catalog.clone())
}

/// Ensure `namespace` exists in `catalog`, creating it if missing. Idempotent:
/// treats already-exists (including losing a concurrent create) as success.
/// Logs on a real create so namespace creation is visible alongside the
/// "Creating empty table" line. Used as defense-in-depth before CREATE TABLE /
/// CTAS so a workspace catalog whose CREATE SCHEMA did not land still gets the
/// namespace, instead of failing opaquely with "namespace does not exist".
pub(crate) async fn ensure_namespace(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
) -> sqe_core::Result<()> {
    if catalog.namespace_exists(namespace).await.unwrap_or(false) {
        return Ok(());
    }
    match catalog
        .create_namespace(namespace, std::collections::HashMap::new())
        .await
    {
        Ok(_) => {
            info!(namespace = ?namespace, "Created namespace (auto, for table create)");
            Ok(())
        }
        Err(_) if catalog.namespace_exists(namespace).await.unwrap_or(false) => Ok(()),
        Err(e) => Err(SqeError::Catalog(format!(
            "Failed to ensure namespace: {e}"
        ))),
    }
}

/// Fold a `Vec<SqlOption>` (from sqlparser-rs `TBLPROPERTIES (...)` or
/// `WITH (...)` clauses) into a property HashMap.
///
/// Only `KeyValue` options are materialised. Existing entries in `props`
/// (typically `format-version` set by the SQE auto-upgrade path) are
/// preserved when the user did not explicitly set them; an explicit
/// user-supplied value wins so callers can pin a different format version.
/// Extract the user-specified option list from a sqlparser 0.62
/// `CreateTableOptions`. The 0.62 AST collapses the old separate
/// `table_properties` (TBLPROPERTIES) and `with_options` (WITH (...)) fields
/// into this single enum; every property-bearing variant carries a
/// `Vec<SqlOption>`, so we return that slice regardless of the syntax the user
/// wrote. `None` (no options) yields an empty slice.
pub(crate) fn create_table_options_as_slice(
    options: &sqlparser::ast::CreateTableOptions,
) -> &[sqlparser::ast::SqlOption] {
    use sqlparser::ast::CreateTableOptions;
    match options {
        CreateTableOptions::With(opts)
        | CreateTableOptions::Options(opts)
        | CreateTableOptions::Plain(opts)
        | CreateTableOptions::TableProperties(opts) => opts.as_slice(),
        CreateTableOptions::None => &[],
    }
}

pub(crate) fn merge_user_table_properties(
    props: &mut std::collections::HashMap<String, String>,
    options: &[sqlparser::ast::SqlOption],
) {
    use sqlparser::ast::SqlOption;
    for opt in options {
        if let SqlOption::KeyValue { key, value } = opt {
            let k = key.value.clone();
            let v = sql_expr_to_property_string(value);
            props.insert(k, v);
        }
    }
}

/// Turn a sqlparser `Expr` value used in TBLPROPERTIES / WITH into the
/// raw string Iceberg expects. We trim surrounding string-literal quotes
/// so `'merge-on-read'` becomes `merge-on-read` rather than `'merge-on-read'`.
fn sql_expr_to_property_string(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            Value::Number(n, _) => n.clone(),
            Value::Boolean(b) => b.to_string(),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// Translate a sqlparser `PARTITIONED BY (...)` clause into an Iceberg
/// [`UnboundPartitionSpec`]. Supports the six standard transforms:
///
/// | SQL              | Transform              |
/// |------------------|------------------------|
/// | `col`            | `Identity`             |
/// | `year(col)`      | `Year`                 |
/// | `month(col)`     | `Month`                |
/// | `day(col)`       | `Day`                  |
/// | `hour(col)`      | `Hour`                 |
/// | `bucket(N, col)` | `Bucket(N)`            |
/// | `truncate(L, col)` | `Truncate(L)`        |
///
/// The partition field name is auto-derived following the Iceberg
/// convention: `<col>` for identity, `<col>_<transform>` for time
/// transforms, `<col>_bucket_<N>` for bucket, `<col>_trunc_<L>` for
/// truncate. Source column ids come from the table's iceberg schema.
///
/// Returns `Ok(None)` when no `PARTITIONED BY` clause is present so
/// callers can pass the result to `TableCreation::builder().partition_spec_opt()`
/// directly.
pub(crate) fn build_partition_spec(
    partition_by: Option<&sqlparser::ast::Expr>,
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<Option<iceberg::spec::UnboundPartitionSpec>> {
    use iceberg::spec::{UnboundPartitionField, UnboundPartitionSpec};
    use sqlparser::ast::Expr;

    let Some(expr) = partition_by else {
        return Ok(None);
    };

    // The PARTITIONED BY clause may be a single expression or a tuple
    // of expressions. sqlparser models multi-column partitioning as
    // `Expr::Tuple(Vec<Expr>)`; single-column as the bare expression.
    let exprs: Vec<&Expr> = match expr {
        Expr::Tuple(items) => items.iter().collect(),
        Expr::Nested(inner) => match inner.as_ref() {
            Expr::Tuple(items) => items.iter().collect(),
            other => vec![other],
        },
        other => vec![other],
    };

    // Iceberg V2 expects partition field ids to be unique across all
    // partition specs. By spec, fresh specs start their field ids at
    // 1000 and increment by 1 per field. iceberg-rust's UnboundPartitionSpec
    // serializes `field-id: null` when None, which Polaris's REST
    // endpoint rejects. Assigning the standard 1000+ ids up front keeps
    // the wire format compatible.
    let mut fields = Vec::with_capacity(exprs.len());
    let mut next_field_id: i32 = 1000;
    for partition_expr in exprs {
        let (source_name, target_name, transform) = parse_partition_transform(partition_expr)?;
        let source_id = iceberg_schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == source_name)
            .map(|f| f.id)
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "PARTITIONED BY references unknown column '{source_name}'"
                ))
            })?;
        fields.push(UnboundPartitionField {
            source_id,
            field_id: Some(next_field_id),
            name: target_name,
            transform,
        });
        next_field_id += 1;
    }

    let spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields(fields)
        .map_err(|e| SqeError::Execution(format!("Invalid PARTITIONED BY clause: {e}")))?
        .build();

    Ok(Some(spec))
}

/// Parse a raw partition transform SQL fragment such as `year(ts)`,
/// `bucket(16, user_id)`, or a bare column name `region`, returning
/// `(source_column, target_name, Transform)`.
///
/// Used by the partition-evolution handler (ALTER TABLE ADD/DROP/REPLACE
/// PARTITION FIELD) so it can share the exact same transform vocabulary as
/// CREATE TABLE's `PARTITIONED BY` clause.
pub(crate) fn parse_partition_transform_sql(
    transform_sql: &str,
) -> sqe_core::Result<(String, String, iceberg::spec::Transform)> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(transform_sql)
        .map_err(|e| {
            SqeError::Execution(format!(
                "Failed to lex partition transform '{transform_sql}': {e}"
            ))
        })?;
    let expr = parser.parse_expr().map_err(|e| {
        SqeError::Execution(format!(
            "Failed to parse partition transform '{transform_sql}': {e}"
        ))
    })?;
    parse_partition_transform(&expr)
}

/// Parse a single partition expression into `(source_column, target_name, Transform)`.
fn parse_partition_transform(
    expr: &sqlparser::ast::Expr,
) -> sqe_core::Result<(String, String, iceberg::spec::Transform)> {
    use iceberg::spec::Transform;
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value};

    match expr {
        // Bare column name = identity transform.
        Expr::Identifier(ident) => Ok((
            ident.value.clone(),
            ident.value.clone(),
            Transform::Identity,
        )),
        // Compound identifier (e.g. `t.col`) — take the last segment.
        Expr::CompoundIdentifier(parts) => {
            let name = parts.last().map(|p| p.value.clone()).ok_or_else(|| {
                SqeError::Execution("PARTITIONED BY: empty compound identifier".to_string())
            })?;
            Ok((name.clone(), name, Transform::Identity))
        }
        // Function call: year(col), bucket(N, col), truncate(L, col), etc.
        Expr::Function(func) => {
            let fn_name = func
                .name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|id| id.value.to_ascii_lowercase())
                .ok_or_else(|| {
                    SqeError::Execution("PARTITIONED BY: function call without a name".to_string())
                })?;
            let args = match &func.args {
                FunctionArguments::List(list) => &list.args,
                _ => {
                    return Err(SqeError::Execution(format!(
                        "PARTITIONED BY: unsupported argument form for {fn_name}()"
                    )));
                }
            };

            // Extract the bare-expr arguments out of FunctionArg.
            let bare_args: Vec<&Expr> = args
                .iter()
                .filter_map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .collect();

            // Helper closures.
            let column_name = |arg: &Expr| -> sqe_core::Result<String> {
                match arg {
                    // Fold the partition-by column ref so it matches the folded
                    // stored column name (#337).
                    Expr::Identifier(id) => Ok(fold_unquoted_ident(id)),
                    Expr::CompoundIdentifier(parts) => {
                        parts.last().map(fold_unquoted_ident).ok_or_else(|| {
                            SqeError::Execution(format!(
                                "PARTITIONED BY {fn_name}(): empty compound identifier"
                            ))
                        })
                    }
                    other => Err(SqeError::Execution(format!(
                        "PARTITIONED BY {fn_name}(): expected column name, got {other}"
                    ))),
                }
            };
            let int_arg = |arg: &Expr| -> sqe_core::Result<u32> {
                match arg {
                    Expr::Value(v) => match &v.value {
                        Value::Number(n, _) => n.parse::<u32>().map_err(|e| {
                            SqeError::Execution(format!(
                                "PARTITIONED BY {fn_name}(): integer parameter '{n}': {e}"
                            ))
                        }),
                        other => Err(SqeError::Execution(format!(
                            "PARTITIONED BY {fn_name}(): expected integer, got {other}"
                        ))),
                    },
                    other => Err(SqeError::Execution(format!(
                        "PARTITIONED BY {fn_name}(): expected integer literal, got {other}"
                    ))),
                }
            };

            match fn_name.as_str() {
                "year" | "years" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY year(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_year"), Transform::Year))
                }
                "month" | "months" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY month(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_month"), Transform::Month))
                }
                "day" | "days" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY day(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_day"), Transform::Day))
                }
                "hour" | "hours" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY hour(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_hour"), Transform::Hour))
                }
                "bucket" => {
                    if bare_args.len() != 2 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY bucket(N, col): expected exactly two arguments".into(),
                        ));
                    }
                    let n = int_arg(bare_args[0])?;
                    let col = column_name(bare_args[1])?;
                    Ok((
                        col.clone(),
                        format!("{col}_bucket_{n}"),
                        Transform::Bucket(n),
                    ))
                }
                "truncate" => {
                    if bare_args.len() != 2 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY truncate(L, col): expected exactly two arguments"
                                .into(),
                        ));
                    }
                    let l = int_arg(bare_args[0])?;
                    let col = column_name(bare_args[1])?;
                    Ok((
                        col.clone(),
                        format!("{col}_trunc_{l}"),
                        Transform::Truncate(l),
                    ))
                }
                "void" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY void(col): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_null"), Transform::Void))
                }
                other => Err(SqeError::Execution(format!(
                    "PARTITIONED BY: unsupported transform '{other}'. \
                     Supported: identity (bare column), year, month, day, hour, \
                     bucket(N, col), truncate(L, col), void."
                ))),
            }
        }
        other => Err(SqeError::Execution(format!(
            "PARTITIONED BY: unsupported expression form: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, TimeUnit};
    use sqlparser::ast::{DataType as SqlType, ExactNumberInfo, TimezoneInfo};

    // -------------------------------------------------------------------------
    // target-catalog resolution: explicit qualifier -> session catalog -> default
    // -------------------------------------------------------------------------

    #[test]
    fn create_table_arrow_fields_folds_unquoted_names() {
        // #337: unquoted column names fold to lowercase in the stored schema;
        // double-quoted names keep their case.
        use sqlparser::parser::Parser;
        let sql = r#"CREATE TABLE t (testInteger int, "KeepMe" int)"#;
        let stmts = Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql).unwrap();
        let cols = match &stmts[0] {
            Statement::CreateTable(ct) => &ct.columns,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        let fields = create_table_arrow_fields(cols).unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["testinteger", "KeepMe"]);
    }

    #[test]
    fn resolve_target_catalog_prefers_explicit_then_session() {
        let mut session = sqe_core::Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("t".to_string()),
            None,
            chrono::Utc::now(),
            vec![],
        );
        // No explicit qualifier, no connection catalog -> None (default warehouse).
        assert_eq!(resolve_target_catalog(None, &session), None);

        // Unqualified statement uses the session's connection catalog (the dbt /
        // Trino case: catalog set once on the connection).
        session.default_catalog = Some("ws_team_a".to_string());
        assert_eq!(
            resolve_target_catalog(None, &session),
            Some("ws_team_a".to_string())
        );

        // An explicit 3-part qualifier wins over the connection catalog.
        assert_eq!(
            resolve_target_catalog(Some("other_wh".to_string()), &session),
            Some("other_wh".to_string())
        );
    }

    fn test_session(default_schema: Option<&str>) -> sqe_core::Session {
        let mut session = sqe_core::Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("t".to_string()),
            None,
            chrono::Utc::now(),
            vec![],
        );
        session.default_schema = default_schema.map(|s| s.to_string());
        session
    }

    #[test]
    fn resolve_table_ident_unqualified_uses_session_schema() {
        // #357: an unqualified write target resolves against the session's
        // X-Trino-Schema, matching the read path, not the hard-coded `default`.
        use sqlparser::ast::Ident;
        let session = test_session(Some("sesstest"));
        let name = ObjectName::from(vec![Ident::new("s357")]);
        let ident = resolve_table_ident(&name, &session).unwrap();
        assert_eq!(
            ident.namespace(),
            &NamespaceIdent::new("sesstest".to_string())
        );
        assert_eq!(ident.name(), "s357");
    }

    #[test]
    fn resolve_table_ident_unqualified_falls_back_to_default() {
        // No session schema set (and empty-string treated as unset) -> `default`.
        use sqlparser::ast::Ident;
        let name = ObjectName::from(vec![Ident::new("s357")]);

        let unset = resolve_table_ident(&name, &test_session(None)).unwrap();
        assert_eq!(
            unset.namespace(),
            &NamespaceIdent::new("default".to_string())
        );

        let empty = resolve_table_ident(&name, &test_session(Some(""))).unwrap();
        assert_eq!(
            empty.namespace(),
            &NamespaceIdent::new("default".to_string())
        );
    }

    #[test]
    fn resolve_table_ident_qualified_names_unchanged() {
        // A 2- or 3-part name already names its namespace; the session schema
        // must not override it.
        use sqlparser::ast::Ident;
        let session = test_session(Some("sesstest"));

        let two = ObjectName::from(vec![Ident::new("otherns"), Ident::new("t")]);
        let ident = resolve_table_ident(&two, &session).unwrap();
        assert_eq!(
            ident.namespace(),
            &NamespaceIdent::new("otherns".to_string())
        );
        assert_eq!(ident.name(), "t");

        let three = ObjectName::from(vec![
            Ident::new("cat"),
            Ident::new("otherns"),
            Ident::new("t"),
        ]);
        let ident = resolve_table_ident(&three, &session).unwrap();
        assert_eq!(
            ident.namespace(),
            &NamespaceIdent::new("otherns".to_string())
        );
        assert_eq!(ident.name(), "t");
    }

    // -------------------------------------------------------------------------
    // format-version property gating by catalog backend
    // -------------------------------------------------------------------------

    #[test]
    fn format_version_property_only_for_rest_backend() {
        use sqe_core::config::CatalogBackend;

        // REST: the reserved `format-version` property IS emitted. iceberg-rust's
        // REST create_table does not translate TableCreation.format_version, so
        // Polaris reads the version from this property.
        let rest = format_version_props_for_backend(&CatalogBackend::Rest, FormatVersion::V2);
        assert_eq!(
            rest.get("format-version").map(String::as_str),
            Some("2"),
            "REST backend must carry the format-version property"
        );

        // Non-REST backends honor TableCreation.format_version directly and
        // REJECT `format-version` as a reserved property at create_table.
        // Regression test for the Glue CREATE TABLE failure: "Table properties
        // should not contain reserved properties, but got: [format-version]".
        let glue = format_version_props_for_backend(
            &CatalogBackend::Glue {
                region: "eu-central-1".to_string(),
                warehouse: String::new(),
                endpoint: None,
            },
            FormatVersion::V2,
        );
        assert!(
            glue.is_empty(),
            "Glue backend must not carry the reserved format-version property, got: {glue:?}"
        );

        let s3tables = format_version_props_for_backend(
            &CatalogBackend::S3tables {
                table_bucket_arn: "arn:aws:s3tables:eu-west-1:123456789012:bucket/b".to_string(),
                endpoint_url: None,
            },
            FormatVersion::V3,
        );
        assert!(
            s3tables.is_empty(),
            "S3 Tables backend must not carry the reserved format-version property"
        );
    }

    // -------------------------------------------------------------------------
    // build_partition_spec JSON shape (for catalog interop debugging)
    // -------------------------------------------------------------------------

    #[test]
    fn build_partition_spec_emits_polaris_compatible_json() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use std::sync::Arc;

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let dialect = sqlparser::dialect::GenericDialect {};
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql("region")
            .unwrap();
        let expr = parser.parse_expr().unwrap();
        let spec = build_partition_spec(Some(&expr), &schema).unwrap().unwrap();
        let json = serde_json::to_string_pretty(&spec).unwrap();
        eprintln!("PARTITION SPEC JSON:\n{json}");
        // The spec must serialize with kebab-case keys that Polaris recognises.
        assert!(json.contains("\"source-id\": 2"), "json: {json}");
        assert!(json.contains("\"transform\": \"identity\""), "json: {json}");
        assert!(json.contains("\"name\": \"region\""), "json: {json}");
    }

    // -------------------------------------------------------------------------
    // arrow_schema_to_iceberg tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_arrow_schema_to_iceberg_basic() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "id");
        assert!(fields[0].required);
        assert_eq!(fields[1].name, "name");
        assert!(!fields[1].required);
        assert_eq!(fields[2].name, "value");
        assert!(!fields[2].required);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_empty() {
        let arrow_schema = ArrowSchema::empty();
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 0);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_field_ids_are_sequential() {
        // Field IDs must start at 1 and be sequential (Iceberg spec requirement).
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields[0].id, 1);
        assert_eq!(fields[1].id, 2);
        assert_eq!(fields[2].id, 3);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_nullable_flags() {
        // Nullable Arrow fields → optional Iceberg fields (required == false).
        // Non-nullable Arrow fields → required Iceberg fields (required == true).
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("required_col", DataType::Int64, false),
            Field::new("optional_col", DataType::Utf8, true),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert!(
            fields[0].required,
            "non-nullable Arrow field should map to required Iceberg field"
        );
        assert!(
            !fields[1].required,
            "nullable Arrow field should map to optional Iceberg field"
        );
    }

    #[test]
    fn test_arrow_schema_to_iceberg_all_required() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("z", DataType::Int32, false),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        for field in &fields {
            assert!(field.required, "all fields should be required");
        }
    }

    #[test]
    fn test_arrow_schema_to_iceberg_wide_schema() {
        // Verify that a schema with many fields produces the correct count and IDs.
        let columns: Vec<Field> = (0..20)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int64, i % 2 == 0))
            .collect();
        let count = columns.len();
        let arrow_schema = ArrowSchema::new(columns);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), count);
        for (i, field) in fields.iter().enumerate() {
            assert_eq!(field.id, (i + 1) as i32);
            assert_eq!(field.name, format!("col_{i}"));
        }
    }

    #[test]
    fn test_arrow_schema_to_iceberg_various_numeric_types() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("i8_col", DataType::Int8, true),
            Field::new("i16_col", DataType::Int16, true),
            Field::new("i32_col", DataType::Int32, true),
            Field::new("i64_col", DataType::Int64, true),
            Field::new("f32_col", DataType::Float32, true),
            Field::new("f64_col", DataType::Float64, true),
        ]);

        // Should convert without error; all numeric types are supported by Iceberg.
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 6);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_temporal_types() {
        // Iceberg only supports Microsecond precision for timestamps (not Millisecond or
        // Nanosecond). This test verifies the supported subset converts cleanly.
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("date_col", DataType::Date32, true),
            Field::new(
                "ts_us_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 2);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_millisecond_timestamp_is_unsupported() {
        // The underlying iceberg-rust library rejects Timestamp(Millisecond) — this is a
        // known limitation and the error path must be exercised rather than silently
        // producing a wrong schema.
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "ts_ms_col",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);

        let result = arrow_schema_to_iceberg(&arrow_schema);
        assert!(
            result.is_err(),
            "Timestamp(Millisecond) should not convert to Iceberg"
        );
    }

    #[test]
    fn test_arrow_schema_to_iceberg_decimal_type() {
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 4),
            false,
        )]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "amount");
        assert!(fields[0].required);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_binary_type() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("payload", DataType::Binary, true)]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 1);
    }

    // -------------------------------------------------------------------------
    // sql_type_to_arrow tests (private fn accessed via super::)
    // -------------------------------------------------------------------------

    #[test]
    fn test_sql_type_to_arrow_boolean() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Boolean).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn test_sql_type_to_arrow_integer_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::TinyInt(None)).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int8(None)).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::SmallInt(None)).unwrap(),
            DataType::Int16
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Int16).unwrap(), DataType::Int16);
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int(None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Integer(None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Int32).unwrap(), DataType::Int32);
        assert_eq!(
            sql_type_to_arrow(&SqlType::BigInt(None)).unwrap(),
            DataType::Int64
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Int64).unwrap(), DataType::Int64);
    }

    /// Parse `CREATE TABLE x (c <type_decl>)` and return the column's SQL type,
    /// so struct/row type tests exercise the same AST shape production hits.
    fn col_sql_type(type_decl: &str) -> SqlType {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let sql = format!("CREATE TABLE x (c {type_decl})");
        let stmts = Parser::parse_sql(&GenericDialect {}, &sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            sqlparser::ast::Statement::CreateTable(ct) => {
                ct.columns.into_iter().next().unwrap().data_type
            }
            _ => panic!("not a CREATE TABLE"),
        }
    }

    #[test]
    fn test_sql_type_to_arrow_struct_native() {
        // STRUCT<a INT, b VARCHAR> -> Arrow Struct with mapped, named fields.
        let dt = sql_type_to_arrow(&col_sql_type("STRUCT<a INT, b VARCHAR>")).unwrap();
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct, got {dt:?}");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "a");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert_eq!(fields[1].name(), "b");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_sql_type_to_arrow_row_maps_to_struct() {
        // #321: Trino ROW(a integer, b varchar) lands as Custom and must map to
        // the same Arrow Struct as the STRUCT<...> spelling.
        let dt = sql_type_to_arrow(&col_sql_type("ROW(a integer, b varchar)")).unwrap();
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct, got {dt:?}");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "a");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert_eq!(fields[1].name(), "b");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_sql_type_to_arrow_nested_struct() {
        // Nested struct recurses through the field-type mapping.
        let dt = sql_type_to_arrow(&col_sql_type("STRUCT<inner STRUCT<x BIGINT>>")).unwrap();
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct, got {dt:?}");
        };
        assert_eq!(fields[0].name(), "inner");
        assert!(
            matches!(fields[0].data_type(), DataType::Struct(inner) if inner[0].name() == "x"
                && inner[0].data_type() == &DataType::Int64),
            "nested struct field not mapped: {:?}",
            fields[0].data_type()
        );
    }

    #[test]
    fn test_arrow_schema_to_iceberg_assigns_nested_struct_field_ids() {
        // #321 regression: a struct column must convert to an Iceberg schema
        // with field IDs assigned recursively. The old top-level-only loop hit
        // iceberg-rust's "Field id not found in metadata" on the nested fields
        // (DataFusion Arrow schemas carry no PARQUET_FIELD_ID), which is what
        // blocked ROW/struct writes.
        use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};

        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let arrow = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Struct(struct_fields), true),
        ]);

        let iceberg = arrow_schema_to_iceberg(&arrow)
            .expect("nested struct schema must convert without a field-id error");

        let top = iceberg.as_struct().fields();
        assert_eq!(top.len(), 2, "two top-level fields");

        // The struct column's nested fields are present and carry their own IDs.
        let iceberg::spec::Type::Struct(nested) = top[1].field_type.as_ref() else {
            panic!(
                "payload should map to a struct type, got {:?}",
                top[1].field_type
            );
        };
        assert_eq!(nested.fields().len(), 2, "nested struct fields preserved");

        // Every field ID across the whole schema must be unique and non-zero --
        // duplicate or zero nested IDs are exactly what breaks the writer.
        let mut ids = vec![top[0].id, top[1].id];
        ids.extend(nested.fields().iter().map(|f| f.id));
        assert!(
            ids.iter().all(|&id| id > 0),
            "all field ids assigned: {ids:?}"
        );
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            ids.len(),
            "field ids unique across nesting: {ids:?}"
        );
    }

    #[test]
    fn test_sql_type_to_arrow_float_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Float(ExactNumberInfo::None)).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Real).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Double(sqlparser::ast::ExactNumberInfo::None)).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::DoublePrecision).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_sql_type_to_arrow_string_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Varchar(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Text).unwrap(), DataType::Utf8);
        assert_eq!(
            sql_type_to_arrow(&SqlType::Char(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Character(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::CharVarying(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::String(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Uuid).unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_sql_type_to_arrow_binary_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Binary(None)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Varbinary(None)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Bytea).unwrap(),
            DataType::Binary
        );
    }

    #[test]
    fn test_sql_type_to_arrow_date() {
        assert_eq!(sql_type_to_arrow(&SqlType::Date).unwrap(), DataType::Date32);
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_default_precision() {
        // No precision → defaults to 6 → Microsecond
        let result = sql_type_to_arrow(&SqlType::Timestamp(None, TimezoneInfo::None)).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_low_precision() {
        // Precision 0-3 → Millisecond
        for p in 0u64..=3 {
            let result =
                sql_type_to_arrow(&SqlType::Timestamp(Some(p), TimezoneInfo::None)).unwrap();
            assert_eq!(
                result,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                "precision {p} should map to Millisecond"
            );
        }
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_mid_precision() {
        // Precision 4-6 → Microsecond
        for p in 4u64..=6 {
            let result =
                sql_type_to_arrow(&SqlType::Timestamp(Some(p), TimezoneInfo::None)).unwrap();
            assert_eq!(
                result,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "precision {p} should map to Microsecond"
            );
        }
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_high_precision() {
        // Precision 7+ → Nanosecond
        let result = sql_type_to_arrow(&SqlType::Timestamp(Some(9), TimezoneInfo::None)).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_ns_custom_type() {
        // TIMESTAMP_NS(9) lands in sqlparser as DataType::Custom.
        use sqlparser::ast::ObjectName;

        let custom = SqlType::Custom(
            ObjectName::from(vec![sqlparser::ast::Ident::new("TIMESTAMP_NS")]),
            vec!["9".to_string()],
        );
        let result = sql_type_to_arrow(&custom).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Nanosecond, None));

        // Lowercase should map to the same type (identifiers are case-insensitive).
        let custom_lower = SqlType::Custom(
            ObjectName::from(vec![sqlparser::ast::Ident::new("timestamp_ns")]),
            vec!["9".to_string()],
        );
        let result_lower = sql_type_to_arrow(&custom_lower).unwrap();
        assert_eq!(
            result_lower,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_sql_type_to_arrow_timestamptz_ns_custom_type() {
        use sqlparser::ast::ObjectName;

        let custom = SqlType::Custom(
            ObjectName::from(vec![sqlparser::ast::Ident::new("TIMESTAMPTZ_NS")]),
            vec!["9".to_string()],
        );
        let result = sql_type_to_arrow(&custom).unwrap();
        assert_eq!(
            result,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_sql_type_to_arrow_ns_via_parser() {
        // Parse CREATE TABLE and feed the resulting column type through the
        // conversion. Locks behaviour across the full parser -> mapper path.
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "CREATE TABLE events (ts TIMESTAMP_NS(9), utcts TIMESTAMPTZ_NS(9))";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0);
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(
            sql_type_to_arrow(&ct.columns[0].data_type).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            sql_type_to_arrow(&ct.columns[1].data_type).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_arrow_to_iceberg_preserves_nanosec() {
        // Nanosecond Arrow timestamps must map to Iceberg TimestampNs / TimestamptzNs.
        use arrow_schema::Field;
        use iceberg::spec::{PrimitiveType, Type};

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "utcts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]);
        let iceberg = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().to_vec();
        assert!(matches!(
            *fields[0].field_type,
            Type::Primitive(PrimitiveType::TimestampNs)
        ));
        assert!(matches!(
            *fields[1].field_type,
            Type::Primitive(PrimitiveType::TimestamptzNs)
        ));
    }

    // ------------------------------------------------------------------
    // DEFAULT literal handling and format-version gating
    // ------------------------------------------------------------------

    fn parse_create_table(sql: &str) -> sqlparser::ast::CreateTable {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .expect("sql parses")
            .into_iter()
            .next()
            .expect("one statement");
        match stmt {
            sqlparser::ast::Statement::CreateTable(ct) => ct,
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    fn parse_where(cond: &str) -> sqlparser::ast::Expr {
        use sqlparser::ast::{SetExpr, Statement};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let sql = format!("SELECT 1 FROM t WHERE {cond}");
        let stmt = Parser::parse_sql(&GenericDialect {}, &sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        match stmt {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(s) => s.selection.expect("has WHERE"),
                _ => panic!("not a select"),
            },
            other => panic!("not a query: {other:?}"),
        }
    }

    #[test]
    fn test_extract_partition_eq_constraints_and_conjuncts() {
        let c = extract_partition_eq_constraints(&parse_where(
            "ol_w_id = 1 AND ol_d_id = 2 AND ol_delivery_d IS NULL",
        ));
        assert_eq!(c.get("ol_w_id"), Some(&PartitionConstraintValue::Int(1)));
        assert_eq!(c.get("ol_d_id"), Some(&PartitionConstraintValue::Int(2)));
        assert!(!c.contains_key("ol_delivery_d")); // IS NULL is not an equality
    }

    #[test]
    fn test_extract_partition_eq_constraints_literal_on_left_and_string() {
        let c = extract_partition_eq_constraints(&parse_where("1 = w_id AND name = 'BAR'"));
        assert_eq!(c.get("w_id"), Some(&PartitionConstraintValue::Int(1)));
        assert_eq!(
            c.get("name"),
            Some(&PartitionConstraintValue::Str("BAR".to_string()))
        );
    }

    #[test]
    fn test_extract_partition_eq_constraints_ignores_or() {
        // OR at the top level is not safe to prune on.
        assert!(extract_partition_eq_constraints(&parse_where("w_id = 1 OR d_id = 2")).is_empty());
        // An OR nested under AND: only the AND-level equality is lifted.
        let c = extract_partition_eq_constraints(&parse_where("w_id = 1 AND (a = 2 OR b = 3)"));
        assert_eq!(c.get("w_id"), Some(&PartitionConstraintValue::Int(1)));
        assert!(!c.contains_key("a") && !c.contains_key("b"));
    }

    #[test]
    fn test_partition_literal_definitely_ne() {
        use iceberg::spec::{Literal, PrimitiveLiteral};
        let int1 = Literal::Primitive(PrimitiveLiteral::Int(1));
        let int2 = Literal::Primitive(PrimitiveLiteral::Int(2));
        let long1 = Literal::Primitive(PrimitiveLiteral::Long(1));
        let bar = Literal::Primitive(PrimitiveLiteral::String("BAR".into()));
        // provably different -> prunable
        assert!(partition_literal_definitely_ne(
            &int2,
            &PartitionConstraintValue::Int(1)
        ));
        // equal -> not prunable
        assert!(!partition_literal_definitely_ne(
            &int1,
            &PartitionConstraintValue::Int(1)
        ));
        assert!(!partition_literal_definitely_ne(
            &long1,
            &PartitionConstraintValue::Int(1)
        ));
        assert!(partition_literal_definitely_ne(
            &bar,
            &PartitionConstraintValue::Str("FOO".into())
        ));
        // type mismatch -> conservatively NOT provably different (keep the file)
        assert!(!partition_literal_definitely_ne(
            &int1,
            &PartitionConstraintValue::Str("1".into())
        ));
    }

    #[test]
    fn test_default_string_sets_write_default() {
        use iceberg::spec::{Literal, PrimitiveLiteral};

        let ct =
            parse_create_table("CREATE TABLE orders (id BIGINT, status STRING DEFAULT 'pending')");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let iceberg = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().to_vec();

        let status = fields.iter().find(|f| f.name == "status").unwrap();
        assert!(
            matches!(
                status.write_default.as_ref(),
                Some(Literal::Primitive(PrimitiveLiteral::String(s))) if s == "pending"
            ),
            "write_default should be 'pending', got {:?}",
            status.write_default
        );
        assert!(
            matches!(
                status.initial_default.as_ref(),
                Some(Literal::Primitive(PrimitiveLiteral::String(s))) if s == "pending"
            ),
            "initial_default should be 'pending', got {:?}",
            status.initial_default
        );
    }

    #[test]
    fn test_default_integer_on_bigint() {
        use iceberg::spec::{Literal, PrimitiveLiteral};

        let ct = parse_create_table("CREATE TABLE t (n BIGINT DEFAULT 42)");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let iceberg = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().to_vec();

        assert!(matches!(
            fields[0].write_default.as_ref(),
            Some(Literal::Primitive(PrimitiveLiteral::Long(42)))
        ));
    }

    #[test]
    fn test_default_function_rejected_with_clear_error() {
        let ct = parse_create_table("CREATE TABLE t (ts TIMESTAMP DEFAULT current_timestamp())");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let err = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("current_timestamp"),
            "error should name rejected function: {msg}"
        );
        assert!(
            msg.contains("Accepted forms"),
            "error should list accepted forms: {msg}"
        );
    }

    #[test]
    fn test_requires_v3_on_nanosec_timestamp() {
        let ct = parse_create_table("CREATE TABLE t (ts TIMESTAMP_NS(9))");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let iceberg =
            arrow_schema_to_iceberg_with_defaults(&ArrowSchema::new(arrow_fields), &ct.columns)
                .unwrap();
        assert!(requires_v3_features(&ct.columns, &iceberg));
    }

    #[test]
    fn test_requires_v3_on_write_default() {
        let ct = parse_create_table("CREATE TABLE t (id BIGINT, status STRING DEFAULT 'pending')");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let iceberg =
            arrow_schema_to_iceberg_with_defaults(&ArrowSchema::new(arrow_fields), &ct.columns)
                .unwrap();
        assert!(requires_v3_features(&ct.columns, &iceberg));
    }

    #[test]
    fn test_does_not_require_v3_when_v2_only() {
        let ct = parse_create_table("CREATE TABLE t (id BIGINT, name STRING)");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let iceberg =
            arrow_schema_to_iceberg_with_defaults(&ArrowSchema::new(arrow_fields), &ct.columns)
                .unwrap();
        assert!(!requires_v3_features(&ct.columns, &iceberg));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_full() {
        let result =
            sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(18, 4)))
                .unwrap();
        assert_eq!(result, DataType::Decimal128(18, 4));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_precision_only() {
        let result = sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::Precision(10))).unwrap();
        // Scale defaults to 0
        assert_eq!(result, DataType::Decimal128(10, 0));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_no_info() {
        let result = sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::None)).unwrap();
        // Defaults to Decimal128(38, 10)
        assert_eq!(result, DataType::Decimal128(38, 10));
    }

    #[test]
    fn test_sql_type_to_arrow_numeric_alias() {
        // NUMERIC is the same as DECIMAL in the implementation.
        let decimal_result =
            sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(12, 2)))
                .unwrap();
        let numeric_result =
            sql_type_to_arrow(&SqlType::Numeric(ExactNumberInfo::PrecisionAndScale(12, 2)))
                .unwrap();
        assert_eq!(decimal_result, numeric_result);
    }

    #[test]
    fn test_sql_type_to_arrow_json_aliases_to_utf8() {
        // JSON has no native Arrow logical type. The Stage-1 path is to
        // alias it to Utf8 so CAST(json_col AS BIGINT|VARCHAR|DOUBLE)
        // rides DataFusion's built-in Utf8 -> target coercion. JSON
        // extraction stays available via the json_extract /
        // json_get_str / json_get_int / json_get_float / json_get_bool
        // UDFs registered at session start.
        let result = sql_type_to_arrow(&SqlType::JSON).unwrap();
        assert_eq!(result, DataType::Utf8);
    }

    #[test]
    fn test_sql_type_to_arrow_time_default_precision() {
        // Default TIME (no precision) is microsecond, matching Iceberg's
        // `time` primitive and iceberg-rust's arrow bridge.
        let result = sql_type_to_arrow(&SqlType::Time(None, TimezoneInfo::None)).unwrap();
        assert_eq!(result, DataType::Time64(TimeUnit::Microsecond));
    }

    #[test]
    fn test_sql_type_to_arrow_time_precision_branches() {
        // Iceberg's `time` primitive is microsecond-only; precisions 0..=6
        // collapse to Time64(Microsecond). Higher precisions are rejected
        // with a clear NotImplemented since Iceberg has no `time_ns` type.
        for p in 0u64..=6 {
            let result = sql_type_to_arrow(&SqlType::Time(Some(p), TimezoneInfo::None)).unwrap();
            assert_eq!(
                result,
                DataType::Time64(TimeUnit::Microsecond),
                "TIME({p}) should map to Time64(Microsecond) for Iceberg compatibility",
            );
        }
        let err = sql_type_to_arrow(&SqlType::Time(Some(9), TimezoneInfo::None));
        assert!(
            matches!(err, Err(sqe_core::SqeError::NotImplemented(ref msg)) if msg.contains("TIME(9)")),
            "TIME(9) should be rejected since Iceberg has no time_ns: got {err:?}",
        );
    }

    #[test]
    fn test_sql_type_to_arrow_time_with_timezone_rejected() {
        // TIME WITH TIME ZONE has no Arrow equivalent. Reject with a
        // clear NotImplemented so users see the TIMESTAMP WITH TIME
        // ZONE workaround in the error message.
        let result = sql_type_to_arrow(&SqlType::Time(None, TimezoneInfo::WithTimeZone));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, sqe_core::SqeError::NotImplemented(ref msg) if msg.contains("TIME WITH TIME ZONE")),
            "expected TIME WITH TIME ZONE NotImplemented, got: {err:?}"
        );
    }

    // -------------------------------------------------------------------------
    // handle_ingest table-name parsing (pure logic, no catalog required)
    // -------------------------------------------------------------------------

    /// The ingest name-parsing logic is embedded in `handle_ingest`. We test it
    /// by extracting the equivalent logic as a free function here so we can unit
    /// test the three cases (valid 2-part, valid 3-part, invalid) without
    /// needing a real catalog connection.
    fn parse_ingest_table_name(table_name: &str) -> sqe_core::Result<(String, String)> {
        let parts: Vec<&str> = table_name.split('.').collect();
        match parts.as_slice() {
            [ns, tbl] => Ok(((*ns).to_string(), (*tbl).to_string())),
            [_cat, ns, tbl] => Ok(((*ns).to_string(), (*tbl).to_string())),
            _ => Err(sqe_core::SqeError::Execution(format!(
                "Invalid table name for ingest: {table_name}"
            ))),
        }
    }

    #[test]
    fn test_ingest_table_name_two_part() {
        let (ns, tbl) = parse_ingest_table_name("my_schema.my_table").unwrap();
        assert_eq!(ns, "my_schema");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_ingest_table_name_three_part_catalog() {
        // "catalog.schema.table" — catalog is discarded, schema + table kept.
        let (ns, tbl) = parse_ingest_table_name("my_catalog.my_schema.my_table").unwrap();
        assert_eq!(ns, "my_schema");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_ingest_table_name_single_part_is_error() {
        let result = parse_ingest_table_name("just_a_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_table_name_four_part_is_error() {
        // More than three parts is also invalid.
        let result = parse_ingest_table_name("a.b.c.d");
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_table_name_empty_string_is_error() {
        // An empty string yields a single empty segment → invalid.
        let result = parse_ingest_table_name("");
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------------
    // Correlated ScalarSubquery decorrelator tests
    // ---------------------------------------------------------------------

    fn parse_update(sql: &str) -> Vec<sqlparser::ast::Assignment> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse UPDATE");
        match stmts.into_iter().next().expect("one statement") {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn decorrelator_rewrites_simple_correlated_subquery() {
        let sql = "\
UPDATE holding_summary \
SET hs_qty = hs_qty + ( \
    SELECT t.t_qty FROM trade t \
    WHERE t.t_ca_id = holding_summary.hs_ca_id \
      AND t.t_st_id = 'PNDG' \
    LIMIT 1 \
)";
        let assignments = parse_update(sql);
        let (rewritten, joins) = decorrelate_scalar_subqueries(&assignments, "holding_summary");
        assert_eq!(rewritten.len(), 1);
        assert_eq!(rewritten[0].col_name, "hs_qty");
        // The scalar subquery must have been replaced with a column reference
        // into the joined lookup.
        assert!(
            rewritten[0].expr_sql.contains("\"__corr0\".\"__val\""),
            "unexpected rewritten expr: {}",
            rewritten[0].expr_sql
        );
        assert_eq!(joins.len(), 1);
        assert!(joins[0].contains("LEFT JOIN"));
        assert!(joins[0].contains("GROUP BY t.t_ca_id"));
        assert!(joins[0].contains("MAX(t.t_qty)"));
        assert!(joins[0].contains("t.t_st_id = 'PNDG'"));
        assert!(joins[0].contains("\"__corr0\".__k0 = \"holding_summary\".hs_ca_id"));
    }

    #[test]
    fn decorrelator_handles_two_correlation_keys() {
        let sql = "\
UPDATE holding_summary \
SET hs_qty = ( \
    SELECT t.t_qty FROM trade t \
    WHERE t.t_ca_id = holding_summary.hs_ca_id \
      AND t.t_s_symb = holding_summary.hs_s_symb \
)";
        let assignments = parse_update(sql);
        let (_, joins) = decorrelate_scalar_subqueries(&assignments, "holding_summary");
        assert_eq!(joins.len(), 1);
        assert!(joins[0].contains("GROUP BY t.t_ca_id, t.t_s_symb"));
        assert!(joins[0].contains("__k0 = \"holding_summary\".hs_ca_id"));
        assert!(joins[0].contains("__k1 = \"holding_summary\".hs_s_symb"));
    }

    #[test]
    fn decorrelator_skips_when_no_correlation() {
        // Subquery with no reference to the UPDATE target — leave as-is.
        let sql = "\
UPDATE customer \
SET c_balance = ( \
    SELECT MAX(trade_price) FROM trade WHERE t_st_id = 'PNDG' \
)";
        let assignments = parse_update(sql);
        let (rewritten, joins) = decorrelate_scalar_subqueries(&assignments, "customer");
        assert!(joins.is_empty(), "should not emit joins: {:?}", joins);
        // Expression should still contain the subquery unchanged.
        assert!(
            rewritten[0].expr_sql.contains("SELECT"),
            "subquery should remain: {}",
            rewritten[0].expr_sql
        );
    }

    #[test]
    fn decorrelator_skips_when_no_subquery() {
        let sql = "UPDATE district SET d_ytd = d_ytd + 2500.00 WHERE d_id = 1";
        let assignments = parse_update(sql);
        let (rewritten, joins) = decorrelate_scalar_subqueries(&assignments, "district");
        assert!(joins.is_empty());
        assert_eq!(rewritten[0].expr_sql, "d_ytd + 2500.00");
    }

    #[test]
    fn decorrelator_skips_non_equality_correlation() {
        // Correlation via `>` — we only decorrelate equality shapes.
        let sql = "\
UPDATE t \
SET c = ( \
    SELECT x FROM s WHERE s.k > t.k \
)";
        let assignments = parse_update(sql);
        let (_, joins) = decorrelate_scalar_subqueries(&assignments, "t");
        assert!(joins.is_empty(), "non-eq correlation should be left alone");
    }

    // -------------------------------------------------------------------------
    // reorder_insert_select: INSERT INTO t (b, a) SELECT ... must not swap
    // columns by position. The writer stamps Iceberg field IDs positionally,
    // so reordering at SQL planning time is the only safe fix.
    // -------------------------------------------------------------------------

    fn make_target_schema() -> IcebergSchema {
        use iceberg::spec::{PrimitiveType, Type};
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "c",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap()
    }

    fn ident(name: &str) -> sqlparser::ast::ObjectName {
        // reorder_insert_select takes a column list of ObjectName (sqlparser 0.62).
        sqlparser::ast::ObjectName::from(vec![sqlparser::ast::Ident::new(name.to_string())])
    }

    #[test]
    fn reorder_insert_swaps_columns_when_explicit_list_reverses_order() {
        let schema = make_target_schema();
        let columns = vec![ident("b"), ident("a"), ident("c")];
        let sql = reorder_insert_select("SELECT 1, 2, 3", &columns, &schema).unwrap();
        assert!(
            sql.contains("\"a\", \"b\", \"c\""),
            "outer projection must be in target order: {sql}"
        );
        assert!(
            sql.contains("__sqe_insert_src(\"b\", \"a\", \"c\")"),
            "inner CTE alias list must match user-provided order: {sql}"
        );
    }

    #[test]
    fn reorder_insert_fills_missing_columns_with_null() {
        let schema = make_target_schema();
        let columns = vec![ident("a")];
        let sql = reorder_insert_select("SELECT 99", &columns, &schema).unwrap();
        assert!(
            sql.contains("NULL AS \"b\""),
            "unmentioned columns must be NULL: {sql}"
        );
        assert!(sql.contains("NULL AS \"c\""), "{sql}");
        assert!(sql.contains("\"a\","), "{sql}");
    }

    #[test]
    fn reorder_insert_rejects_unknown_column() {
        let schema = make_target_schema();
        let columns = vec![ident("does_not_exist")];
        let err = reorder_insert_select("SELECT 1", &columns, &schema).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("does not exist"), "msg: {msg}");
    }

    #[test]
    fn reorder_insert_rejects_duplicate_column() {
        let schema = make_target_schema();
        let columns = vec![ident("a"), ident("a")];
        let err = reorder_insert_select("SELECT 1, 2", &columns, &schema).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate"), "msg: {msg}");
    }

    #[test]
    fn reorder_insert_rejects_more_columns_than_target() {
        let schema = make_target_schema();
        let columns = vec![ident("a"), ident("b"), ident("c"), ident("d")];
        let err = reorder_insert_select("SELECT 1, 2, 3, 4", &columns, &schema).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("has 4 entries"), "msg: {msg}");
    }

    #[test]
    fn reorder_insert_is_case_insensitive() {
        let schema = make_target_schema();
        let columns = vec![ident("A"), ident("B")];
        // Should not error; both "A" and "B" map to lowercase target names.
        reorder_insert_select("SELECT 1, 2", &columns, &schema).unwrap();
    }

    // -------------------------------------------------------------------------
    // Policy enforcement on write paths (#36). The WriteHandler must run the
    // source SELECT through the configured PolicyEnforcer before writing.
    // -------------------------------------------------------------------------

    fn write_test_config() -> SqeConfig {
        let toml_text = r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]
catalog_url = "http://127.0.0.1:9/unused"
warehouse = "test_wh"

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#;
        toml::from_str::<SqeConfig>(toml_text).expect("config parses")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enforce_source_plan_passes_through_when_no_enforcer() {
        use datafusion::logical_expr::LogicalPlanBuilder;
        let handler = WriteHandler::new(write_test_config());
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let session = sqe_core::Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("tok".to_string()),
            None,
            chrono::Utc::now(),
            vec![],
        );
        let before = format!("{plan:?}");
        let (after, summary) = handler.enforce_source_plan(&session, plan).await.unwrap();
        assert_eq!(before, format!("{after:?}"));
        assert_eq!(summary, sqe_policy::PolicySummary::default());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enforce_source_plan_applies_mask_via_rewriter() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema2};
        use datafusion::common::TableReference;
        use datafusion::datasource::{provider_as_source, MemTable};
        use datafusion::logical_expr::LogicalPlanBuilder;
        use sqe_policy::plan_rewriter::PolicyPlanRewriter;
        use sqe_policy::policy_store::InMemoryPolicyStore;
        use sqe_policy::{MaskType, ResolvedPolicy};

        let arrow_schema = Arc::new(ArrowSchema2::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ssn", DataType::Utf8, true),
        ]));
        let mem = Arc::new(MemTable::try_new(arrow_schema, vec![vec![]]).unwrap());
        let plan = LogicalPlanBuilder::scan(
            TableReference::bare("employees"),
            provider_as_source(mem),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.column_masks
            .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
        store.add_table_policy("default", "employees", pol).await;
        let enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
            Arc::new(PolicyPlanRewriter::new(Arc::new(store)));

        let handler = WriteHandler::new(write_test_config()).with_policy_enforcer(enforcer);
        let session = sqe_core::Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("tok".to_string()),
            None,
            chrono::Utc::now(),
            vec![],
        );
        let (rewritten, summary) = handler.enforce_source_plan(&session, plan).await.unwrap();
        let s = format!("{}", rewritten.display_indent());
        assert!(
            s.contains("Projection:") && s.contains("\"***\""),
            "expected redacted projection, got: {s}"
        );
        assert_eq!(summary.columns_masked, vec!["ssn".to_string()]);

        // Also verify the rewriter trait method can be called via the field
        // (the `_` import keeps the trait in scope even if a future code
        // change drops the explicit call site above).
        let _ = sqe_policy::PassthroughEnforcer;
    }

    // -------------------------------------------------------------------------
    // Policy enforcement on DELETE / UPDATE WHERE-evaluator paths (#36 finish).
    // Issue #36 documented an integrity bypass on INSERT and CTAS source SELECTs
    // that MR !195 closed. The DELETE and UPDATE handlers read parquet files
    // directly and evaluate WHERE per batch, which the rewriter never saw.
    // These tests drive the per-batch helpers with policy-derived predicates
    // and assert the WHERE actually gets the policy row filter ANDed onto it
    // and any mask substitution gets applied before evaluation.
    // -------------------------------------------------------------------------

    fn make_employee_batch() -> arrow_array::RecordBatch {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema2};
        let schema = Arc::new(ArrowSchema2::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ssn", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "111-11", "222-22", "333-33", "444-44",
                ])),
                Arc::new(StringArray::from(vec!["EU", "US", "EU", "US"])),
            ],
        )
        .unwrap()
    }

    async fn employees_handler_with_policy(
        row_filter: Option<datafusion::logical_expr::Expr>,
        masks: Vec<(&str, sqe_policy::MaskType)>,
    ) -> (
        WriteHandler,
        sqe_core::Session,
        TableIdent,
        DFSessionContext,
    ) {
        use sqe_policy::plan_rewriter::PolicyPlanRewriter;
        use sqe_policy::policy_store::InMemoryPolicyStore;
        use sqe_policy::ResolvedPolicy;

        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        if let Some(rf) = row_filter {
            pol.row_filters.push(rf);
        }
        for (col, mask) in masks {
            pol.column_masks.insert(col.to_string(), mask);
        }
        store.add_table_policy("default", "employees", pol).await;
        let enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
            Arc::new(PolicyPlanRewriter::new(Arc::new(store)));

        let handler = WriteHandler::new(write_test_config()).with_policy_enforcer(enforcer);
        let session = sqe_core::Session::new(
            "alice".to_string(),
            "tok".to_string().into(),
            None,
            chrono::Utc::now(),
            vec![],
        );
        let table_ident = TableIdent::new(
            NamespaceIdent::new("default".to_string()),
            "employees".to_string(),
        );
        let ctx = DFSessionContext::new();
        (handler, session, table_ident, ctx)
    }

    async fn compute_predicates_for_employees(
        handler: &WriteHandler,
        session: &sqe_core::Session,
        table_ident: &TableIdent,
    ) -> sqe_policy::write_predicates::WritePolicyPredicates {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema2};
        let schema = Arc::new(ArrowSchema2::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ssn", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let mut policy_summary = None;
        handler
            .compute_write_predicates(session, table_ident, schema, &mut policy_summary)
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_with_row_filter_keeps_rows_outside_user_view() {
        use arrow_array::Array as _;
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        assert!(predicates.row_filter_sql.is_some(), "row filter expected");

        // User says DELETE everything (WHERE TRUE). With the EU row filter the
        // effective predicate is "TRUE AND region = 'EU'", so only EU rows
        // (ids 1 and 3) get filtered out, leaving the US rows (ids 2 and 4).
        let batch = make_employee_batch();
        let survivors = handler
            .filter_batch_negate(&ctx, &batch, "TRUE", "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(survivors.num_rows(), 2, "US rows must survive");
        let regions = survivors
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .clone();
        for i in 0..regions.len() {
            assert_eq!(regions.value(i), "US", "EU rows must not survive");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_count_with_row_filter_matches_admitted_rows() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        // User says DELETE WHERE TRUE. Policy restricts to EU rows. Match
        // count must be the EU rows (2), not every row (4).
        let batch = make_employee_batch();
        let mask = handler
            .filter_batch_match(&ctx, &batch, "TRUE", "", &table_ident, &predicates)
            .await
            .unwrap();
        let matched: u32 = (0..mask.len()).map(|i| mask.value(i) as u32).sum();
        assert_eq!(matched, 2, "only EU rows should be marked for deletion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_match_with_column_mask_uses_masked_value_in_where() {
        use sqe_policy::MaskType;
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(None, vec![("ssn", MaskType::Redact("***".to_string()))])
                .await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        assert!(predicates.column_mask_sqls.contains_key("ssn"));

        // User asks to delete rows where ssn = '111-11'. The mask redacts
        // ssn to '***', so the WHERE evaluator must see '***' for every row
        // and match zero rows.
        let batch = make_employee_batch();
        let mask = handler
            .filter_batch_match(
                &ctx,
                &batch,
                "ssn = '111-11'",
                "",
                &table_ident,
                &predicates,
            )
            .await
            .unwrap();
        let matched: u32 = (0..mask.len()).map(|i| mask.value(i) as u32).sum();
        assert_eq!(matched, 0, "masked ssn must not match the raw value");

        // The complementary check: WHERE ssn = '***' matches every row when
        // the mask is in effect. Confirms the policy substitution actually
        // runs in the evaluator.
        let mask2 = handler
            .filter_batch_match(&ctx, &batch, "ssn = '***'", "", &table_ident, &predicates)
            .await
            .unwrap();
        let matched2: u32 = (0..mask2.len()).map(|i| mask2.value(i) as u32).sum();
        assert_eq!(matched2, 4, "every row matches the redacted ssn");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_with_row_filter_only_modifies_admitted_rows() {
        use arrow_array::Array as _;
        use datafusion::logical_expr::{col, lit};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        // UPDATE employees SET ssn = 'X' WHERE TRUE. Policy restricts to EU.
        let sql = "UPDATE employees SET ssn = 'X'";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();
        let assignments = match stmt {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        };
        let batch = make_employee_batch();
        let rewritten = handler
            .apply_update(
                &ctx,
                &batch,
                &assignments,
                "TRUE",
                "",
                &table_ident,
                &predicates,
            )
            .await
            .unwrap();
        // EU rows (ids 1 and 3) should have ssn = 'X'; US rows (ids 2 and 4)
        // should keep their original ssn.
        let ssn = rewritten
            .column_by_name("ssn")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .clone();
        let region = rewritten
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .clone();
        for i in 0..rewritten.num_rows() {
            if region.value(i) == "EU" {
                assert_eq!(ssn.value(i), "X", "EU row must be updated at index {i}");
            } else {
                assert_ne!(
                    ssn.value(i),
                    "X",
                    "US row must keep its original ssn at index {i}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_with_column_mask_writes_raw_value_for_unchanged_rows() {
        use arrow_array::Array as _;
        use sqe_policy::MaskType;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(None, vec![("ssn", MaskType::Redact("***".to_string()))])
                .await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        // UPDATE employees SET region = 'XX' WHERE id = 1. The ssn column is
        // masked but is NOT being assigned. The CASE-ELSE branch must write
        // the raw ssn back to disk for every row, not the masked '***'.
        let sql = "UPDATE employees SET region = 'XX' WHERE id = 1";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();
        let (assignments, where_sql) = match stmt {
            sqlparser::ast::Statement::Update(update) => {
                (update.assignments, format!("{}", update.selection.unwrap()))
            }
            _ => panic!("expected UPDATE"),
        };

        let batch = make_employee_batch();
        let rewritten = handler
            .apply_update(
                &ctx,
                &batch,
                &assignments,
                &where_sql,
                "",
                &table_ident,
                &predicates,
            )
            .await
            .unwrap();
        let ssn = rewritten
            .column_by_name("ssn")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .clone();
        for i in 0..ssn.len() {
            assert_ne!(
                ssn.value(i),
                "***",
                "raw ssn must persist for unchanged column at index {i}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_count_with_row_filter_matches_admitted_rows() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let batch = make_employee_batch();
        let count = handler
            .count_matching_rows(&ctx, &batch, "TRUE", "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(count, 2, "row filter must shrink the affected count");
    }

    // -------------------------------------------------------------------------
    // commit conflict retry classifier (#47)
    // -------------------------------------------------------------------------

    #[test]
    fn is_conflict_message_matches_vendor_conflicts() {
        assert!(is_conflict_message("Commit conflicted for table: db.t"));
        assert!(is_conflict_message(
            "CatalogCommitConflicts: stale snapshot 12345"
        ));
        assert!(is_conflict_message("RowDelta conflict on partition"));
        assert!(is_conflict_message("Operation marked retryable=true"));
    }

    #[test]
    fn is_conflict_message_ignores_unrelated_errors() {
        assert!(!is_conflict_message("table not found"));
        assert!(!is_conflict_message("malformed parquet footer"));
        assert!(!is_conflict_message("connection refused"));
    }
}
