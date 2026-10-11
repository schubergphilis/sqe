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

use sqe_catalog::puffin_stats::{puffin_stats_enabled, ColumnSketches, ThetaSketchAccumulator};
use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::table_properties::{
    resolve_delete_mode, resolve_merge_mode, resolve_update_mode, WriteMode,
};
use sqe_core::{Session, SqeConfig, SqeError, SqeErrorCode};
use tracing::instrument;

use crate::commit::{commit_with_retry, is_conflict_message};
use crate::ident::{catalog_qualifier, fold_unquoted_ident, parse_table_ref};
use crate::writer::{
    new_upload_tracker, project_to_identifier_columns, resolve_write_compression,
    write_data_files_streaming_with_metrics, write_data_files_with_metrics,
    write_equality_delete_key_files, FanoutLimits, PositionDeleteWriter, WriteCleanupGuard,
};
use sqe_compaction::write_memory::{TrackedBatchBuffer, WriteReservation};

/// Check an INSERT's projected columns against the target table's schema
/// before execution starts (architecture review 505).
///
/// Arity is an error. Without this the mismatch surfaces deep inside the write
/// task: a wider SELECT used to index past the end of the stamped schema and
/// panic (the client saw a generic internal error and the log a panic), and a
/// narrower one failed at `RecordBatch::try_new` after the full SELECT had run
/// and the first parquet part had been opened.
///
/// Names are NOT an error: INSERT matches columns positionally per the SQL
/// standard, so `INSERT INTO t SELECT b, a FROM s` legitimately swaps them.
/// A positional name difference is logged at info so an operator can spot an
/// accidental swap.
fn check_insert_arity(
    source_columns: &[String],
    target_schema: &IcebergSchema,
    table_ident: &TableIdent,
) -> sqe_core::Result<()> {
    let target_fields = target_schema.as_struct().fields();
    if source_columns.len() != target_fields.len() {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "INSERT has {} columns but target has {}",
            source_columns.len(),
            target_fields.len()
        )));
    }
    let swapped: Vec<String> = source_columns
        .iter()
        .zip(target_fields.iter())
        .filter(|(src, tgt)| !src.eq_ignore_ascii_case(&tgt.name))
        .map(|(src, tgt)| format!("{src} -> {}", tgt.name))
        .collect();
    if !swapped.is_empty() {
        info!(
            table = %table_ident,
            positions = %swapped.join(", "),
            "INSERT column names differ from the target's in the same position; \
             columns are written positionally"
        );
    }
    Ok(())
}

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
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "INSERT column list contains duplicate column '{name}'"
            )));
        }
        if !target_names.iter().any(|t| t.eq_ignore_ascii_case(name)) {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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

/// Shared handle to the streaming-path sketch accumulator.
type SharedSketchAccumulator = Arc<std::sync::Mutex<ThetaSketchAccumulator>>;

/// Wrap a write stream so every batch also feeds the Puffin NDV sketches,
/// when `table` opted in via `write.puffin.stats`. Returns the stream
/// unchanged and `None` otherwise, so opted-out tables pay nothing.
fn tap_stream_for_puffin_sketches(
    table: &IcebergTable,
    stream: datafusion::execution::SendableRecordBatchStream,
) -> (
    datafusion::execution::SendableRecordBatchStream,
    Option<SharedSketchAccumulator>,
) {
    if !puffin_stats_enabled(table.metadata().properties()) {
        return (stream, None);
    }
    let acc: SharedSketchAccumulator = Arc::new(std::sync::Mutex::new(
        ThetaSketchAccumulator::new(table.metadata().current_schema()),
    ));
    let tap = Arc::clone(&acc);
    let schema = stream.schema();
    let tapped = stream.inspect(move |item| {
        if let Ok(batch) = item {
            // A poisoned lock means an earlier panic in this task; skipping
            // the sketch update loses statistics, never data.
            if let Ok(mut guard) = tap.lock() {
                guard.update(batch);
            }
        }
    });
    let stream: datafusion::execution::SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(schema, Box::pin(tapped)));
    (stream, Some(acc))
}

/// Take the accumulated sketches once the writer has drained the stream.
/// `None` when the table did not opt in or no batch was ever seen.
fn finish_puffin_sketches(acc: Option<SharedSketchAccumulator>) -> Option<ColumnSketches> {
    let acc = acc?;
    let acc = Arc::try_unwrap(acc).ok()?.into_inner().ok()?;
    if acc.is_empty() {
        return None;
    }
    Some(acc.finish())
}

/// How many rows a MoR position DELETE newly hides in one data file (514).
///
/// `raw_matches` is the number of WHERE matches found by the raw,
/// position-preserving scan — the scan that produces the physical positions,
/// and the one that must stay raw because position deletes address physical
/// offsets. `live_matches` is `Some(n)` when the file carries live delete
/// files and the same predicate was re-evaluated over a delete-aware read of
/// it, and `None` when the file has no delete files (every raw match is then
/// live by construction).
///
/// A row an earlier DELETE already hid is re-marked by the raw scan but is not
/// deleted again, so it must not be counted: otherwise the second run of an
/// idempotent `DELETE ... WHERE x = 1` reports the same non-zero count as the
/// first. `min` clamps the pathological case where the delete-aware read
/// somehow returns more matches than the raw one (a snapshot skew between the
/// two reads) so the count can never exceed the positions actually marked.
pub(crate) fn newly_deleted_row_count(raw_matches: usize, live_matches: Option<usize>) -> usize {
    match live_matches {
        Some(live) => live.min(raw_matches),
        None => raw_matches,
    }
}

/// V3 merge-on-read DELETE for one data file.
///
/// Unions the file's existing deletes (its deletion vector and any position
/// delete files, from `task`) with `new_positions`, writes one deletion vector
/// holding the result, and returns it with the delete files it supersedes: the
/// old vector and position-delete files scoped to this data file. Position
/// deletes that span several data files stay; the new vector already carries
/// their positions for this file and supersedes them for it.
async fn merge_into_deletion_vector(
    table: &IcebergTable,
    data_file: &DataFile,
    task: Option<&iceberg::scan::FileScanTask>,
    new_positions: Vec<i64>,
) -> sqe_core::Result<(Vec<DataFile>, Vec<DataFile>)> {
    let failed = |what: String| SqeError::with_code(SqeErrorCode::ExecutionFailed, what);
    let mut positions = match task {
        Some(task) => iceberg::arrow::load_position_deletes(table.file_io().clone(), task)
            .await
            .map_err(|e| {
                failed(format!(
                    "Failed to load existing deletes of {}: {e}",
                    data_file.file_path()
                ))
            })?,
        None => iceberg::delete_vector::DeleteVector::default(),
    };
    for pos in new_positions {
        let pos = u64::try_from(pos).map_err(|_| failed(format!("negative row position {pos}")))?;
        positions.insert(pos);
    }
    let vectors = crate::writer::write_deletion_vectors(
        table,
        vec![(data_file.clone(), positions.iter().collect())],
    )
    .await?;

    let mut retired = Vec::new();
    for old in task.map(|t| t.deletes()).unwrap_or_default() {
        if old.file_type != DataContentType::PositionDeletes
            || old.referenced_data_file.as_deref() != Some(data_file.file_path())
        {
            continue;
        }
        // Removal matches on (path, content offset, content size); the rest
        // feeds the snapshot summary's removed-delete counts.
        retired.push(
            iceberg::spec::DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(old.file_path.clone())
                .file_format(old.file_format)
                .file_size_in_bytes(old.file_size_in_bytes)
                .record_count(old.record_count.unwrap_or(0))
                .referenced_data_file(old.referenced_data_file.clone())
                .content_offset(old.content_offset)
                .content_size_in_bytes(old.content_size_in_bytes)
                .partition_spec_id(old.partition_spec_id)
                .partition(data_file.partition().clone())
                .build()
                .map_err(|e| failed(format!("Failed to describe retired delete file: {e}")))?,
        );
    }
    Ok((vectors, retired))
}

/// Sum of the manifest `record_count` over a set of data files.
///
/// The truncate paths drop whole files, so the number of rows they removed is
/// known from metadata alone; reporting it beats handing the driver an empty
/// result it renders as "unknown" (514).
///
/// Caveat: on a table that still carries live position or equality deletes,
/// some of those rows were already hidden, so this over-reports by the number
/// of live delete records. Counting exactly would mean a full delete-aware
/// scan of a table that is about to be emptied anyway.
pub(crate) fn data_file_record_count(files: &[DataFile]) -> usize {
    files.iter().map(|f| f.record_count() as usize).sum()
}

/// Build a single-row RecordBatch reporting affected row count.
/// Matches Trino's DML response which returns the update count.
///
/// Every DML handler (INSERT / DELETE / UPDATE / MERGE, including every early
/// return on a no-op) must return through this helper. A bare `Ok(vec![])` is
/// an empty *result set*, which ADBC, Trino JDBC and dbt each render
/// differently ("unknown", 0, or nothing at all) — so the same statement
/// reported different numbers depending on which early return it hit (514).
/// The `Result` return is already `#[must_use]`, so a dropped call stands out
/// in review; only DDL (CREATE TABLE / CTAS) legitimately returns no rows at all.
pub fn affected_rows_batch(count: u64) -> sqe_core::Result<Vec<RecordBatch>> {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    let n = i64::try_from(count).map_err(|_| {
        SqeError::with_code(
            SqeErrorCode::InternalError,
            format!("rows_affected {count} does not fit in i64"),
        )
    })?;
    let schema = Arc::new(ArrowSchema::new(vec![Field::new("rows_affected", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n]))]).map_err(
        |e| {
            SqeError::with_code(
                SqeErrorCode::InternalError,
                format!("failed to build rows_affected batch: {e}"),
            )
        },
    )?;
    Ok(vec![batch])
}

/// Run a planned one-row, one-column `COUNT(*)` query and return the value.
/// `label` names the caller in the error message. Shared by the MERGE
/// cardinality check and the empty-source affected-row count (501).
async fn scalar_count(df: datafusion::dataframe::DataFrame, label: &str) -> sqe_core::Result<i64> {
    let batches = df
        .collect()
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("{label} failed: {e}")))?;
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
/// The target stub's schema is `target` when the write goes into an existing
/// table (INSERT: the table's own columns, which the extractor names the
/// output fields after, by position), else `source.schema()` (CTAS, and the
/// self-rewrites whose source IS the table).
fn build_lineage_insert_plan(
    source: LogicalPlan,
    target: Option<ArrowSchema>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> sqe_core::Result<LogicalPlan> {
    let arrow_schema: ArrowSchema = target.unwrap_or_else(|| source.schema().as_arrow().clone());
    let target_mem = MemTable::try_new(Arc::new(arrow_schema), vec![vec![]])
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build lineage target stub: {e}")))?;
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
    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build lineage INSERT wrapper: {e}")))?
    .build()
    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build lineage plan: {e}")))
}

/// Record the Iceberg snapshot a write committed on the lineage wrapper it
/// captured, so the emitter reports it as the OL `version` facet. The wrapper
/// is rebuilt with the id in its target stub's schema metadata
/// (`sqe_lineage::extract::SNAPSHOT_ID_METADATA_KEY`); anything other than a
/// captured `Dml` wrapper, or no snapshot, leaves `plan_out` as it is.
fn record_lineage_snapshot(
    plan_out: &mut Option<sqe_lineage::PlanOrHint>,
    snapshot_id: Option<i64>,
) {
    let Some(snapshot_id) = snapshot_id else {
        return;
    };
    let Some(sqe_lineage::PlanOrHint::Plan(plan)) = plan_out.as_mut() else {
        return;
    };
    let LogicalPlan::Dml(dml) = plan.as_ref() else {
        return;
    };
    let target = dml.target.schema();
    let mut metadata = target.metadata().clone();
    metadata.insert(
        sqe_lineage::extract::SNAPSHOT_ID_METADATA_KEY.to_string(),
        snapshot_id.to_string(),
    );
    let schema = target.as_ref().clone().with_metadata(metadata);
    let rebuilt = MemTable::try_new(Arc::new(schema), vec![vec![]]).and_then(|stub| {
        LogicalPlanBuilder::insert_into(
            dml.input.as_ref().clone(),
            dml.table_name.clone(),
            provider_as_source(Arc::new(stub)),
            InsertOp::Append,
        )?
        .build()
    });
    match rebuilt {
        Ok(wrapper) => **plan = wrapper,
        Err(e) => tracing::debug!(error = %e, "lineage: could not record the committed snapshot"),
    }
}

/// Extract the target table `ObjectName` from a `Statement::Delete`. Centralises
/// the FromTable + TableFactor matching so a future sqlparser variant change only
/// needs to be patched once.
pub fn delete_target_object_name(stmt: &Statement) -> sqe_core::Result<&ObjectName> {
    let delete = match stmt {
        Statement::Delete(d) => d,
        other => {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
        .ok_or_else(|| SqeError::with_code(SqeErrorCode::ExecutionFailed, "DELETE statement has no target table".to_string()))?;
    match &first.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name),
        other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
pub fn update_target_object_name(stmt: &Statement) -> sqe_core::Result<&ObjectName> {
    let table_factor = match stmt {
        Statement::Update(update) => &update.table,
        other => {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "Expected UPDATE statement, got: {other}"
            )));
        }
    };
    match &table_factor.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name),
        other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
/// in OL events. The catalog is the one the write resolves against
/// ([`resolve_target_catalog`]: the statement's qualifier, else the session
/// catalog), falling back to `resolve_default_catalog()` for an unqualified
/// statement in a session that names none. It used to be the configured
/// default always, so a write into a workspace catalog was labelled with the
/// default one. The OL `catalog_lookup` maps the name to its Iceberg catalog.
fn lineage_target_parts(
    config: &SqeConfig,
    session: &Session,
    name: &ObjectName,
    table_ident: &TableIdent,
) -> (String, String, String) {
    let catalog = resolve_target_catalog(catalog_qualifier(name), session)
        .filter(|c| !c.is_empty())
        .unwrap_or_else(|| config.resolve_default_catalog());
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Iceberg -> Arrow schema: {e}")))?;
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

/// Alias used inside the policy subquery to expose the stored value of a
/// column the caller reads through a mask or the row-filter guard. UPDATE's
/// CASE-ELSE branch must reference this so unchanged rows persist their
/// stored values instead of the masked (or guarded NULL) value. `nonce` is
/// fresh per statement evaluation so user SQL spliced into the same SELECT
/// cannot name the alias and read the raw value.
fn policy_raw_alias(col: &str, nonce: &str) -> String {
    format!("__pol_raw_{nonce}_{col}")
}

/// Alias of the row-filter verdict inside the policy subquery: TRUE exactly
/// when the caller's row filter admits the row, computed on stored values.
fn policy_admit_alias(nonce: &str) -> String {
    format!("__pol_admit_{nonce}")
}

/// Refuse user SQL that names the internal `__pol_` aliases. The per-call
/// nonce already makes them unguessable.
fn refuse_internal_policy_aliases<'a>(
    stmt: &str,
    sqls: impl IntoIterator<Item = &'a str>,
) -> sqe_core::Result<()> {
    if sqls
        .into_iter()
        .any(|s| s.to_ascii_lowercase().contains("__pol_"))
    {
        return Err(SqeError::with_code(
            SqeErrorCode::AccessDenied,
            format!("{stmt} may not reference internal `__pol_` identifiers"),
        ));
    }
    Ok(())
}

/// Synthetic flag column marking the rows an UPDATE evaluation changed;
/// stripped before the batch leaves `apply_update`.
const UPDATE_CHANGED_COLUMN: &str = "__sqe_update_changed";

/// Combine the user's WHERE clause with the policy row filter's verdict, so
/// DELETE and UPDATE only act on rows the policy admits, matching PostgreSQL
/// RLS semantics on the SELECT path. The verdict is the admit column of the
/// policy relation ([`policy_projection_sql`]); the user's WHERE reads
/// columns through the row-filter guard there, so it never sees a hidden
/// row's values and cannot fail on them.
fn augment_where_with_row_filter(
    where_sql: &str,
    predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    orig_alias: &str,
    nonce: &str,
) -> String {
    match &predicates.row_filter_sql {
        Some(_) => format!(
            "({where_sql}) AND \"{orig_alias}\".\"{}\"",
            policy_admit_alias(nonce)
        ),
        None => where_sql.to_string(),
    }
}

/// FROM clause for the per-batch evaluator, plus the guard that deregisters
/// the masked view behind it (if any) on drop.
struct PolicyFrom {
    sql: String,
    _view: Option<MergeScratchCleanup>,
}

/// Build the FROM clause for the per-batch evaluator. With no policy this
/// is the scratch table aliased to the target name. Otherwise it is the
/// policy relation of [`policy_projection_sql`] over the scratch table. With
/// column masks that relation is a view planned in the separate mask context
/// ([`crate::policy_view`]) because a HASH mask calls the internal keyed
/// function, which the session planning the user's WHERE / SET cannot
/// resolve. A row filter alone stays inline.
async fn build_policy_from_clause(
    ctx: &DFSessionContext,
    scratch_table: &str,
    orig_alias: &str,
    schema: &ArrowSchema,
    predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    nonce: &str,
    raw_aliases: bool,
) -> sqe_core::Result<PolicyFrom> {
    let Some(select_sql) =
        policy_projection_sql(scratch_table, orig_alias, schema, predicates, nonce, raw_aliases)
    else {
        return Ok(PolicyFrom {
            sql: format!("{scratch_table} AS \"{orig_alias}\""),
            _view: None,
        });
    };
    if predicates.column_mask_sqls.is_empty() {
        return Ok(PolicyFrom {
            sql: format!("({select_sql}) AS \"{orig_alias}\""),
            _view: None,
        });
    }
    let view = format!(
        "datafusion.public.{DML_SCRATCH_PREFIX}pol_{}",
        Uuid::now_v7().simple()
    );
    // Guard first so a failed registration leaves nothing behind.
    let cleanup = MergeScratchCleanup::new(ctx, vec![view.clone()]);
    crate::policy_view::register_policy_view(ctx, &view, &select_sql, predicates).await?;
    Ok(PolicyFrom {
        sql: format!("{view} AS \"{orig_alias}\""),
        _view: Some(cleanup),
    })
}

/// The policy SELECT over the scratch table, or `None` without a policy.
/// Only column names, the scratch name and policy text go in.
///
/// Each column is projected as the caller sees it: through its mask, and
/// with a row filter through the guard `CASE WHEN <row filter> THEN <col>
/// END`, so a hidden row is all NULL to the user's WHERE and SET (they are
/// spliced into the same SELECT and may be evaluated on every row; a
/// fallible one, say `1 / (id - 2)`, would otherwise fail on a hidden row and
/// reveal its values). The row filter's verdict on the stored values is
/// projected as [`policy_admit_alias`], TRUE-only so a NULL verdict hides the
/// row. With `raw_aliases` (UPDATE's write-back), every column read through a
/// mask or the guard also gets a [`policy_raw_alias`] carrying its stored
/// value. Unlike the read path, which guards only queries holding a fallible
/// expression, this always guards: the scratch table has no scan pruning to
/// lose.
fn policy_projection_sql(
    scratch_table: &str,
    orig_alias: &str,
    schema: &ArrowSchema,
    predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    nonce: &str,
    raw_aliases: bool,
) -> Option<String> {
    let row_filter = predicates.row_filter_sql.as_deref();
    if predicates.column_mask_sqls.is_empty() && row_filter.is_none() {
        return None;
    }

    let mut projections: Vec<String> = Vec::with_capacity(2 * schema.fields().len() + 1);
    for f in schema.fields() {
        let name = f.name();
        let mask = predicates.column_mask_sqls.get(name);
        let seen = match mask {
            Some(mask_sql) => format!("({mask_sql})"),
            None => format!("\"{name}\""),
        };
        match row_filter {
            Some(rf) => projections.push(format!("CASE WHEN ({rf}) THEN {seen} END AS \"{name}\"")),
            None if mask.is_some() => projections.push(format!("{seen} AS \"{name}\"")),
            None => projections.push(seen),
        }
        if raw_aliases && (mask.is_some() || row_filter.is_some()) {
            let raw = policy_raw_alias(name, nonce);
            projections.push(format!("\"{name}\" AS \"{raw}\""));
        }
    }
    if let Some(rf) = row_filter {
        projections.push(format!("(({rf}) IS TRUE) AS \"{}\"", policy_admit_alias(nonce)));
    }
    let projection_sql = projections.join(", ");
    // Aliased to the target name: the policy SQL may qualify its columns.
    Some(format!(
        "SELECT {projection_sql} FROM {scratch_table} AS \"{orig_alias}\""
    ))
}

/// Best-effort cleanup of a partially-created CTAS table.
///
/// The CTAS streaming path follows the same
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

/// Max attempts for a CoW UPDATE/DELETE that loses an optimistic-concurrency race.
const COW_MAX_ATTEMPTS: u32 = 4;

/// One attempt of a copy-on-write rewrite, produced by the closure
/// [`WriteHandler::commit_rewrite_with_retry`] runs against a freshly loaded
/// snapshot.
///
/// `add` / `remove` become the `RewriteFilesAction`'s file sets. `cleanup` is
/// the attempt's [`WriteCleanupGuard`]: the helper marks it committed when the
/// commit lands and drops it (which deletes the attempt's orphan parquet files)
/// when the attempt loses its commit race. `value` is whatever the caller wants
/// back from the winning attempt — a row count, `()`, ...
pub(crate) struct RewriteAttempt<T> {
    pub add: Vec<DataFile>,
    pub remove: Vec<DataFile>,
    pub cleanup: Option<WriteCleanupGuard>,
    pub value: T,
}

impl WriteHandler {
    /// Bounded retry around a copy-on-write `rewrite_files` commit, with the
    /// whole rewrite re-run against a reloaded snapshot on an
    /// optimistic-concurrency conflict (architecture review 506).
    ///
    /// [`commit_with_retry`] cannot be reused for these: it re-commits the
    /// *same* data files on every attempt, which is correct for an append but
    /// wrong for a rewrite, whose removed-file set and rewritten contents are
    /// snapshot-specific. Before this helper, four commit sites carried a
    /// hand-rolled copy of the loop (truncate, CoW DELETE, CoW UPDATE, INSERT
    /// OVERWRITE) and three had no retry at all (CoW MERGE and the two MoR
    /// truncate fast paths), so the same statement survived or failed a
    /// concurrent writer depending on `write.delete.mode`.
    ///
    /// `build_attempt` receives the reloaded table and must do the whole read →
    /// rewrite → collect-files pass against it. Returning `Ok(None)` means
    /// "nothing to do on this snapshot" (empty table, no partition matched the
    /// WHERE): no commit is attempted and the helper returns `Ok(None)`.
    ///
    /// `op` is the low-cardinality metric/log label
    /// (`sqe_write_commit_retries_total{op}`); `stmt_label` is what the user
    /// sees in `Failed to commit {stmt_label}`.
    async fn commit_rewrite_with_retry<T, F, Fut>(
        &self,
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        op: &'static str,
        stmt_label: &str,
        mut build_attempt: F,
    ) -> sqe_core::Result<Option<T>>
    where
        F: FnMut(IcebergTable) -> Fut,
        Fut: std::future::Future<Output = sqe_core::Result<Option<RewriteAttempt<T>>>>,
    {
        let target_branch = crate::commit::txn_target_branch(catalog, table_ident).await?;
        let mut attempt = 1u32;
        loop {
            let table = catalog.load_table(table_ident).await.map_err(|e| {
                SqeError::Catalog(format!("Failed to load table {table_ident}: {e}"))
            })?;
            let Some(RewriteAttempt {
                add,
                remove,
                cleanup,
                value,
            }) = build_attempt(table.clone()).await?
            else {
                return Ok(None);
            };

            let tx = Transaction::new(&table);
            let mut action = tx.rewrite_files();
            if let Some(branch) = &target_branch {
                action = action.set_target_branch(branch.clone());
            }
            if !add.is_empty() {
                action = action.add_data_files(add);
            }
            if !remove.is_empty() {
                action = action.delete_files(remove);
            }
            let commit_result = match action.apply(tx) {
                Ok(tx) => {
                    self.before_commit_seam().await;
                    tx.commit(catalog).await
                }
                Err(e) => Err(e),
            };

            match commit_result {
                Ok(_) => {
                    if let Some(guard) = &cleanup {
                        guard.mark_committed();
                    }
                    if attempt > 1 {
                        info!(
                            table = %table_ident,
                            op,
                            attempts = attempt,
                            "rewrite commit succeeded after retry"
                        );
                    }
                    return Ok(Some(value));
                }
                Err(e)
                    if (e.retryable() || is_conflict_message(&e.to_string()))
                        && attempt < COW_MAX_ATTEMPTS =>
                {
                    // Drop the guard so this attempt's orphan parquet files are
                    // deleted before the rewrite re-runs against the reloaded
                    // snapshot and writes a fresh set.
                    drop(cleanup);
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .write_commit_retries_total
                            .with_label_values(&[op])
                            .inc();
                    }
                    let sleep_ms = crate::commit::conflict_backoff_ms(attempt);
                    warn!(
                        table = %table_ident,
                        op,
                        attempt,
                        backoff_ms = sleep_ms,
                        error = %e,
                        "commit conflict; reloading table and retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "Failed to commit {stmt_label}: {e}"
                    )));
                }
            }
        }
    }

    /// Truncate fast path shared by the CoW, MoR-position-delete and
    /// MoR-equality-delete DELETE handlers: one retryable rewrite that removes
    /// every live data file plus the position deletes those files supersede.
    ///
    /// Returns the number of rows the removed manifests accounted for (514),
    /// or 0 when the table had no data files. Before 506 the CoW handler
    /// retried this commit and the two MoR handlers did not, so a
    /// `DELETE FROM t` racing another writer succeeded or failed depending on
    /// `write.delete.mode`.
    async fn truncate_via_rewrite(
        &self,
        session: &Session,
        catalog: &Arc<SessionCatalog>,
        table_ident: &TableIdent,
        log_label: &str,
    ) -> sqe_core::Result<usize> {
        let truncated = self
            .commit_rewrite_with_retry(
                catalog.as_catalog().as_ref(),
                table_ident,
                "truncate",
                "truncate",
                move |fresh_table| async move {
                    let old_data_files = self.collect_data_files(&fresh_table).await?;
                    if old_data_files.is_empty() {
                        info!(
                            table = %table_ident,
                            "{log_label}: table has no data files, nothing to delete"
                        );
                        return Ok(None);
                    }
                    // The manifest record counts are in hand, so report them
                    // (514) rather than an empty result the driver renders as
                    // "unknown".
                    let truncated_rows = data_file_record_count(&old_data_files);
                    info!(
                        table = %table_ident,
                        file_count = old_data_files.len(),
                        spec_ids = ?distinct_spec_ids(&old_data_files),
                        "{log_label}: truncating table (no WHERE clause)"
                    );
                    let files_to_remove = self
                        .extend_with_superseded_deletes(&fresh_table, old_data_files)
                        .await?;
                    Ok(Some(RewriteAttempt {
                        add: vec![],
                        remove: files_to_remove,
                        cleanup: None,
                        value: truncated_rows,
                    }))
                },
            )
            .await?;

        match truncated {
            Some(truncated_rows) => {
                info!(table = %table_ident, truncated_rows, "{log_label}: table truncated successfully");
                self.after_commit(session, catalog.as_ref(), table_ident)
                    .await;
                Ok(truncated_rows)
            }
            None => Ok(0),
        }
    }

    /// Post-commit cache invalidation, at the commit site rather than at the
    /// caller (architecture review 507).
    ///
    /// Two caches hold pre-commit state after a successful Iceberg commit:
    ///
    /// 1. the writer's own cached DataFusion `SessionContext`, whose registered
    ///    table providers and TVF MemTables were built from the old snapshot;
    /// 2. the shared table-metadata cache, for *every* token — table metadata
    ///    is a property of the table, not of the principal that wrote it.
    ///
    /// Before this hook the invariant lived at ~20 call sites in
    /// `query_handler`, and the one caller outside it (Flight `DoPut` ingest)
    /// had no call at all: an ADBC client that bulk-loaded and then counted
    /// rows on the same connection read the pre-ingest snapshot until the TTL
    /// expired. Every `WriteHandler` path that reaches a commit calls this.
    async fn after_commit(
        &self,
        session: &Session,
        catalog: &dyn TableCacheInvalidator,
        table_ident: &TableIdent,
    ) {
        if let Some(discovery) = &self.catalog_discovery {
            discovery
                .invalidate_session_cache(&session.user.username)
                .await;
        }
        catalog.invalidate_table_all_tokens(table_ident).await;
    }
}

/// What the write handler needs from the coordinator's session layer, as a
/// trait so the write path does not depend on `session_context` (the moka
/// session cache and per-catalog bearer resolution are coordinator state).
/// The coordinator implements it as `session_context::PolarisCatalogDiscovery`.
#[async_trait::async_trait]
pub trait CatalogDiscovery: Send + Sync {
    /// Resolve a non-default `warehouse` for `session` via Polaris discovery,
    /// or `None` when it is not resolvable (nonexistent or not authorized).
    async fn discover_session_catalog(
        &self,
        warehouse: &str,
        config: &SqeConfig,
        session: &Session,
        table_cache: Option<&TableMetadataCache>,
    ) -> Option<Arc<SessionCatalog>>;

    /// Drop every cached session context of `username` after a commit so the
    /// next query sees the new schema state.
    async fn invalidate_session_cache(&self, username: &str);
}

/// The one thing `after_commit` needs from a catalog, extracted as a trait so
/// the hook is unit-testable (architecture review 507).
///
/// `SessionCatalog` is only constructible against a live REST catalog (it warms
/// itself with a `list_namespaces` round-trip), so a test cannot hand
/// `after_commit` a real one and assert the invalidation happened. Everything
/// on the write path that commits holds either an `Arc<SessionCatalog>` or the
/// `Arc<dyn Catalog>` bridge built from one, and both deref to something that
/// implements this trait.
#[async_trait::async_trait]
pub(crate) trait TableCacheInvalidator: Send + Sync {
    /// Evict every cached copy of the table, regardless of the bearer token
    /// that populated it: a commit is visible to every principal, so evicting
    /// only the writer's key leaves others planning against pre-commit
    /// metadata until the soft TTL (architecture review 302).
    async fn invalidate_table_all_tokens(&self, table_ident: &TableIdent);
}

#[async_trait::async_trait]
impl TableCacheInvalidator for SessionCatalog {
    async fn invalidate_table_all_tokens(&self, table_ident: &TableIdent) {
        SessionCatalog::invalidate_table_all_tokens(self, table_ident).await;
    }
}

/// Can `CREATE OR REPLACE TABLE ... AS SELECT` swap this table's contents in
/// place, or does the replacement have to be staged and renamed (500)?
///
/// In place requires that what the streaming writer would produce fits the live
/// table exactly, because the writer stamps every batch onto the live table's
/// schema and partitions it with the live default spec:
///
/// - same column names, Arrow types and nullability, in the same order. A
///   positional type difference would be silently cast to the OLD type, which
///   is not what `CREATE OR REPLACE` asked for, and a different column set
///   cannot be written into the live schema at all.
/// - an explicit `PARTITIONED BY` must name the same source columns with the
///   same transforms as the live default spec (source columns are compared by
///   name: field ids differ between a freshly assigned schema and an evolved
///   one). No `PARTITIONED BY` keeps the live table's partitioning.
fn ctas_replace_is_in_place(
    live: &IcebergTable,
    new_arrow_schema: &ArrowSchema,
    new_partition_spec: Option<&iceberg::spec::UnboundPartitionSpec>,
    new_iceberg_schema: &IcebergSchema,
) -> bool {
    let live_schema = live.metadata().current_schema();
    let Ok(live_arrow) = iceberg::arrow::schema_to_arrow_schema(live_schema) else {
        return false;
    };
    if live_arrow.fields().len() != new_arrow_schema.fields().len() {
        return false;
    }
    for (a, b) in live_arrow
        .fields()
        .iter()
        .zip(new_arrow_schema.fields().iter())
    {
        if a.name() != b.name()
            || a.data_type() != b.data_type()
            || a.is_nullable() != b.is_nullable()
        {
            return false;
        }
    }

    let Some(unbound) = new_partition_spec else {
        return true;
    };
    let live_spec = live.metadata().default_partition_spec();
    if unbound.fields().len() != live_spec.fields().len() {
        return false;
    }
    unbound
        .fields()
        .iter()
        .zip(live_spec.fields().iter())
        .all(|(new_field, live_field)| {
            if new_field.transform != live_field.transform || new_field.name != live_field.name {
                return false;
            }
            let new_source = new_iceberg_schema
                .field_by_id(new_field.source_id)
                .map(|f| f.name.as_str());
            let live_source = live_schema
                .field_by_id(live_field.source_id)
                .map(|f| f.name.as_str());
            new_source.is_some() && new_source == live_source
        })
}

/// Distinct partition-spec ids across a file set, for the CoW/overwrite file
/// summary logs. A set with more than one entry means the table has been
/// through `ALTER TABLE ... ADD/DROP PARTITION FIELD` and the rewrite is
/// spanning spec generations (architecture review 502).
fn distinct_spec_ids(files: &[DataFile]) -> Vec<i32> {
    let mut ids: Vec<i32> = files.iter().map(|f| f.partition_spec_id()).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
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
            .any(|t| !t.deletes().is_empty())
    }
}

/// TEST-ONLY seam: a future the handler awaits right before each Iceberg
/// commit it makes (architecture review 513). Lets an in-process test land a
/// concurrent commit deterministically inside the window between "read the
/// snapshot and compute the rewrite" and "commit it", which is the only place
/// an optimistic-concurrency conflict can arise and which wall-clock timing
/// cannot hit reliably. Compiled only for this crate's tests and under the
/// `test-sqlite` feature; production builds have no field, no builder and an
/// empty `before_commit_seam`.
#[cfg(any(test, feature = "test-sqlite"))]
pub type CommitRaceHook = Arc<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

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
    /// The coordinator's session-cache and catalog-discovery hooks; see
    /// [`CatalogDiscovery`]. `None` in unit tests and in any embedding that
    /// has no per-user session cache to invalidate.
    catalog_discovery: Option<Arc<dyn CatalogDiscovery>>,
    /// See [`CommitRaceHook`].
    #[cfg(any(test, feature = "test-sqlite"))]
    commit_race_hook: Option<CommitRaceHook>,
}

impl WriteHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self {
            config,
            metrics: None,
            table_cache: None,
            policy_enforcer: None,
            catalog_discovery: None,
            #[cfg(any(test, feature = "test-sqlite"))]
            commit_race_hook: None,
        }
    }

    /// Install the coordinator's [`CatalogDiscovery`] hooks. Without them a
    /// 3-part target naming a non-default catalog is an "unknown catalog"
    /// error and commits do not invalidate any session cache.
    #[must_use = "with_catalog_discovery consumes self; bind the returned handler"]
    pub fn with_catalog_discovery(mut self, discovery: Arc<dyn CatalogDiscovery>) -> Self {
        self.catalog_discovery = Some(discovery);
        self
    }

    /// TEST-ONLY: install a [`CommitRaceHook`]. See its doc comment.
    #[cfg(any(test, feature = "test-sqlite"))]
    #[must_use = "with_test_commit_race_hook consumes self; bind the returned handler"]
    pub fn with_test_commit_race_hook(mut self, hook: CommitRaceHook) -> Self {
        self.commit_race_hook = Some(hook);
        self
    }

    /// Await the test-only [`CommitRaceHook`], if one is installed. Called
    /// immediately before `tx.commit(..)` on the retrying commit paths. A
    /// no-op in production builds.
    async fn before_commit_seam(&self) {
        #[cfg(any(test, feature = "test-sqlite"))]
        if let Some(hook) = &self.commit_race_hook {
            hook().await;
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
        scope: &sqe_policy::TableScope,
        plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, sqe_policy::PolicySummary)> {
        match &self.policy_enforcer {
            Some(enf) => enf.evaluate(&session.user, scope, plan).await,
            None => Ok((plan, sqe_policy::PolicySummary::default())),
        }
    }

    /// Govern every table a per-batch DML evaluation reads besides its own
    /// scratch tables: subqueries in WHERE / SET and the joins the
    /// scalar-subquery decorrelator emits. Each such scan is replaced by the
    /// enforcer's rewrite of that scan alone, so the scratch tables (the
    /// target's rows, governed by `WritePolicyPredicates`, and the
    /// already-enforced IN-subquery keysets) are never re-filtered, which
    /// would misalign the positional match mask.
    async fn enforce_dml_eval(
        &self,
        ctx: &DFSessionContext,
        session: &Session,
        df: datafusion::dataframe::DataFrame,
    ) -> sqe_core::Result<datafusion::dataframe::DataFrame> {
        use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
        let Some(enf) = self.policy_enforcer.as_deref() else {
            return Ok(df);
        };
        let mut scans: Vec<LogicalPlan> = Vec::new();
        df.logical_plan()
            .apply_with_subqueries(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    let scratch = scan.table_name.catalog() == Some("datafusion")
                        && scan.table_name.table().starts_with("__sqe_");
                    if !scratch && !scans.contains(node) {
                        scans.push(node.clone());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("DML plan traversal failed: {e}")))?;
        if scans.is_empty() {
            return Ok(df);
        }
        let scope = sqe_policy::TableScope::of(ctx);
        let mut enforced: Vec<(LogicalPlan, LogicalPlan)> = Vec::with_capacity(scans.len());
        for scan in scans {
            let (plan, _summary) = enf.evaluate(&session.user, &scope, scan.clone()).await?;
            enforced.push((scan, plan));
        }
        let plan = df
            .logical_plan()
            .clone()
            .transform_down_with_subqueries(|node| {
                Ok(match enforced.iter().find(|(scan, _)| *scan == node) {
                    // Jump: the replacement contains the original scan.
                    Some((_, plan)) => Transformed::new(plan.clone(), true, TreeNodeRecursion::Jump),
                    None => Transformed::no(node),
                })
            })
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("DML policy rewrite failed: {e}")))?
            .data;
        ctx.execute_logical_plan(plan).await.map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("DML policy plan failed: {e}"))
        })
    }

    /// Plan one engine-built MERGE query over the `__sqe_merge_*` scratch
    /// relations and govern every other table it reads (#804). The ON, WHEN
    /// and SET text is the caller's, so its subqueries name catalog tables
    /// directly; the source was enforced before it became a scratch table and
    /// the target is read through its policy relation.
    async fn plan_merge_sql(
        &self,
        ctx: &DFSessionContext,
        session: &Session,
        sql: &str,
        label: &str,
    ) -> sqe_core::Result<datafusion::dataframe::DataFrame> {
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("{label} failed to plan: {e}")))?;
        self.enforce_dml_eval(ctx, session, df).await
    }

    /// Run the MERGE cardinality check against the registered target/source
    /// scratch tables. Errors when a single target row is matched by more than
    /// one source row (Spark / Trino / Athena all reject this rather than
    /// silently duplicating the target row). Shared by the copy-on-write and
    /// merge-on-read merge paths; the caller gates on `merge_cardinality_check`
    /// and on the statement actually having a matched clause (source-only /
    /// by-source rows cannot violate cardinality).
    async fn check_merge_cardinality(
        &self,
        ctx: &DFSessionContext,
        session: &Session,
        pair_count_sql: &str,
        matched_count_sql: &str,
    ) -> sqe_core::Result<()> {
        const LABEL: &str = "MERGE cardinality check";
        let pair_count = scalar_count(self.plan_merge_sql(ctx, session, pair_count_sql, LABEL).await?, LABEL).await?;
        let matched_count =
            scalar_count(self.plan_merge_sql(ctx, session, matched_count_sql, LABEL).await?, LABEL).await?;
        if pair_count > matched_count {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                "One MERGE target table row matched more than one source row \
                 (cardinality violation). Deduplicate the source rows or narrow the \
                 ON condition; set query.merge_cardinality_check = false to disable \
                 this check."
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Resolve write-path policy predicates for the target of a DELETE / UPDATE.
    /// Returns row-filter SQL and per-column mask SQL fragments derived from
    /// the configured PolicyEnforcer. With no enforcer the result is empty
    /// and the DML path is unchanged.
    async fn compute_write_predicates(
        &self,
        session: &Session,
        qualifier: Option<String>,
        table_ident: &TableIdent,
        schema: Arc<ArrowSchema>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<sqe_policy::write_predicates::WritePolicyPredicates> {
        let Some(enf) = &self.policy_enforcer else {
            return Ok(sqe_policy::write_predicates::WritePolicyPredicates::default());
        };
        // One key rule for reads and writes (architecture review 201): the
        // rewriter keys policy by the FULL dotted namespace it sees in the
        // planner's TableReference, so hand the probe the same string. The
        // old last-component flattening made a Ranger filter on `a.b` apply
        // to SELECT but not to DELETE/UPDATE.
        let namespace_key = if table_ident.namespace().as_ref().is_empty() {
            "default".to_string()
        } else {
            table_ident.namespace().to_string()
        };
        // The catalog the target lives in: the statement's qualifier, else the
        // session's, else the configured default. The read path governs the
        // same table as `catalog.ns.t` (`TableScope::qualify`), and a
        // catalog-scoped policy store keys on that catalog.
        let catalog_name = self.policy_catalog_name(qualifier, session);
        let (predicates, summary) = sqe_policy::write_predicates::extract_write_predicates(
            enf.as_ref(),
            &session.user,
            Some(&catalog_name),
            &namespace_key,
            table_ident.name(),
            schema,
        )
        .await?;
        *policy_summary_out = Some(summary);
        Ok(predicates)
    }

    /// Name of the catalog a write target lives in, for policy keys: the
    /// statement's explicit qualifier, else the session's default catalog,
    /// else the configured default. Same rule as `resolve_target_catalog`
    /// plus the configured fallback, which a policy key cannot do without.
    fn policy_catalog_name(&self, qualifier: Option<String>, session: &Session) -> String {
        resolve_target_catalog(qualifier, session)
            .unwrap_or_else(|| self.config.resolve_default_catalog())
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
    /// Bounded fanout is the default (spec open decision #4, closed by
    /// security/scale review 905): a knob left at 0 is auto-derived from the
    /// coordinator memory pool ([`auto_fanout_caps`]) so an unsorted write over
    /// many partitions never holds an unbounded set of open Parquet writers.
    /// `pool`, when supplied, charges the buffered fanout estimate to the
    /// shared pool so it competes with operator memory instead of growing
    /// Plan and start a write source the way `DataFrame::execute_stream`
    /// does, with one step in between: the same `SortMemoryRule` the read
    /// path runs in post-planning (#412).
    ///
    /// DataFusion's external sort merges through `ExternalSorterMerge`,
    /// which cannot spill. A sort-on-write source (CTAS or INSERT with ORDER
    /// BY, a MERGE rewrite that sorts) whose merge fan-in does not fit the
    /// sort grant used to run anyway and take the coordinator down with an
    /// OOM mid-write, leaving half-uploaded files for the cleanup guard.
    /// Sorted SELECTs were already refused at plan time by the rule in
    /// `post_planning`; the write path now gets the same answer, typed
    /// `RESOURCE_EXHAUSTED` with the knobs named, before a byte is written.
    /// A plan without an unbounded `SortExec` passes untouched.
    async fn execute_source_stream(
        &self,
        df: datafusion::dataframe::DataFrame,
        what: &str,
    ) -> sqe_core::Result<datafusion::execution::SendableRecordBatchStream> {
        use datafusion::physical_optimizer::PhysicalOptimizerRule;

        let (state, plan) = df.into_parts();
        let physical = state.create_physical_plan(&plan).await.map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("{what}: physical planning failed: {e}"),
            )
        })?;
        // The rule budgets one merge buffer per input partition, read from
        // the session's `target_partitions` in `optimize`.
        let gate = sqe_planner::SortMemoryRule::new(sqe_planner::sort_grant_bytes(
            &self.config.query.max_query_memory,
        ));
        // The refusal is `DataFusionError::ResourcesExhausted`; the `From`
        // conversion keeps it `ResourceExhausted` on the wire. Name the
        // statement shape and the write-side remedy: the planner's text only
        // knows about sorts, the user typed a CTAS/INSERT with ORDER BY.
        let physical = gate
            .optimize(physical, state.config_options())
            .map_err(|e| match e {
                datafusion::error::DataFusionError::ResourcesExhausted(msg) => SqeError::with_code(
                    SqeErrorCode::ResourceExhausted,
                    format!(
                        "{what} with sort-on-write (ORDER BY) cannot be admitted: {msg} \
                             Write without the sort hint or raise query.max_query_memory."
                    ),
                ),
                other => SqeError::from(other),
            })?;
        datafusion::physical_plan::execute_stream(physical, state.task_ctx()).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("{what}: failed to start execution stream: {e}"),
            )
        })
    }

    /// beside it. `write_buffer_tracking = false` is the escape hatch back to
    /// the vendored unbounded `TaskWriter` path. A malformed
    /// `fanout_buffer_budget` string warns and falls back to the auto budget.
    fn fanout_limits(
        &self,
        pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    ) -> FanoutLimits {
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
                        "invalid fanout_buffer_budget; using the auto-derived budget"
                    );
                    0
                }
            };

        // Auto-derive whichever knob is 0 from the coordinator pool size so
        // the operator never has to set either for a bounded write.
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
            pool,
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
        session: &Session,
        ctx: &DFSessionContext,
        table_ident: &TableIdent,
        table_factor_name: &sqlparser::ast::ObjectName,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
    ) {
        let select_sql = format!("SELECT * FROM {table_factor_name}");
        let df = match sqe_catalog::file_tvf_prepare::plan_sql(ctx, &select_sql).await {
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
            lineage_target_parts(&self.config, session, table_factor_name, table_ident);
        match build_lineage_insert_plan(source_plan, None, &lin_catalog, &lin_namespace, &lin_table)
        {
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

    /// Register Puffin NDV statistics for the snapshot a commit just
    /// produced, if the table opted in via `write.puffin.stats`.
    ///
    /// Append-shaped commits pass the sketches accumulated on the write
    /// stream (`CarryOp::Insert` / `InsertOverwrite`); row deltas pass `None`
    /// with `CarryOp::RowDelta` so the previous snapshot's statistics are
    /// carried as an upper bound instead of vanishing (architecture review
    /// 512). The statistics commit goes through `commit_with_retry`; every
    /// failure is logged, counted in `sqe_puffin_sidecar_total`, and leaves
    /// no staged sidecar behind. The data commit already succeeded, so
    /// nothing here can fail the statement.
    ///
    /// The table is reloaded here because the rewrite helpers return the
    /// attempt's value, not the committed table; a foreign commit landing in
    /// that window makes the carry a no-op for our snapshot, never a wrong
    /// annotation (the foreign snapshot's parent is ours and has none).
    async fn carry_puffin_statistics(
        &self,
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        op: crate::puffin_carry::CarryOp,
        sketches: Option<&ColumnSketches>,
    ) {
        let committed = match catalog.load_table(table_ident).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, table = %table_ident, "puffin: could not reload table for statistics");
                return;
            }
        };
        crate::puffin_carry::carry_forward_statistics(
            catalog,
            table_ident,
            &committed,
            op,
            sketches,
            self.metrics.as_deref(),
        )
        .await;
    }

    /// Handle CREATE TABLE [OR REPLACE] ns.table AS SELECT ... — streaming variant.
    ///
    /// Instead of buffering the full SELECT result in memory, this method:
    /// 1. Plans the SELECT via DataFusion and derives the Iceberg schema from the
    ///    DataFrame schema (before execution — no data buffered yet).
    /// 2. Creates the table in the catalog.
    /// 3. Streams batches directly to the Parquet writer via
    ///    `execute_source_stream` (`df.execute_stream()` plus the sort
    ///    admission gate, #412).
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
        let (table_name, or_replace, partition_by, user_options) = match stmt {
            Statement::CreateTable(ct) => {
                if ct.query.is_none() {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                        "CTAS statement has no SELECT query",
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
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;
        let namespace = table_ident.namespace().clone();
        let name = table_ident.name().to_string();

        // Plan the SELECT without executing it; gives us the output schema
        // cheaply. Planning and the policy rewrite are recursive and run on
        // the dedicated planning stack (#640); this is the CTAS + rewriter
        // path that overflowed a request thread. `carried`: the planning
        // thread does not see the task's transaction scope.
        let (df, policy_summary) = sqe_core::runtime::on_planning_stack(sqe_catalog::txn::carried(async {
            let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, select_sql)
                .await
                .map_err(SqeError::planning)?;

            // Enforce policy on the source SELECT so masks and restrictions
            // shape the target table. Without this, CTAS over a masked table
            // creates a sink with plaintext columns.
            let (enforced_source, policy_summary) = self
                .enforce_source_plan(
                    session,
                    &sqe_policy::TableScope::of(ctx),
                    df.logical_plan().clone(),
                )
                .await?;
            let df = ctx
                .execute_logical_plan(enforced_source)
                .await
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Plan execution failed: {e}")))?;
            Ok::<_, SqeError>((df, policy_summary))
        }))
        .await?;
        *policy_summary_out = Some(policy_summary);

        // Iceberg v2 has no timestamp_ns (v3 only); DataFusion's
        // `current_timestamp`/`now()` yield ns. Derive the target schema at
        // the precision the format version can hold; the streaming sink casts
        // every batch onto the table schema (`apply_stamped_schema`), so the
        // ns values are narrowed to µs on write (#773).
        let create_format_version = self.format_version();
        let arrow_schema = Arc::new(timestamps_for_format_version(
            df.schema().as_arrow(),
            create_format_version,
        ));
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            "Executing CTAS (streaming)"
        );

        let target_catalog = resolve_target_catalog(catalog_qualifier(table_name), session);
        // Keep the `SessionCatalog`: `after_commit` (507) and the OR REPLACE
        // path (500) both need more than the erased `Catalog` trait object.
        let session_catalog = self
            .resolve_session_catalog(session, target_catalog.as_deref())
            .await?;
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();

        // Defense-in-depth: ensure the target namespace exists in this catalog
        // (dbt's CREATE SCHEMA may not have landed here); idempotent.
        ensure_namespace(catalog.as_ref(), &namespace).await?;

        // Honor PARTITIONED BY on CTAS (the plain CREATE TABLE path already
        // does this). Without it, `CREATE TABLE ... PARTITIONED BY (month(d))
        // AS SELECT ...` silently produced an unpartitioned table.
        let partition_spec = build_partition_spec(partition_by, &iceberg_schema)?;

        // Capture the SELECT plan as a synthetic INSERT-into-target wrapper for
        // OpenLineage extraction. The wrapper is never executed; it lets the
        // extractor walk the source plan for inputs + column lineage and the
        // wrapper for the output dataset. Captured before either CREATE OR
        // REPLACE path executes the stream.
        let source_plan = df.logical_plan().clone();
        let (lin_catalog, lin_namespace, lin_table) =
            lineage_target_parts(&self.config, session, table_name, &table_ident);
        if let Ok(wrapper) =
            build_lineage_insert_plan(source_plan, None, &lin_catalog, &lin_namespace, &lin_table)
        {
            *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
        }

        // CREATE OR REPLACE TABLE ... AS SELECT on an EXISTING table (500).
        // This used to be an unconditional `drop_table` in `query_handler`,
        // resolved against the default warehouse, issued BEFORE the SELECT ran:
        // `CREATE OR REPLACE TABLE t AS SELECT ... FROM t` dropped `t` and then
        // failed to plan, any later failure left the user with no table at all,
        // and a 3-part name dropped the same-named table in the wrong catalog.
        // Two replacement shapes now, both of which keep the original table
        // until the new data is on storage:
        //
        //  - the SELECT's schema and partitioning match the live table: swap
        //    the contents in with one `rewrite_files` commit. The table keeps
        //    its UUID, its snapshot history and its grants.
        //  - they differ (the streaming writer stamps every batch onto the
        //    live table's schema, so a different column set, order or type
        //    cannot be written into it): stage the replacement under a
        //    temporary name in the same namespace and swap with DROP + RENAME.
        let existing = if or_replace {
            if session_catalog.table_exists(&table_ident).await? {
                Some(catalog.load_table(&table_ident).await.map_err(|e| {
                    SqeError::Catalog(format!(
                        "CREATE OR REPLACE: failed to load the existing table '{table_ident}': {e}"
                    ))
                })?)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(live) = &existing {
            self.refuse_replace_under_policy(
                session,
                target_catalog.clone(),
                &table_ident,
                live,
                "CREATE OR REPLACE",
            )
            .await?;
        }

        let staged_ident = match &existing {
            Some(live)
                if ctas_replace_is_in_place(
                    live,
                    &arrow_schema,
                    partition_spec.as_ref(),
                    &iceberg_schema,
                ) =>
            {
                let total_rows = self
                    .ctas_replace_in_place(
                        session,
                        ctx,
                        df,
                        &session_catalog,
                        live.clone(),
                        &table_ident,
                    )
                    .await?;
                return affected_rows_batch(total_rows as u64);
            }
            Some(_) => Some(TableIdent::new(
                namespace.clone(),
                format!("{name}__sqe_replace_{}", Uuid::new_v4().simple()),
            )),
            None => None,
        };

        // The name the table is CREATEd under: the target itself, or the
        // staging name when an incompatible replacement has to be swapped in.
        let create_ident = staged_ident.clone().unwrap_or_else(|| table_ident.clone());
        let create_name = create_ident.name().to_string();
        if let Some(staged) = &staged_ident {
            warn!(
                table = %table_ident,
                staged = %staged,
                "CREATE OR REPLACE: the SELECT's schema or partitioning differs from \
                 the live table, so the replacement is staged under a temporary name \
                 and swapped in with DROP + RENAME. The existing table survives a \
                 failing SELECT; only the swap itself is two catalog calls, and the \
                 replaced table's snapshot history does not carry over."
            );
        }

        let location = unique_table_location(catalog.as_ref(), &namespace, &create_name).await;
        // Merge user TBLPROPERTIES / WITH options (see handle_ctas for why):
        // a CTAS must keep properties like write.parquet.bloom-filter-* that
        // drive the data write, matching the plain CREATE TABLE path.
        let mut props = self.format_version_props(create_format_version);
        merge_user_table_properties(&mut props, user_options);
        let table_creation = TableCreation::builder()
            .name(create_name.clone())
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
            .load_table(&create_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load created table: {e}")))?;

        // Stream + commit wrapped so any failure after `catalog.create_table`
        // rolls back the partially-created table. Same rationale as the
        // non-streaming `handle_ctas` path: a failed write or commit leaves
        // the catalog entry plus S3 prefix in place, which Polaris's
        // location-uniqueness check rejects on every retry.
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "ctas-streaming")
                .with_metrics(self.metrics.clone());
        let post_create: sqe_core::Result<(usize, Option<i64>)> = async {
            let stream = self.execute_source_stream(df, "CTAS").await?;
            // Puffin NDV sketches ride along on the stream when the table
            // opted in; the buffered path clones batches instead, which the
            // streaming path exists to avoid.
            let (stream, sketch_acc) = tap_stream_for_puffin_sketches(&table, stream);

            let (data_files, total_rows) = write_data_files_streaming_with_metrics(
                &table,
                stream,
                "ctas",
                self.metrics.as_ref(),
                self.compression(session),
                tracker,
                self.fanout_limits(Some(ctx.runtime_env().memory_pool.clone())),
            )
            .await?;

            let mut snapshot_id = None;
            if !data_files.is_empty() {
                let files_for_retry = data_files.clone();
                let catalog_for_commit = catalog.clone();
                let committed = commit_with_retry(
                    catalog.as_ref(),
                    &create_ident,
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
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit CTAS transaction: {e}"))
                })?;
                snapshot_id = committed.metadata().current_snapshot_id();
                cleanup_guard.mark_committed();
                info!(
                    table = %create_ident,
                    total_rows,
                    "CTAS committed successfully (streaming)"
                );
                if let Some(sketches) = finish_puffin_sketches(sketch_acc) {
                    self.carry_puffin_statistics(
                        catalog.as_ref(),
                        &create_ident,
                        crate::puffin_carry::CarryOp::Insert,
                        Some(&sketches),
                    )
                    .await;
                }
            } else {
                cleanup_guard.mark_committed();
                info!(
                    table = %create_ident,
                    "CTAS created empty table (no data to write)"
                );
            }
            Ok((total_rows, snapshot_id))
        }
        .await;

        if let Err(ref err) = post_create {
            warn!(
                table = %create_ident,
                error = %err,
                "CTAS (streaming) failed after create_table; rolling back to prevent orphan"
            );
            rollback_ctas_partial_create(&catalog, &table, &create_ident).await;
        }
        let (total_rows, snapshot_id) = post_create?;
        // A staged replacement is renamed into place below and keeps its
        // snapshot, so the id names the target's data either way.
        record_lineage_snapshot(plan_out, snapshot_id);

        // Swap a staged replacement into place (500). Both calls are metadata
        // only. The failure message names the staged table so the swap can be
        // finished by hand rather than leaving the data unreachable.
        if let Some(staged) = &staged_ident {
            catalog.drop_table(&table_ident).await.map_err(|e| {
                SqeError::Catalog(format!(
                    "CREATE OR REPLACE: failed to drop the existing table \
                     '{table_ident}': {e}. The replacement is committed as '{staged}'; \
                     no data was lost."
                ))
            })?;
            catalog
                .rename_table(staged, &table_ident)
                .await
                .map_err(|e| {
                    SqeError::Catalog(format!(
                        "CREATE OR REPLACE: '{table_ident}' was dropped and the replacement \
                     is committed as '{staged}', but renaming it into place failed: {e}. \
                     Run ALTER TABLE {staged} RENAME TO {table_ident} to finish the swap."
                    ))
                })?;
            self.after_commit(session, session_catalog.as_ref(), staged)
                .await;
        }

        // The table exists and holds its data now: drop the caller's cached
        // session context and every token's metadata entry for it (507).
        self.after_commit(session, session_catalog.as_ref(), &table_ident)
            .await;

        // Report the written-row count as a 1-row `rows_affected` batch,
        // consistent with the buffered INSERT path and the DELETE/UPDATE/MERGE
        // handlers. The coordinator recovers it (O2) into query history + audit.
        affected_rows_batch(total_rows as u64)
    }

    /// `CREATE OR REPLACE TABLE ... AS SELECT` against an existing Iceberg
    /// table the SELECT already fits (architecture review 500).
    ///
    /// Writes the new data files into the live table first, then swaps them in
    /// with ONE `rewrite_files` commit that removes every live data file plus
    /// the position deletes those files supersede. Consequences, all of which
    /// the previous drop-then-create got wrong:
    ///
    /// - the table keeps its UUID and its snapshot history, so time travel and
    ///   `rollback_to_snapshot` still cross the replace;
    /// - `CREATE OR REPLACE TABLE t AS SELECT ... FROM t` works: the stream is
    ///   fully consumed (against the pre-swap snapshot) before the commit;
    /// - a failing SELECT, a failing write or four lost commit races leave the
    ///   original table and its data exactly as they were — the cleanup guard
    ///   removes the parquet files this attempt wrote;
    /// - grants survive, because nothing is dropped.
    ///
    /// Table properties from the statement's `WITH` / `TBLPROPERTIES` clause
    /// are NOT re-applied: an in-place replace keeps the live table's
    /// properties. `ALTER TABLE ... SET TBLPROPERTIES` changes them.
    async fn ctas_replace_in_place(
        &self,
        session: &Session,
        ctx: &DFSessionContext,
        df: datafusion::dataframe::DataFrame,
        catalog: &Arc<SessionCatalog>,
        table: IcebergTable,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<usize> {
        info!(
            table = %table_ident,
            "CREATE OR REPLACE TABLE: replacing contents in place (schema and \
             partitioning unchanged)"
        );

        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "ctas-replace")
                .with_metrics(self.metrics.clone());

        let stream = self
            .execute_source_stream(df, "CREATE OR REPLACE TABLE")
            .await?;
        let (stream, sketch_acc) = tap_stream_for_puffin_sketches(&table, stream);
        let (data_files, total_rows) = write_data_files_streaming_with_metrics(
            &table,
            stream,
            "ctas",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
            self.fanout_limits(Some(ctx.runtime_env().memory_pool.clone())),
        )
        .await?;

        let borrowed = (&data_files, table_ident);
        let committed = self
            .commit_rewrite_with_retry(
                catalog.as_catalog().as_ref(),
                table_ident,
                "ctas-replace",
                "CREATE OR REPLACE TABLE",
                move |fresh_table| {
                    let (data_files, table_ident) = borrowed;
                    async move {
                        let live = self.collect_data_files(&fresh_table).await?;
                        if live.is_empty() && data_files.is_empty() {
                            info!(
                                table = %table_ident,
                                "CREATE OR REPLACE: empty table replaced by an empty \
                                 SELECT; nothing to commit"
                            );
                            return Ok(None);
                        }
                        let removed_files = live.len();
                        let files_to_remove = self
                            .extend_with_superseded_deletes(&fresh_table, live)
                            .await?;
                        info!(
                            table = %table_ident,
                            removed_files,
                            new_files = data_files.len(),
                            "CREATE OR REPLACE: committing atomic content swap"
                        );
                        Ok(Some(RewriteAttempt {
                            add: data_files.clone(),
                            remove: files_to_remove,
                            cleanup: None,
                            value: (),
                        }))
                    }
                },
            )
            .await;

        // On failure the guard's Drop deletes the parquet files this statement
        // wrote; the table keeps the contents it had.
        committed?;
        cleanup_guard.mark_committed();

        if let Some(sketches) = finish_puffin_sketches(sketch_acc) {
            // The replaced contents are gone, so the previous snapshot's
            // statistics do not apply: only the new sketches are registered
            // (previous stats are still unioned as an upper bound if present).
            self.carry_puffin_statistics(
                catalog.as_catalog().as_ref(),
                table_ident,
                crate::puffin_carry::CarryOp::InsertOverwrite,
                Some(&sketches),
            )
            .await;
        }

        self.after_commit(session, catalog.as_ref(), table_ident)
            .await;
        info!(
            table = %table_ident,
            total_rows,
            "CREATE OR REPLACE TABLE committed successfully (in-place swap)"
        );
        Ok(total_rows)
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
        let (table_name, explicit_columns, overwrite) = match stmt {
            Statement::Insert(ins) => {
                if ins.partitioned.is_some() {
                    return Err(SqeError::NotImplemented(
                        "INSERT OVERWRITE with a static PARTITION (col=val) clause is not supported; \
                         omit the PARTITION clause for dynamic partition overwrite"
                            .into(),
                    ));
                }
                match &ins.table {
                    sqlparser::ast::TableObject::TableName(name) => {
                        (name, &ins.columns, ins.overwrite)
                    }
                    other => {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                            "INSERT INTO table functions not supported: {other}"
                        )));
                    }
                }
            }
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected Insert statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;

        info!(
            username = %session.user.username,
            table = %table_ident,
            overwrite,
            "Executing INSERT INTO SELECT (streaming)"
        );

        let target_catalog = resolve_target_catalog(catalog_qualifier(table_name), session);
        // Keep the resolved `SessionCatalog`, not only the erased bridge:
        // `commit_written_files` needs it for the post-commit cache
        // invalidation (507).
        let session_catalog = self
            .resolve_session_catalog(session, target_catalog.as_deref())
            .await?;
        session_catalog.admit_txn_write(&table_ident).await?;
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;
        if overwrite {
            self.refuse_replace_under_policy(
                session,
                target_catalog.clone(),
                &table_ident,
                &table,
                "INSERT OVERWRITE",
            )
            .await?;
        }

        let effective_sql = if explicit_columns.is_empty() {
            select_sql.to_string()
        } else {
            reorder_insert_select(
                select_sql,
                explicit_columns,
                table.metadata().current_schema().as_ref(),
            )?
        };

        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &effective_sql)
            .await
            .map_err(SqeError::planning)?;

        // 505: fail a mis-shaped INSERT here, at planning time, before the
        // stream starts and before the writer opens its first parquet part.
        let source_columns: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        check_insert_arity(
            &source_columns,
            table.metadata().current_schema().as_ref(),
            &table_ident,
        )?;

        // Enforce row filters, column masks, and restricted columns on the
        // source SELECT. Without this, INSERT INTO sink SELECT FROM masked
        // would copy plaintext into sink, bypassing every policy. The rewrite
        // and the physical planning in `execute_stream` are recursive and run
        // on the dedicated planning stack (#640); the stream itself is polled
        // on the runtime threads, in the transaction scope `carried` brings.
        let source_plan = df.logical_plan().clone();
        let (stream, policy_summary, lineage_plan) =
            sqe_core::runtime::on_planning_stack(sqe_catalog::txn::carried(async {
                let (enforced_plan, policy_summary) =
                    self.enforce_source_plan(session, &sqe_policy::TableScope::of(ctx), source_plan)
                        .await?;

                // The stub carries the target table's columns, so lineage
                // names output column i after the table's column i (the
                // source is in table order, reordered above for an explicit
                // column list), not after the SELECT expression feeding it.
                let (lin_catalog, lin_namespace, lin_table) =
                    lineage_target_parts(&self.config, session, table_name, &table_ident);
                let lineage_plan = build_lineage_insert_plan(
                    enforced_plan.clone(),
                    build_arrow_schema_for_table(&table)
                        .ok()
                        .map(|s| s.as_ref().clone()),
                    &lin_catalog,
                    &lin_namespace,
                    &lin_table,
                )
                .ok();

                let df = ctx
                    .execute_logical_plan(enforced_plan)
                    .await
                    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Plan execution failed: {e}")))?;

                let stream = self.execute_source_stream(df, "INSERT source").await?;
                Ok::<_, SqeError>((stream, policy_summary, lineage_plan))
            }))
            .await?;
        *policy_summary_out = Some(policy_summary);
        if let Some(wrapper) = lineage_plan {
            *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
        }

        let stream = self
            .with_check_guard(
                session,
                ctx,
                catalog_qualifier(table_name),
                &table_ident,
                &table,
                stream,
            )
            .await?;
        let (stream, sketch_acc) = tap_stream_for_puffin_sketches(&table, stream);

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
            self.fanout_limits(Some(ctx.runtime_env().memory_pool.clone())),
        )
        .await?;

        if total_rows == 0 && !overwrite {
            info!(table = %table_ident, "INSERT SELECT returned no rows — nothing to write");
            cleanup_guard.mark_committed();
            return affected_rows_batch(0);
        }

        let snapshot_id = self
            .commit_written_files(
                session,
                &session_catalog,
                &table_ident,
                data_files,
                overwrite,
            )
            .await?;
        cleanup_guard.mark_committed();
        record_lineage_snapshot(plan_out, snapshot_id);

        if let Some(sketches) = finish_puffin_sketches(sketch_acc) {
            let op = if overwrite {
                crate::puffin_carry::CarryOp::InsertOverwrite
            } else {
                crate::puffin_carry::CarryOp::Insert
            };
            self.carry_puffin_statistics(catalog.as_ref(), &table_ident, op, Some(&sketches))
                .await;
            // The sidecar is a SECOND metadata commit on this table, after the
            // data commit's own `after_commit` already refreshed the caches.
            self.after_commit(session, session_catalog.as_ref(), &table_ident)
                .await;
        }

        info!(
            table = %table_ident,
            total_rows,
            overwrite,
            "INSERT committed successfully (streaming)"
        );

        // Report the written-row count as a 1-row `rows_affected` batch,
        // consistent with the buffered INSERT path and the DELETE/UPDATE/MERGE
        // handlers. The coordinator recovers it (O2) into query history + audit.
        affected_rows_batch(total_rows as u64)
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
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                "CREATE TABLE requires at least one column definition",
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
        let session_catalog = self
            .resolve_session_catalog(session, target_catalog.as_deref())
            .await?;
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();

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

        // A new table changes what the caller's cached session context can
        // resolve, so the same hook every write path uses runs here too (507).
        self.after_commit(session, session_catalog.as_ref(), &table_ident)
            .await;

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
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("CREATE TABLE LIKE: source schema conversion: {e}"))
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
        let session_catalog = self
            .resolve_session_catalog(session, target_catalog.as_deref())
            .await?;
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();

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
        self.after_commit(session, session_catalog.as_ref(), &table_ident)
            .await;
        Ok(vec![])
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
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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

        let session_catalog = self
            .resolve_session_catalog(session, target_catalog)
            .await?;
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        // The streaming sink re-stamps every batch onto the table schema, so the
        // adapter's declared schema is only the stream contract, not the write
        // schema. Derive it from the table's current Iceberg schema.
        //
        // There is nothing to arity-check up front here: the uploaded batches
        // have not arrived yet. A mis-shaped batch is caught per batch by
        // `apply_stamped_schema`, which since 505 returns
        // "INSERT has N columns but target has M" instead of panicking.
        let arrow_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                |e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to derive ingest Arrow schema: {e}")),
            )?,
        );

        let mapped =
            batch_stream.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)));
        let stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
            RecordBatchStreamAdapter::new(arrow_schema, Box::pin(mapped)),
        );
        // DoPut has no session SessionContext; a bare one evaluates the
        // target's row filter over each uploaded batch (#729). A filter that
        // needs another table fails closed here rather than being skipped.
        let stream = self
            .with_check_guard(
                session,
                &DFSessionContext::new(),
                target_catalog.map(str::to_string),
                &table_ident,
                &table,
                stream,
            )
            .await?;

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
            // Flight DoPut has no session context; the auto-derived caps still bound
            // the writer set, only the pool charge is unavailable here.
            self.fanout_limits(None),
        )
        .await?;

        if !data_files.is_empty() {
            let files_for_retry = data_files.clone();
            let catalog_for_commit = catalog.clone();
            let target_branch =
                crate::commit::txn_target_branch(catalog.as_ref(), &table_ident).await?;
            commit_with_retry(
                catalog.as_ref(),
                &table_ident,
                "ingest",
                move |fresh_table| {
                    let files = files_for_retry.clone();
                    let cat = catalog_for_commit.clone();
                    let target_branch = target_branch.clone();
                    async move {
                        let tx = Transaction::new(&fresh_table);
                        let mut action = tx.fast_append().add_data_files(files);
                        if let Some(branch) = target_branch {
                            action = action.set_target_branch(branch);
                        }
                        let tx = action.apply(tx)?;
                        tx.commit(cat.as_ref()).await
                    }
                },
            )
            .await
            .map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit ingest transaction: {e}"))
            })?;
            cleanup_guard.mark_committed();

            // The Flight `DoPut` caller has no query_handler statement to
            // mirror the invalidation for it: before 507 an ADBC client that
            // bulk-loaded and then ran `SELECT count(*)` on the same connection
            // read the pre-ingest snapshot until the cache TTL expired.
            self.after_commit(session, session_catalog.as_ref(), &table_ident)
                .await;

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
        catalog.admit_txn_write(&table_ident).await?;
        let delete = match stmt {
            Statement::Delete(d) => d,
            // unreachable: resolve_delete_target_ident above already filtered.
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected DELETE statement, got: {other}"
                )))
            }
        };

        let table = catalog.load_table(&table_ident).await?;

        let where_clause = &delete.selection;
        // Snapshot-independent setup (schema + policy predicates) runs once;
        // the data-file changes are retried against a reloaded snapshot on an
        // optimistic-concurrency conflict.
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(
                session,
                catalog_qualifier(delete_target_object_name(stmt)?),
                &table_ident,
                target_schema,
                policy_summary_out,
            )
            .await?;

        // No WHERE = truncate. With no policy row filter we keep the fast
        // path: drop every file. With a row filter we must preserve rows
        // outside the user's view, so the truncate degrades into a CoW
        // rewrite under the row filter (handled by the main path below).
        if where_clause.is_none() && predicates.row_filter_sql.is_none() {
            let truncated_rows = self
                .truncate_via_rewrite(session, &catalog, &table_ident, "DELETE")
                .await?;
            return affected_rows_batch(truncated_rows as u64);
        }

        // Lift any `IN (subquery)` expressions out of the WHERE into materialised
        // scratch MemTables joined via LEFT JOIN. The guard must outlive every
        // rewrite below; its Drop deregisters the scratch tables.
        let raw_where = where_clause
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(session, &raw_where, ctx).await?;

        // Partition-pruned CoW under optimistic-concurrency retry: each attempt
        // re-reads the snapshot, rewrites only data files whose partition could
        // match the WHERE (rest untouched, #263), and commits. A commit conflict
        // re-runs the whole rewrite against the reloaded snapshot (#47) — the
        // loop itself lives in `commit_rewrite_with_retry` (506).
        let borrowed = (&where_sql[..], &joins_sql[..], &predicates, &table_ident);
        let total_deleted = self
            .commit_rewrite_with_retry(
                catalog.as_catalog().as_ref(),
                &table_ident,
                "delete-cow",
                "DELETE",
                move |fresh_table| {
                    // Every capture is a `Copy` reference so the per-attempt
                    // `async move` block can take its own copy out of the
                    // `FnMut` closure.
                    let (where_sql, joins_sql, predicates, table_ident) = borrowed;
                    async move {
                        let old_data_files = self.collect_data_files(&fresh_table).await?;
                        if old_data_files.is_empty() {
                            info!(table = %table_ident, "DELETE: table has no data files, nothing to delete");
                            return Ok(None);
                        }
                        let (to_rewrite, pruned_files) = partition_prune_cow_files(
                            old_data_files,
                            &fresh_table,
                            where_clause.as_ref(),
                        );
                        if to_rewrite.is_empty() {
                            info!(table = %table_ident, "DELETE: no partitions match the WHERE clause; nothing to rewrite");
                            return Ok(None);
                        }
                        if pruned_files > 0 {
                            info!(table = %table_ident, pruned_files, rewrite_files = to_rewrite.len(), "DELETE: partition-pruned CoW rewrite");
                        }
                        info!(
                            table = %table_ident,
                            file_count = to_rewrite.len(),
                            spec_ids = ?distinct_spec_ids(&to_rewrite),
                            where_clause = %where_sql,
                            policy_row_filter = predicates.row_filter_sql.is_some(),
                            policy_masks = predicates.column_mask_sqls.len(),
                            "DELETE: CoW rewrite"
                        );

                        let mut new_data_files = Vec::new();
                        let mut deleted_this_attempt = 0usize;
                        let tracker = new_upload_tracker();
                        let cleanup_guard = WriteCleanupGuard::new(
                            fresh_table.file_io().clone(),
                            tracker.clone(),
                            "delete-cow",
                        )
                        .with_metrics(self.metrics.clone());
                        let pool = ctx.runtime_env().memory_pool.clone();
                        let read_plan = self.plan_delete_aware_read(&fresh_table).await?;

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
                                        session,
                                        batch,
                                        where_sql,
                                        joins_sql,
                                        table_ident,
                                        predicates,
                                    )
                                    .await?;
                                deleted_this_attempt += batch.num_rows() - filtered.num_rows();
                                if filtered.num_rows() > 0 {
                                    surviving.push(filtered)?;
                                }
                            }
                            if !surviving.is_empty() {
                                let new_files = write_data_files_with_metrics(
                                    &fresh_table,
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

                        let files_to_remove = self
                            .extend_with_superseded_deletes(&fresh_table, to_rewrite)
                            .await?;
                        Ok(Some(RewriteAttempt {
                            add: new_data_files,
                            remove: files_to_remove,
                            cleanup: Some(cleanup_guard),
                            value: deleted_this_attempt,
                        }))
                    }
                },
            )
            .await?;

        let Some(total_deleted) = total_deleted else {
            return affected_rows_batch(0);
        };
        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;
        info!(table = %table_ident, deleted_rows = total_deleted, "DELETE committed successfully");
        affected_rows_batch(total_deleted as u64)
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
        catalog.admit_txn_write(&table_ident).await?;
        let delete = match stmt {
            Statement::Delete(d) => d,
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected DELETE statement, got: {other}"
                )))
            }
        };

        let table = catalog.load_table(&table_ident).await?;

        let old_data_files = self.collect_data_files(&table).await?;

        if old_data_files.is_empty() {
            info!(table = %table_ident, "MoR DELETE: table has no data files, nothing to delete");
            return affected_rows_batch(0);
        }

        let where_clause = &delete.selection;
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(
                session,
                catalog_qualifier(delete_target_object_name(stmt)?),
                &table_ident,
                target_schema,
                policy_summary_out,
            )
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
            // Through the shared retryable truncate (506): this commit used to
            // be single-shot, so the same statement survived a concurrent
            // writer on a copy-on-write table and failed on a merge-on-read one.
            let truncated_rows = self
                .truncate_via_rewrite(session, &catalog, &table_ident, "MoR DELETE")
                .await?;
            return affected_rows_batch(truncated_rows as u64);
        }

        let raw_where = where_clause
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        // Lift any `IN (subquery)` expressions; see `handle_delete` for details.
        // The guard must outlive the per-batch loop below.
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(session, &raw_where, ctx).await?;
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
        //
        // It is NOT harmless for the reported count (514): a row a previous
        // DELETE already hid is marked again, so re-running the same statement
        // reported the same non-zero count instead of 0 and defeated the
        // simplest reconciliation check. The count therefore comes from a
        // second, delete-aware read of the same file — only rows still visible
        // can be newly deleted. That read only happens for files that both
        // carry delete files and had a raw match, so an ordinary MoR DELETE on
        // a table with no deletes still reads each file once.
        let read_plan = self.plan_delete_aware_read(&table).await?;
        let mut deleted_count: usize = 0;

        // Architecture review 504: positions are flushed into the delete
        // writer one data file at a time instead of being collected for the
        // whole statement. The Iceberg spec wants rows inside a delete file
        // sorted by (file_path, pos), so files are visited in path order and
        // the writer sorts positions within each file; the accumulator then
        // holds at most one file's positions (8 bytes each, one shared
        // `Arc<str>` for the path) and those bytes are pool-visible through
        // `mor-delete-position-buffer`.
        let mut sorted_files: Vec<&iceberg::spec::DataFile> = old_data_files.iter().collect();
        sorted_files.sort_by(|a, b| a.file_path().cmp(b.file_path()));
        let pool = ctx.runtime_env().memory_pool.clone();
        let mut position_res = self
            .config
            .query
            .write_buffer_tracking
            .then(|| WriteReservation::new(&pool, "mor-delete-position-buffer"));
        let mut delete_writer =
            PositionDeleteWriter::open(&table, self.compression(session)).await?;
        let mut marked_positions: usize = 0;
        // V3: position-delete files are forbidden; each touched data file gets
        // one deletion vector holding ALL its deleted positions, and the files
        // it supersedes are retired in the same commit.
        let v3 = table.metadata().format_version() >= iceberg::spec::FormatVersion::V3;
        let base_snapshot_id = table.metadata().current_snapshot_id();
        let mut new_vectors: Vec<iceberg::spec::DataFile> = Vec::new();
        let mut retired_deletes: Vec<iceberg::spec::DataFile> = Vec::new();
        // V2: data files the position deletes point at, checked at commit.
        let mut referenced_paths: Vec<String> = Vec::new();

        for data_file in sorted_files {
            let file_path: Arc<str> = Arc::from(data_file.file_path());
            let batches = self
                .read_parquet_via_table(&table, &file_path, ctx, true)
                .await?;

            if batches.is_empty() {
                continue;
            }

            // Row positions are 0-based and contiguous across all batches in the file.
            let mut positions: Vec<i64> = Vec::new();
            let mut row_offset: i64 = 0;
            for batch in &batches {
                let match_mask = self
                    .filter_batch_match(
                        ctx,
                        session,
                        batch,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;

                let batch_matches = match_mask.true_count();
                if batch_matches > 0 {
                    if let Some(r) = position_res.as_mut() {
                        r.try_grow(batch_matches * std::mem::size_of::<i64>())?;
                    }
                    positions.reserve(batch_matches);
                    for row_idx in 0..batch.num_rows() {
                        if match_mask.value(row_idx) {
                            positions.push(row_offset + row_idx as i64);
                        }
                    }
                }
                row_offset += batch.num_rows() as i64;
            }
            let raw_matches = positions.len();

            let file_has_deletes = read_plan
                .tasks_by_path
                .get(file_path.as_ref())
                .is_some_and(|tasks| tasks.iter().any(|t| !t.deletes().is_empty()));
            let live_matches = if raw_matches > 0 && file_has_deletes {
                let mut live = 0usize;
                for batch in self
                    .read_data_file_applying_deletes(&read_plan, &file_path, ctx, true)
                    .await?
                {
                    let mask = self
                        .filter_batch_match(
                            ctx,
                            session,
                            &batch,
                            &where_sql,
                            &joins_sql,
                            &table_ident,
                            &predicates,
                        )
                        .await?;
                    live += mask.true_count();
                }
                Some(live)
            } else {
                None
            };
            let newly_deleted = newly_deleted_row_count(raw_matches, live_matches);
            deleted_count += newly_deleted;

            // Only files that still have a visible matching row are marked:
            // re-marking positions a live delete file already hides would
            // upload bytes for a statement that may end up a no-op below.
            if newly_deleted > 0 {
                marked_positions += raw_matches;
                if v3 {
                    let task = read_plan
                        .tasks_by_path
                        .get(file_path.as_ref())
                        .and_then(|tasks| tasks.first());
                    let (vector, retired) = merge_into_deletion_vector(
                        &table,
                        data_file,
                        task,
                        std::mem::take(&mut positions),
                    )
                    .await?;
                    new_vectors.extend(vector);
                    retired_deletes.extend(retired);
                } else {
                    referenced_paths.push(file_path.to_string());
                    delete_writer
                        .write_file(Arc::clone(&file_path), std::mem::take(&mut positions))
                        .await?;
                }
            }
            if let Some(r) = position_res.as_mut() {
                r.shrink(raw_matches * std::mem::size_of::<i64>());
            }
        }

        // Zero live matches means either nothing matched at all or every match
        // is already hidden by a live delete file. Both are no-ops: writing a
        // delete file that re-marks already-deleted positions would burn a
        // snapshot without changing a single visible row.
        if deleted_count == 0 {
            info!(
                table = %table_ident,
                "MoR DELETE: no live matching rows, nothing to commit"
            );
            return affected_rows_batch(0);
        }

        info!(
            table = %table_ident,
            delete_count = deleted_count,
            marked_positions,
            "MoR DELETE: closing position delete files"
        );

        let delete_files = delete_writer.close().await?;

        // Commit. V2: a RowDelta adding the position delete files. V3: a RowDelta
        // that adds the new deletion vectors and retires what they supersede,
        // pinned to the snapshot the vectors were merged against. A concurrent
        // writer would otherwise leave two vectors on one data file, which the
        // spec forbids, so a moved table fails the commit instead.
        // V2 tolerates a moved table (a concurrent append is harmless) but
        // checks it by hand: a position delete names its data file by path,
        // and if a concurrent commit (compaction, CoW rewrite) retired that
        // file the delete would match nothing and the rows would survive a
        // DELETE that reported success. The V2 RowDelta is pinned to the
        // snapshot that check ran against, so a commit landing after the
        // check forces a retry that checks again.
        let files_for_retry = delete_files.clone();
        let vectors_for_retry = new_vectors.clone();
        let retired_for_retry = retired_deletes.clone();
        let referenced_for_retry = Arc::new(referenced_paths);
        let catalog_for_commit = catalog.clone();
        let target_branch =
            crate::commit::txn_target_branch(catalog.as_catalog().as_ref(), &table_ident).await?;
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "mor-delete",
            move |fresh_table| {
                let files = files_for_retry.clone();
                let vectors = vectors_for_retry.clone();
                let retired = retired_for_retry.clone();
                let referenced = Arc::clone(&referenced_for_retry);
                let cat = catalog_for_commit.clone();
                let target_branch = target_branch.clone();
                async move {
                    if !v3 && fresh_table.metadata().current_snapshot_id() != base_snapshot_id {
                        let live: std::collections::HashSet<String> =
                            crate::writer::collect_live_data_files(&fresh_table)
                                .await
                                .map_err(|e| {
                                    iceberg::Error::new(
                                        iceberg::ErrorKind::Unexpected,
                                        e.to_string(),
                                    )
                                })?
                                .iter()
                                .map(|f| f.file_path().to_string())
                                .collect();
                        if let Some(gone) = referenced.iter().find(|p| !live.contains(p.as_str())) {
                            return Err(iceberg::Error::new(
                                iceberg::ErrorKind::DataInvalid,
                                format!(
                                    "data file {gone} was replaced by a concurrent commit; \
                                     re-run the DELETE"
                                ),
                            ));
                        }
                    }
                    let tx = Transaction::new(&fresh_table);
                    let tx = if v3 {
                        let mut action = tx
                            .row_delta()
                            .add_delete_files(vectors)
                            .remove_delete_files(retired);
                        if let Some(base) = base_snapshot_id {
                            action = action.validate_from_snapshot(base);
                        }
                        if let Some(branch) = target_branch {
                            action = action.set_target_branch(branch);
                        }
                        action.apply(tx)?
                    } else {
                        // Pinned to the snapshot the live-file check above
                        // ran against. `Transaction::commit` reloads the
                        // table and silently re-applies an unpinned action
                        // on whatever is current, so a fast_append would
                        // land on a snapshot nobody checked. The pin turns
                        // that into a retryable "RowDelta conflict", and the
                        // next attempt re-runs the check on the new snapshot.
                        let mut action = tx.row_delta().add_delete_files(files);
                        if let Some(snap) = fresh_table.metadata().current_snapshot_id() {
                            action = action.validate_from_snapshot(snap);
                        }
                        if let Some(branch) = target_branch {
                            action = action.set_target_branch(branch);
                        }
                        action.apply(tx)?
                    };
                    self.before_commit_seam().await;
                    tx.commit(cat.as_catalog().as_ref()).await
                }
            },
        )
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit MoR DELETE: {e}")))?;

        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;

        info!(table = %table_ident, deleted_rows = deleted_count, "MoR DELETE committed successfully");
        affected_rows_batch(deleted_count as u64)
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
        catalog.admit_txn_write(&table_ident).await?;
        let delete = match stmt {
            Statement::Delete(d) => d,
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "table {table_ident} has no identifier-field-ids; equality-delete path requires a primary key"
            )));
        }

        let old_data_files = self.collect_data_files(&table).await?;
        if old_data_files.is_empty() {
            info!(table = %table_ident, "equality DELETE: table has no data files");
            return affected_rows_batch(0);
        }

        let where_clause = &delete.selection;
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(
                session,
                catalog_qualifier(delete_target_object_name(stmt)?),
                &table_ident,
                target_schema,
                policy_summary_out,
            )
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
            // Shared retryable truncate (506); see `handle_delete_mor`.
            let truncated_rows = self
                .truncate_via_rewrite(session, &catalog, &table_ident, "equality DELETE")
                .await?;
            return affected_rows_batch(truncated_rows as u64);
        }

        let raw_where = where_clause
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(session, &raw_where, ctx).await?;
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
        // deletes need only the identifier columns, so each matched batch is
        // projected to the key columns before it is buffered, and the buffer
        // is pool-tracked (architecture review 504). Deletes are applied
        // during the read (#371) so already-deleted rows neither inflate the
        // affected-row count nor emit redundant delete keys.
        let pool = ctx.runtime_env().memory_pool.clone();
        let mut key_batches = TrackedBatchBuffer::gated(
            &pool,
            "mor-delete-key-buffer",
            self.config.query.write_buffer_tracking,
        );
        let iceberg_schema = table.metadata().current_schema().clone();
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
                        session,
                        &batch,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;
                let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("failed to filter match rows: {e}"))
                })?;
                if filtered.num_rows() == 0 {
                    continue;
                }
                total_matched += filtered.num_rows();
                let keys = project_to_identifier_columns(
                    &filtered,
                    &iceberg_schema,
                    &identifier_field_ids,
                )?;
                key_batches.push(keys)?;
            }
        }

        if total_matched == 0 {
            info!(table = %table_ident, "equality DELETE: no matching rows, nothing to commit");
            return affected_rows_batch(0);
        }

        // `key_batches` stays alive (and reserved) across the write.
        let delete_files = write_equality_delete_key_files(
            &table,
            key_batches.as_slice().to_vec(),
            identifier_field_ids,
            self.compression(session),
        )
        .await?;

        // Commit via RowDeltaAction: this emits Operation::Overwrite with
        // added-delete-files > 0 and no removed/added data files. The
        // SnapshotProducer's added-delete-files summary key mirrors Spark.
        let files_for_retry = delete_files.clone();
        let catalog_for_commit = catalog.clone();
        let target_branch =
            crate::commit::txn_target_branch(catalog.as_catalog().as_ref(), &table_ident).await?;
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "equality-delete",
            move |fresh_table| {
                let files = files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                let target_branch = target_branch.clone();
                async move {
                    let snapshot_id = fresh_table.metadata().current_snapshot_id();
                    let tx = Transaction::new(&fresh_table);
                    let mut action = tx.row_delta().add_delete_files(files);
                    if let Some(snap) = snapshot_id {
                        action = action.validate_from_snapshot(snap);
                    }
                    if let Some(branch) = target_branch {
                        action = action.set_target_branch(branch);
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit equality DELETE: {e}"))
            }
        })?;

        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;

        info!(
            table = %table_ident,
            deleted_rows = total_matched,
            "equality DELETE committed successfully"
        );
        affected_rows_batch(total_matched as u64)
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
        self.capture_self_lineage(session, ctx, &table_ident, table_factor_name, plan_out)
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
        catalog.admit_txn_write(&table_ident).await?;
        let (assignments, selection) = match stmt {
            Statement::Update(update) => (&update.assignments, &update.selection),
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected UPDATE statement, got: {other}"
                )))
            }
        };

        let table = catalog.load_table(&table_ident).await?;

        // Snapshot-independent setup runs once. Schema, policy predicates, and
        // lifted IN-subqueries do not depend on the data snapshot, so they are
        // computed before the retry loop. Subquery values are evaluated against
        // the initial snapshot, consistent with UPDATE's snapshot isolation.
        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(
                session,
                catalog_qualifier(update_target_object_name(stmt)?),
                &table_ident,
                target_schema,
                policy_summary_out,
            )
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
            self.lift_in_subqueries(session, &raw_where, ctx).await?;

        // Partition-pruned CoW under optimistic-concurrency retry. Each attempt
        // re-reads the current snapshot, rewrites only the data files whose
        // partition could match the WHERE (the rest stay untouched, #263), and
        // commits. On a commit conflict the whole rewrite re-runs against the
        // reloaded snapshot -- re-committing snapshot-specific rewritten files
        // would drop a concurrent writer's changes (#47, write-path race).
        let borrowed = (
            &where_sql[..],
            &joins_sql[..],
            &predicates,
            &table_ident,
            assignments,
        );
        let total_updated = self
            .commit_rewrite_with_retry(
                catalog.as_catalog().as_ref(),
                &table_ident,
                "update-cow",
                "UPDATE",
                move |fresh_table| {
                    // Copy references only; see `handle_delete` for why.
                    let (where_sql, joins_sql, predicates, table_ident, assignments) = borrowed;
                    async move {
                        let old_data_files = self.collect_data_files(&fresh_table).await?;
                        if old_data_files.is_empty() {
                            info!(table = %table_ident, "UPDATE: table has no data files");
                            return Ok(None);
                        }
                        let (to_rewrite, pruned_files) = partition_prune_cow_files(
                            old_data_files,
                            &fresh_table,
                            selection.as_ref(),
                        );
                        if to_rewrite.is_empty() {
                            info!(table = %table_ident, "UPDATE: no partitions match the WHERE clause; nothing to rewrite");
                            return Ok(None);
                        }
                        if pruned_files > 0 {
                            info!(table = %table_ident, pruned_files, rewrite_files = to_rewrite.len(), "UPDATE: partition-pruned CoW rewrite");
                        }
                        info!(
                            table = %table_ident,
                            file_count = to_rewrite.len(),
                            spec_ids = ?distinct_spec_ids(&to_rewrite),
                            assignments = assignments.len(),
                            where_clause = %where_sql,
                            policy_row_filter = predicates.row_filter_sql.is_some(),
                            policy_masks = predicates.column_mask_sqls.len(),
                            "UPDATE: CoW rewrite"
                        );

                        let mut new_data_files = Vec::new();
                        let mut updated_this_attempt = 0usize;
                        let tracker = new_upload_tracker();
                        let cleanup_guard = WriteCleanupGuard::new(
                            fresh_table.file_io().clone(),
                            tracker.clone(),
                            "update-cow",
                        )
                        .with_metrics(self.metrics.clone());
                        let pool = ctx.runtime_env().memory_pool.clone();
                        let read_plan = self.plan_delete_aware_read(&fresh_table).await?;

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
                                // 509: the affected-row count is the true count of
                                // the same match mask the MoR path computes, not a
                                // second planned-and-executed COUNT(*) per batch.
                                let mask = self
                                    .filter_batch_match(
                                        ctx,
                                        session,
                                        batch,
                                        where_sql,
                                        joins_sql,
                                        table_ident,
                                        predicates,
                                    )
                                    .await?;
                                let matched = mask.true_count();
                                if matched == 0 {
                                    // Nothing to rewrite in this batch: pass it
                                    // through untouched (no CASE evaluation).
                                    rewritten.push(batch.clone())?;
                                    continue;
                                }
                                updated_this_attempt += matched;
                                let out = self
                                    .apply_update(
                                        ctx,
                                        session,
                                        batch,
                                        assignments,
                                        where_sql,
                                        joins_sql,
                                        table_ident,
                                        predicates,
                                    )
                                    .await?;
                                rewritten.push(out)?;
                            }
                            let new_files = write_data_files_with_metrics(
                                &fresh_table,
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

                        let files_to_remove = self
                            .extend_with_superseded_deletes(&fresh_table, to_rewrite)
                            .await?;
                        Ok(Some(RewriteAttempt {
                            add: new_data_files,
                            remove: files_to_remove,
                            cleanup: Some(cleanup_guard),
                            value: updated_this_attempt,
                        }))
                    }
                },
            )
            .await?;

        let Some(total_updated) = total_updated else {
            return affected_rows_batch(0);
        };
        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;
        info!(table = %table_ident, updated_rows = total_updated, "UPDATE committed successfully");
        affected_rows_batch(total_updated as u64)
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
        self.capture_self_lineage(session, ctx, &table_ident, table_factor_name, plan_out)
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
        catalog.admit_txn_write(&table_ident).await?;
        let (assignments, selection) = match stmt {
            Statement::Update(update) => (&update.assignments, &update.selection),
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "MoR UPDATE on {table_ident} requires identifier-field-ids (primary key)"
            )));
        }

        let old_data_files = self.collect_data_files(&table).await?;
        if old_data_files.is_empty() {
            info!(table = %table_ident, "MoR UPDATE: table has no data files");
            return affected_rows_batch(0);
        }

        let target_schema = build_arrow_schema_for_table(&table)?;
        let predicates = self
            .compute_write_predicates(
                session,
                catalog_qualifier(update_target_object_name(stmt)?),
                &table_ident,
                target_schema,
                policy_summary_out,
            )
            .await?;

        let raw_where = selection
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(session, &raw_where, ctx).await?;

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
        //
        // Both accumulators are pool-tracked (architecture review 504): the
        // new-row side cannot stream because `apply_update` evaluates one
        // batch at a time, and the key side holds only the projected
        // identifier columns.
        let pool = ctx.runtime_env().memory_pool.clone();
        let mut new_row_batches = TrackedBatchBuffer::gated(
            &pool,
            "mor-update-row-buffer",
            self.config.query.write_buffer_tracking,
        );
        let mut key_batches = TrackedBatchBuffer::gated(
            &pool,
            "mor-update-key-buffer",
            self.config.query.write_buffer_tracking,
        );
        let iceberg_schema = table.metadata().current_schema().clone();
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
                        session,
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
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("failed to filter match rows: {e}"))
                })?;
                if old_keys.num_rows() > 0 {
                    let keys = project_to_identifier_columns(
                        &old_keys,
                        &iceberg_schema,
                        &identifier_field_ids,
                    )?;
                    key_batches.push(keys)?;
                }

                // New values for the data file. `apply_update` produces a
                // full-batch rewrite with CASE WHEN where THEN new ELSE
                // old END, then we filter to only the matched rows so we
                // do not re-write the unchanged ones.
                let full_rewrite = self
                    .apply_update(
                        ctx,
                        session,
                        &batch,
                        assignments,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                        &predicates,
                    )
                    .await?;
                let new_rows = filter_record_batch(&full_rewrite, &match_mask).map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("failed to filter updated rows: {e}"))
                })?;
                if new_rows.num_rows() > 0 {
                    new_row_batches.push(new_rows)?;
                }
            }
        }

        if total_updated == 0 {
            info!(table = %table_ident, "MoR UPDATE: no matching rows, nothing to commit");
            return affected_rows_batch(0);
        }

        // Write the data file with the new values and the equality delete
        // file with the old keys. Both go into one RowDelta commit. The
        // tracked buffers stay alive (reserved) across both writes.
        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "update-mor");
        let new_data_files = write_data_files_with_metrics(
            &table,
            new_row_batches.as_slice().to_vec(),
            "update-mor",
            self.metrics.as_ref(),
            self.compression(session),
            tracker,
        )
        .await?;

        let delete_files = write_equality_delete_key_files(
            &table,
            key_batches.as_slice().to_vec(),
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
        let target_branch =
            crate::commit::txn_target_branch(catalog.as_catalog().as_ref(), &table_ident).await?;
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "mor-update",
            move |fresh_table| {
                let dfs = data_files_for_retry.clone();
                let dels = delete_files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                let target_branch = target_branch.clone();
                async move {
                    let snapshot_id = fresh_table.metadata().current_snapshot_id();
                    let tx = Transaction::new(&fresh_table);
                    let mut action = tx.row_delta().add_data_files(dfs).add_delete_files(dels);
                    if let Some(snap) = snapshot_id {
                        action = action.validate_from_snapshot(snap);
                    }
                    if let Some(branch) = target_branch {
                        action = action.set_target_branch(branch);
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit MoR UPDATE: {e}"))
            }
        })?;
        cleanup_guard.mark_committed();
        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;

        info!(
            table = %table_ident,
            updated_rows = total_updated,
            "MoR UPDATE committed successfully"
        );
        affected_rows_batch(total_updated as u64)
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
    ///
    /// `source_schema_hint` is the Arrow schema of the source's logical plan,
    /// supplied by the caller because zero batches carry no schema
    /// (architecture review 501). It is only consulted when the source
    /// returned no rows and the statement carries a `WHEN NOT MATCHED BY
    /// SOURCE` clause, which must still run against the whole target.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, session, stmt, source_batches, source_schema_hint, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_merge(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        source_schema_hint: Option<arrow_schema::SchemaRef>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::TableFactor;

        let (table_factor, source_factor, on_expr, clauses) = match stmt {
            Statement::Merge(merge) => (&merge.table, &merge.source, &merge.on, &merge.clauses),
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected table name in MERGE target, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(target_table_name, session)?;
        catalog.admit_txn_write(&table_ident).await?;

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

        // Load the target table for the snapshot-independent setup below (its
        // column names, the policy predicates, the compiled merge SQL). The
        // rewrite itself re-reads the target inside the retry closure: on a
        // commit conflict the whole merge re-runs against the reloaded
        // snapshot, because rewritten files are snapshot-specific (506).
        let table = catalog.load_table(&table_ident).await?;

        // Column names for the merge SQL come from the table's current schema.
        // The per-attempt target relation derives its own Arrow schema from the
        // batches it read (or from this same metadata when the table is empty),
        // and the column names are identical either way.
        let target_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                |e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to convert Iceberg schema to Arrow: {e}")),
            )?,
        );

        let mut target_columns: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        if target_columns.is_empty() {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "MERGE target table {table_ident} has no columns for match detection"
            )));
        }
        // V3: the target relation carries the row-lineage columns (the rewrite
        // read selects them); project them through so the writer persists them.
        if table.metadata().format_version() >= FormatVersion::V3 {
            target_columns.push(iceberg::metadata_columns::RESERVED_COL_NAME_ROW_ID.to_string());
            target_columns.push(
                iceberg::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER
                    .to_string(),
            );
        }

        // The caller's row filter and masks on the TARGET (architecture review
        // 202): DELETE and UPDATE already gate their target through
        // `compute_write_predicates`; MERGE enforced its source only, so a
        // caller filtered to EU could delete or update US rows by matching
        // them. The predicates are folded into the merge SQL by
        // `merge_sql` (guarded ON, guarded BY SOURCE class, masked user
        // references); hidden rows pass through the rewrite unchanged.
        let target_policy = self
            .compute_write_predicates(
                session,
                catalog_qualifier(target_table_name),
                &table_ident,
                target_schema.clone(),
                policy_summary_out,
            )
            .await?;

        // Use the target alias (or a default) for the merge scratch table names.
        // The scratch table names embed a per-invocation uuid so concurrent
        // MERGEs running in the same session do not clobber each other's
        // registrations.
        let t_alias = target_alias.clone().unwrap_or_else(|| "t".to_string());
        let s_alias = source_alias.clone().unwrap_or_else(|| "s".to_string());
        let merge_id = Uuid::now_v7().simple().to_string();
        // The `__sqe_` prefix marks both as engine scratch, so
        // `enforce_dml_eval` governs only the catalog tables around them.
        let target_table_ref = format!("__sqe_merge_target_{merge_id}");
        let source_table_ref = format!("__sqe_merge_source_{merge_id}");
        let qualified_target_ref = format!("datafusion.public.{target_table_ref}");
        let qualified_source_ref = format!("datafusion.public.{source_table_ref}");
        let _merge_scratch_guard = MergeScratchCleanup::new(
            ctx,
            vec![qualified_target_ref.clone(), qualified_source_ref.clone()],
        );

        // Use the pre-executed source batches (caller handles source query
        // execution). An empty source is NOT a shortcut: with no source rows
        // every target row is unmatched-by-source, so a WHEN NOT MATCHED BY
        // SOURCE clause has to run (501). The decision needs the clause
        // classification, which needs the source's column names, which zero
        // batches cannot supply -- hence `source_schema_hint` from the
        // caller's source plan. Without a hint we cannot build the merge SQL
        // at all and fall back to the historical no-op.
        let source_empty = source_batches.is_empty();
        let source_schema = match source_batches.first() {
            Some(first) => first.schema(),
            None => match source_schema_hint {
                Some(schema) => schema,
                None => {
                    info!(
                        table = %table_ident,
                        "MERGE: source returned no data and no source schema was \
                         supplied, nothing to merge"
                    );
                    return affected_rows_batch(0);
                }
            },
        };

        // Register source data as a MemTable (an empty partition list when the
        // source produced no batches, so the FULL OUTER JOIN still plans). The
        // source is snapshot-independent, so it is registered once; only the
        // target relation is rebuilt per attempt.
        let source_partitions = if source_empty {
            vec![vec![]]
        } else {
            vec![source_batches]
        };
        let source_mem =
            datafusion::datasource::MemTable::try_new(source_schema.clone(), source_partitions)
                .map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to create source MemTable: {e}"))
                })?;
        ctx.register_table(&qualified_source_ref, Arc::new(source_mem))
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to register source MemTable: {e}")))?;

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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
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
            target_policy: &target_policy,
        };
        let classified = crate::merge_sql::classify_merge_clauses(clauses, &merge_names)?;
        let (matched_clauses, not_matched_clauses, by_source_clauses) = classified.counts();
        // The masked target view is (re)registered per attempt below, over
        // that attempt's target; this guard drops it with the statement.
        let _policy_view_guard = crate::merge_sql::target_policy_view(&merge_names)
            .map(|view| MergeScratchCleanup::new(ctx, vec![view]));

        // 501: the emptiness shortcut lives *after* classification. With an
        // empty source and no BY SOURCE clause nothing can change, so return
        // before the rewrite (no new snapshot). With a BY SOURCE clause the
        // merge runs: every target row is unmatched-by-source.
        //
        // The CoW affected-row count is normally the number of rows written,
        // which is 0 after a BY SOURCE DELETE emptied the table. Count the
        // claimed target rows instead, which is exact here: an empty source
        // matches nothing and inserts nothing, so BY SOURCE is the only class
        // that can change a row. The count runs per attempt (inside the retry
        // closure) because it reads the target relation of that attempt.
        let mut empty_source_count_sql: Option<String> = None;
        if source_empty {
            if crate::merge_sql::empty_source_is_no_op(&classified) {
                info!(
                    table = %table_ident,
                    "MERGE: source returned no data and no WHEN NOT MATCHED BY \
                     SOURCE clause, nothing to merge"
                );
                return affected_rows_batch(0);
            }
            empty_source_count_sql = crate::merge_sql::empty_source_claim_count_sql(
                &classified,
                &merge_names,
                &qualified_target_ref,
            );
            info!(
                table = %table_ident,
                by_source_clauses,
                "MERGE: empty source, applying WHEN NOT MATCHED BY SOURCE to \
                 every target row"
            );
        }

        // Cardinality check: a target row matched by >1 source row would be
        // silently duplicated by the FULL OUTER JOIN. Only matched clauses can
        // trigger it, so skip the extra count pass for insert-only / by-source
        // merges.
        let cardinality_sql = if self.config.query.merge_cardinality_check && matched_clauses > 0 {
            Some(crate::merge_sql::build_cardinality_check_sql(
                &merge_names,
                &qualified_target_ref,
                &qualified_source_ref,
                &on_rewritten,
            ))
        } else {
            None
        };

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

        // The rewrite under optimistic-concurrency retry (506). Each attempt
        // re-reads the target's data files, re-registers the target scratch
        // relation over that snapshot, re-runs the merge SELECT and writes new
        // files; a conflict drops the attempt's parquet files and starts over.
        // Before this, MERGE — the dbt incremental hot path, and the statement
        // most likely to run against a table the compaction scheduler is
        // rewriting — failed the whole statement on the first lost race.
        let with_check_sql = crate::merge_sql::build_merge_with_check_sql(
            &classified,
            &merge_names,
            &qualified_target_ref,
            &qualified_source_ref,
            &on_rewritten,
        );
        let borrowed = (
            &table_ident,
            &qualified_target_ref[..],
            &select_sql[..],
            empty_source_count_sql.as_deref(),
            cardinality_sql.as_ref(),
            with_check_sql.as_deref(),
            &merge_names,
        );
        let outcome = self
            .commit_rewrite_with_retry(
                catalog.as_catalog().as_ref(),
                &table_ident,
                "merge-cow",
                "MERGE",
                move |fresh_table| {
                    let (
                        table_ident,
                        qualified_target_ref,
                        select_sql,
                        empty_source_count_sql,
                        cardinality_sql,
                        with_check_sql,
                        merge_names,
                    ) = borrowed;
                    async move {
                        let old_data_files = self.collect_data_files(&fresh_table).await?;
                        let read_plan = self.plan_delete_aware_read(&fresh_table).await?;
                        let (target_provider, _target_buf_hold) = self
                            .build_merge_target(
                                &fresh_table,
                                &old_data_files,
                                &read_plan,
                                ctx,
                                table_ident,
                            )
                            .await?;

                        // Register the target relation in the full session
                        // context (which has all catalog tables registered for
                        // cross-table subqueries). A retry replaces the
                        // previous attempt's registration under the same name.
                        ctx.register_table(qualified_target_ref, target_provider)
                            .map_err(|e| {
                                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                                    "Failed to register target provider: {e}"
                                ))
                            })?;
                        // The masked target view captures the provider it is
                        // planned against, so rebuild it over this attempt's.
                        crate::merge_sql::register_target_policy_view(
                            ctx,
                            merge_names,
                            qualified_target_ref,
                        )
                        .await?;

                        if let Some((pair_sql, matched_sql)) = cardinality_sql {
                            self.check_merge_cardinality(ctx, session, pair_sql, matched_sql).await?;
                        }
                        if let Some(check_sql) = with_check_sql {
                            let label = "MERGE WITH CHECK";
                            let outside =
                                scalar_count(self.plan_merge_sql(ctx, session, check_sql, label).await?, label).await?;
                            if outside > 0 {
                                return Err(SqeError::with_code(
                                    SqeErrorCode::AccessDenied,
                                    format!(
                                        "write to {table_ident} violates row-level security policy: \
                                         {outside} row(s) fall outside the caller's row filter"
                                    ),
                                ));
                            }
                        }

                        let mut empty_source_affected: Option<usize> = None;
                        if let Some(count_sql) = empty_source_count_sql {
                            let label = "MERGE empty-source row count";
                            let claimed =
                                scalar_count(self.plan_merge_sql(ctx, session, count_sql, label).await?, label)
                                    .await?;
                            empty_source_affected = Some(claimed.max(0) as usize);
                        }

                        let df = self.plan_merge_sql(ctx, session, select_sql, "MERGE query").await?;

                        // Layer B (output side): stream the merged output straight into the
                        // streaming sink instead of `df.collect()`-ing the whole merged result
                        // into a Vec and buffering it through `write_data_files_with_metrics`.
                        // The merged output is at least as large as the target table, so this
                        // removes the single biggest MERGE buffer. The join/CASE state stays
                        // inside DataFusion operators, which are pool-tracked (and spillable
                        // under subsystem A) rather than an invisible Vec.
                        //
                        // The stream must be fully consumed by the awaited write below before
                        // the `_target_buf_hold` reservation and the `MergeScratchCleanup`
                        // guard drop: the MemTableExec feeding the join holds Arc refs into
                        // the tracked target batches for the duration of the read.
                        // Rows removed by the MERGE (DELETE clauses, unclaimed source-only
                        // rows) are filtered by the keep-column WHERE inside `select_sql`, so
                        // the stream carries exactly the surviving rows.
                        let stream = self.execute_source_stream(df, "MERGE rewrite").await?;

                        // Write new data files from the merged results (streaming).
                        let tracker = new_upload_tracker();
                        let cleanup_guard = WriteCleanupGuard::new(
                            fresh_table.file_io().clone(),
                            tracker.clone(),
                            "merge-cow",
                        )
                        .with_metrics(self.metrics.clone());
                        let (new_data_files, total_rows) = write_data_files_streaming_with_metrics(
                            &fresh_table,
                            stream,
                            "merge",
                            self.metrics.as_ref(),
                            self.compression(session),
                            tracker,
                            self.fanout_limits(Some(ctx.runtime_env().memory_pool.clone())),
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
                            return Ok(None);
                        }

                        let files_to_remove = self
                            .extend_with_superseded_deletes(&fresh_table, old_data_files)
                            .await?;
                        Ok(Some(RewriteAttempt {
                            add: new_data_files,
                            remove: files_to_remove,
                            cleanup: Some(cleanup_guard),
                            value: (total_rows, empty_source_affected),
                        }))
                    }
                },
            )
            .await?;

        let Some((total_rows, empty_source_affected)) = outcome else {
            return affected_rows_batch(0);
        };
        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;

        // With an empty source the rows the statement *changed* are the target
        // rows a BY SOURCE clause claimed, counted before the rewrite; the
        // rows it *wrote* (`total_rows`) is 0 after a BY SOURCE DELETE (501).
        let affected = empty_source_affected.unwrap_or(total_rows);
        info!(table = %table_ident, total_rows, affected, "MERGE committed successfully");
        affected_rows_batch(affected as u64)
    }

    /// Build the DataFusion relation the merge SELECT reads its target from,
    /// for one snapshot. Returns the provider plus the pool reservation the
    /// caller must hold until the merged stream has been fully consumed
    /// (buffered path only; `None` in the streaming path).
    ///
    /// Two paths:
    ///  - Buffered (default): read the whole target into a pool-tracked Vec
    ///    (Layer A `merge-target-buffer`) and register it as a MemTable.
    ///  - Streaming (B2, opt-in via `merge_target_streaming`): scan the pinned
    ///    `old_data_files` lazily so the target flows through the merge join as
    ///    governed operator memory instead of a MemTable Vec.
    ///    See docs/internal/specs/2026-07-02-write-path-memory-safety-design.md.
    ///
    /// The streaming provider decodes raw parquet, so it must stand down when
    /// the snapshot carries delete files (#371): rewriting a target read
    /// without its position/equality deletes applied resurrects MoR-deleted
    /// rows. The buffered path applies them.
    async fn build_merge_target(
        &self,
        table: &IcebergTable,
        old_data_files: &[DataFile],
        read_plan: &DeleteAwareReadPlan,
        ctx: &DFSessionContext,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<(
        Arc<dyn datafusion::datasource::TableProvider>,
        Option<TrackedBatchBuffer>,
    )> {
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

        if stream_target {
            let schema = Arc::new(
                iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                    |e| {
                        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                            "Failed to convert Iceberg schema to Arrow: {e}"
                        ))
                    },
                )?,
            );
            let provider =
                crate::merge_target_provider::merge_target_table(table, old_data_files, schema)?;
            info!(
                table = %table_ident,
                files = old_data_files.len(),
                "MERGE: streaming target from pinned data files (B2)"
            );
            return Ok((Arc::new(provider), None));
        }

        let pool = ctx.runtime_env().memory_pool.clone();
        let mut target_buf = TrackedBatchBuffer::gated(
            &pool,
            "merge-target-buffer",
            self.config.query.write_buffer_tracking,
        );
        for data_file in old_data_files {
            let file_path = data_file.file_path();
            // track = false: the returned batches are re-registered into
            // `merge-target-buffer` just below, so tracking the decode here
            // too would double-count the same bytes.
            let batches = self
                .read_data_file_applying_deletes(read_plan, file_path, ctx, false)
                .await?;
            for batch in batches {
                target_buf.push(batch)?;
            }
        }

        // Target schema from existing data (or table metadata if empty).
        let target_schema = if let Some(first) = target_buf.as_slice().first() {
            first.schema()
        } else {
            // V3: same shape the rewrite read would have produced, lineage included.
            let iceberg_schema = if table.metadata().format_version() >= FormatVersion::V3 {
                sqe_compaction::writer::schema_with_row_lineage(table.metadata().current_schema())?
            } else {
                table.metadata().current_schema().clone()
            };
            Arc::new(
                iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to convert Iceberg schema to Arrow: {e}"))
                })?,
            )
        };

        let target_mem = if target_buf.is_empty() {
            datafusion::datasource::MemTable::try_new(target_schema, vec![vec![]])
        } else {
            datafusion::datasource::MemTable::try_new(
                target_schema,
                vec![target_buf.as_slice().to_vec()],
            )
        }
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to create target MemTable: {e}")))?;
        Ok((Arc::new(target_mem), Some(target_buf)))
    }

    /// Plan one MoR MERGE new-row sub-query and return it as a stream whose
    /// rows are counted into `counter` as they pass (architecture review 504).
    /// The streams of every clause are chained into the streaming data-file
    /// writer by the caller, so new rows never materialise in a Vec.
    async fn merge_subquery_stream(
        &self,
        ctx: &DFSessionContext,
        session: &Session,
        sql: &str,
        label: &str,
        counter: Arc<std::sync::atomic::AtomicUsize>,
    ) -> sqe_core::Result<datafusion::execution::SendableRecordBatchStream> {
        let df = self.plan_merge_sql(ctx, session, sql, &format!("MoR MERGE {label}")).await?;
        let stream = self
            .execute_source_stream(df, &format!("MoR MERGE {label}"))
            .await?;
        let schema = stream.schema();
        let label = label.to_string();
        let counted = stream.map(move |item| match item {
            Ok(batch) => {
                counter.fetch_add(batch.num_rows(), std::sync::atomic::Ordering::Relaxed);
                Ok(batch)
            }
            Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                "MoR MERGE {label} execution failed: {e}"
            ))),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(counted),
        )))
    }

    /// Plan + drain one MoR MERGE old-key sub-query into the pool-tracked
    /// equality-delete key buffer, returning the number of key rows. The
    /// SELECT already projects only the identifier columns.
    async fn collect_merge_keys(
        &self,
        ctx: &DFSessionContext,
        session: &Session,
        sql: &str,
        label: &str,
        keys: &mut TrackedBatchBuffer,
    ) -> sqe_core::Result<usize> {
        let df = self.plan_merge_sql(ctx, session, sql, &format!("MoR MERGE {label}")).await?;
        let mut stream = self
            .execute_source_stream(df, &format!("MoR MERGE {label}"))
            .await?;
        let mut rows = 0usize;
        while let Some(item) = stream.next().await {
            let batch = item.map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("MoR MERGE {label} execution failed: {e}"))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            rows += batch.num_rows();
            keys.push(batch)?;
        }
        Ok(rows)
    }

    /// Dispatch MERGE to CoW or MoR based on `write.merge.mode`
    /// (Phase H, task 9.7).
    ///
    /// - `copy-on-write` (default): rewrite all target files via
    ///   `handle_merge` (pre-existing behaviour).
    /// - `merge-on-read`: route to `handle_merge_equality` when the table
    ///   declares a primary key. Without a PK we fall back to CoW because
    ///   the MATCHED clauses need old-row keys for the equality delete.
    ///
    /// `source_schema_hint` carries the source plan's Arrow schema so an empty
    /// source can still be registered as a typed relation and a `WHEN NOT
    /// MATCHED BY SOURCE` clause can run against it (501).
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_merge_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        source_schema_hint: Option<arrow_schema::SchemaRef>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        source_plan: Option<LogicalPlan>,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
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
                    .handle_merge(
                        session,
                        stmt,
                        source_batches,
                        source_schema_hint,
                        catalog,
                        ctx,
                        policy_summary_out,
                    )
                    .await;
            }
        };
        let target_name = match merge_table {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            _ => {
                return self
                    .handle_merge(
                        session,
                        stmt,
                        source_batches,
                        source_schema_hint,
                        catalog,
                        ctx,
                        policy_summary_out,
                    )
                    .await;
            }
        };
        let Ok(table_ident) = resolve_table_ident(target_name, session) else {
            return self
                .handle_merge(
                    session,
                    stmt,
                    source_batches,
                    source_schema_hint,
                    catalog,
                    ctx,
                    policy_summary_out,
                )
                .await;
        };
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self
                .handle_merge(
                    session,
                    stmt,
                    source_batches,
                    source_schema_hint,
                    catalog,
                    ctx,
                    policy_summary_out,
                )
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
                lineage_target_parts(&self.config, session, target_name, &table_ident);
            match build_lineage_insert_plan(src_plan, None, &lin_catalog, &lin_namespace, &lin_table)
            {
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
                    self.handle_merge_equality(
                        session,
                        stmt,
                        source_batches,
                        source_schema_hint,
                        catalog,
                        ctx,
                        policy_summary_out,
                    )
                    .await
                } else {
                    info!(
                        table = %table_ident,
                        "MERGE dispatch: MoR requested but no PK; falling back to CoW"
                    );
                    self.handle_merge(
                        session,
                        stmt,
                        source_batches,
                        source_schema_hint,
                        catalog,
                        ctx,
                        policy_summary_out,
                    )
                    .await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "MERGE dispatch: CoW");
                self.handle_merge(
                    session,
                    stmt,
                    source_batches,
                    source_schema_hint,
                    catalog,
                    ctx,
                    policy_summary_out,
                )
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
    ///
    /// `source_schema_hint` supplies the source's Arrow schema when the source
    /// produced zero batches, so a `WHEN NOT MATCHED BY SOURCE` clause still
    /// runs against every target row (501).
    #[allow(clippy::too_many_arguments)]
    #[instrument(
        skip(self, session, stmt, source_batches, source_schema_hint, catalog, ctx),
        fields(username = %session.user.username)
    )]
    pub async fn handle_merge_equality(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        source_schema_hint: Option<arrow_schema::SchemaRef>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
        policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::TableFactor;

        let (table_factor, source_factor, on_expr, clauses) = match stmt {
            Statement::Merge(merge) => (&merge.table, &merge.source, &merge.on, &merge.clauses),
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Expected table name in MERGE target, got: {other}"
                )));
            }
        };
        let table_ident = resolve_table_ident(target_table_name, session)?;
        catalog.admit_txn_write(&table_ident).await?;

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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to convert Iceberg schema to Arrow: {e}"))
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
        // `__sqe_` marks engine scratch for `enforce_dml_eval` (#804).
        let target_ref = format!("__sqe_merge_mor_target_{merge_id}");
        let source_ref = format!("__sqe_merge_mor_source_{merge_id}");
        let q_target = format!("datafusion.public.{target_ref}");
        let q_source = format!("datafusion.public.{source_ref}");
        let _merge_scratch_guard =
            MergeScratchCleanup::new(ctx, vec![q_target.clone(), q_source.clone()]);

        let target_mem = if target_buf.is_empty() {
            datafusion::datasource::MemTable::try_new(target_schema.clone(), vec![vec![]])
        } else {
            datafusion::datasource::MemTable::try_new(
                target_schema.clone(),
                vec![target_buf.as_slice().to_vec()],
            )
        }
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to create target MemTable: {e}")))?;
        ctx.register_table(&q_target, Arc::new(target_mem))
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to register target MemTable: {e}")))?;

        // An empty source is not a shortcut here either (501): the decision
        // needs the clause classification, which is below. Register the source
        // as an empty typed relation so the per-clause anti-joins still plan.
        let source_empty = source_batches.is_empty();
        let source_schema = match source_batches.first() {
            Some(first) => first.schema(),
            None => match source_schema_hint {
                Some(schema) => schema,
                None => {
                    info!(
                        table = %table_ident,
                        "MoR MERGE: source returned no data and no source schema \
                         was supplied, nothing to merge"
                    );
                    return affected_rows_batch(0);
                }
            },
        };
        let source_partitions = if source_empty {
            vec![vec![]]
        } else {
            vec![source_batches]
        };
        let source_mem =
            datafusion::datasource::MemTable::try_new(source_schema.clone(), source_partitions)
                .map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to create source MemTable: {e}"))
                })?;
        ctx.register_table(&q_source, Arc::new(source_mem))
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to register source MemTable: {e}")))?;

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
        // Target policy (architecture review 202), see the CoW path.
        let target_policy = self
            .compute_write_predicates(
                session,
                catalog_qualifier(target_table_name),
                &table_ident,
                target_schema.clone(),
                policy_summary_out,
            )
            .await?;
        let merge_names = crate::merge_sql::MergeNames {
            target_ref: &target_ref,
            source_ref: &source_ref,
            t_alias: &t_alias,
            s_alias: &s_alias,
            target_columns: &target_columns,
            source_columns: &source_columns,
            target_policy: &target_policy,
        };
        // Every per-clause query below reads the target through its policy
        // relation (admit verdict + masked columns) and the guarded ON, so a
        // hidden target row is never matched, deleted or updated. Masked
        // columns come from a view planned in the separate mask context.
        let _policy_view_guard = crate::merge_sql::target_policy_view(&merge_names)
            .map(|view| MergeScratchCleanup::new(ctx, vec![view]));
        crate::merge_sql::register_target_policy_view(ctx, &merge_names, &q_target).await?;
        let q_target_rel = format!(
            "{} AS {target_ref}",
            crate::merge_sql::target_relation(&merge_names, &q_target)
        );
        let on_guarded = crate::merge_sql::guarded_on(&merge_names, &on_rewritten);
        // Target-only rows the caller cannot see are not "not matched by
        // source" for them: they are out of scope.
        let by_source_admit = crate::merge_sql::admit_guard(&merge_names)
            .map(|g| format!(" AND {g}"))
            .unwrap_or_default();
        let classified = crate::merge_sql::classify_merge_clauses(clauses, &merge_names)?;
        let (matched_count, not_matched_count, by_source_count) = classified.counts();

        // 501: emptiness shortcut after classification. Without a BY SOURCE
        // clause an empty source cannot change anything, so return before the
        // row delta (no new snapshot). With one, every target row is
        // unmatched-by-source and the BY SOURCE loop below acts on all of them.
        if source_empty && crate::merge_sql::empty_source_is_no_op(&classified) {
            info!(
                table = %table_ident,
                "MoR MERGE: source returned no data and no WHEN NOT MATCHED BY \
                 SOURCE clause, nothing to merge"
            );
            return affected_rows_batch(0);
        }

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
            self.check_merge_cardinality(ctx, session, &pair_sql, &matched_sql).await?;
        }

        info!(
            table = %table_ident,
            on_condition = %on_sql,
            matched_count,
            not_matched_count,
            by_source_count,
            "MoR MERGE: planning row delta"
        );

        // Architecture review 504: the new-row side is no longer collected.
        // Each per-clause SELECT is `execute_stream`ed and the streams are
        // chained into one `select_all` stream feeding the streaming data-file
        // writer, so new rows never sit in a Vec; the per-clause row counts
        // come from tap counters on the streams. The old-key side is still
        // buffered (an equality-delete file needs all keys before it closes)
        // but only the projected identifier columns, in a pool-tracked buffer.
        let mut new_row_streams: Vec<datafusion::execution::SendableRecordBatchStream> = Vec::new();
        let mut equality_delete_batches = TrackedBatchBuffer::gated(
            &pool,
            "mor-merge-key-buffer",
            self.config.query.write_buffer_tracking,
        );
        let updated_rows = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inserted_rows = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut deleted_rows: usize = 0;

        // Old-key projection for an equality delete: only the identifier
        // columns, in identifier-field order, matching
        // `write_equality_delete_key_files`.
        let iceberg_schema = table.metadata().current_schema();
        let key_column_names = identifier_field_ids
            .iter()
            .map(|id| {
                iceberg_schema
                    .field_by_id(*id)
                    .map(|f| f.name.clone())
                    .ok_or_else(|| {
                        SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!("identifier field id {id} is not in the table schema"),
                        )
                    })
            })
            .collect::<sqe_core::Result<Vec<String>>>()?;
        let old_key_cols = || -> String {
            key_column_names
                .iter()
                .map(|col| {
                    let raw = crate::merge_sql::raw_target_col(&merge_names, col);
                    format!("{raw} AS \"{col}\"")
                })
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
                            // V3 row lineage: the updated row keeps `_row_id`
                            // (passthrough below) but its sequence number is
                            // nulled so it inherits the new file's.
                            if col == iceberg::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER {
                                return format!("CAST(NULL AS BIGINT) AS \"{col}\"");
                            }
                            let expr = crate::merge_sql::resolve_update_expr(
                                col,
                                assignments,
                                &merge_names,
                            );
                            format!("{expr} AS \"{col}\"")
                        })
                        .collect();
                    let new_sql = format!(
                        "SELECT {} FROM {q_target_rel} INNER JOIN {q_source} ON {on_guarded} WHERE {guard}",
                        update_cols.join(", ")
                    );
                    new_row_streams.push(
                        self.merge_subquery_stream(
                            ctx,
                            session,
                            &new_sql,
                            "MATCHED UPDATE new-row",
                            Arc::clone(&updated_rows),
                        )
                        .await?,
                    );
                    let old_sql = format!(
                        "SELECT {} FROM {q_target_rel} INNER JOIN {q_source} ON {on_guarded} WHERE {guard}",
                        old_key_cols()
                    );
                    self.collect_merge_keys(
                        ctx,
                        session,
                        &old_sql,
                        "MATCHED UPDATE old-key",
                        &mut equality_delete_batches,
                    )
                    .await?;
                }
                crate::merge_sql::MergeOp::Delete => {
                    let del_sql = format!(
                        "SELECT {} FROM {q_target_rel} INNER JOIN {q_source} ON {on_guarded} WHERE {guard}",
                        old_key_cols()
                    );
                    deleted_rows += self
                        .collect_merge_keys(
                            ctx,
                            session,
                            &del_sql,
                            "MATCHED DELETE",
                            &mut equality_delete_batches,
                        )
                        .await?;
                }
                crate::merge_sql::MergeOp::Insert(..) => {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
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
                        // V3 row lineage: an inserted row has none; typed so the
                        // chained new-row streams share one schema.
                        if col == iceberg::metadata_columns::RESERVED_COL_NAME_ROW_ID
                            || col == iceberg::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER
                        {
                            return format!("CAST(NULL AS BIGINT) AS \"{col}\"");
                        }
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
                     (SELECT 1 FROM {q_target_rel} WHERE {on_guarded}) AND ({guard})",
                    insert_exprs.join(", ")
                );
                new_row_streams.push(
                    self.merge_subquery_stream(
                        ctx,
                        session,
                        &insert_sql,
                        "NOT MATCHED INSERT",
                        Arc::clone(&inserted_rows),
                    )
                    .await?,
                );
            }
        }

        // NOT MATCHED BY SOURCE (target-only): UPDATE / DELETE target rows with
        // no source match. Source columns are absent for this class, so a BY
        // SOURCE clause references target columns only (SQL standard); a source
        // reference would fail to plan here.
        for (i, clause) in classified.by_source.iter().enumerate() {
            let guard = crate::merge_sql::first_match_guard(&classified.by_source, i);
            let anti = format!(
                "FROM {q_target_rel} WHERE NOT EXISTS \
                 (SELECT 1 FROM {q_source} WHERE {on_guarded}) AND ({guard}){by_source_admit}"
            );
            match &clause.op {
                crate::merge_sql::MergeOp::Delete => {
                    let del_sql = format!("SELECT {} {anti}", old_key_cols());
                    deleted_rows += self
                        .collect_merge_keys(
                            ctx,
                            session,
                            &del_sql,
                            "BY SOURCE DELETE",
                            &mut equality_delete_batches,
                        )
                        .await?;
                }
                crate::merge_sql::MergeOp::Update(assignments) => {
                    let new_cols: Vec<String> = target_columns
                        .iter()
                        .map(|col| {
                            // V3 row lineage: the updated row keeps `_row_id`
                            // (passthrough below) but its sequence number is
                            // nulled so it inherits the new file's.
                            if col == iceberg::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER {
                                return format!("CAST(NULL AS BIGINT) AS \"{col}\"");
                            }
                            let expr = crate::merge_sql::resolve_update_expr(
                                col,
                                assignments,
                                &merge_names,
                            );
                            format!("{expr} AS \"{col}\"")
                        })
                        .collect();
                    let new_sql = format!("SELECT {} {anti}", new_cols.join(", "));
                    new_row_streams.push(
                        self.merge_subquery_stream(
                            ctx,
                            session,
                            &new_sql,
                            "BY SOURCE UPDATE new-row",
                            Arc::clone(&updated_rows),
                        )
                        .await?,
                    );
                    let old_sql = format!("SELECT {} {anti}", old_key_cols());
                    self.collect_merge_keys(
                        ctx,
                        session,
                        &old_sql,
                        "BY SOURCE UPDATE old-key",
                        &mut equality_delete_batches,
                    )
                    .await?;
                }
                crate::merge_sql::MergeOp::Insert(..) => {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                        "internal error: INSERT classified as a BY SOURCE MERGE clause".to_string(),
                    ));
                }
            }
        }

        // Scratch MemTables are deregistered by the `MergeScratchCleanup`
        // drop guard declared above. The guard and `target_buf` must outlive
        // the streaming write below: the MemTableExecs feeding the joins hold
        // references into the tracked target batches until the streams drain.

        let tracker = new_upload_tracker();
        let cleanup_guard =
            WriteCleanupGuard::new(table.file_io().clone(), tracker.clone(), "merge-mor");
        let new_data_files = if new_row_streams.is_empty() {
            vec![]
        } else {
            let chained = futures::stream::select_all(new_row_streams);
            let mut stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
                RecordBatchStreamAdapter::new(target_schema.clone(), Box::pin(chained)),
            );
            // WITH CHECK (#729): every inserted or updated row must satisfy
            // the caller's row filter. Row filter only: an unassigned
            // restricted column carries its stored value through an update.
            if target_policy.row_filter_sql.is_some() {
                let row_filter_only = sqe_policy::write_predicates::WritePolicyPredicates {
                    row_filter_sql: target_policy.row_filter_sql.clone(),
                    column_mask_sqls: Default::default(),
                    mask_udfs: Vec::new(),
                };
                stream = checked_stream(ctx, &table_ident, row_filter_only, stream);
            }
            // A zero-row stream closes the writer without producing a file.
            let (files, _rows) = write_data_files_streaming_with_metrics(
                &table,
                stream,
                "merge-mor",
                self.metrics.as_ref(),
                self.compression(session),
                tracker,
                self.fanout_limits(Some(pool.clone())),
            )
            .await?;
            files
        };
        let updated_rows = updated_rows.load(std::sync::atomic::Ordering::Relaxed);
        let inserted_rows = inserted_rows.load(std::sync::atomic::Ordering::Relaxed);

        let total_touched = updated_rows + deleted_rows + inserted_rows;
        if total_touched == 0 {
            info!(table = %table_ident, "MoR MERGE: no matched or not-matched rows");
            return affected_rows_batch(0);
        }

        let delete_files = if !equality_delete_batches.is_empty() {
            write_equality_delete_key_files(
                &table,
                equality_delete_batches.as_slice().to_vec(),
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
        let target_branch =
            crate::commit::txn_target_branch(catalog.as_catalog().as_ref(), &table_ident).await?;
        commit_with_retry(
            catalog.as_catalog().as_ref(),
            &table_ident,
            "mor-merge",
            move |fresh_table| {
                let dfs = data_files_for_retry.clone();
                let dels = delete_files_for_retry.clone();
                let cat = catalog_for_commit.clone();
                let target_branch = target_branch.clone();
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
                    if let Some(branch) = target_branch {
                        action = action.set_target_branch(branch);
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit MoR MERGE: {e}"))
            }
        })?;
        cleanup_guard.mark_committed();
        // Row delta: carry the previous snapshot's Puffin NDV statistics as an
        // upper bound so the table does not lose them on its first DML (512).
        self.carry_puffin_statistics(
            catalog.as_catalog().as_ref(),
            &table_ident,
            crate::puffin_carry::CarryOp::RowDelta,
            None,
        )
        .await;
        self.after_commit(session, catalog.as_ref(), &table_ident)
            .await;

        info!(
            table = %table_ident,
            updated_rows,
            deleted_rows,
            inserted_rows,
            "MoR MERGE committed successfully"
        );
        affected_rows_batch(total_touched as u64)
    }

    // -------------------------------------------------------------------------
    // CoW helper methods
    // -------------------------------------------------------------------------

    /// Commit already-written data files, either as an append (overwrite=false)
    /// or as an atomic overwrite swap (overwrite=true).
    ///
    /// Overwrite semantics:
    /// - Unpartitioned table (or a spec with no fields): full replace. Every
    ///   current data file is removed.
    /// - Partitioned table: dynamic overwrite. Only current data files whose
    ///   partition value appears in `new_data_files` are removed; untouched
    ///   partitions are preserved.
    ///
    /// In both cases the position-delete files covering the removed data files
    /// are dropped in the same commit (no superseded-delete debris). Equality
    /// deletes are left alone: `covered_position_deletes` only targets position
    /// deletes, and stale equality deletes are harmless here because the new
    /// data files land at a higher sequence number and are never matched by an
    /// older equality delete.
    ///
    /// A zero-length `new_data_files` with overwrite=true is a truncate on an
    /// unpartitioned table and a no-op on a partitioned one (no partitions
    /// touched). Callers MUST route zero-row overwrites here rather than taking
    /// an "empty SELECT" early return.
    ///
    /// `catalog` is the `SessionCatalog` the caller resolved: the erased
    /// `&dyn Catalog` bridge is derived from it for the commit, and the
    /// `SessionCatalog` itself is what [`Self::after_commit`] needs to evict
    /// the committed table for every token (507).
    async fn commit_written_files(
        &self,
        session: &Session,
        catalog: &Arc<SessionCatalog>,
        table_ident: &TableIdent,
        new_data_files: Vec<DataFile>,
        overwrite: bool,
    ) -> sqe_core::Result<Option<i64>> {
        let bridge = catalog.as_catalog();
        if !overwrite {
            if new_data_files.is_empty() {
                return Ok(None);
            }
            let bridge_ref = bridge.as_ref();
            let target_branch = crate::commit::txn_target_branch(bridge_ref, table_ident).await?;
            let committed = commit_with_retry(bridge_ref, table_ident, "insert", move |fresh_table| {
                let files = new_data_files.clone();
                let target_branch = target_branch.clone();
                async move {
                    let tx = Transaction::new(&fresh_table);
                    let mut action = tx.fast_append().add_data_files(files);
                    if let Some(branch) = target_branch {
                        action = action.set_target_branch(branch);
                    }
                    let tx = action.apply(tx)?;
                    tx.commit(bridge_ref).await
                }
            })
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to commit INSERT: {e}")))?;
            self.after_commit(session, catalog.as_ref(), table_ident)
                .await;
            // The append's snapshot, for lineage (the OL `version` facet).
            return Ok(committed.metadata().current_snapshot_id());
        }

        // Distinct partitions touched by the new files (dynamic overwrite).
        // `Struct` derives PartialEq/Eq/Hash; partition cardinality per write
        // is small, so a Vec + linear membership check is fine.
        let mut touched_partitions: Vec<iceberg::spec::Struct> = Vec::new();
        for f in &new_data_files {
            let p = f.partition().clone();
            if !touched_partitions.contains(&p) {
                touched_partitions.push(p);
            }
        }

        let borrowed = (&new_data_files, &touched_partitions, table_ident);
        let committed = self
            .commit_rewrite_with_retry(
                bridge.as_ref(),
                table_ident,
                "insert-overwrite",
                "INSERT OVERWRITE",
                move |fresh_table| {
                    let (new_data_files, touched_partitions, table_ident) = borrowed;
                    async move {
                        // Partitioned iff the *current* default spec has fields;
                        // read it from the reloaded snapshot, not from a table
                        // loaded before the retry.
                        let default_spec = fresh_table.metadata().default_partition_spec().clone();
                        let partitioned = !default_spec.fields().is_empty();
                        let all_old = self.collect_data_files(&fresh_table).await?;

                        let removed_data: Vec<DataFile> = if !partitioned {
                            all_old
                        } else {
                            // A live file written under an older partition spec
                            // has a partition struct with a different arity or
                            // field order, so it never equals a new file's
                            // struct and dynamic overwrite used to leave its
                            // rows next to the replacement ones (architecture
                            // review 502). Legacy-spec files are therefore
                            // treated as touched: full-replace semantics for
                            // the spec generations the writer cannot address
                            // partition-by-partition.
                            let legacy: Vec<&DataFile> = all_old
                                .iter()
                                .filter(|df| df.partition_spec_id() != default_spec.spec_id())
                                .collect();
                            if !legacy.is_empty() {
                                warn!(
                                    table = %table_ident,
                                    default_spec_id = default_spec.spec_id(),
                                    legacy_files = legacy.len(),
                                    legacy_spec_ids = ?distinct_spec_ids(
                                        &legacy.into_iter().cloned().collect::<Vec<_>>()
                                    ),
                                    "INSERT OVERWRITE: table holds files from an older \
                                     partition spec; replacing all of them (their partition \
                                     values cannot be compared against the new spec). Run \
                                     rewrite_data_files to normalise the layout."
                                );
                            }
                            all_old
                                .into_iter()
                                .filter(|df| {
                                    df.partition_spec_id() != default_spec.spec_id()
                                        || touched_partitions.contains(df.partition())
                                })
                                .collect()
                        };

                        // Nothing to replace and nothing to add: genuine no-op.
                        if removed_data.is_empty() && new_data_files.is_empty() {
                            info!(table = %table_ident, "INSERT OVERWRITE: no partitions touched and no new rows; no-op");
                            return Ok(None);
                        }

                        // Drop position-delete files whose data files are all being
                        // removed, in the same atomic swap (#376).
                        let removed_data_count = removed_data.len();
                        let removed_spec_ids = distinct_spec_ids(&removed_data);
                        let files_to_remove = self
                            .extend_with_superseded_deletes(&fresh_table, removed_data)
                            .await?;

                        info!(
                            table = %table_ident,
                            partitioned,
                            default_spec_id = default_spec.spec_id(),
                            new_files = new_data_files.len(),
                            removed_data = removed_data_count,
                            spec_ids = ?removed_spec_ids,
                            "INSERT OVERWRITE: committing atomic swap"
                        );

                        Ok(Some(RewriteAttempt {
                            add: new_data_files.clone(),
                            remove: files_to_remove,
                            cleanup: None,
                            value: (),
                        }))
                    }
                },
            )
            .await?;

        if committed.is_some() {
            info!(table = %table_ident, "INSERT OVERWRITE committed successfully");
            self.after_commit(session, catalog.as_ref(), table_ident)
                .await;
        }
        // The rewrite helper does not hand back the committed table; lineage
        // records no snapshot for an INSERT OVERWRITE.
        Ok(None)
    }

    /// Extend an about-to-be-removed set of data files with the position-delete
    /// files those data files fully supersede, so a copy-on-write rewrite drops
    /// dangling delete-file debris in the same commit (#376). This is the same
    /// cleanup the compaction path performs (`maintenance.rs`).
    ///
    /// Only position deletes whose referenced data file is entirely within
    /// `removed_data` are added. Equality deletes (which apply by value +
    /// sequence number, not by data-file path) and position deletes that still
    /// cover a surviving data file are left untouched: removing either could
    /// resurrect rows in files this rewrite keeps.
    async fn extend_with_superseded_deletes(
        &self,
        table: &IcebergTable,
        removed_data: Vec<DataFile>,
    ) -> sqe_core::Result<Vec<DataFile>> {
        use std::collections::HashSet;
        let removed_paths: HashSet<String> = removed_data
            .iter()
            .map(|d| d.file_path().to_string())
            .collect();
        let live_deletes = sqe_compaction::collect_live_delete_files(table).await?;
        let covered = sqe_compaction::covered_position_deletes(&removed_paths, &live_deletes);
        Ok(removed_data.into_iter().chain(covered).collect())
    }

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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to load manifest list: {e}")))?;

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
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to load manifest: {e}")))?;

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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to open file '{file_path}': {e}")))?;

        let input_file = input
            .read()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to read file '{file_path}': {e}")))?;

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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "Failed to create Parquet reader for '{file_path}': {e}"
                ))
            })?;

        let reader = reader.build().map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "Failed to build Parquet reader for '{file_path}': {e}"
            ))
        })?;

        let batches: Vec<RecordBatch> = if let Some(p) = pool.as_ref() {
            // Reserve each decoded batch as it is produced so an oversized file
            // is denied mid-decode rather than fully materialised first.
            let mut buf = TrackedBatchBuffer::new(p, "cow-decode-buffer");
            for item in reader {
                let batch = item.map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to read Parquet file '{file_path}': {e}"))
                })?;
                buf.push(batch)?;
            }
            buf.into_inner()
        } else {
            reader
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to read Parquet file '{file_path}': {e}"))
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
        // V3: also selects the row-lineage columns (see rewrite_scan_builder).
        let scan = sqe_compaction::rewrite::rewrite_scan_builder(table).build().map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build rewrite read scan: {e}"))
        })?;
        let tasks: Vec<iceberg::scan::FileScanTask> = scan
            .plan_files()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to plan rewrite read: {e}")))?
            .try_collect()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to plan rewrite read: {e}")))?;
        let mut tasks_by_path: std::collections::HashMap<String, Vec<iceberg::scan::FileScanTask>> =
            std::collections::HashMap::new();
        for task in tasks {
            tasks_by_path
                .entry(task.data_file_path().to_string())
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to read data file '{file_path}': {e}"))
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to read data file '{file_path}': {e}"))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            // V3: plain longs for the row-lineage columns (no-op otherwise).
            let batch = sqe_compaction::writer::decode_lineage_columns(batch)?;
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
        session: &Session,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<RecordBatch> {
        use arrow::compute::not;
        use datafusion::arrow::array::BooleanArray;

        // Register the batch as a scratch table so DataFusion can evaluate the
        // predicate; the guard deregisters it on every exit path (509).
        let scratch = ScratchTable::register(ctx, batch)?;
        let orig_name = table_ident.name();

        refuse_internal_policy_aliases("WHERE", [where_sql])?;
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let augmented_where =
            augment_where_with_row_filter(where_sql, predicates, orig_name, &nonce);
        let policy_from = build_policy_from_clause(
            ctx,
            scratch.qualified(),
            orig_name,
            &batch.schema(),
            predicates,
            &nonce,
            false,
        )
        .await?;
        let from_clause = &policy_from.sql;

        // Execute: SELECT <where_clause> AS __match FROM __sqe_dml_<uuid>
        // Alias the scratch table to the original target name (see apply_update
        // for rationale) so correlated subqueries inside the WHERE clause can
        // reference `tablename.col`.
        // TRUE-only: a NULL verdict neither deletes the row nor reports it.
        let eval_sql = format!(
            "SELECT COALESCE(CAST(({augmented_where}) AS BOOLEAN), FALSE) AS __match \
             FROM {from_clause}{joins_sql}"
        );
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &eval_sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to evaluate WHERE clause: {e}")))?;
        let df = self.enforce_dml_eval(ctx, session, df).await?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to collect WHERE evaluation: {e}")))?;
        drop(scratch);

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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, "WHERE evaluation did not produce a boolean column")
            })?;
        let negated = not(match_col)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to negate WHERE mask: {e}")))?;

        // Apply the mask to the original batch
        filter_record_batch(batch, &negated)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to filter batch: {e}")))
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
        session: &Session,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<arrow_array::BooleanArray> {
        use arrow_array::BooleanArray;

        // Scratch table with a per-call unique name, deregistered on drop (509).
        let scratch = ScratchTable::register(ctx, batch)?;
        let orig_name = table_ident.name();

        refuse_internal_policy_aliases("WHERE", [where_sql])?;
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let augmented_where =
            augment_where_with_row_filter(where_sql, predicates, orig_name, &nonce);
        let policy_from = build_policy_from_clause(
            ctx,
            scratch.qualified(),
            orig_name,
            &batch.schema(),
            predicates,
            &nonce,
            false,
        )
        .await?;
        let from_clause = &policy_from.sql;

        // Alias the scratch table to the original target name so correlated
        // subqueries inside WHERE can reference `tablename.col`.
        // TRUE-only: a NULL verdict neither deletes the row nor reports it.
        let eval_sql = format!(
            "SELECT COALESCE(CAST(({augmented_where}) AS BOOLEAN), FALSE) AS __match \
             FROM {from_clause}{joins_sql}"
        );
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &eval_sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to evaluate WHERE clause: {e}")))?;
        let df = self.enforce_dml_eval(ctx, session, df).await?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to collect WHERE evaluation: {e}")))?;
        drop(scratch);

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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, "WHERE evaluation did not produce a boolean column")
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
        session: &Session,
        batch: &RecordBatch,
        assignments: &[sqlparser::ast::Assignment],
        where_sql: &str,
        in_subquery_joins: &str,
        table_ident: &TableIdent,
        predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
    ) -> sqe_core::Result<RecordBatch> {
        // Scratch table with a per-call unique name, deregistered on drop (509).
        let scratch = ScratchTable::register(ctx, batch)?;
        let orig_name = table_ident.name();

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

        // The raw and admit aliases are internal: user SQL naming them is
        // refused outright (the per-call nonce below already makes them
        // unguessable).
        refuse_internal_policy_aliases(
            "UPDATE",
            assignment_map.values().map(String::as_str).chain([where_sql]),
        )?;
        let raw_nonce = uuid::Uuid::new_v4().simple().to_string();

        let augmented_where =
            augment_where_with_row_filter(where_sql, predicates, orig_name, &raw_nonce);

        // Build SELECT with CASE expressions for assigned columns. A column
        // the caller reads through a mask or the row-filter guard needs
        // special handling: the ELSE branch must reference the raw alias so
        // unchanged rows (hidden ones included) keep their stored values on
        // disk. The policy subquery exposes a nonce-suffixed raw alias for
        // every such column next to the masked / guarded projection.
        let row_filtered = predicates.row_filter_sql.is_some();
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let col = f.name().clone();
                let via_policy = predicates.column_mask_sqls.contains_key(&col) || row_filtered;
                let else_ref = if via_policy {
                    format!("\"{}\"", policy_raw_alias(&col, &raw_nonce))
                } else {
                    format!("\"{col}\"")
                };
                if let Some(expr) = assignment_map.get(&col) {
                    format!(
                        "CASE WHEN ({augmented_where}) THEN ({expr}) ELSE {else_ref} END AS \"{col}\""
                    )
                } else if col == iceberg::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER {
                    // V3 row lineage: an updated row keeps `_row_id` but its
                    // `_last_updated_sequence_number` is nulled so it inherits
                    // the new file's sequence number (spec, Row Lineage).
                    format!(
                        "CASE WHEN ({augmented_where}) THEN CAST(NULL AS BIGINT) ELSE {else_ref} END AS \"{col}\""
                    )
                } else if via_policy {
                    format!("{else_ref} AS \"{col}\"")
                } else {
                    format!("\"{col}\"")
                }
            })
            .collect();
        // WITH CHECK (as for INSERT, #729): flag the rows this UPDATE changes
        // so their new values can be held to the caller's row filter below.
        let mut columns = columns;
        if predicates.row_filter_sql.is_some() {
            columns.push(format!(
                "CAST(({augmented_where}) AS BOOLEAN) AS \"{UPDATE_CHANGED_COLUMN}\""
            ));
        }

        // Alias the scratch table back to the UPDATE target's original name so
        // correlated subqueries inside the SET expression can reference it.
        // e.g. `SET x = (SELECT ... WHERE ... = holding_summary.hs_ca_id)` needs
        // `holding_summary` to be in scope; without the alias DataFusion only
        // sees `__sqe_dml_<uuid>` and fails to resolve the correlation.
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
        let policy_from = build_policy_from_clause(
            ctx,
            scratch.qualified(),
            orig_name,
            &batch.schema(),
            predicates,
            &raw_nonce,
            true,
        )
        .await?;
        let from_clause = &policy_from.sql;
        let select_sql = format!(
            "SELECT {cols} FROM {from_clause}{joins}",
            cols = columns.join(", "),
            joins = joins_sql,
        );
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &select_sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to evaluate UPDATE: {e}")))?;
        let df = self.enforce_dml_eval(ctx, session, df).await?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to collect UPDATE results: {e}")))?;
        drop(scratch);

        // Return the first (and only) result batch
        let out = result_batches
            .into_iter()
            .next()
            .ok_or_else(|| SqeError::with_code(SqeErrorCode::ExecutionFailed, "UPDATE produced no output batches".to_string()))?;
        let Some(changed_idx) = out.schema().index_of(UPDATE_CHANGED_COLUMN).ok() else {
            return Ok(out);
        };
        let changed = out
            .column(changed_idx)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| SqeError::with_code(SqeErrorCode::ExecutionFailed, "UPDATE change flag is not boolean"))?
            .clone();
        let mut out = out;
        out.remove_column(changed_idx);
        let updated = filter_record_batch(&out, &changed)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to filter updated rows: {e}")))?;
        // Row filter only: an unassigned restricted column legitimately
        // carries its stored value through an UPDATE.
        let row_filter_only = sqe_policy::write_predicates::WritePolicyPredicates {
            row_filter_sql: predicates.row_filter_sql.clone(),
            column_mask_sqls: Default::default(),
            mask_udfs: Vec::new(),
        };
        check_batch_within_policy(ctx, &updated, table_ident, &row_filter_only).await?;
        Ok(out)
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
        session: &Session,
        where_sql: &str,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<(String, String, InSubqueryCleanup)> {
        // #731: the keyset materialiser is a SELECT the user wrote; run it
        // through the same enforcer as INSERT/MERGE sources so a row filter
        // on the subquery's table cannot be probed via DELETE ... WHERE IN.
        let enforce = self
            .policy_enforcer
            .as_deref()
            .map(|enf| (enf, &session.user));
        lift_in_subqueries_enforced(where_sql, ctx, enforce).await
    }

    /// Refuse a statement that replaces `table`'s whole contents (INSERT
    /// OVERWRITE, CREATE OR REPLACE ... AS SELECT) when a row filter governs
    /// the caller on it. Follows the PostgreSQL RLS model: a whole-table
    /// replace is a bulk delete, and a caller may only delete rows they can
    /// see. Column masks and restrictions hide values, not rows, so they do
    /// not block a replace (dbt's `table` materialization relies on this).
    async fn refuse_replace_under_policy(
        &self,
        session: &Session,
        qualifier: Option<String>,
        table_ident: &TableIdent,
        table: &IcebergTable,
        what: &str,
    ) -> sqe_core::Result<()> {
        if self.policy_enforcer.is_none() {
            return Ok(());
        }
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                |e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to derive target Arrow schema: {e}")),
            )?,
        );
        let predicates = self
            .compute_write_predicates(session, qualifier, table_ident, schema, &mut None)
            .await?;
        if predicates.row_filter_sql.is_none() {
            return Ok(());
        }
        Err(SqeError::with_code(
            SqeErrorCode::AccessDenied,
            format!(
                "write to {table_ident} violates row-level security policy: \
                 {what} would remove rows hidden from this user"
            ),
        ))
    }

    /// PostgreSQL RLS `WITH CHECK` for appends (#729): a caller whose SELECT
    /// on `table` is row-filtered may not insert rows that filter would hide,
    /// nor non-NULL values into a column restricted (nullified) for them.
    /// Wraps `stream` so the first offending batch fails the statement before
    /// anything is committed. No enforcer or no target policy: pass-through.
    async fn with_check_guard(
        &self,
        session: &Session,
        ctx: &DFSessionContext,
        qualifier: Option<String>,
        table_ident: &TableIdent,
        table: &IcebergTable,
        stream: datafusion::execution::SendableRecordBatchStream,
    ) -> sqe_core::Result<datafusion::execution::SendableRecordBatchStream> {
        if self.policy_enforcer.is_none() {
            return Ok(stream);
        }
        let target_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
                |e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to derive target Arrow schema: {e}")),
            )?,
        );
        let predicates = self
            .compute_write_predicates(session, qualifier, table_ident, target_schema, &mut None)
            .await?;
        if predicates.is_empty() {
            return Ok(stream);
        }
        Ok(checked_stream(ctx, table_ident, predicates, stream))
    }
}

/// Wrap `stream` so every batch passes [`check_batch_within_policy`].
fn checked_stream(
    ctx: &DFSessionContext,
    table_ident: &TableIdent,
    predicates: sqe_policy::write_predicates::WritePolicyPredicates,
    stream: datafusion::execution::SendableRecordBatchStream,
) -> datafusion::execution::SendableRecordBatchStream {
    let shared = Arc::new((ctx.clone(), table_ident.clone(), predicates));
    let schema = stream.schema();
    let checked = stream.and_then(move |batch| {
        let shared = shared.clone();
        async move {
            let (ctx, ident, predicates) = &*shared;
            check_batch_within_policy(ctx, &batch, ident, predicates)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(batch)
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, checked))
}

/// Refuse `batch` if any row falls outside the caller's row filter on the
/// target, or sets a restricted (nullified) column to a non-NULL value.
/// Row-filter evaluation reuses the DML scratch-table evaluator; the filter
/// runs over the RAW incoming values (masks do not gate writes, as in
/// PostgreSQL).
pub(crate) async fn check_batch_within_policy(
    ctx: &DFSessionContext,
    batch: &RecordBatch,
    table_ident: &TableIdent,
    predicates: &sqe_policy::write_predicates::WritePolicyPredicates,
) -> sqe_core::Result<()> {
    use arrow_array::Array as _;
    use datafusion::arrow::array::Int64Array;

    let denied = |what: String| {
        SqeError::with_code(
            SqeErrorCode::AccessDenied,
            format!("write to {table_ident} violates row-level security policy: {what}"),
        )
    };

    for (col, mask_sql) in &predicates.column_mask_sqls {
        let upper = mask_sql.trim().to_ascii_uppercase();
        let is_nullify = upper == "NULL" || upper.starts_with("CAST(NULL ");
        if !is_nullify {
            continue;
        }
        if let Some(array) = batch.column_by_name(col) {
            if array.null_count() < array.len() {
                return Err(denied(format!("column `{col}` is restricted for this user")));
            }
        }
    }

    let Some(rf) = &predicates.row_filter_sql else {
        return Ok(());
    };
    let scratch = ScratchTable::register(ctx, batch)?;
    let eval_sql = format!(
        "SELECT CAST(COUNT(*) AS BIGINT) FROM {scratch} AS \"{name}\" \
         WHERE NOT COALESCE(CAST(({rf}) AS BOOLEAN), FALSE)",
        scratch = scratch.qualified(),
        name = table_ident.name(),
    );
    let batches = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &eval_sql)
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("WITH CHECK evaluation failed: {e}")))?
        .collect()
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("WITH CHECK evaluation failed: {e}")))?;
    drop(scratch);
    let outside = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .filter(|a| a.len() == 1)
        .map(|a| a.value(0))
        .unwrap_or(0);
    if outside > 0 {
        return Err(denied(format!("{outside} row(s) fall outside the caller's row filter")));
    }
    Ok(())
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
    lift_in_subqueries_enforced(where_sql, ctx, None).await
}

/// [`lift_in_subqueries`] with the keyset SELECT run through `enforce`
/// (enforcer + user) before it is materialised (#731).
async fn lift_in_subqueries_enforced(
    where_sql: &str,
    ctx: &DFSessionContext,
    enforce: Option<(&dyn sqe_policy::PolicyEnforcer, &sqe_core::SessionUser)>,
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("IN-subquery lift parse error: {e}")))?;

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
        let pre_df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &preflight_sql).await.map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
        let mut mat_df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, &materialiser_sql).await.map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("IN-subquery materialiser plan failed: {e}"))
        })?;
        // #731: the subquery is user SQL over policy-governed tables; apply
        // row filters / masks / restrictions exactly as a SELECT would.
        if let Some((enf, user)) = enforce {
            let (enforced, _summary) = enf
                .evaluate(
                    user,
                    &sqe_policy::TableScope::of(ctx),
                    mat_df.logical_plan().clone(),
                )
                .await?;
            mat_df = ctx.execute_logical_plan(enforced).await.map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("IN-subquery materialiser policy plan failed: {e}"))
            })?;
        }
        let schema: Arc<ArrowSchema> = Arc::new(mat_df.schema().as_arrow().clone());
        let batches = mat_df.collect().await.map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("IN-subquery materialiser execution failed: {e}"))
        })?;

        let mem = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "Failed to build MemTable for IN-subquery keyset: {e}"
            ))
        })?;

        // Register the scratch MemTable under the built-in `datafusion.public`
        // catalog/schema rather than the session's default. In production the
        // DML handler hands us a `SessionContext` whose default catalog is the
        // Iceberg catalog, whose `SchemaProvider` rejects `register_table` with
        // "schema provider does not support registering tables". The other
        // scratch-table call sites in this file (`ScratchTable::register`, used
        // by `filter_batch_negate`, `filter_batch_match`, `apply_update`) already
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                let discovered = match &self.catalog_discovery {
                    Some(discovery) => {
                        discovery
                            .discover_session_catalog(
                                warehouse,
                                &self.config,
                                session,
                                self.table_cache.as_ref(),
                            )
                            .await
                    }
                    None => None,
                };
                discovered.ok_or_else(|| {
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
/// scratch-table cleanup behaviour of [`ScratchTable`] and
/// [`MergeScratchCleanup`] in this file.
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

/// Prefix of every per-batch DML scratch MemTable (architecture review 509).
const DML_SCRATCH_PREFIX: &str = "__sqe_dml_";

/// RAII guard for one per-batch DML evaluator scratch table.
///
/// `filter_batch_negate`, `filter_batch_match` and `apply_update` register the
/// batch under evaluation as a `MemTable` in the shared session context and
/// run a generated SELECT against it. They used to register fixed names
/// (`__delete_<table>`, `__update_<table>`, ...) and deregister only on the
/// success path: a planning or collect error left the whole batch pinned in
/// the per-user context until the next DML on the same table overwrote it,
/// and two concurrent statements on one table raced on the same name.
///
/// `register` names the table `__sqe_dml_<uuid>` so concurrent evaluations
/// never collide, and the guard deregisters on drop (reusing
/// [`MergeScratchCleanup`]) so every `?` between register and result releases
/// the batch.
struct ScratchTable {
    qualified: String,
    _cleanup: MergeScratchCleanup,
}

impl ScratchTable {
    /// Register `batch` as a single-partition MemTable under a fresh
    /// `datafusion.public.__sqe_dml_<uuid>` name.
    fn register(ctx: &DFSessionContext, batch: &RecordBatch) -> sqe_core::Result<Self> {
        let qualified = format!(
            "datafusion.public.{DML_SCRATCH_PREFIX}{}",
            Uuid::now_v7().simple()
        );
        let mem_table =
            MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to create MemTable: {e}"),
                )
            })?;
        // Build the guard before registering so a registration error leaves
        // nothing behind (deregistering an absent name is a logged no-op).
        let cleanup = MergeScratchCleanup::new(ctx, vec![qualified.clone()]);
        ctx.register_table(qualified.as_str(), Arc::new(mem_table))
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to register temp table: {e}")))?;
        Ok(Self {
            qualified,
            _cleanup: cleanup,
        })
    }

    /// Fully-qualified `datafusion.public.__sqe_dml_<uuid>` reference.
    fn qualified(&self) -> &str {
        &self.qualified
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
pub fn sql_type_to_arrow(
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
/// all files when the WHERE is absent or no identity-partition equality can be
/// lifted. Returns `(to_rewrite, pruned_count)`.
///
/// Each file is evaluated against *its own* partition spec
/// (`DataFile::partition_spec_id`), not the table's default one (architecture
/// review 502). After `ALTER TABLE ... ADD/DROP PARTITION FIELD` a table holds
/// files from several spec generations whose partition structs differ in arity
/// and field order; indexing an old-spec struct with the default spec's field
/// index lands on a different column, and a literal that "provably differs"
/// there pruned a file that did hold matching rows — a
/// `DELETE ... WHERE region = 'EU'` left EU rows alive in pre-evolution files.
/// A file whose spec id is absent from the metadata (should not happen, but a
/// manifest is data we do not control) is kept, as is one written under a spec
/// with no fields.
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
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let total = files.len();
    let to_rewrite: Vec<DataFile> = files
        .into_iter()
        .filter(|df| {
            let Some(spec) = metadata.partition_spec_by_id(df.partition_spec_id()) else {
                return true;
            };
            if spec.fields().is_empty() {
                return true;
            }
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build Iceberg schema: {e}")))
}

/// Narrow nanosecond timestamps to microseconds when the target format
/// version cannot store them: Iceberg `timestamp_ns`/`timestamptz_ns` exist
/// only from v3 (#773). V3 targets are returned unchanged.
// ponytail: top-level columns only; recurse into struct/list/map if a nested
// ns timestamp ever reaches a v2 CTAS.
pub(crate) fn timestamps_for_format_version(
    arrow_schema: &ArrowSchema,
    format_version: FormatVersion,
) -> ArrowSchema {
    use arrow_schema::{DataType, TimeUnit};
    if format_version == FormatVersion::V3 {
        return arrow_schema.clone();
    }
    let fields: Vec<arrow_schema::Field> = arrow_schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => f
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            _ => f.as_ref().clone(),
        })
        .collect();
    ArrowSchema::new_with_metadata(fields, arrow_schema.metadata().clone())
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

pub fn arrow_schema_to_iceberg_with_defaults(
    arrow_schema: &ArrowSchema,
    column_defs: &[sqlparser::ast::ColumnDef],
) -> sqe_core::Result<IcebergSchema> {
    use sqe_sql::{extract_default_literal, DefaultLiteral};

    if arrow_schema.fields().len() != column_defs.len() {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build Iceberg schema: {e}")))?;

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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "DEFAULT literal for column '{}' is not compatible with type {:?}",
                        col_def.name.value, iceberg_type
                    )));
                }
                Err(msg) => {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build Iceberg schema: {e}")))
}

/// Convert a SQL-surface default literal into an Iceberg `Literal`.
///
/// Returns `Ok(None)` if the combination of SQL literal and target Iceberg
/// type is not representable but is not an error (the caller decides whether
/// that is a hard error or a silent NULL). Returns `Err` when the literal
/// is structurally compatible but out of range for the target type, so the
/// user sees the failure instead of a silently wrapped value. Issue #77.
pub fn default_to_iceberg_literal(
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
pub fn requires_v3_features(
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
    for (next_field_id, partition_expr) in (1000_i32..).zip(exprs) {
        let (source_name, target_name, transform) = parse_partition_transform(partition_expr)?;
        let source_id = iceberg_schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == source_name)
            .map(|f| f.id)
            .ok_or_else(|| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "PARTITIONED BY references unknown column '{source_name}'"
                ))
            })?;
        fields.push(UnboundPartitionField {
            source_id,
            field_id: Some(next_field_id),
            name: target_name,
            transform,
        });
    }

    let spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields(fields)
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Invalid PARTITIONED BY clause: {e}")))?
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
pub fn parse_partition_transform_sql(
    transform_sql: &str,
) -> sqe_core::Result<(String, String, iceberg::spec::Transform)> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(transform_sql)
        .map_err(|e| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "Failed to lex partition transform '{transform_sql}': {e}"
            ))
        })?;
    let expr = parser.parse_expr().map_err(|e| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                SqeError::with_code(SqeErrorCode::ExecutionFailed, "PARTITIONED BY: empty compound identifier".to_string())
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
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, "PARTITIONED BY: function call without a name".to_string())
                })?;
            let args = match &func.args {
                FunctionArguments::List(list) => &list.args,
                _ => {
                    return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                                "PARTITIONED BY {fn_name}(): empty compound identifier"
                            ))
                        })
                    }
                    other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "PARTITIONED BY {fn_name}(): expected column name, got {other}"
                    ))),
                }
            };
            let int_arg = |arg: &Expr| -> sqe_core::Result<u32> {
                match arg {
                    Expr::Value(v) => match &v.value {
                        Value::Number(n, _) => n.parse::<u32>().map_err(|e| {
                            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                                "PARTITIONED BY {fn_name}(): integer parameter '{n}': {e}"
                            ))
                        }),
                        other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                            "PARTITIONED BY {fn_name}(): expected integer, got {other}"
                        ))),
                    },
                    other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "PARTITIONED BY {fn_name}(): expected integer literal, got {other}"
                    ))),
                }
            };

            match fn_name.as_str() {
                "year" | "years" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY year(): expected exactly one column",
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_year"), Transform::Year))
                }
                "month" | "months" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY month(): expected exactly one column",
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_month"), Transform::Month))
                }
                "day" | "days" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY day(): expected exactly one column",
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_day"), Transform::Day))
                }
                "hour" | "hours" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY hour(): expected exactly one column",
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_hour"), Transform::Hour))
                }
                "bucket" => {
                    if bare_args.len() != 2 {
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY bucket(N, col): expected exactly two arguments",
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
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY truncate(L, col): expected exactly two arguments",
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
                        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                            "PARTITIONED BY void(col): expected exactly one column",
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_null"), Transform::Void))
                }
                other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "PARTITIONED BY: unsupported transform '{other}'. \
                     Supported: identity (bare column), year, month, day, hour, \
                     bucket(N, col), truncate(L, col), void."
                ))),
            }
        }
        other => Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "PARTITIONED BY: unsupported expression form: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, TimeUnit};
    use sqlparser::ast::{DataType as SqlType, ExactNumberInfo, TimezoneInfo};

    /// #412: a write source with an unbounded sort is refused at plan time
    /// when the sort grant cannot hold one merge buffer per input partition,
    /// with the same typed error a sorted SELECT gets; the same plan with a
    /// small fan-in is admitted and streams.
    #[tokio::test]
    async fn sorted_write_source_is_gated_by_merge_headroom() {
        use arrow_array::Int64Array;
        use datafusion::prelude::SessionConfig;

        let mut config = write_test_config();
        // 8 MiB is the grant floor; a quarter of it is merge headroom (2 MiB),
        // which holds 32 merge buffers of 64 KiB and not 64.
        config.query.max_query_memory = "8MB".to_string();
        let handler = WriteHandler::new(config);

        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "a",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3, 1, 2])) as _],
        )
        .unwrap();
        let source = |target_partitions: usize| {
            let ctx = DFSessionContext::new_with_config(
                SessionConfig::new().with_target_partitions(target_partitions),
            );
            ctx.register_table(
                "t",
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]]).unwrap()),
            )
            .unwrap();
            ctx
        };

        let refused = handler
            .execute_source_stream(
                source(64).sql("SELECT a FROM t ORDER BY a").await.unwrap(),
                "test",
            )
            .await
            .err()
            .expect("64-way merge must not fit a 2 MiB headroom");
        assert_eq!(
            refused.error_code(),
            SqeErrorCode::ResourceExhausted,
            "{refused}"
        );
        assert!(
            refused.to_string().contains("sort merge admission failed"),
            "{refused}"
        );
        assert!(
            refused
                .to_string()
                .contains("test with sort-on-write (ORDER BY)")
                && refused.to_string().contains("Write without the sort hint"),
            "refusal must name the statement shape and remedy: {refused}"
        );

        let stream = handler
            .execute_source_stream(
                source(2).sql("SELECT a FROM t ORDER BY a").await.unwrap(),
                "test",
            )
            .await
            .expect("2-way merge fits");
        let rows: usize = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
    }

    /// 905: partitioned writes are bounded by default and charge the pool.
    #[test]
    fn fanout_limits_default_to_bounded_with_pool() {
        use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
        let handler = WriteHandler::new(write_test_config());
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());

        let limits = handler.fanout_limits(Some(pool));
        assert!(limits.is_bounded(), "both knobs at 0 must auto-derive caps");
        assert!(
            limits.max_open >= 8 && limits.max_open <= 64,
            "{}",
            limits.max_open
        );
        assert!(limits.byte_budget > 0);
        assert!(
            limits.pool.is_some(),
            "pool is forwarded for shared accounting"
        );

        let mut config = write_test_config();
        config.query.fanout_max_open_writers = 3;
        let limits = WriteHandler::new(config).fanout_limits(None);
        assert_eq!(limits.max_open, 3, "explicit cap wins over auto");
        assert!(limits.byte_budget > 0, "budget still auto-derived");
        assert!(limits.pool.is_none());

        let mut config = write_test_config();
        config.query.write_buffer_tracking = false;
        assert!(
            !WriteHandler::new(config).fanout_limits(None).is_bounded(),
            "write_buffer_tracking = false is the escape hatch"
        );
    }

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

    /// Lineage labels a write with the catalog it resolves against, not the
    /// configured default: explicit qualifier, then the session catalog.
    #[test]
    fn lineage_target_parts_follow_the_write_catalog() {
        use sqlparser::ast::Ident;
        let config = write_test_config();
        let mut session = test_session(Some("sales"));
        let bare = ObjectName::from(vec![Ident::new("t")]);
        let ident = resolve_table_ident(&bare, &session).unwrap();
        assert_eq!(
            lineage_target_parts(&config, &session, &bare, &ident),
            (
                config.resolve_default_catalog(),
                "sales".to_string(),
                "t".to_string()
            )
        );

        session.default_catalog = Some("workspace1".to_string());
        assert_eq!(
            lineage_target_parts(&config, &session, &bare, &ident).0,
            "workspace1"
        );

        let full = ObjectName::from(vec![Ident::new("ws2"), Ident::new("s"), Ident::new("t")]);
        let ident = resolve_table_ident(&full, &session).unwrap();
        assert_eq!(
            lineage_target_parts(&config, &session, &full, &ident),
            ("ws2".to_string(), "s".to_string(), "t".to_string())
        );
    }

    /// An INSERT's lineage wrapper carries the target table's columns, so the
    /// output fields are the table's (`country, seg, n`), not the SELECT's
    /// (`country, segment, Int64(1)`).
    #[tokio::test]
    async fn insert_lineage_names_fields_after_the_target_table() {
        let ctx = DFSessionContext::new();
        let customers = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("country", DataType::Utf8, false),
            Field::new("segment", DataType::Utf8, false),
        ]));
        ctx.register_table(
            "customers",
            Arc::new(MemTable::try_new(customers, vec![vec![]]).unwrap()),
        )
        .unwrap();
        let source = ctx
            .sql("SELECT country, segment, 1 FROM customers")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let target = arrow_schema::Schema::new(vec![
            Field::new("country", DataType::Utf8, true),
            Field::new("seg", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]);
        let wrapper =
            build_lineage_insert_plan(source, Some(target), "workspace1", "default", "lin_probe")
                .unwrap();
        let lookup: sqe_lineage::extract::CatalogLookup =
            Arc::new(|n: &str| sqe_lineage::extract::CatalogInfo::new(n, None));
        let (_, outputs) = sqe_lineage::extract::extract_lineage(&wrapper, &lookup);
        let fields: Vec<&str> = outputs[0]
            .facets
            .schema
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(fields, ["country", "seg", "n"]);
        let lineage = outputs[0].facets.columnLineage.as_ref().unwrap();
        assert_eq!(
            lineage.fields["seg"].inputFields[0].field, "segment",
            "seg is fed by customers.segment"
        );
        assert!(lineage.fields["n"].inputFields.is_empty());

        // After the commit the write records its snapshot on the wrapper;
        // the extractor reports it as the OL `version` facet and nothing else
        // about the output changes.
        let mut plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(wrapper)));
        record_lineage_snapshot(&mut plan_out, Some(4205761418235361718));
        let Some(sqe_lineage::PlanOrHint::Plan(recorded)) = plan_out else {
            panic!("wrapper kept")
        };
        let (_, outputs) = sqe_lineage::extract::extract_lineage(&recorded, &lookup);
        assert_eq!(
            outputs[0].facets.version.as_ref().unwrap().datasetVersion,
            "4205761418235361718"
        );
        assert_eq!(outputs[0].name, "default.lin_probe");
        assert_eq!(outputs[0].facets.schema.as_ref().unwrap().fields.len(), 3);

        // No snapshot (empty write, INSERT OVERWRITE): nothing recorded.
        let mut untouched = Some(sqe_lineage::PlanOrHint::Plan(recorded));
        record_lineage_snapshot(&mut untouched, None);
        assert!(matches!(untouched, Some(sqe_lineage::PlanOrHint::Plan(_))));
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
    fn test_timestamps_for_format_version_narrows_ns_to_us_on_v2_only() {
        // #773: `current_timestamp` is ns in DataFusion; v2 has no timestamp_ns.
        use arrow_array::TimestampNanosecondArray;
        use arrow_schema::Field;
        use iceberg::spec::{PrimitiveType, Type};

        let ns = ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "utcts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("id", DataType::Int64, false),
        ]);

        let v2 = timestamps_for_format_version(&ns, FormatVersion::V2);
        assert_eq!(
            v2.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            v2.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(v2.field(2).data_type(), &DataType::Int64);
        let iceberg = arrow_schema_to_iceberg(&v2).unwrap();
        let fields = iceberg.as_struct().fields();
        assert!(matches!(*fields[0].field_type, Type::Primitive(PrimitiveType::Timestamp)));
        assert!(matches!(*fields[1].field_type, Type::Primitive(PrimitiveType::Timestamptz)));

        // The sink casts ns batches onto the (µs) table schema.
        let stamped = sqe_compaction::writer::build_stamped_schema(&iceberg).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ns.clone()),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789i64])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789i64])
                        .with_timezone("UTC"),
                ),
                Arc::new(arrow_array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let cast = sqe_compaction::writer::apply_stamped_schema(batch, &stamped).unwrap();
        let us = cast
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(us.value(0), 1_700_000_000_123_456);

        // V3 keeps nanoseconds.
        let v3 = timestamps_for_format_version(&ns, FormatVersion::V3);
        assert_eq!(v3.field(0).data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, None));
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
            _ => Err(sqe_core::SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
    // 505: INSERT arity is checked at planning time, before the writer opens a
    // parquet part. A wider SELECT used to panic inside `apply_stamped_schema`
    // (indexing past the end of the stamped schema) in the write task.
    // -------------------------------------------------------------------------

    fn arity_table_ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("default".to_string()), "t".to_string())
    }

    #[test]
    fn check_insert_arity_rejects_wider_select() {
        let schema = make_target_schema(); // a, b, c
        let source = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let err = check_insert_arity(&source, &schema, &arity_table_ident()).unwrap_err();
        assert_eq!(
            format!("{err}"),
            "INSERT has 4 columns but target has 3",
            "the arity message must name both widths"
        );
    }

    #[test]
    fn check_insert_arity_rejects_narrower_select() {
        let schema = make_target_schema();
        let source = vec!["a".to_string(), "b".to_string()];
        let err = check_insert_arity(&source, &schema, &arity_table_ident()).unwrap_err();
        assert!(
            format!("{err}").contains("INSERT has 2 columns but target has 3"),
            "err: {err}"
        );
    }

    #[test]
    fn check_insert_arity_accepts_matching_and_reordered_names() {
        let schema = make_target_schema();
        // Exact match.
        check_insert_arity(
            &["a".to_string(), "b".to_string(), "c".to_string()],
            &schema,
            &arity_table_ident(),
        )
        .unwrap();
        // Positional semantics are SQL-standard: a name swap is legal (and
        // only logged), never an error.
        check_insert_arity(
            &["c".to_string(), "b".to_string(), "a".to_string()],
            &schema,
            &arity_table_ident(),
        )
        .unwrap();
        // Case differences are not a swap.
        check_insert_arity(
            &["A".to_string(), "B".to_string(), "C".to_string()],
            &schema,
            &arity_table_ident(),
        )
        .unwrap();
    }

    // -------------------------------------------------------------------------
    // 514: a MoR position DELETE must not count rows a live delete file
    // already hides, or the second run of an idempotent DELETE reports the
    // same non-zero count as the first.
    // -------------------------------------------------------------------------

    #[test]
    fn newly_deleted_row_count_ignores_already_hidden_rows() {
        // No delete files on the data file: every raw match is live.
        assert_eq!(newly_deleted_row_count(3, None), 3);
        assert_eq!(newly_deleted_row_count(0, None), 0);
        // Re-running the same DELETE: the rows still match the raw scan but
        // none of them is visible any more.
        assert_eq!(newly_deleted_row_count(3, Some(0)), 0);
        // Partially overlapping: only the rows still visible are deleted.
        assert_eq!(newly_deleted_row_count(3, Some(2)), 2);
        // Snapshot skew between the two reads can never inflate the count
        // past the positions actually marked.
        assert_eq!(newly_deleted_row_count(3, Some(9)), 3);
    }

    #[test]
    fn data_file_record_count_sums_manifest_counts() {
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
        let file = |rows: u64| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("s3://b/{rows}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(1)
                .record_count(rows)
                .partition(Struct::from_iter(std::iter::empty()))
                .partition_spec_id(0)
                .build()
                .unwrap()
        };
        assert_eq!(data_file_record_count(&[]), 0);
        assert_eq!(data_file_record_count(&[file(10), file(5), file(1)]), 16);
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
        let (after, summary) = handler.enforce_source_plan(&session, &sqe_policy::TableScope::default(), plan).await.unwrap();
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
        let (rewritten, summary) = handler.enforce_source_plan(&session, &sqe_policy::TableScope::default(), plan).await.unwrap();
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
        employees_handler_with_keyed_policy(row_filter, masks, None).await
    }

    /// [`employees_handler_with_policy`] with a HASH-mask key.
    async fn employees_handler_with_keyed_policy(
        row_filter: Option<datafusion::logical_expr::Expr>,
        masks: Vec<(&str, sqe_policy::MaskType)>,
        mask_key: Option<Arc<Vec<u8>>>,
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
            Arc::new(PolicyPlanRewriter::new(Arc::new(store)).with_mask_key(mask_key));

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
        // Shaped like the coordinator's context (session_context.rs): the
        // default schema is the session's, else `default`, and
        // `datafusion.public` exists for DML scratch tables. With DataFusion's
        // own default (`public`) a bare `employees` resolved to
        // `public.employees`, a different table from the governed
        // `default.employees`.
        let ctx = DFSessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new()
                .with_default_catalog_and_schema("datafusion", "default"),
        );
        ctx.catalog("datafusion")
            .expect("default catalog")
            .register_schema(
                "public",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .expect("register public");
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
            .compute_write_predicates(session, None, table_ident, schema, &mut policy_summary)
            .await
            .unwrap()
    }

    /// DELETE, UPDATE and MERGE resolve the target's policy under the
    /// target's catalog: the statement's qualifier, else the session catalog.
    /// A catalog-scoped store holds the row filter under `ws_a.default`, so
    /// the same `default.employees` in `ws_b` is not filtered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_target_policy_is_keyed_on_the_target_catalog() {
        use datafusion::logical_expr::{col, lit};
        use sqe_policy::plan_rewriter::PolicyPlanRewriter;
        use sqe_policy::policy_store::InMemoryPolicyStore;
        use sqe_policy::{PolicyStore, ResolvedPolicy};
        use sqlparser::ast::TableFactor;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        struct Scoped(InMemoryPolicyStore);
        #[async_trait::async_trait]
        impl PolicyStore for Scoped {
            fn catalog_scoped(&self) -> bool {
                true
            }
            async fn resolve(
                &self,
                user: &sqe_core::SessionUser,
                table: &str,
                namespace: &str,
            ) -> sqe_core::Result<ResolvedPolicy> {
                self.0.resolve(user, table, namespace).await
            }
        }
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.row_filters.push(col("region").eq(lit("EU")));
        store.add_table_policy("ws_a.default", "employees", pol).await;
        let enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
            Arc::new(PolicyPlanRewriter::new(Arc::new(Scoped(store))));
        let handler = WriteHandler::new(write_test_config()).with_policy_enforcer(enforcer);
        let mut session = sqe_core::Session::new(
            "alice".to_string(),
            "tok".to_string().into(),
            None,
            chrono::Utc::now(),
            vec![],
        );
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("region", arrow_schema::DataType::Utf8, true),
        ]));

        let target = |sql: &str| -> ObjectName {
            let stmt = Parser::parse_sql(&GenericDialect {}, sql)
                .unwrap()
                .remove(0);
            match &stmt {
                Statement::Delete(_) => delete_target_object_name(&stmt).unwrap().clone(),
                Statement::Update(_) => update_target_object_name(&stmt).unwrap().clone(),
                Statement::Merge(m) => match &m.table {
                    TableFactor::Table { name, .. } => name.clone(),
                    other => panic!("merge target: {other}"),
                },
                other => panic!("unexpected statement: {other}"),
            }
        };
        let filtered = |session: &sqe_core::Session, name: ObjectName| {
            let (handler, schema) = (&handler, schema.clone());
            let session = session.clone();
            async move {
                let ident = resolve_table_ident(&name, &session).unwrap();
                handler
                    .compute_write_predicates(
                        &session,
                        catalog_qualifier(&name),
                        &ident,
                        schema,
                        &mut None,
                    )
                    .await
                    .unwrap()
                    .row_filter_sql
                    .is_some()
            }
        };

        for (catalog, expect) in [("ws_a", true), ("ws_b", false)] {
            for sql in [
                format!("DELETE FROM {catalog}.default.employees WHERE id = 1"),
                format!("UPDATE {catalog}.default.employees SET region = 'US'"),
                format!(
                    "MERGE INTO {catalog}.default.employees t USING src s ON t.id = s.id \
                     WHEN MATCHED THEN DELETE"
                ),
            ] {
                assert_eq!(filtered(&session, target(&sql)).await, expect, "{sql}");
            }
        }

        // An unqualified target takes the session catalog.
        session.default_catalog = Some("ws_a".to_string());
        assert!(filtered(&session, target("DELETE FROM default.employees WHERE id = 1")).await);
        session.default_catalog = Some("ws_b".to_string());
        assert!(!filtered(&session, target("DELETE FROM default.employees WHERE id = 1")).await);
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
            .filter_batch_negate(&ctx, &session, &batch, "TRUE", "", &table_ident, &predicates)
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

    fn int64_column(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<i64>> {
        let col = batch.column_by_name(name).expect("column");
        let col = arrow::compute::cast(col, &arrow_schema::DataType::Int64).unwrap();
        col.as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    /// A fallible WHERE / SET never runs on a row the caller's row filter
    /// hides: the evaluators read every column through the row-filter guard,
    /// so a hidden row is all NULL to the user's SQL. `1 / (id - 2)` divides
    /// by zero only on id 2, a hidden US row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_caller_predicate_cannot_error_on_hidden_rows() {
        use datafusion::logical_expr::{col, lit};
        use sqe_policy::MaskType;
        let fallible = "1 / (id - 2) = 1";
        let batch = make_employee_batch();

        // Unguarded, the predicate fails on id 2.
        let (handler, session, table_ident, ctx) = employees_handler_with_policy(None, vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let err = handler
            .filter_batch_match(&ctx, &session, &batch, fallible, "", &table_ident, &predicates)
            .await
            .expect_err("unguarded, the predicate divides by zero");
        assert!(err.to_string().contains("Divide by zero"), "{err}");

        let (handler, session, table_ident, ctx) = employees_handler_with_policy(
            Some(col("region").eq(lit("EU"))),
            vec![("ssn", MaskType::Redact("***".to_string()))],
        )
        .await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        let survivors = handler
            .filter_batch_negate(&ctx, &session, &batch, fallible, "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(int64_column(&survivors, "id"), vec![Some(1), Some(2), Some(4)]);

        let matched = handler
            .filter_batch_match(&ctx, &session, &batch, fallible, "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(
            matched.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(false), Some(true), Some(false)]
        );

        // UPDATE: only id 3 changes; every hidden column, the masked one
        // included, is written back as stored.
        let stmt = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            "UPDATE employees SET ssn = CAST(1 / (id - 2) AS VARCHAR)",
        )
        .unwrap()
        .pop()
        .unwrap();
        let sqlparser::ast::Statement::Update(update) = stmt else {
            panic!("expected UPDATE");
        };
        let out = handler
            .apply_update(&ctx, &session, &batch, &update.assignments, fallible, "", &table_ident, &predicates)
            .await
            .unwrap();
        let expected = arrow_array::RecordBatch::try_new(
            batch.schema(),
            vec![
                batch.column(0).clone(),
                Arc::new(arrow_array::StringArray::from(vec!["111-11", "222-22", "1", "444-44"])),
                batch.column(2).clone(),
            ],
        )
        .unwrap();
        let out = arrow::compute::cast(out.column(1), &arrow_schema::DataType::Utf8)
            .map(|ssn| {
                arrow_array::RecordBatch::try_new(
                    batch.schema(),
                    vec![out.column(0).clone(), ssn, out.column(2).clone()],
                )
                .unwrap()
            })
            .unwrap();
        assert_eq!(out, expected);
    }

    /// A row whose row-filter verdict is NULL is hidden: DELETE keeps it, and
    /// a boolean column reads NULL through the guard on it, so a predicate
    /// that fails unless the hidden value is NULL does not fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_null_row_filter_verdict_hides_the_row() {
        use arrow_array::{BooleanArray, Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema2};
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(ArrowSchema2::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("ssn", DataType::Utf8, true),
                Field::new("region", DataType::Utf8, true),
                Field::new("active", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(StringArray::from(vec!["111-11", "222-22", "333-33"])),
                Arc::new(StringArray::from(vec![Some("EU"), Some("US"), None])),
                Arc::new(BooleanArray::from(vec![true, false, false])),
            ],
        )
        .unwrap();

        let survivors = handler
            .filter_batch_negate(&ctx, &session, &batch, "TRUE", "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(int64_column(&survivors, "id"), vec![Some(2), Some(3)]);

        let probe = "1 / (CASE WHEN active IS NULL OR id = 1 THEN 1 ELSE 0 END) = 1";
        let survivors = handler
            .filter_batch_negate(&ctx, &session, &batch, probe, "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(int64_column(&survivors, "id"), vec![Some(2), Some(3)]);

        // Without a policy, a NULL WHERE verdict does not delete the row either.
        let (handler, session, table_ident, ctx) = employees_handler_with_policy(None, vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let survivors = handler
            .filter_batch_negate(&ctx, &session, &batch, "region = 'EU'", "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(int64_column(&survivors, "id"), vec![Some(2), Some(3)]);
    }

    /// The admit and raw aliases are internal to the policy relation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_where_cannot_reference_internal_policy_aliases() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let err = handler
            .filter_batch_match(&ctx, &session, &make_employee_batch(), "\"__pol_admit_x\"", "", &table_ident, &predicates)
            .await
            .expect_err("internal alias refused");
        assert!(err.to_string().contains("__pol_"), "{err}");
    }

    // #729: appends honour the target row filter / restricted columns
    // (PostgreSQL RLS WITH CHECK) instead of only the source SELECT.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn with_check_refuses_rows_outside_row_filter_and_restricted_values() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) = employees_handler_with_policy(
            Some(col("region").eq(lit("EU"))),
            vec![("ssn", sqe_policy::MaskType::Nullify)],
        )
        .await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        // Mixed EU/US batch with plaintext ssn: refused (restricted column set).
        let err = check_batch_within_policy(&ctx, &make_employee_batch(), &table_ident, &predicates)
            .await
            .expect_err("non-NULL restricted column must be refused");
        assert!(err.to_string().contains("restricted"), "{err}");

        // NULL ssn, but a US row: refused by the row filter.
        let batch = make_employee_batch();
        let ssn_null = Arc::new(arrow_array::StringArray::from(vec![None::<&str>; 4]));
        let mixed = arrow_array::RecordBatch::try_new(
            batch.schema(),
            vec![batch.column(0).clone(), ssn_null.clone(), batch.column(2).clone()],
        )
        .unwrap();
        let err = check_batch_within_policy(&ctx, &mixed, &table_ident, &predicates)
            .await
            .expect_err("rows outside the row filter must be refused");
        assert!(err.to_string().contains("2 row(s) fall outside"), "{err}");

        // All EU, NULL ssn: admitted.
        let eu_only = arrow::compute::filter_record_batch(
            &mixed,
            &arrow_array::BooleanArray::from(vec![true, false, true, false]),
        )
        .unwrap();
        check_batch_within_policy(&ctx, &eu_only, &table_ident, &predicates)
            .await
            .expect("visible rows are admitted");
    }

    // Non-IN subqueries in a DML WHERE read other tables under the caller's
    // policy, not raw: a scalar subquery over a row-filtered table sees only
    // the caller's rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_where_scalar_subquery_applies_row_filter() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, _employees, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        ctx.register_batch("employees", make_employee_batch()).unwrap();
        let mine = TableIdent::new(NamespaceIdent::new("default".to_string()), "mine".to_string());
        let no_policy = sqe_policy::write_predicates::WritePolicyPredicates::default();

        let mask = handler
            .filter_batch_match(
                &ctx,
                &session,
                &make_employee_batch(),
                "(SELECT COUNT(*) FROM employees WHERE region = 'US') > 0",
                "",
                &mine,
                &no_policy,
            )
            .await
            .unwrap();
        assert_eq!(mask.true_count(), 0, "US rows are hidden from the caller");
    }

    // A correlated SET subquery (decorrelated into a join) reads the other
    // table masked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_set_scalar_subquery_applies_column_mask() {
        use arrow_array::Array as _;
        use sqe_policy::MaskType;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let (handler, session, _employees, ctx) =
            employees_handler_with_policy(None, vec![("ssn", MaskType::Redact("***".to_string()))])
                .await;
        ctx.register_batch("employees", make_employee_batch()).unwrap();
        let mine = TableIdent::new(NamespaceIdent::new("default".to_string()), "mine".to_string());
        let no_policy = sqe_policy::write_predicates::WritePolicyPredicates::default();

        let sql = "UPDATE mine SET region = (SELECT e.ssn FROM employees e WHERE e.id = mine.id)";
        let assignments = match Parser::parse_sql(&GenericDialect {}, sql).unwrap().pop().unwrap() {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        };
        let out = handler
            .apply_update(
                &ctx,
                &session,
                &make_employee_batch(),
                &assignments,
                "TRUE",
                "",
                &mine,
                &no_policy,
            )
            .await
            .unwrap();
        let region = out
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .clone();
        for i in 0..region.len() {
            assert_eq!(region.value(i), "***", "raw ssn copied at index {i}");
        }
    }

    /// Register MERGE-shaped scratch relations (`__sqe_merge_*`, as the CoW
    /// and MoR paths name them) over the employee rows, for #804.
    fn register_merge_scratch(ctx: &DFSessionContext) {
        for name in ["__sqe_merge_target_x", "__sqe_merge_source_x"] {
            let provider = datafusion::datasource::MemTable::try_new(
                make_employee_batch().schema(),
                vec![vec![make_employee_batch()]],
            )
            .unwrap();
            ctx.register_table(format!("datafusion.public.{name}"), Arc::new(provider)).unwrap();
        }
    }

    // #804: a correlated SET subquery in a MERGE reads the other table
    // masked, while the scratch target and source are read as registered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_set_subquery_applies_column_mask() {
        use arrow_array::Array as _;
        use sqe_policy::MaskType;
        let (handler, session, _employees, ctx) =
            employees_handler_with_policy(None, vec![("ssn", MaskType::Redact("***".to_string()))])
                .await;
        ctx.register_batch("employees", make_employee_batch()).unwrap();
        register_merge_scratch(&ctx);

        let sql = "SELECT t.ssn AS kept, (SELECT MAX(e.ssn) FROM employees e WHERE e.id = t.id) AS copied \
                   FROM datafusion.public.__sqe_merge_target_x t \
                   JOIN datafusion.public.__sqe_merge_source_x s ON t.id = s.id ORDER BY t.id";
        let batches = handler
            .plan_merge_sql(&ctx, &session, sql, "MERGE query")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let strings = |name: &str| -> Vec<String> {
            let col = arrow::compute::cast(batch.column_by_name(name).unwrap(), &arrow_schema::DataType::Utf8)
                .unwrap();
            let col = col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
            (0..col.len()).map(|i| col.value(i).to_string()).collect()
        };
        assert_eq!(strings("copied"), vec!["***"; 4], "raw ssn copied from the other table");
        assert_eq!(strings("kept"), vec!["111-11", "222-22", "333-33", "444-44"], "scratch re-masked");
    }

    // #804: an ON / WHEN subquery reads the other table row-filtered, in the
    // merge query and in the count queries (cardinality, WITH CHECK, empty
    // source) alike, so neither the merged rows nor a count reveal hidden rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_on_subquery_applies_row_filter() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, _employees, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        ctx.register_batch("employees", make_employee_batch()).unwrap();
        register_merge_scratch(&ctx);

        let from = "FROM datafusion.public.__sqe_merge_target_x t \
                    JOIN datafusion.public.__sqe_merge_source_x s ON t.id = s.id \
                    AND EXISTS (SELECT 1 FROM employees e WHERE e.id = t.id AND e.region = 'US')";
        let count = |sql: String| {
            let handler = &handler;
            let (ctx, session) = (&ctx, &session);
            async move {
                scalar_count(handler.plan_merge_sql(ctx, session, &sql, "MERGE count").await.unwrap(), "MERGE count")
                    .await
                    .unwrap()
            }
        };
        assert_eq!(count(format!("SELECT COUNT(*) {from}")).await, 0, "US rows are hidden from the caller");
        // The scratch relations themselves are not filtered: 4 matched pairs.
        assert_eq!(
            count(
                "SELECT COUNT(*) FROM datafusion.public.__sqe_merge_target_x t \
                 JOIN datafusion.public.__sqe_merge_source_x s ON t.id = s.id"
                    .to_string()
            )
            .await,
            4
        );

        // The cardinality check runs the same governed planning: a duplicate
        // match that only exists through hidden rows is not a violation.
        let pair = "SELECT COUNT(*) FROM datafusion.public.__sqe_merge_target_x t \
                    JOIN datafusion.public.__sqe_merge_source_x s \
                    ON t.id = s.id OR EXISTS (SELECT 1 FROM employees e WHERE e.region = 'US')";
        let matched = "SELECT COUNT(*) FROM datafusion.public.__sqe_merge_target_x";
        handler
            .check_merge_cardinality(&ctx, &session, pair, matched)
            .await
            .expect("hidden rows cannot create a cardinality violation");
    }

    // #731: the IN-subquery keyset SELECT goes through the enforcer, so a
    // DELETE/UPDATE ... WHERE id IN (SELECT ...) only sees the caller's rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_subquery_lift_applies_row_filter_to_keyset() {
        use datafusion::logical_expr::{col, lit};
        let (handler, session, _table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        ctx.register_batch("employees", make_employee_batch()).unwrap();

        let (where_sql, joins_sql, _guard) = handler
            .lift_in_subqueries(&session, "id IN (SELECT id FROM employees)", &ctx)
            .await
            .unwrap();
        // The outer scan is deliberately unenforced: only the keyset decides.
        let rows: usize = ctx
            .sql(&format!("SELECT id FROM employees{joins_sql} WHERE {where_sql}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2, "keyset must contain only the EU ids (1, 3)");
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
            .filter_batch_match(&ctx, &session, &batch, "TRUE", "", &table_ident, &predicates)
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
                &session,
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
            .filter_batch_match(&ctx, &session, &batch, "ssn = '***'", "", &table_ident, &predicates)
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
                &session,
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

    // A HASH mask calls the internal keyed function, which the user's session
    // never registers. UPDATE and DELETE still plan (the masked relation comes
    // from the separate mask context), read the keyed hash, keep the stored
    // value of an unassigned masked column, and leave the function out of
    // reach of user SQL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dml_under_keyed_hash_mask_plans_without_exposing_mask_udf() {
        use arrow_array::StringArray;
        use sqe_policy::sha256_udf::{sha256_udf, user_sha256_udf, MASK_HASH_UDF_NAME};
        use sqe_policy::MaskType;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let key = Arc::new(b"mask-key".to_vec());
        let (handler, session, table_ident, ctx) = employees_handler_with_keyed_policy(
            None,
            vec![("ssn", MaskType::Hash)],
            Some(key.clone()),
        )
        .await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        let hash_of = |udf: datafusion::logical_expr::ScalarUDF, v: &str| {
            let v = v.to_string();
            async move {
                let c = DFSessionContext::new();
                c.register_udf(udf.clone());
                let b = c
                    .sql(&format!("SELECT {}('{v}')", udf.name()))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                let a =
                    arrow::compute::cast(b[0].column(0), &arrow_schema::DataType::Utf8).unwrap();
                a.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_string()
            }
        };
        let keyed = hash_of(sha256_udf(Some(key.clone())), "111-11").await;
        let unkeyed = hash_of(user_sha256_udf(None), "111-11").await;
        assert_ne!(keyed, unkeyed, "the mask must be the keyed hash");

        let assignments = |sql: &str| match Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap()
        {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        };
        let batch = make_employee_batch();
        let out = handler
            .apply_update(
                &ctx,
                &session,
                &batch,
                &assignments("UPDATE employees SET region = ssn WHERE id = 1"),
                "id = 1",
                "",
                &table_ident,
                &predicates,
            )
            .await
            .expect("UPDATE under a HASH mask must plan");
        let column = |name: &str| {
            out.column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (ssn, region) = (column("ssn"), column("region"));
        assert_eq!(region.value(0), keyed, "SET reads the keyed mask");
        assert_eq!(
            (0..4).map(|i| ssn.value(i)).collect::<Vec<_>>(),
            vec!["111-11", "222-22", "333-33", "444-44"],
            "the unassigned masked column keeps its stored values"
        );
        assert_eq!(
            (1..4).map(|i| region.value(i)).collect::<Vec<_>>(),
            vec!["US", "EU", "US"],
            "rows outside WHERE are unchanged"
        );

        // DELETE's WHERE reads the same keyed mask.
        let matched = handler
            .filter_batch_match(
                &ctx,
                &session,
                &batch,
                &format!("ssn = '{keyed}'"),
                "",
                &table_ident,
                &predicates,
            )
            .await
            .expect("DELETE under a HASH mask must plan");
        assert_eq!(matched.true_count(), 1);

        // The internal function stays out of the user's session.
        assert!(
            !ctx.state()
                .scalar_functions()
                .contains_key(MASK_HASH_UDF_NAME),
            "mask UDF leaked into the session"
        );
        assert!(
            ctx.sql(&format!("SELECT {MASK_HASH_UDF_NAME}('111-11')"))
                .await
                .is_err(),
            "user SQL must not resolve the keyed mask function"
        );
        let res = handler
            .apply_update(
                &ctx,
                &session,
                &batch,
                &assignments(&format!(
                    "UPDATE employees SET region = {MASK_HASH_UDF_NAME}(region)"
                )),
                "TRUE",
                "",
                &table_ident,
                &predicates,
            )
            .await;
        assert!(
            res.is_err(),
            "user SET must not resolve the keyed mask function"
        );
    }

    // WITH CHECK: an updated row must still satisfy the caller's row filter,
    // so it cannot be moved out of their view.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_refuses_rows_moved_outside_row_filter() {
        use datafusion::logical_expr::{col, lit};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let update = |sql: &str| match Parser::parse_sql(&GenericDialect {}, sql).unwrap().pop().unwrap() {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        };
        let batch = make_employee_batch();

        let err = handler
            .apply_update(&ctx, &session, &batch, &update("UPDATE employees SET region = 'US'"), "TRUE", "", &table_ident, &predicates)
            .await
            .expect_err("moving EU rows to US must be refused");
        assert!(err.to_string().contains("row-level security"), "{err}");

        // Staying inside the filter is fine, and hidden US rows are untouched.
        handler
            .apply_update(&ctx, &session, &batch, &update("UPDATE employees SET ssn = 'X'"), "TRUE", "", &table_ident, &predicates)
            .await
            .expect("in-filter update is allowed");
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
                &session,
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

    // The raw-value alias for a masked column is internal: a SET or WHERE
    // naming it must not read the unmasked value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_cannot_reference_internal_raw_alias() {
        use arrow_array::Array as _;
        use sqe_policy::MaskType;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(None, vec![("ssn", MaskType::Redact("***".to_string()))])
                .await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        let sql = "UPDATE employees SET region = \"__pol_raw_ssn\"";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();
        let assignments = match stmt {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        };
        let batch = make_employee_batch();
        let result = handler
            .apply_update(&ctx, &session, &batch, &assignments, "TRUE", "", &table_ident, &predicates)
            .await;
        if let Ok(rewritten) = result {
            let region = rewritten
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .clone();
            for i in 0..region.len() {
                assert!(
                    !region.value(i).contains('-'),
                    "raw ssn leaked into region at index {i}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_count_with_row_filter_matches_admitted_rows() {
        // 509: the CoW UPDATE derives its affected-row count from the match
        // mask's true_count() instead of a second COUNT(*) plan per batch.
        use datafusion::logical_expr::{col, lit};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(Some(col("region").eq(lit("EU"))), vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let batch = make_employee_batch();
        let mask = handler
            .filter_batch_match(&ctx, &session, &batch, "TRUE", "", &table_ident, &predicates)
            .await
            .unwrap();
        assert_eq!(
            mask.true_count(),
            2,
            "row filter must shrink the affected count"
        );
    }

    // -------------------------------------------------------------------------
    // 509: per-batch evaluator scratch tables
    // -------------------------------------------------------------------------

    /// Names of every table registered in the `datafusion.public` schema of
    /// `ctx` that looks like a DML evaluator scratch table (new or legacy).
    fn dml_scratch_tables(ctx: &DFSessionContext) -> Vec<String> {
        ctx.catalog("datafusion")
            .expect("datafusion catalog")
            .schema("public")
            .expect("public schema")
            .table_names()
            .into_iter()
            .filter(|n| {
                n.starts_with(DML_SCRATCH_PREFIX)
                    || n.starts_with("__delete_")
                    || n.starts_with("__mor_delete_")
                    || n.starts_with("__update_")
                    || n.starts_with("__count_")
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_predicate_leaves_no_scratch_tables_registered() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(None, vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;
        let batch = make_employee_batch();

        // A WHERE that references a column the batch does not have fails at
        // planning, after the scratch table was registered.
        let err = handler
            .filter_batch_match(
                &ctx,
                &session,
                &batch,
                "no_such_column = 1",
                "",
                &table_ident,
                &predicates,
            )
            .await
            .expect_err("unknown column must fail to plan");
        assert!(err.to_string().contains("no_such_column"), "{err}");
        assert!(
            dml_scratch_tables(&ctx).is_empty(),
            "filter_batch_match leaked: {:?}",
            dml_scratch_tables(&ctx)
        );

        handler
            .filter_batch_negate(
                &ctx,
                &session,
                &batch,
                "no_such_column = 1",
                "",
                &table_ident,
                &predicates,
            )
            .await
            .expect_err("unknown column must fail to plan");
        assert!(
            dml_scratch_tables(&ctx).is_empty(),
            "filter_batch_negate leaked: {:?}",
            dml_scratch_tables(&ctx)
        );

        let stmt = Parser::parse_sql(
            &GenericDialect {},
            "UPDATE employees SET ssn = no_such_column WHERE id = 1",
        )
        .unwrap()
        .pop()
        .unwrap();
        let assignments = match stmt {
            sqlparser::ast::Statement::Update(update) => update.assignments,
            _ => panic!("expected UPDATE"),
        };
        handler
            .apply_update(
                &ctx,
                &session,
                &batch,
                &assignments,
                "id = 1",
                "",
                &table_ident,
                &predicates,
            )
            .await
            .expect_err("unknown column must fail to plan");
        assert!(
            dml_scratch_tables(&ctx).is_empty(),
            "apply_update leaked: {:?}",
            dml_scratch_tables(&ctx)
        );

        // And the success path leaves nothing behind either.
        handler
            .filter_batch_match(&ctx, &session, &batch, "id > 2", "", &table_ident, &predicates)
            .await
            .unwrap();
        assert!(dml_scratch_tables(&ctx).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn interleaved_evaluations_on_same_table_are_independent() {
        use arrow_array::{Int64Array, StringArray};
        let (handler, session, table_ident, ctx) =
            employees_handler_with_policy(None, vec![]).await;
        let predicates = compute_predicates_for_employees(&handler, &session, &table_ident).await;

        // Two different batches of the same table evaluated concurrently in
        // one session context. With the old fixed `__mor_delete_employees`
        // name the second registration replaced the first and one evaluation
        // saw the other's rows.
        let batch_a = make_employee_batch(); // ids 1..4 -> "id > 2" matches 2
        let batch_b = arrow_array::RecordBatch::try_new(
            batch_a.schema(),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40, 50])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec!["EU", "EU", "EU", "EU", "EU"])),
            ],
        )
        .unwrap(); // "id > 2" matches all 5

        let mut joined = Vec::new();
        for _ in 0..8 {
            let a = handler.filter_batch_match(
                &ctx,
                &session,
                &batch_a,
                "id > 2",
                "",
                &table_ident,
                &predicates,
            );
            let b = handler.filter_batch_match(
                &ctx,
                &session,
                &batch_b,
                "id > 2",
                "",
                &table_ident,
                &predicates,
            );
            let (ra, rb) = futures::join!(a, b);
            joined.push((ra.unwrap(), rb.unwrap()));
        }
        for (ma, mb) in joined {
            assert_eq!(ma.len(), 4, "mask A must be batch A's length");
            assert_eq!(ma.true_count(), 2, "batch A: ids 3 and 4 match");
            assert_eq!(mb.len(), 5, "mask B must be batch B's length");
            assert_eq!(mb.true_count(), 5, "batch B: every id matches");
        }
        assert!(dml_scratch_tables(&ctx).is_empty());
    }

    // -------------------------------------------------------------------------
    // 504: MoR key accumulation is pool-visible
    // -------------------------------------------------------------------------

    /// A MoR DELETE whose matched keys exceed the pool fails with a typed
    /// `ResourceExhausted` naming the consumer, not an abort. Exercised at the
    /// accumulator level: the handler pushes each projected key batch through
    /// exactly this buffer, and a full MoR DELETE needs a live catalog + table
    /// (integration rig), which the unit suite does not have.
    #[test]
    fn mor_key_buffer_denies_over_pool_naming_consumer() {
        use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(4 * 1024));
        let mut keys = TrackedBatchBuffer::gated(&pool, "mor-delete-key-buffer", true);
        let batch = make_employee_batch();
        let key_only = batch.project(&[0]).unwrap();
        let mut denied = None;
        for _ in 0..1024 {
            if let Err(e) = keys.push(key_only.clone()) {
                denied = Some(e);
                break;
            }
        }
        let err = denied.expect("4 KiB pool must deny the key accumulation");
        assert_eq!(err.error_code(), SqeErrorCode::ResourceExhausted);
        assert!(
            err.to_string().contains("mor-delete-key-buffer"),
            "error must name the consumer: {err}"
        );
        // Pool accounting is intact after the denial.
        assert_eq!(pool.reserved(), keys.reserved_bytes());
        drop(keys);
        assert_eq!(pool.reserved(), 0, "reservation released on drop");
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

    // -------------------------------------------------------------------------
    // 506: the shared rewrite-commit retry helper
    // 502: partition-spec-aware CoW pruning
    // 507: the post-commit invalidation hook
    // -------------------------------------------------------------------------

    /// Local `Table` with fs FileIO rooted at `dir` and the metadata in `json`
    /// (the `{location}` placeholder is filled in). No snapshots: the rewrite
    /// commit under test only ADDS files, and `check_file_existence` is off by
    /// default in `RewriteFilesAction`, so nothing has to exist on storage.
    fn local_table(dir: &tempfile::TempDir, json: &str) -> IcebergTable {
        let json = json.replace("{location}", dir.path().to_str().unwrap());
        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(&json).expect("test table metadata");
        let file_io = iceberg::io::FileIO::new_with_fs();
        IcebergTable::builder()
            .runtime(iceberg::Runtime::current())
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "tbl"]).unwrap())
            .file_io(file_io)
            .build()
            .expect("test table")
    }

    const UNPARTITIONED_META: &str = r#"{
      "format-version": 2,
      "table-uuid": "0a1b2c3d-4e5f-4a7b-8c9d-0e1f2a3b4c5d",
      "location": "{location}",
      "last-sequence-number": 1,
      "last-updated-ms": 1602638573590,
      "last-column-id": 1,
      "current-schema-id": 0,
      "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [
          {"id": 1, "name": "id", "required": true, "type": "int"}
        ]}
      ],
      "default-spec-id": 0,
      "partition-specs": [{"spec-id": 0, "fields": []}],
      "last-partition-id": 999,
      "default-sort-order-id": 0,
      "sort-orders": [{"order-id": 0, "fields": []}],
      "properties": {}
    }"#;

    /// A table that has been through `ALTER TABLE ... REPLACE PARTITION FIELD`:
    /// spec 0 (the generation the older files were written under) partitions by
    /// `country`, spec 1 — the default — by `region`. Both partition values are
    /// strings at index 0, which is exactly the shape the pre-502 pruning
    /// misread: it indexed an old-spec file's struct with the DEFAULT spec's
    /// field index and compared the file's `country` value against a `region`
    /// constraint.
    const EVOLVED_SPEC_META: &str = r#"{
      "format-version": 2,
      "table-uuid": "1a1b2c3d-4e5f-4a7b-8c9d-0e1f2a3b4c5d",
      "location": "{location}",
      "last-sequence-number": 3,
      "last-updated-ms": 1602638573590,
      "last-column-id": 3,
      "current-schema-id": 0,
      "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [
          {"id": 1, "name": "id", "required": true, "type": "int"},
          {"id": 2, "name": "region", "required": false, "type": "string"},
          {"id": 3, "name": "country", "required": false, "type": "string"}
        ]}
      ],
      "default-spec-id": 1,
      "partition-specs": [
        {"spec-id": 0, "fields": [
          {"source-id": 3, "field-id": 1000, "name": "country", "transform": "identity"}
        ]},
        {"spec-id": 1, "fields": [
          {"source-id": 2, "field-id": 1001, "name": "region", "transform": "identity"}
        ]}
      ],
      "last-partition-id": 1001,
      "default-sort-order-id": 0,
      "sort-orders": [{"order-id": 0, "fields": []}],
      "properties": {}
    }"#;

    /// Fabricated `DataFile` carrying an explicit spec id and partition struct
    /// (same shape as `crates/sqe-compaction/src/rewrite.rs`'s `data_file_part`).
    fn test_data_file(path: &str, spec_id: i32, partition: iceberg::spec::Struct) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(128)
            .record_count(1)
            .partition(partition)
            .partition_spec_id(spec_id)
            .build()
            .expect("build data file")
    }

    /// `iceberg::Catalog` whose `update_table` loses the first `fail_first`
    /// optimistic-concurrency races. The conflict is reported the way a real
    /// catalog reports it in the *message* but WITHOUT `retryable`: the
    /// vendored `Transaction::commit` has its own inner retry for retryable
    /// errors, which would swallow the conflict before the helper under test
    /// ever saw it.
    ///
    /// `unauthorized_load`, when set, is the 1-based `load_table` call that
    /// fails with the 401 `SessionCatalogBridge` hands back (sqe#803).
    #[derive(Debug)]
    struct FlakyCatalog {
        table: IcebergTable,
        fail_first: usize,
        commits: std::sync::Mutex<usize>,
        unauthorized_load: Option<usize>,
        loads: std::sync::Mutex<usize>,
    }

    impl FlakyCatalog {
        fn new(table: IcebergTable, fail_first: usize) -> Self {
            Self {
                table,
                fail_first,
                commits: std::sync::Mutex::new(0),
                unauthorized_load: None,
                loads: std::sync::Mutex::new(0),
            }
        }

        fn commit_count(&self) -> usize {
            *self.commits.lock().expect("commit counter")
        }
    }

    #[async_trait::async_trait]
    impl Catalog for FlakyCatalog {
        async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<IcebergTable> {
            let mut n = self.loads.lock().expect("load counter");
            *n += 1;
            if self.unauthorized_load == Some(*n) {
                // The shape `SessionCatalogBridge::load_table` produces for a
                // catalog 401.
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "Catalog HTTP 401 (other): Failed to load table: Unexpected, context: \
                     { status: 401 Unauthorized } => Received response with unexpected status code",
                )
                .with_context("status", "401"));
            }
            Ok(self.table.clone())
        }

        async fn update_table(
            &self,
            _commit: iceberg::TableCommit,
        ) -> iceberg::Result<IcebergTable> {
            let mut n = self.commits.lock().expect("commit counter");
            *n += 1;
            if *n <= self.fail_first {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "Commit conflicted for table: ns.tbl",
                ))
            } else {
                Ok(self.table.clone())
            }
        }

        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            unimplemented!("not used by the retry helper")
        }
        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: std::collections::HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            unimplemented!("not used by the retry helper")
        }
        async fn get_namespace(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            unimplemented!("not used by the retry helper")
        }
        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            unimplemented!("not used by the retry helper")
        }
        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: std::collections::HashMap<String, String>,
        ) -> iceberg::Result<()> {
            unimplemented!("not used by the retry helper")
        }
        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            unimplemented!("not used by the retry helper")
        }
        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            unimplemented!("not used by the retry helper")
        }
        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<IcebergTable> {
            unimplemented!("not used by the retry helper")
        }
        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            unimplemented!("not used by the retry helper")
        }
        async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            unimplemented!("not used by the retry helper")
        }
        async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
            unimplemented!("not used by the retry helper")
        }
        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            unimplemented!("not used by the retry helper")
        }
        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<IcebergTable> {
            unimplemented!("not used by the retry helper")
        }
    }

    /// 506: the closure re-runs the whole rewrite per attempt, the helper stops
    /// once the commit lands, and each lost race is counted on
    /// `sqe_write_commit_retries_total{op}`.
    #[tokio::test]
    async fn commit_rewrite_with_retry_reruns_the_rewrite_and_counts_conflicts() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let dir = tempfile::tempdir().expect("tempdir");
        let table = local_table(&dir, UNPARTITIONED_META);
        let catalog = FlakyCatalog::new(table.clone(), 2);
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().expect("metrics"));
        let handler = WriteHandler::new(write_test_config()).with_metrics(metrics.clone());
        let ident = TableIdent::from_strs(["ns", "tbl"]).unwrap();

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let outcome = handler
            .commit_rewrite_with_retry(&catalog, &ident, "unit-test", "UNIT TEST", move |_fresh| {
                let attempts = attempts_for_closure.clone();
                async move {
                    let n = attempts.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    Ok(Some(RewriteAttempt {
                        add: vec![test_data_file(
                            &format!("s3://bucket/attempt-{n}.parquet"),
                            0,
                            iceberg::spec::Struct::empty(),
                        )],
                        remove: vec![],
                        cleanup: None,
                        value: n,
                    }))
                }
            })
            .await
            .expect("the third attempt commits");

        assert_eq!(outcome, Some(3), "the winning attempt's value is returned");
        assert_eq!(
            attempts.load(AtomicOrdering::SeqCst),
            3,
            "the rewrite re-runs per attempt; re-committing stale files would \
             drop the concurrent writer's changes"
        );
        assert_eq!(catalog.commit_count(), 3);
        assert_eq!(
            metrics
                .write_commit_retries_total
                .with_label_values(&["unit-test"])
                .get(),
            2,
            "one increment per lost race, not per attempt"
        );
    }

    /// 506: a closure that reports "nothing to do" never reaches a commit.
    #[tokio::test]
    async fn commit_rewrite_with_retry_skips_the_commit_when_there_is_nothing_to_do() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = local_table(&dir, UNPARTITIONED_META);
        let catalog = FlakyCatalog::new(table, 0);
        let handler = WriteHandler::new(write_test_config());
        let ident = TableIdent::from_strs(["ns", "tbl"]).unwrap();

        let outcome: Option<usize> = handler
            .commit_rewrite_with_retry(&catalog, &ident, "unit-test", "UNIT TEST", |_fresh| async {
                Ok(None)
            })
            .await
            .expect("no-op is not an error");

        assert!(outcome.is_none());
        assert_eq!(catalog.commit_count(), 0, "no snapshot for a no-op");
    }

    /// 506: a non-conflict error propagates on the first attempt, labelled with
    /// the statement the user typed.
    #[tokio::test]
    async fn commit_rewrite_with_retry_does_not_retry_a_non_conflict_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = local_table(&dir, UNPARTITIONED_META);
        // Any positive count: the error message below is what decides.
        let catalog = FlakyCatalog::new(table, 1);
        let handler = WriteHandler::new(write_test_config());
        let ident = TableIdent::from_strs(["ns", "tbl"]).unwrap();

        let err = handler
            .commit_rewrite_with_retry(
                &catalog,
                &ident,
                "unit-test",
                "DELETE",
                |_fresh| async move {
                    Err::<Option<RewriteAttempt<()>>, _>(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                        "read failed".to_string(),
                    ))
                },
            )
            .await
            .expect_err("the closure's error propagates");
        assert!(format!("{err}").contains("read failed"), "err: {err}");
        assert_eq!(catalog.commit_count(), 0);
    }

    /// sqe#803: a 401 on the reload inside `Transaction::commit` (the second
    /// `load_table`) used to end a finished CTAS. The attempt is retried and
    /// the data files are appended exactly once.
    #[tokio::test]
    async fn commit_with_retry_survives_a_401_on_the_commit_reload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = local_table(&dir, UNPARTITIONED_META);
        let catalog = FlakyCatalog {
            unauthorized_load: Some(2),
            ..FlakyCatalog::new(table, 0)
        };
        let ident = TableIdent::from_strs(["ns", "tbl"]).unwrap();
        let files = vec![test_data_file(
            "s3://bucket/ctas.parquet",
            0,
            iceberg::spec::Struct::empty(),
        )];

        commit_with_retry(&catalog, &ident, "unit-test", |fresh| {
            let files = files.clone();
            let catalog = &catalog;
            async move {
                let tx = Transaction::new(&fresh);
                let tx = tx.fast_append().add_data_files(files).apply(tx)?;
                tx.commit(catalog).await
            }
        })
        .await
        .expect("the 401 is retried");

        assert_eq!(
            catalog.commit_count(),
            1,
            "one update_table, no double append"
        );
        assert_eq!(
            *catalog.loads.lock().expect("load counter"),
            4,
            "two loads per attempt, two attempts"
        );
    }

    /// 502: an old-spec file is partitioned by `country`, not by `region`, so
    /// `DELETE ... WHERE region = 'EU'` cannot rule it out. Evaluated against
    /// the DEFAULT spec (as it was before this fix) its `country = 'US'` value
    /// read as `region = 'US'`, "provably differed" from `'EU'`, and the file
    /// was dropped from the rewrite set — leaving its EU rows alive.
    #[tokio::test]
    async fn partition_prune_keeps_files_from_an_older_partition_spec() {
        use iceberg::spec::{Literal, Struct};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dir = tempfile::tempdir().expect("tempdir");
        let table = local_table(&dir, EVOLVED_SPEC_META);

        let sql = "DELETE FROM t WHERE region = 'EU'";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0);
        let where_expr = match &stmt {
            Statement::Delete(d) => d.selection.clone().expect("WHERE"),
            other => panic!("expected DELETE, got {other}"),
        };

        // Old-spec file partitioned by country=US (it may still hold EU rows),
        // plus two files from the current spec.
        let legacy = test_data_file(
            "s3://b/legacy.parquet",
            0,
            Struct::from_iter([Some(Literal::string("US"))]),
        );
        let eu = test_data_file(
            "s3://b/eu.parquet",
            1,
            Struct::from_iter([Some(Literal::string("EU"))]),
        );
        let us = test_data_file(
            "s3://b/us.parquet",
            1,
            Struct::from_iter([Some(Literal::string("US"))]),
        );

        let (to_rewrite, pruned) = partition_prune_cow_files(
            vec![legacy.clone(), eu.clone(), us.clone()],
            &table,
            Some(&where_expr),
        );
        let paths: Vec<&str> = to_rewrite.iter().map(|f| f.file_path()).collect();
        assert!(
            paths.contains(&"s3://b/legacy.parquet"),
            "a pre-evolution file must never be pruned: {paths:?}"
        );
        assert!(paths.contains(&"s3://b/eu.parquet"), "{paths:?}");
        assert!(
            !paths.contains(&"s3://b/us.parquet"),
            "a new-spec file whose partition provably differs is still pruned: {paths:?}"
        );
        assert_eq!(pruned, 1);

        // The bug's fingerprint, pinned so the fix cannot be undone quietly:
        // evaluated against the DEFAULT spec the legacy file DOES look
        // provably different, which is what used to prune it.
        let constraints = extract_partition_eq_constraints(&where_expr);
        assert!(
            !partition_could_match(
                legacy.partition(),
                table.metadata().default_partition_spec().as_ref(),
                table.metadata().current_schema().as_ref(),
                &constraints,
            ),
            "the default spec misreads an old-spec file; per-file spec lookup is the fix"
        );
    }

    /// 502: a file whose spec id is not in the table metadata is kept. The
    /// manifest is data we do not control, and dropping such a file from the
    /// rewrite set loses rows silently.
    #[tokio::test]
    async fn partition_prune_keeps_files_with_an_unknown_spec_id() {
        use iceberg::spec::{Literal, Struct};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dir = tempfile::tempdir().expect("tempdir");
        let table = local_table(&dir, EVOLVED_SPEC_META);
        let stmt = Parser::parse_sql(&GenericDialect {}, "DELETE FROM t WHERE region = 'EU'")
            .unwrap()
            .remove(0);
        let where_expr = match &stmt {
            Statement::Delete(d) => d.selection.clone().expect("WHERE"),
            other => panic!("expected DELETE, got {other}"),
        };

        let unknown = test_data_file(
            "s3://b/unknown-spec.parquet",
            7,
            Struct::from_iter([Some(Literal::string("US"))]),
        );
        let (to_rewrite, pruned) =
            partition_prune_cow_files(vec![unknown], &table, Some(&where_expr));
        assert_eq!(to_rewrite.len(), 1, "unknown spec id is never pruned");
        assert_eq!(pruned, 0);
    }

    /// 507: `after_commit` evicts the committed table for EVERY token, not just
    /// the writer's. `SessionCatalog` needs a live REST catalog to construct,
    /// so the assertion runs through the `TableCacheInvalidator` seam the hook
    /// takes.
    #[tokio::test]
    async fn after_commit_evicts_the_table_for_every_token() {
        #[derive(Default)]
        struct RecordingInvalidator {
            seen: std::sync::Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl TableCacheInvalidator for RecordingInvalidator {
            async fn invalidate_table_all_tokens(&self, table_ident: &TableIdent) {
                self.seen
                    .lock()
                    .expect("recorded idents")
                    .push(table_ident.to_string());
            }
        }

        let handler = WriteHandler::new(write_test_config());
        let session = test_session(Some("ns"));
        let recorder = RecordingInvalidator::default();
        let ident = TableIdent::from_strs(["ns", "tbl"]).unwrap();

        handler.after_commit(&session, &recorder, &ident).await;

        assert_eq!(
            recorder.seen.lock().expect("recorded idents").as_slice(),
            &["ns.tbl".to_string()]
        );
    }

    /// 502: the file summary logs report which spec generations a rewrite spans.
    #[test]
    fn distinct_spec_ids_reports_every_generation_once() {
        use iceberg::spec::Struct;
        let files = vec![
            test_data_file("s3://b/a.parquet", 1, Struct::empty()),
            test_data_file("s3://b/b.parquet", 0, Struct::empty()),
            test_data_file("s3://b/c.parquet", 1, Struct::empty()),
        ];
        assert_eq!(distinct_spec_ids(&files), vec![0, 1]);
        assert!(distinct_spec_ids(&[]).is_empty());
    }

    // -------------------------------------------------------------------------
    // 500: CREATE OR REPLACE TABLE AS SELECT — in-place swap vs. staged rename
    // -------------------------------------------------------------------------

    /// Iceberg + Arrow schema pair for a SELECT, the way
    /// `handle_ctas_streaming` derives them from the DataFrame.
    fn select_schemas(fields: Vec<arrow_schema::Field>) -> (Arc<ArrowSchema>, IcebergSchema) {
        let arrow = Arc::new(ArrowSchema::new(fields));
        let iceberg = arrow_schema_to_iceberg(&arrow).expect("iceberg schema");
        (arrow, iceberg)
    }

    /// A SELECT that reproduces the live table's schema swaps in place: the
    /// table keeps its identity, its history and its grants.
    #[tokio::test]
    async fn ctas_replace_is_in_place_for_a_matching_schema() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = local_table(&dir, UNPARTITIONED_META);
        let (arrow, iceberg) = select_schemas(vec![Field::new("id", DataType::Int32, false)]);
        assert!(ctas_replace_is_in_place(&live, &arrow, None, &iceberg));
    }

    /// Column set, order, type and nullability all have to match: the
    /// streaming writer stamps each batch onto the LIVE schema positionally, so
    /// anything else would cast into the old types or fail mid-write.
    #[tokio::test]
    async fn ctas_replace_is_not_in_place_when_the_schema_differs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = local_table(&dir, UNPARTITIONED_META);

        let (extra_col, extra_ice) = select_schemas(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("added", DataType::Utf8, true),
        ]);
        assert!(!ctas_replace_is_in_place(
            &live, &extra_col, None, &extra_ice
        ));

        let (widened, widened_ice) = select_schemas(vec![Field::new("id", DataType::Int64, false)]);
        assert!(!ctas_replace_is_in_place(
            &live,
            &widened,
            None,
            &widened_ice
        ));

        let (renamed, renamed_ice) =
            select_schemas(vec![Field::new("key", DataType::Int32, false)]);
        assert!(!ctas_replace_is_in_place(
            &live,
            &renamed,
            None,
            &renamed_ice
        ));

        let (nullable, nullable_ice) =
            select_schemas(vec![Field::new("id", DataType::Int32, true)]);
        assert!(!ctas_replace_is_in_place(
            &live,
            &nullable,
            None,
            &nullable_ice
        ));
    }

    /// An explicit `PARTITIONED BY` has to name the same source columns and
    /// transforms as the live default spec — the writer partitions with the
    /// live spec, so a differing clause would be silently ignored.
    #[tokio::test]
    async fn ctas_replace_in_place_requires_matching_partitioning() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dir = tempfile::tempdir().expect("tempdir");
        // Default spec partitions by `region`; spec 0 by `country`.
        let live = local_table(&dir, EVOLVED_SPEC_META);
        let (arrow, iceberg) = select_schemas(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("country", DataType::Utf8, true),
        ]);

        let partition_expr = |sql: &str| -> sqlparser::ast::Expr {
            let stmt = Parser::parse_sql(&GenericDialect {}, sql)
                .unwrap()
                .remove(0);
            match stmt {
                Statement::Query(q) => match *q.body {
                    sqlparser::ast::SetExpr::Select(select) => {
                        select.projection.first().cloned().map(|p| match p {
                            sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
                            other => panic!("expected a bare expression, got {other}"),
                        })
                    }
                    other => panic!("expected SELECT, got {other}"),
                }
                .expect("projection"),
                other => panic!("expected query, got {other}"),
            }
        };

        let same = build_partition_spec(Some(&partition_expr("SELECT region")), &iceberg)
            .expect("spec")
            .expect("some");
        assert!(ctas_replace_is_in_place(
            &live,
            &arrow,
            Some(&same),
            &iceberg
        ));

        let different = build_partition_spec(Some(&partition_expr("SELECT country")), &iceberg)
            .expect("spec")
            .expect("some");
        assert!(!ctas_replace_is_in_place(
            &live,
            &arrow,
            Some(&different),
            &iceberg
        ));
    }

    // -------------------------------------------------------------------------
    // 513: the committing write paths, in process.
    //
    // Every test below drives a production `WriteHandler` (built from the
    // rig's config, so `resolve_session_catalog` yields a real
    // `SessionCatalog` over sqlite) through the same entry points
    // `query_handler` calls, commits real Parquet to a tempdir warehouse, and
    // reads the result back through the Iceberg table scan (delete-aware) or
    // a DataFusion `SELECT` over `SqeTableProvider`.
    //
    // Run with:
    //   RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator \
    //       --features test-sqlite -- write_handler::
    //
    // CI: `aikido-test-rust` runs `cargo test --workspace --all-features`
    // under the same RUST_MIN_STACK, which enables `test-sqlite`, so these run
    // on every MR. Rig details: `crate::test_support::sqlite_rig`.
    // -------------------------------------------------------------------------
    #[cfg(feature = "test-sqlite")]
    mod committing {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        use super::*;
        use crate::test_support::sqlite_rig::{
            append_rows, ctas_select_sql, id_v_batch, id_v_pairs, id_v_schema, identity_spec,
            insert_select_sql, parse_one, rows_affected, SqliteRig,
        };
        use arrow_schema::{DataType, Field};

        fn handler(rig: &SqliteRig) -> WriteHandler {
            WriteHandler::new(rig.config.clone())
        }

        fn snapshot_count(table: &IcebergTable) -> usize {
            table.metadata().snapshots().count()
        }

        /// `CREATE TABLE ...` through `handle_create_table`.
        async fn run_create(h: &WriteHandler, rig: &SqliteRig, sql: &str) {
            h.handle_create_table(&rig.session(), &parse_one(sql))
                .await
                .expect("CREATE TABLE");
        }

        /// `INSERT ...` the way `query_handler` issues it; returns
        /// `rows_affected`.
        async fn run_insert(h: &WriteHandler, rig: &SqliteRig, sql: &str) -> sqe_core::Result<i64> {
            let stmt = parse_one(sql);
            let select_sql = insert_select_sql(&stmt);
            let ctx = DFSessionContext::new();
            let out = h
                .handle_insert_streaming(
                    &rig.session(),
                    &stmt,
                    &ctx,
                    &select_sql,
                    &mut None,
                    &mut None,
                )
                .await?;
            Ok(rows_affected(&out))
        }

        /// `CREATE [OR REPLACE] TABLE ... AS SELECT` through
        /// `handle_ctas_streaming`.
        async fn run_ctas(h: &WriteHandler, rig: &SqliteRig, sql: &str) -> Vec<RecordBatch> {
            let stmt = parse_one(sql);
            let select_sql = ctas_select_sql(&stmt);
            let ctx = DFSessionContext::new();
            h.handle_ctas_streaming(
                &rig.session(),
                &stmt,
                &ctx,
                &select_sql,
                &mut None,
                &mut None,
            )
            .await
            .expect("CTAS")
        }

        /// `DELETE ...` through the CoW/MoR dispatcher; returns
        /// `rows_affected`.
        async fn run_delete(h: &WriteHandler, rig: &SqliteRig, sql: &str) -> sqe_core::Result<i64> {
            let ctx = DFSessionContext::new();
            let out = h
                .handle_delete_dispatch(
                    &rig.session(),
                    &parse_one(sql),
                    rig.session_catalog().await,
                    &ctx,
                    &mut None,
                    &mut None,
                )
                .await?;
            Ok(rows_affected(&out))
        }

        /// `UPDATE ...` through the dispatcher; returns `rows_affected`.
        async fn run_update(h: &WriteHandler, rig: &SqliteRig, sql: &str) -> sqe_core::Result<i64> {
            let ctx = DFSessionContext::new();
            let out = h
                .handle_update_dispatch(
                    &rig.session(),
                    &parse_one(sql),
                    rig.session_catalog().await,
                    &ctx,
                    &mut None,
                    &mut None,
                )
                .await?;
            Ok(rows_affected(&out))
        }

        /// `MERGE ...` through the dispatcher with pre-executed source
        /// batches (the caller runs the source query in production); returns
        /// `rows_affected`.
        async fn run_merge(
            h: &WriteHandler,
            rig: &SqliteRig,
            sql: &str,
            source: Vec<RecordBatch>,
            source_schema_hint: Option<arrow_schema::SchemaRef>,
        ) -> sqe_core::Result<i64> {
            let ctx = DFSessionContext::new();
            let out = h
                .handle_merge_dispatch(
                    &rig.session(),
                    &parse_one(sql),
                    source,
                    source_schema_hint,
                    rig.session_catalog().await,
                    &ctx,
                    None,
                    &mut None,
                    &mut None,
                )
                .await?;
            Ok(rows_affected(&out))
        }

        /// A seeded `(id, v)` table `default.<name>` with rows 1..=3.
        async fn seeded_id_v(rig: &SqliteRig, name: &str) -> TableIdent {
            seeded_id_v_with(rig, name, std::collections::HashMap::new()).await
        }

        async fn seeded_id_v_with(
            rig: &SqliteRig,
            name: &str,
            props: std::collections::HashMap<String, String>,
        ) -> TableIdent {
            let table = rig
                .local_fs_table_with_properties(name, &id_v_schema(), None, props)
                .await;
            rig.commit_rows(&table, vec![id_v_batch(&[(1, "a"), (2, "b"), (3, "c")])])
                .await;
            rig.ident(name)
        }

        /// A hook that lands ONE concurrent append of `(99, "late")` on
        /// `ident` the first time a commit is about to happen, then stays
        /// quiet so the retry can succeed.
        fn late_append_hook(
            rig: &SqliteRig,
            ident: &TableIdent,
        ) -> (CommitRaceHook, Arc<AtomicBool>) {
            let fired = Arc::new(AtomicBool::new(false));
            let catalog = rig.catalog.clone();
            let ident = ident.clone();
            let fired_for_hook = fired.clone();
            let hook: CommitRaceHook = Arc::new(move || {
                let fired = fired_for_hook.clone();
                let catalog = catalog.clone();
                let ident = ident.clone();
                Box::pin(async move {
                    if fired.swap(true, AtomicOrdering::SeqCst) {
                        return;
                    }
                    append_rows(&catalog, &ident, vec![id_v_batch(&[(99, "late")])]).await;
                })
            });
            (hook, fired)
        }

        fn s(v: &str) -> Option<String> {
            Some(v.to_string())
        }

        // --- Explicit transactions (#821) ---------------------------------
        //
        // The coordinator runs every statement of a transaction inside
        // `sqe_catalog::txn::scope`; these drive the real write paths the
        // same way and publish or discard outside it, as COMMIT / ROLLBACK do.

        fn new_txn() -> Arc<sqe_catalog::txn::TxnScope> {
            sqe_catalog::txn::TxnScope::new(uuid::Uuid::new_v4().to_string())
        }

        /// `(id, v)` rows of `table` at its current snapshot, ordered by id.
        async fn rows_of(table: IcebergTable) -> Vec<(i64, Option<String>)> {
            if table.metadata().current_snapshot_id().is_none() {
                return Vec::new();
            }
            let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(table)
                .await
                .expect("table provider");
            let ctx = DFSessionContext::new();
            ctx.register_table("t", Arc::new(provider))
                .expect("register");
            let batches = ctx
                .sql("SELECT id, v FROM t ORDER BY id")
                .await
                .expect("plan")
                .collect()
                .await
                .expect("execute");
            id_v_pairs(&batches)
        }

        /// What a statement inside `txn` reads for `ident`: the load goes
        /// through the session catalog, which pins it to the branch.
        async fn rows_in_txn(
            rig: &SqliteRig,
            txn: &Arc<sqe_catalog::txn::TxnScope>,
            ident: &TableIdent,
        ) -> Vec<(i64, Option<String>)> {
            let catalog = rig.session_catalog().await.as_catalog();
            let table = sqe_catalog::txn::scope(Some(Arc::clone(txn)), catalog.load_table(ident))
                .await
                .expect("load inside the transaction");
            rows_of(table).await
        }

        async fn insert_in(
            h: &WriteHandler,
            rig: &SqliteRig,
            txn: &Arc<sqe_catalog::txn::TxnScope>,
            sql: &str,
        ) -> sqe_core::Result<i64> {
            sqe_catalog::txn::scope(Some(Arc::clone(txn)), run_insert(h, rig, sql)).await
        }

        fn txn_refs(table: &IcebergTable) -> Vec<String> {
            table
                .metadata()
                .refs_iter()
                .map(|(name, _)| name.to_string())
                .filter(|name| sqe_catalog::txn::is_txn_ref(name))
                .collect()
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn transaction_writes_stay_on_the_branch_until_commit() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let ident = seeded_id_v(&rig, "tx_commit").await;
            let main_before = rig.load(&ident).await.metadata().current_snapshot_id();
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_commit VALUES (4, 'd')")
                .await
                .expect("first INSERT");
            // The second INSERT commits on top of the first one's branch head.
            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_commit VALUES (5, 'e')")
                .await
                .expect("second INSERT");

            let all = vec![(1, s("a")), (2, s("b")), (3, s("c")), (4, s("d")), (5, s("e"))];
            assert_eq!(rows_in_txn(&rig, &txn, &ident).await, all, "reads its own writes");
            let outside = rig.load(&ident).await;
            assert_eq!(outside.metadata().current_snapshot_id(), main_before);
            assert_eq!(
                rows_of(outside).await,
                vec![(1, s("a")), (2, s("b")), (3, s("c"))],
                "other sessions see main"
            );

            sqe_catalog::txn::publish(&txn).await.expect("COMMIT");
            let published = rig.load(&ident).await;
            assert!(txn_refs(&published).is_empty(), "COMMIT removes the branch");
            assert_eq!(rows_of(published).await, all);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn dml_inside_a_transaction_sees_its_own_inserts() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let ident = seeded_id_v(&rig, "tx_dml").await;
            let txn = new_txn();
            let scope = || Some(Arc::clone(&txn));

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_dml VALUES (4, 'd')")
                .await
                .expect("INSERT");
            let deleted = sqe_catalog::txn::scope(
                scope(),
                run_delete(&h, &rig, "DELETE FROM default.tx_dml WHERE id = 4"),
            )
            .await
            .expect("DELETE");
            assert_eq!(deleted, 1, "the DELETE reads the transaction's branch");
            let updated = sqe_catalog::txn::scope(
                scope(),
                run_update(&h, &rig, "UPDATE default.tx_dml SET v = 'z' WHERE id = 1"),
            )
            .await
            .expect("UPDATE");
            assert_eq!(updated, 1);

            sqe_catalog::txn::publish(&txn).await.expect("COMMIT");
            assert_eq!(
                rows_of(rig.load(&ident).await).await,
                vec![(1, s("z")), (2, s("b")), (3, s("c"))]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_concurrent_commit_to_main_rolls_the_transaction_back() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let ident = seeded_id_v(&rig, "tx_conflict").await;
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_conflict VALUES (4, 'd')")
                .await
                .expect("INSERT");
            append_rows(&rig.catalog, &ident, vec![id_v_batch(&[(99, "late")])]).await;

            let err = sqe_catalog::txn::publish(&txn)
                .await
                .expect_err("main moved under the transaction");
            assert_eq!(err.error_code(), sqe_core::SqeErrorCode::TransactionConflict, "{err}");
            let table = rig.load(&ident).await;
            assert!(txn_refs(&table).is_empty(), "a conflict drops the branch");
            assert_eq!(
                rows_of(table).await,
                vec![(1, s("a")), (2, s("b")), (3, s("c")), (99, s("late"))],
                "the transaction's row never reaches main"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn rollback_drops_the_branch_and_leaves_main_alone() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let ident = seeded_id_v(&rig, "tx_rollback").await;
            let main_before = rig.load(&ident).await.metadata().current_snapshot_id();
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_rollback VALUES (4, 'd')")
                .await
                .expect("INSERT");
            assert_eq!(txn_refs(&rig.load(&ident).await).len(), 1);

            sqe_catalog::txn::discard(&txn).await;
            let table = rig.load(&ident).await;
            assert!(txn_refs(&table).is_empty());
            assert_eq!(table.metadata().current_snapshot_id(), main_before);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn the_stale_branch_sweep_drops_only_branches_past_the_lifetime() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let ident = seeded_id_v(&rig, "tx_sweep").await;
            let main_before = rig.load(&ident).await.metadata().current_snapshot_id();
            let txn = new_txn();
            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_sweep VALUES (4, 'd')")
                .await
                .expect("INSERT");
            let sweep = |now_ms: i64| {
                sqe_catalog::txn::drop_stale_branches(rig.catalog.as_ref(), &ident, now_ms)
            };

            let now_ms = chrono::Utc::now().timestamp_millis();
            let dropped = sweep(now_ms).await.expect("sweep now");
            assert!(dropped.is_empty(), "an open transaction's branch is not stale");
            assert_eq!(txn_refs(&rig.load(&ident).await).len(), 1);

            // Two days on, no transaction can still be open.
            let lifetime_ms = sqe_catalog::txn::MAX_LIFETIME.as_millis() as i64;
            let dropped = sweep(now_ms + 2 * lifetime_ms).await.expect("sweep later");
            assert_eq!(dropped.len(), 1);
            let table = rig.load(&ident).await;
            assert!(txn_refs(&table).is_empty());
            assert_eq!(table.metadata().current_snapshot_id(), main_before);

            // Had the transaction still been open, it aborts: no lost write.
            let err = sqe_catalog::txn::publish(&txn).await.expect_err("COMMIT");
            assert_eq!(err.error_code(), sqe_core::SqeErrorCode::TransactionConflict, "{err}");
            assert_eq!(rows_of(rig.load(&ident).await).await.len(), 3);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_transaction_on_an_empty_table_creates_its_branch_on_first_write() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            run_create(&h, &rig, "CREATE TABLE default.tx_empty (id BIGINT NOT NULL, v VARCHAR)")
                .await;
            let ident = rig.ident("tx_empty");
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_empty VALUES (1, 'a')")
                .await
                .expect("first INSERT");
            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_empty VALUES (2, 'b')")
                .await
                .expect("second INSERT");
            assert!(rig.load(&ident).await.metadata().current_snapshot_id().is_none());
            assert_eq!(rows_in_txn(&rig, &txn, &ident).await, vec![(1, s("a")), (2, s("b"))]);

            sqe_catalog::txn::publish(&txn).await.expect("COMMIT");
            assert_eq!(
                rows_of(rig.load(&ident).await).await,
                vec![(1, s("a")), (2, s("b"))]
            );
        }

        // --- Multi-table transactions (#823) -----------------------------

        #[tokio::test(flavor = "multi_thread")]
        async fn two_tables_publish_together_at_commit() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let a = seeded_id_v(&rig, "tx_multi_a").await;
            let b = seeded_id_v(&rig, "tx_multi_b").await;
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_multi_a VALUES (4, 'd')")
                .await
                .expect("INSERT into a");
            let deleted = sqe_catalog::txn::scope(
                Some(Arc::clone(&txn)),
                run_delete(&h, &rig, "DELETE FROM default.tx_multi_b WHERE id = 2"),
            )
            .await
            .expect("DELETE from b");
            assert_eq!(deleted, 1);
            assert_eq!(txn.tables().len(), 2);
            assert_eq!(
                rows_in_txn(&rig, &txn, &b).await,
                vec![(1, s("a")), (3, s("c"))],
                "reads its own delete"
            );
            let seed = vec![(1, s("a")), (2, s("b")), (3, s("c"))];
            assert_eq!(rows_of(rig.load(&a).await).await, seed, "a: main untouched");
            assert_eq!(rows_of(rig.load(&b).await).await, seed, "b: main untouched");

            sqe_catalog::txn::publish(&txn).await.expect("COMMIT");
            let (ta, tb) = (rig.load(&a).await, rig.load(&b).await);
            assert!(txn_refs(&ta).is_empty() && txn_refs(&tb).is_empty());
            assert_eq!(
                rows_of(ta).await,
                vec![(1, s("a")), (2, s("b")), (3, s("c")), (4, s("d"))]
            );
            assert_eq!(rows_of(tb).await, vec![(1, s("a")), (3, s("c"))]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_conflict_on_one_table_rolls_back_both() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let a = seeded_id_v(&rig, "tx_multi_conflict_a").await;
            let b = seeded_id_v(&rig, "tx_multi_conflict_b").await;
            let a_main = rig.load(&a).await.metadata().current_snapshot_id();
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_multi_conflict_a VALUES (4, 'd')")
                .await
                .expect("INSERT into a");
            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_multi_conflict_b VALUES (4, 'd')")
                .await
                .expect("INSERT into b");
            // Another session commits to b only.
            append_rows(&rig.catalog, &b, vec![id_v_batch(&[(99, "late")])]).await;

            let err = sqe_catalog::txn::publish(&txn)
                .await
                .expect_err("b's main moved under the transaction");
            assert_eq!(err.error_code(), sqe_core::SqeErrorCode::TransactionConflict, "{err}");
            let (ta, tb) = (rig.load(&a).await, rig.load(&b).await);
            assert!(txn_refs(&ta).is_empty(), "a conflict drops a's branch too");
            assert!(txn_refs(&tb).is_empty());
            assert_eq!(ta.metadata().current_snapshot_id(), a_main, "a never moved");
            assert_eq!(
                rows_of(tb).await,
                vec![(1, s("a")), (2, s("b")), (3, s("c")), (99, s("late"))]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn rollback_drops_every_branch() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            let a = seeded_id_v(&rig, "tx_multi_rb_a").await;
            let b = seeded_id_v(&rig, "tx_multi_rb_b").await;
            let txn = new_txn();

            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_multi_rb_a VALUES (4, 'd')")
                .await
                .expect("INSERT into a");
            insert_in(&h, &rig, &txn, "INSERT INTO default.tx_multi_rb_b VALUES (4, 'd')")
                .await
                .expect("INSERT into b");
            assert_eq!(txn_refs(&rig.load(&a).await).len(), 1);
            assert_eq!(txn_refs(&rig.load(&b).await).len(), 1);

            sqe_catalog::txn::discard(&txn).await;
            assert!(txn_refs(&rig.load(&a).await).is_empty());
            assert!(txn_refs(&rig.load(&b).await).is_empty());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn insert_then_select_round_trip() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            run_create(
                &h,
                &rig,
                "CREATE TABLE default.rt (id BIGINT NOT NULL, v VARCHAR)",
            )
            .await;

            let n = run_insert(
                &h,
                &rig,
                "INSERT INTO default.rt SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS s(id, v)",
            )
            .await
            .expect("INSERT");
            assert_eq!(n, 3, "rows_affected reports the written rows");

            // SELECT through DataFusion over the committed table.
            let table = rig.load(&rig.ident("rt")).await;
            assert_eq!(snapshot_count(&table), 1);
            let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(table)
                .await
                .expect("table provider");
            let ctx = DFSessionContext::new();
            ctx.register_table("rt", Arc::new(provider))
                .expect("register");
            let batches = ctx
                .sql("SELECT id, v FROM rt ORDER BY id")
                .await
                .expect("plan")
                .collect()
                .await
                .expect("execute");
            assert_eq!(
                id_v_pairs(&batches),
                vec![(1, s("a")), (2, s("b")), (3, s("c"))]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn insert_overwrite_replaces_only_touched_partitions() {
            let rig = SqliteRig::new().await;
            let schema = ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("part", DataType::Utf8, false),
            ]);
            rig.local_fs_table("ow", &schema, Some(identity_spec(&schema, &["part"])))
                .await;
            let h = handler(&rig);

            let n = run_insert(
                &h,
                &rig,
                "INSERT INTO default.ow SELECT * FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS s(id, part)",
            )
            .await
            .expect("INSERT");
            assert_eq!(n, 3);

            // Dynamic overwrite: only partition `a` is touched; `b` survives.
            let n = run_insert(
                &h,
                &rig,
                "INSERT OVERWRITE default.ow SELECT * FROM (VALUES (10, 'a')) AS s(id, part)",
            )
            .await
            .expect("INSERT OVERWRITE");
            assert_eq!(n, 1);

            let ident = rig.ident("ow");
            assert_eq!(rig.scan_i64(&ident, "id").await, vec![3, 10]);
            let files = rig.live_files(&ident).await;
            assert_eq!(files.len(), 2, "one file per live partition: {files:?}");
            let rows: u64 = files.iter().map(|f| f.record_count()).sum();
            assert_eq!(rows, 2);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn cow_delete_retries_after_concurrent_append() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "cow").await;
            let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().expect("metrics"));
            let (hook, fired) = late_append_hook(&rig, &ident);
            let h = handler(&rig)
                .with_metrics(metrics.clone())
                .with_test_commit_race_hook(hook);

            let n = run_delete(&h, &rig, "DELETE FROM default.cow WHERE id = 2")
                .await
                .expect("CoW DELETE survives the lost race");

            assert!(fired.load(AtomicOrdering::SeqCst), "the race was exercised");
            assert_eq!(n, 1, "rows_affected counts the deleted row once");
            assert_eq!(
                rig.scan_i64(&ident, "id").await,
                vec![1, 3, 99],
                "deleted row gone, concurrently appended row kept"
            );
            // Snapshot order proves the append landed first and the DELETE
            // committed on top of it: seed, late append, then the rewrite.
            let table = rig.load(&ident).await;
            assert_eq!(snapshot_count(&table), 3, "seed + late append + rewrite");
            // The rewrite carries `RefSnapshotIdMatch`, so its first commit
            // against the pre-append snapshot is a retryable conflict. The
            // vendored `Transaction::commit` absorbs retryable conflicts with
            // its own reload-and-reapply loop before `commit_rewrite_with_retry`
            // sees them, so the handler-level counter only counts conflicts
            // that escape that loop: zero here, never more than one.
            assert!(
                metrics
                    .write_commit_retries_total
                    .with_label_values(&["delete-cow"])
                    .get()
                    <= 1
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn mor_delete_retries_after_concurrent_append() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v_with(
                &rig,
                "mor",
                std::collections::HashMap::from([(
                    sqe_core::table_properties::WRITE_DELETE_MODE.to_string(),
                    "merge-on-read".to_string(),
                )]),
            )
            .await;
            let (hook, fired) = late_append_hook(&rig, &ident);
            let h = handler(&rig).with_test_commit_race_hook(hook);

            let n = run_delete(&h, &rig, "DELETE FROM default.mor WHERE id = 2")
                .await
                .expect("MoR DELETE survives the lost race");

            assert!(fired.load(AtomicOrdering::SeqCst), "the race was exercised");
            assert_eq!(n, 1);
            // Logical view applies the position delete; the physical data
            // file is untouched (MoR) and the late append is a second file.
            assert_eq!(rig.scan_i64(&ident, "id").await, vec![1, 3, 99]);
            let data_files = rig.live_files(&ident).await;
            assert_eq!(
                data_files.len(),
                2,
                "seed file kept + late file: {data_files:?}"
            );
            let table = rig.load(&ident).await;
            let deletes = sqe_compaction::collect_live_delete_files(&table)
                .await
                .expect("live delete files");
            assert_eq!(deletes.len(), 1, "one position-delete file committed");
        }

        /// A compaction that rewrites the scanned data file between the scan
        /// and the commit leaves the position deletes pointing at a file that
        /// is no longer live. The DELETE must fail instead of reporting rows
        /// it never removed.
        #[tokio::test(flavor = "multi_thread")]
        async fn mor_delete_fails_when_concurrent_compaction_replaces_file() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v_with(
                &rig,
                "mor_compact",
                std::collections::HashMap::from([(
                    sqe_core::table_properties::WRITE_DELETE_MODE.to_string(),
                    "merge-on-read".to_string(),
                )]),
            )
            .await;
            let fired = Arc::new(AtomicBool::new(false));
            let catalog = rig.catalog.clone();
            let hook_ident = ident.clone();
            let fired_for_hook = fired.clone();
            let hook: CommitRaceHook = Arc::new(move || {
                let fired = fired_for_hook.clone();
                let catalog = catalog.clone();
                let ident = hook_ident.clone();
                Box::pin(async move {
                    if fired.swap(true, AtomicOrdering::SeqCst) {
                        return;
                    }
                    let table = catalog.load_table(&ident).await.expect("reload");
                    let old = crate::writer::collect_live_data_files(&table)
                        .await
                        .expect("live files");
                    let new = crate::writer::write_data_files(
                        &table,
                        vec![id_v_batch(&[(1, "a"), (2, "b"), (3, "c")])],
                        "compact",
                        crate::writer::parse_parquet_compression("zstd"),
                        crate::writer::new_upload_tracker(),
                    )
                    .await
                    .expect("write compacted file");
                    let tx = Transaction::new(&table);
                    let tx = tx
                        .rewrite_files()
                        .add_data_files(new)
                        .delete_files(old)
                        .apply(tx)
                        .expect("apply rewrite");
                    tx.commit(catalog.as_ref())
                        .await
                        .expect("commit compaction");
                })
            });
            let h = handler(&rig).with_test_commit_race_hook(hook);

            let res = run_delete(&h, &rig, "DELETE FROM default.mor_compact WHERE id = 2").await;

            assert!(fired.load(AtomicOrdering::SeqCst), "the race was exercised");
            let err = res.expect_err("DELETE must not report success");
            assert!(
                err.to_string().contains("replaced by a concurrent commit"),
                "fails on the live-file check, not an exhausted retry: {err}"
            );
            assert_eq!(rig.scan_i64(&ident, "id").await, vec![1, 2, 3]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn merge_matched_update_and_not_matched_insert() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "m1").await;
            let h = handler(&rig);
            let n = run_merge(
                &h,
                &rig,
                "MERGE INTO default.m1 AS t USING src AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.v \
                 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
                vec![id_v_batch(&[(2, "B"), (4, "d")])],
                None,
            )
            .await
            .expect("MERGE");
            // CoW MERGE reports the rows it WROTE (the rewritten target: 3
            // seed rows, one of them updated, plus the insert), not the rows
            // the clauses touched; only the empty-source BY SOURCE path (501)
            // counts claimed rows instead.
            assert_eq!(n, 4, "rows written by the rewrite");
            assert_eq!(
                rig.scan_id_v(&ident).await,
                vec![(1, s("a")), (2, s("B")), (3, s("c")), (4, s("d"))]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn merge_matched_delete() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "m2").await;
            let h = handler(&rig);
            let n = run_merge(
                &h,
                &rig,
                "MERGE INTO default.m2 AS t USING src AS s ON t.id = s.id \
                 WHEN MATCHED THEN DELETE",
                vec![id_v_batch(&[(1, "x")])],
                None,
            )
            .await
            .expect("MERGE");
            assert_eq!(n, 2, "rows written: the two survivors");
            assert_eq!(rig.scan_id_v(&ident).await, vec![(2, s("b")), (3, s("c"))]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn merge_not_matched_by_source_delete() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "m3").await;
            let h = handler(&rig);
            let n = run_merge(
                &h,
                &rig,
                "MERGE INTO default.m3 AS t USING src AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.v \
                 WHEN NOT MATCHED BY SOURCE THEN DELETE",
                vec![id_v_batch(&[(1, "A")])],
                None,
            )
            .await
            .expect("MERGE");
            assert_eq!(n, 1, "rows written: the one updated survivor");
            assert_eq!(rig.scan_id_v(&ident).await, vec![(1, s("A"))]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn merge_not_matched_by_source_update() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "m4").await;
            let h = handler(&rig);
            let n = run_merge(
                &h,
                &rig,
                "MERGE INTO default.m4 AS t USING src AS s ON t.id = s.id \
                 WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 'gone'",
                vec![id_v_batch(&[(1, "a")])],
                None,
            )
            .await
            .expect("MERGE");
            assert_eq!(n, 3, "rows written: all three target rows");
            assert_eq!(
                rig.scan_id_v(&ident).await,
                vec![(1, s("a")), (2, s("gone")), (3, s("gone"))]
            );
        }

        /// 501: an empty source is not a shortcut when a BY SOURCE clause
        /// exists; every target row is unmatched-by-source.
        #[tokio::test(flavor = "multi_thread")]
        async fn merge_empty_source_still_runs_not_matched_by_source() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "m5").await;
            let h = handler(&rig);
            let n = run_merge(
                &h,
                &rig,
                "MERGE INTO default.m5 AS t USING src AS s ON t.id = s.id \
                 WHEN MATCHED THEN DELETE \
                 WHEN NOT MATCHED BY SOURCE THEN DELETE",
                vec![],
                Some(id_v_schema()),
            )
            .await
            .expect("MERGE");
            assert_eq!(n, 3, "all target rows were unmatched by source");
            assert!(rig.scan_id_v(&ident).await.is_empty());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn merge_empty_source_without_by_source_clause_is_a_noop() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "m6").await;
            let before = snapshot_count(&rig.load(&ident).await);
            let h = handler(&rig);
            let n = run_merge(
                &h,
                &rig,
                "MERGE INTO default.m6 AS t USING src AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.v \
                 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
                vec![],
                Some(id_v_schema()),
            )
            .await
            .expect("MERGE");
            assert_eq!(n, 0);
            assert_eq!(
                rig.scan_id_v(&ident).await,
                vec![(1, s("a")), (2, s("b")), (3, s("c"))]
            );
            assert_eq!(snapshot_count(&rig.load(&ident).await), before, "no commit");
        }

        /// 500: same schema and partitioning -> the contents are swapped in
        /// with one rewrite commit; the table keeps its UUID and history.
        #[tokio::test(flavor = "multi_thread")]
        async fn create_or_replace_in_place_keeps_table_identity() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            run_ctas(
                &h,
                &rig,
                "CREATE TABLE default.cr AS SELECT * FROM (VALUES (1, 'a')) AS s(id, v)",
            )
            .await;
            let ident = rig.ident("cr");
            let before = rig.load(&ident).await;
            let uuid_before = before.metadata().uuid();
            assert_eq!(rig.scan_id_v(&ident).await, vec![(1, s("a"))]);

            let out = run_ctas(
                &h,
                &rig,
                "CREATE OR REPLACE TABLE default.cr AS SELECT * FROM (VALUES (7, 'z'), (8, 'y')) AS s(id, v)",
            )
            .await;
            assert_eq!(rows_affected(&out), 2);

            let after = rig.load(&ident).await;
            assert_eq!(after.metadata().uuid(), uuid_before, "same table");
            assert!(
                snapshot_count(&after) > snapshot_count(&before),
                "history kept and extended"
            );
            assert_eq!(rig.scan_id_v(&ident).await, vec![(7, s("z")), (8, s("y"))]);
        }

        /// A caller with a row filter on the target cannot replace its whole
        /// contents (INSERT OVERWRITE / CREATE OR REPLACE ... AS SELECT):
        /// that would remove rows the filter hides from them.
        #[tokio::test(flavor = "multi_thread")]
        async fn overwrite_and_replace_refused_under_target_row_filter() {
            use datafusion::logical_expr::{col, lit};
            let rig = SqliteRig::new().await;
            run_ctas(
                &handler(&rig),
                &rig,
                "CREATE TABLE default.rf AS SELECT * FROM (VALUES (1, 'a'), (100, 'b')) AS s(id, v)",
            )
            .await;

            let store = sqe_policy::policy_store::InMemoryPolicyStore::new();
            let mut pol = sqe_policy::ResolvedPolicy::default();
            pol.row_filters.push(col("id").lt(lit(50_i64)));
            store.add_table_policy("default", "rf", pol).await;
            let h = handler(&rig).with_policy_enforcer(Arc::new(
                sqe_policy::plan_rewriter::PolicyPlanRewriter::new(Arc::new(store)),
            ));

            let err = run_insert(
                &h,
                &rig,
                "INSERT OVERWRITE default.rf SELECT * FROM (VALUES (2, 'c')) AS s(id, v)",
            )
            .await
            .expect_err("INSERT OVERWRITE under a row filter must be refused");
            assert!(err.to_string().contains("row-level security"), "{err}");

            let stmt = parse_one(
                "CREATE OR REPLACE TABLE default.rf AS SELECT * FROM (VALUES (2, 'c')) AS s(id, v)",
            );
            let err = h
                .handle_ctas_streaming(
                    &rig.session(),
                    &stmt,
                    &DFSessionContext::new(),
                    &ctas_select_sql(&stmt),
                    &mut None,
                    &mut None,
                )
                .await
                .expect_err("CREATE OR REPLACE under a row filter must be refused");
            assert!(err.to_string().contains("row-level security"), "{err}");

            assert_eq!(
                rig.scan_id_v(&rig.ident("rf")).await,
                vec![(1, s("a")), (100, s("b"))],
                "hidden row survives"
            );
        }

        /// Masks and restrictions hide values, not rows: a caller with only a
        /// column mask (no row filter) on the target may still replace its
        /// whole contents, as dbt's `table` materialization does.
        #[tokio::test(flavor = "multi_thread")]
        async fn overwrite_and_replace_allowed_under_mask_only() {
            let rig = SqliteRig::new().await;
            run_ctas(
                &handler(&rig),
                &rig,
                "CREATE TABLE default.mk AS SELECT * FROM (VALUES (1, 'a'), (100, 'b')) AS s(id, v)",
            )
            .await;

            let store = sqe_policy::policy_store::InMemoryPolicyStore::new();
            let mut pol = sqe_policy::ResolvedPolicy::default();
            pol.column_masks.insert(
                "v".to_string(),
                sqe_policy::MaskType::Redact("***".to_string()),
            );
            store.add_table_policy("default", "mk", pol).await;
            let h = handler(&rig).with_policy_enforcer(Arc::new(
                sqe_policy::plan_rewriter::PolicyPlanRewriter::new(Arc::new(store)),
            ));

            run_insert(
                &h,
                &rig,
                "INSERT OVERWRITE default.mk SELECT * FROM (VALUES (2, 'c')) AS s(id, v)",
            )
            .await
            .expect("INSERT OVERWRITE under a mask only must be allowed");
            assert_eq!(rig.scan_id_v(&rig.ident("mk")).await, vec![(2, s("c"))]);

            run_ctas(
                &h,
                &rig,
                "CREATE OR REPLACE TABLE default.mk AS SELECT * FROM (VALUES (3, 'd')) AS s(id, v)",
            )
            .await;
            assert_eq!(rig.scan_id_v(&rig.ident("mk")).await, vec![(3, s("d"))]);
        }

        /// 500: a different column set cannot be written into the live table,
        /// so the replacement is staged under a temporary name and swapped in
        /// with DROP + RENAME, leaving no staging table behind.
        #[tokio::test(flavor = "multi_thread")]
        async fn create_or_replace_with_new_schema_swaps_tables() {
            let rig = SqliteRig::new().await;
            let h = handler(&rig);
            run_ctas(
                &h,
                &rig,
                "CREATE TABLE default.cs AS SELECT * FROM (VALUES (1, 'a')) AS s(id, v)",
            )
            .await;
            let ident = rig.ident("cs");
            let uuid_before = rig.load(&ident).await.metadata().uuid();

            run_ctas(
                &h,
                &rig,
                "CREATE OR REPLACE TABLE default.cs AS SELECT * FROM (VALUES (5, 'q', 2.5)) AS s(id, v, w)",
            )
            .await;

            let after = rig.load(&ident).await;
            assert_ne!(
                after.metadata().uuid(),
                uuid_before,
                "a new table took the name"
            );
            assert_eq!(
                after.metadata().current_schema().as_struct().fields().len(),
                3
            );
            assert_eq!(rig.scan_i64(&ident, "id").await, vec![5]);
            let tables = rig
                .catalog
                .list_tables(&NamespaceIdent::new("default".to_string()))
                .await
                .expect("list tables");
            assert_eq!(
                tables
                    .iter()
                    .map(|t| t.name().to_string())
                    .collect::<Vec<_>>(),
                vec!["cs".to_string()],
                "no staging table left behind"
            );
        }

        /// 502: files written under an older partition spec are still
        /// candidates for a DELETE whose predicate names the NEW partition
        /// column. Spec 0 partitions by `country`; after evolving to `region`,
        /// `DELETE ... WHERE region = 'EU'` must reach the EU rows inside the
        /// `country`-partitioned legacy files too.
        #[tokio::test(flavor = "multi_thread")]
        async fn partition_evolution_then_delete_reaches_legacy_spec_files() {
            let rig = SqliteRig::new().await;
            let schema = ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("country", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, false),
            ]);
            rig.local_fs_table("evo", &schema, Some(identity_spec(&schema, &["country"])))
                .await;
            let ident = rig.ident("evo");
            let h = handler(&rig);

            let n = run_insert(
                &h,
                &rig,
                "INSERT INTO default.evo SELECT * FROM (VALUES \
                 (1, 'US', 'EU'), (2, 'DE', 'EU'), (3, 'US', 'NA')) AS s(id, country, region)",
            )
            .await
            .expect("INSERT under spec 0");
            assert_eq!(n, 3);
            let legacy_files = rig.live_files(&ident).await;
            assert_eq!(legacy_files.len(), 2, "US and DE partitions");

            rig.evolve_partition_spec(&ident, identity_spec(&schema, &["region"]))
                .await;
            let evolved = rig.load(&ident).await;
            assert_eq!(evolved.metadata().default_partition_spec().spec_id(), 1);

            let n = run_insert(
                &h,
                &rig,
                "INSERT INTO default.evo SELECT * FROM (VALUES (4, 'FR', 'EU')) AS s(id, country, region)",
            )
            .await
            .expect("INSERT under spec 1");
            assert_eq!(n, 1);
            let spec_ids: std::collections::BTreeSet<i32> = rig
                .live_files(&ident)
                .await
                .iter()
                .map(|f| f.partition_spec_id())
                .collect();
            assert_eq!(
                spec_ids,
                [0, 1].into_iter().collect(),
                "both spec generations live"
            );

            let n = run_delete(&h, &rig, "DELETE FROM default.evo WHERE region = 'EU'")
                .await
                .expect("DELETE across spec generations");
            assert_eq!(n, 3, "EU rows in legacy files (1, 2) and the new file (4)");
            assert_eq!(rig.scan_i64(&ident, "id").await, vec![3]);
        }

        /// 505: a mis-shaped INSERT fails at planning time, before any file is
        /// written or committed.
        #[tokio::test(flavor = "multi_thread")]
        async fn insert_with_wider_arity_is_a_planning_error() {
            let rig = SqliteRig::new().await;
            rig.local_fs_table("ar", &id_v_schema(), None).await;
            let ident = rig.ident("ar");
            let h = handler(&rig);

            let err = run_insert(
                &h,
                &rig,
                "INSERT INTO default.ar SELECT * FROM (VALUES (1, 'a', 2)) AS s(id, v, w)",
            )
            .await
            .expect_err("three source columns into a two-column table");
            let msg = err.to_string();
            assert!(
                msg.contains("INSERT has 3 columns but target has 2"),
                "planning-time arity error, got: {msg}"
            );

            let table = rig.load(&ident).await;
            assert_eq!(snapshot_count(&table), 0, "nothing was committed");
            assert!(rig.live_files(&ident).await.is_empty());
            let data_dir = format!(
                "{}/data",
                table.metadata().location().trim_start_matches("file://")
            );
            let stray = std::fs::read_dir(&data_dir)
                .map(|d| d.filter_map(Result::ok).count())
                .unwrap_or(0);
            assert_eq!(stray, 0, "no parquet part was written");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn update_reports_rows_affected() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "up").await;
            let h = handler(&rig);
            let n = run_update(&h, &rig, "UPDATE default.up SET v = 'z' WHERE id >= 2")
                .await
                .expect("UPDATE");
            assert_eq!(n, 2);
            assert_eq!(
                rig.scan_id_v(&ident).await,
                vec![(1, s("a")), (2, s("z")), (3, s("z"))]
            );
        }

        /// 514: a truncate reports the manifest row count, not "unknown".
        #[tokio::test(flavor = "multi_thread")]
        async fn delete_without_where_truncates_and_reports_rows_affected() {
            let rig = SqliteRig::new().await;
            let ident = seeded_id_v(&rig, "tr").await;
            let h = handler(&rig);
            let n = run_delete(&h, &rig, "DELETE FROM default.tr")
                .await
                .expect("truncate");
            assert_eq!(n, 3);
            assert!(rig.scan_id_v(&ident).await.is_empty());
            assert!(rig.live_files(&ident).await.is_empty());
        }
    }
}
