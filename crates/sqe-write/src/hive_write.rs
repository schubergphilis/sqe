//! Writes into a Hive external table.
//!
//! Cycle 4. The Iceberg write path (`write_handler`) is not reusable here and
//! is not reused: it ends in `commit_written_files`, an atomic
//! `rewrite_files`/`fast_append` against table metadata. A Hive external table
//! has no metadata pointer to swap -- its content is whatever is in the
//! directory -- so there is nothing for that commit to do.
//!
//! What replaces it is DataFusion's own listing-table sink. `HiveTableProvider`
//! delegates `insert_into` to its inner `ListingTable`, and DataFusion 55's
//! `ListingTable::insert_into` carries `table_partition_cols` into
//! `FileSinkConfig`, so the `k=v/` fan-out already exists upstream. This module
//! therefore routes rather than writes: it plans the statement against the
//! session context, which is what makes `insert_into` get called at all.
//!
//! ## The visibility contract, up front
//!
//! Files land in their final partition directories. A single object's
//! appearance is atomic on an object store, so no reader ever sees a torn file,
//! but a MULTI-file write is not atomic and a concurrent reader may see a
//! subset of it. That is the format's contract, not a gap in this module: every
//! other engine reading the directory decides the table's content the same way.
//! A caller who needs snapshot isolation on write needs an Iceberg table.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::source_as_provider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{dml::InsertOp, DmlStatement, LogicalPlan, WriteOp};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::{NamespaceIdent, TableIdent};
use sqlparser::ast::Statement;

use sqe_core::{SqeError, SqeErrorCode};

use crate::write_handler::check_batch_within_policy;

fn exec_err(what: &str, e: impl std::fmt::Display) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::ExecutionFailed,
        format!("hive insert: {what}: {e}"),
    )
}

/// One-row `count` result, matching what the Iceberg insert path returns so a
/// client cannot tell which engine path served the statement.
fn rows_written(n: u64) -> Result<Vec<RecordBatch>, SqeError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let col: ArrayRef = Arc::new(UInt64Array::from(vec![n]));
    RecordBatch::try_new(schema, vec![col])
        .map(|b| vec![b])
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("hive insert result: {e}")))
}

/// Reject `INSERT OVERWRITE` against a Hive external table, with the reason.
///
/// Matched on the parsed statement's `overwrite` flag rather than left to the
/// sink. **The failure to prevent is not the refusal, it is an overwrite
/// reaching the sink unnoticed and being executed as an append**, which doubles
/// the data and returns success. That is the same class of silent wrong answer
/// the whole Hive program has been guarding against.
///
/// Deferred rather than unimplementable: without an atomic commit an overwrite
/// is delete-then-write, and an interruption between the two loses the
/// partition. That is real data loss from a statement that reads as routine, so
/// it does not ship alongside the first working append.
pub fn reject_overwrite(stmt: &Statement) -> Result<(), SqeError> {
    let overwrite = match stmt {
        Statement::Insert(ins) => ins.overwrite || ins.replace_into,
        _ => false,
    };
    if overwrite {
        return Err(SqeError::NotImplemented(
            "hive_external: INSERT OVERWRITE is not supported for Hive external tables. \
             An overwrite would have to delete the partition's objects and then write new \
             ones, and no object store can make those two steps atomic, so an interruption \
             between them loses the partition. Use an Iceberg table for overwrite \
             semantics, or DROP and recreate the partition directory deliberately."
                .to_string(),
        ));
    }
    Ok(())
}

/// Plan and execute an `INSERT INTO` against a Hive external table.
///
/// `sql` is the original statement text. It is planned through the session
/// context on purpose: DataFusion's own INSERT planning is what calls
/// `TableProvider::insert_into`, and reimplementing the fan-out here would mean
/// a second writer that has to agree with the read path's file schema.
///
/// The source SELECT runs through `enforcer` exactly as the Iceberg INSERT path
/// does (`WriteHandler::enforce_source_plan`): without it, `INSERT INTO
/// lake.dump.t SELECT * FROM masked_table` copied unmasked, unfiltered rows
/// into a Hive directory the caller can read with any tool (architecture
/// review 503). The policy summary is returned for the audit entry.
pub async fn handle_insert(
    ctx: &SessionContext,
    stmt: &Statement,
    sql: &str,
    enforcer: &dyn sqe_policy::PolicyEnforcer,
    user: &sqe_core::SessionUser,
    policy_summary_out: &mut Option<sqe_policy::PolicySummary>,
) -> Result<Vec<RecordBatch>, SqeError> {
    reject_overwrite(stmt)?;

    // Pin `keep_partition_by_columns = false` rather than inheriting the
    // default. Partition columns are directory-encoded, and the read path
    // derives its file schema from the manifest's declared column list with the
    // partition columns EXCLUDED. A data file that also contained them would
    // have more fields than the reader expects -- and the failure would surface
    // on the next SELECT, not on this write. Relying on a default for something
    // that breaks reads is relying on the wrong thing.
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .keep_partition_by_columns = false;

    let state = ctx.state();
    let plan = sqe_catalog::file_tvf_prepare::create_logical_plan(&state, sql)
        .await
        .map_err(|e| exec_err("planning failed", e))?;
    let LogicalPlan::Dml(dml) = plan else {
        return Err(SqeError::Execution(
            "hive insert: statement did not plan to an INSERT".to_string(),
        ));
    };
    let source = Arc::unwrap_or_clone(dml.input);
    let (enforced, summary) = enforcer
        .evaluate(user, &sqe_policy::TableScope::of(ctx), source)
        .await?;
    *policy_summary_out = Some(summary);

    // WITH CHECK on the TARGET (#729), same as the Iceberg INSERT path: the
    // source enforcement above only filters what the caller may read; a
    // row-filtered caller must also not append rows their own SELECT on the
    // target would hide. Keyed exactly like the read path's
    // `resolve_policy_key` for a Hive catalog: `<catalog>.<schema>`. The
    // target is qualified from the session scope first, as the read path's
    // rewriter does: an unqualified `INSERT INTO t` under a Hive-root default
    // catalog must not fall back to the bare `default` key and miss the
    // `<root>.<schema>` row filter.
    let target = sqe_policy::TableScope::of(ctx).qualify(&dml.table_name);
    let namespace_key = match (target.catalog(), target.schema()) {
        (Some(cat), Some(schema)) => format!("{cat}.{schema}"),
        (None, Some(schema)) => schema.to_string(),
        _ => "default".to_string(),
    };
    let (predicates, _) = sqe_policy::write_predicates::extract_write_predicates(
        enforcer,
        user,
        None,
        &namespace_key,
        dml.table_name.table(),
        dml.target.schema(),
    )
    .await?;

    let batches = if predicates.is_empty() {
        let plan = LogicalPlan::Dml(DmlStatement::new(
            dml.table_name,
            dml.target,
            dml.op,
            Arc::new(enforced),
        ));
        datafusion::dataframe::DataFrame::new(state, plan)
            .collect()
            .await
            .map_err(|e| exec_err("failed", e))?
    } else {
        let physical = state
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| exec_err("physical planning failed", e))?;
        let stream = datafusion::physical_plan::execute_stream(physical, state.task_ctx())
            .map_err(|e| exec_err("failed to start source stream", e))?;
        let ident = TableIdent::new(
            NamespaceIdent::new(namespace_key),
            dml.table_name.table().to_string(),
        );
        let shared = Arc::new((ctx.clone(), ident, predicates));
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
        let checked: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), checked));
        let input = StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(OneShotPartition {
                schema,
                stream: Mutex::new(Some(checked)),
            })],
            None,
            vec![],
            false,
            None,
        )
        .map_err(|e| exec_err("checked source", e))?;
        let insert_op = match dml.op {
            WriteOp::Insert(op) => op,
            _ => InsertOp::Append,
        };
        let sink = source_as_provider(&dml.target)
            .map_err(|e| exec_err("target provider", e))?
            .insert_into(&state, Arc::new(input), insert_op)
            .await
            .map_err(|e| exec_err("sink planning failed", e))?;
        datafusion::physical_plan::collect(sink, state.task_ctx())
            .await
            .map_err(|e| exec_err("failed", e))?
    };

    // DataFusion's insert returns a one-row count. Read it rather than
    // re-deriving one, so the number reported is the number the sink wrote.
    let written = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .filter(|a| a.len() == 1)
        .map(|a| a.value(0))
        .unwrap_or(0);

    rows_written(written)
}

/// A single-partition [`PartitionStream`] handing out an already-started
/// stream once. `StreamingTableExec` is the smallest `ExecutionPlan` that
/// accepts a stream, and the sink executes its input exactly once.
struct OneShotPartition {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}

impl std::fmt::Debug for OneShotPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneShotPartition")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for OneShotPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        match self.stream.lock().ok().and_then(|mut s| s.take()) {
            Some(s) => s,
            None => Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::once(async {
                    Err(datafusion::error::DataFusionError::Internal(
                        "hive insert: checked source stream executed twice".to_string(),
                    ))
                }),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn a_decimal_column_records_a_hive_decimal_that_reads_back_the_same() {
        // A TPC-H load (s_acctbal DECIMAL(15,2)) used to fail its Hive CTAS
        // with "has no Hive equivalent this release records".
        for dt in [DataType::Decimal128(15, 2), DataType::Decimal128(38, 0)] {
            let name = hive_type_of(&dt).expect("decimal maps");
            let back = sqe_catalog::hive::serde_map::arrow_type(&name).expect("reader parses");
            assert_eq!(back, dt, "{name} must read back as {dt}");
        }
        assert_eq!(hive_type_of(&DataType::Decimal128(10, -2)), None, "Hive has no negative scale");
        assert_eq!(hive_type_of(&DataType::Decimal256(40, 2)), None, "beyond Hive's 38 digits");
    }

    #[test]
    fn a_plain_insert_is_allowed() {
        assert!(reject_overwrite(&parse("INSERT INTO t SELECT 1")).is_ok());
    }

    #[test]
    fn insert_overwrite_is_refused_and_says_why_it_cannot_be_atomic() {
        let err = reject_overwrite(&parse("INSERT OVERWRITE t SELECT 1"))
            .expect_err("overwrite must be refused");
        let msg = err.to_string();
        // The reason has to be in the message. "Not supported" alone sends the
        // user looking for a flag to turn on.
        assert!(msg.contains("atomic"), "the reason must be stated: {msg}");
        assert!(
            msg.contains("Iceberg"),
            "the message must name the alternative: {msg}"
        );
    }

    #[test]
    fn replace_into_is_refused_too() {
        // `REPLACE INTO` parses to the same statement with `replace_into` set
        // rather than `overwrite`. Checking only `overwrite` would let this
        // reach the sink and append.
        let err = reject_overwrite(&parse("REPLACE INTO t SELECT 1"));
        assert!(
            err.is_err(),
            "REPLACE INTO must not fall through to an append"
        );
    }
}

/// Map an Arrow type back to the Hive type name a manifest records.
///
/// The inverse of `serde_map::arrow_type_of`, and deliberately narrower than
/// it: only the types that round-trip. A CTAS whose query produces something
/// unmappable fails at CREATE, naming the column, rather than writing a
/// manifest the read path cannot parse. That is the difference between an error
/// and a table nobody can select from.
fn hive_type_of(dt: &DataType) -> Option<String> {
    // `decimal(p,s)`: Hive's precision limit is 38, the same as Decimal128's,
    // and the reader (`serde_map::arrow_type`) parses it back to the same
    // Decimal128. Hive has no negative scale; Decimal256 can exceed 38 digits.
    if let DataType::Decimal128(p, s) = dt {
        return (*s >= 0).then(|| format!("decimal({p},{s})"));
    }
    Some(match dt {
        DataType::Int8 => "tinyint",
        DataType::Int16 => "smallint",
        DataType::Int32 => "int",
        DataType::Int64 => "bigint",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Boolean => "boolean",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::Date32 => "date",
        DataType::Timestamp(_, None) => "timestamp",
        _ => return None,
    }
    .to_string())
}

/// The partition key names a `PARTITIONED BY` clause declares, with the type
/// each one was declared as when it was given one.
///
/// sqlparser puts Hive's `PARTITIONED BY` in `hive_distribution`, NOT in
/// `partition_by` (which is the BigQuery/ClickHouse expression form and stays
/// `None` here). Reading the wrong field yields an unpartitioned table from a
/// statement that asked for partitions, silently.
///
/// Both spellings are accepted because both are in the wild:
/// `PARTITIONED BY (dt string)` declares the type (Hive/Athena), and
/// `PARTITIONED BY (dt)` refers to a column the query already produces
/// (Spark/Trino), leaving the type to be taken from the query schema.
fn declared_partition_keys(ct: &sqlparser::ast::CreateTable) -> Vec<(String, Option<String>)> {
    use sqlparser::ast::HiveDistributionStyle;
    match &ct.hive_distribution {
        HiveDistributionStyle::PARTITIONED { columns } => columns
            .iter()
            .map(|c| {
                let declared = match &c.data_type {
                    sqlparser::ast::DataType::Unspecified => None,
                    other => Some(other.to_string()),
                };
                (c.name.value.clone(), declared)
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Build the manifest spec for `CREATE TABLE <hive> ... AS SELECT`.
///
/// `query_schema` is the SELECT's output schema, which is where the column list
/// comes from: a CTAS need not declare one, and when it does the query is still
/// the authority on types.
///
/// ## Partition columns must be the trailing ones
///
/// Hive's own convention, and load-bearing rather than stylistic. The read path
/// derives its file schema as "the full schema minus the trailing
/// `partition_keys.len()` fields", and the worker's
/// `append_partition_columns` splits at the same index. A partition column in
/// the middle of the query's output would put a file column where a partition
/// column is expected, with no error and the wrong values.
///
/// So this VALIDATES the order rather than reordering to fix it. Reordering
/// would silently change which column the user's data lands in.
pub fn ctas_spec(
    ct: &sqlparser::ast::CreateTable,
    query_schema: &Schema,
) -> Result<sqe_sql::hive_ddl::HiveTableSpec, SqeError> {
    use sqe_sql::hive_ddl::{HiveColumn, HiveTableSpec};

    if ct.or_replace {
        // `DROP TABLE` on a Hive external table deletes the MANIFEST and never
        // the data, which is the documented cycle-1 contract. So
        // `CREATE OR REPLACE ... AS SELECT` would write new files into a
        // directory that still holds the old ones, and the table would read
        // back as the union of both. Refused rather than shipped.
        return Err(SqeError::NotImplemented(
            "hive_external: CREATE OR REPLACE TABLE ... AS SELECT is not supported for Hive \
             external tables. DROP TABLE removes the manifest but never the data, so the \
             replacement would read back as the union of the old and new rows. Drop the table \
             and delete its data directory, then create it again."
                .to_string(),
        ));
    }

    let location = ct
        .hive_formats
        .as_ref()
        .and_then(|f| f.location.clone())
        .ok_or_else(|| {
            // Required, not defaulted. SQE never invents a data path for an
            // external table -- cycle 1 made LOCATION mandatory on
            // CREATE EXTERNAL TABLE for the same reason, and guessing one here
            // would write a user's data somewhere they did not name.
            SqeError::Execution(
                "hive_external: CREATE TABLE ... AS SELECT against a Hive root requires an \
                 explicit LOCATION, because SQE does not choose data paths for external \
                 tables. Add LOCATION 's3://bucket/prefix/' before AS SELECT."
                    .to_string(),
            )
        })?;

    let parts: Vec<String> = ct
        .name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
        .collect();
    let (catalog, database, name) = match parts.as_slice() {
        [t] => (None, None, t.clone()),
        [d, t] => (None, Some(d.clone()), t.clone()),
        [c, d, t] => (Some(c.clone()), Some(d.clone()), t.clone()),
        _ => {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: `{}` is not a one, two, or three part table name",
                ct.name
            )))
        }
    };

    let declared = declared_partition_keys(ct);
    let n_fields = query_schema.fields().len();
    if declared.len() >= n_fields {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "hive_external: the query produces {n_fields} column(s) and {} of them were \
             declared as partition keys, leaving nothing to store in the data files",
            declared.len()
        )));
    }
    let split = n_fields - declared.len();

    // Validate the trailing-column convention rather than silently reordering.
    for (i, (key, _)) in declared.iter().enumerate() {
        let field = query_schema.field(split + i);
        if !field.name().eq_ignore_ascii_case(key) {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: partition column `{key}` must be column {} of the SELECT, but \
                 that column is `{}`. Hive stores partition columns in the directory name and \
                 the read path expects them last, so the SELECT must end with them in the \
                 declared order.",
                split + i + 1,
                field.name()
            )));
        }
    }

    let to_column = |field: &arrow_schema::Field,
                     declared_type: Option<&String>|
     -> Result<HiveColumn, SqeError> {
        let sql_type = match declared_type {
            Some(t) => t.to_ascii_lowercase(),
            None => hive_type_of(field.data_type())
                .ok_or_else(|| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "hive_external: column `{}` has type {} which has no Hive equivalent \
                         this release records. Cast it in the SELECT, or declare the column \
                         type explicitly.",
                        field.name(),
                        field.data_type()
                    ))
                })?,
        };
        Ok(HiveColumn {
            name: field.name().clone(),
            sql_type,
            comment: None,
        })
    };

    let mut columns = Vec::with_capacity(split);
    for field in query_schema.fields().iter().take(split) {
        columns.push(to_column(field, None)?);
    }
    let mut partition_keys = Vec::with_capacity(declared.len());
    for (i, (_, declared_type)) in declared.iter().enumerate() {
        partition_keys.push(to_column(
            query_schema.field(split + i),
            declared_type.as_ref(),
        )?);
    }

    let stored_as = match ct.hive_formats.as_ref().and_then(|f| f.storage.as_ref()) {
        // Matched on the variant, not formatted from it. `HiveIOFormat` has no
        // `Display`, and munging its `Debug` output would break the moment
        // sqlparser renamed a field.
        Some(sqlparser::ast::HiveIOFormat::FileFormat { format }) => format!("{format}"),
        // Both remaining variants enumerated rather than caught by a wildcard.
        // A variant added to `HiveIOFormat` later must be a compile error here,
        // not silently treated as parquet -- that would write parquet files
        // into a table whose manifest claims a different format, and the
        // failure would surface on the next SELECT.
        Some(sqlparser::ast::HiveIOFormat::IOF { .. })
        | Some(sqlparser::ast::HiveIOFormat::Using { .. }) => {
            return Err(SqeError::NotImplemented(
                "hive_external: CREATE TABLE ... AS SELECT with an explicit INPUTFORMAT / \
                 OUTPUTFORMAT or USING clause is not supported. Use STORED AS PARQUET, or \
                 omit the clause."
                    .to_string(),
            ))
        }
        // Parquet rather than Hive's own TEXTFILE default, on purpose: a CTAS
        // is SQE writing the files, and writing text would discard the types
        // the query schema just established.
        None => "PARQUET".to_string(),
    };

    Ok(HiveTableSpec {
        catalog,
        database,
        name,
        if_not_exists: ct.if_not_exists,
        comment: None,
        columns,
        partition_keys,
        location,
        serde_class: None,
        serde_properties: BTreeMap::new(),
        stored_as: Some(stored_as),
        input_format: None,
        output_format: None,
        table_properties: BTreeMap::new(),
    })
}

/// The SELECT text of a CTAS, ready to plan.
///
/// Rendered back from the AST rather than sliced out of the original SQL,
/// because the original may carry the `PARTITIONED BY` and `LOCATION` clauses
/// interleaved and a textual split would take them along.
pub fn ctas_select_sql(ct: &sqlparser::ast::CreateTable) -> Option<String> {
    ct.query
        .as_ref()
        .map(|q| sqe_sql::rewrite_named_tvf_args(&format!("{q}")))
}

/// The `INSERT INTO` that loads a freshly created CTAS table.
///
/// Reuses the INSERT path deliberately: it already carries the fan-out, the
/// partition-index refresh, the ordering guarantee, and the session-cache
/// invalidation, all of which a second loader would have to reimplement and
/// could get subtly different.
pub fn ctas_load_sql(table: &sqlparser::ast::ObjectName, select_sql: &str) -> String {
    format!("INSERT INTO {table} {select_sql}")
}

#[cfg(test)]
mod ctas_tests {
    use super::*;
    use arrow_schema::Field;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn ct(sql: &str) -> sqlparser::ast::CreateTable {
        match Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .remove(0)
        {
            Statement::CreateTable(ct) => ct,
            other => panic!("not a CREATE TABLE: {other:?}"),
        }
    }

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn partition_keys_come_from_hive_distribution_not_partition_by() {
        // sqlparser puts Hive's PARTITIONED BY in `hive_distribution` and leaves
        // `partition_by` as None. Reading the wrong field yields an
        // UNPARTITIONED table from a statement that asked for partitions, with
        // no error -- so this asserts the keys actually arrive.
        let c = ct(
            "CREATE TABLE lake.sales.o (id BIGINT) PARTITIONED BY (dt string) \
             LOCATION 's3://b/p/' AS SELECT 1 AS id, 'x' AS dt",
        );
        assert!(
            c.partition_by.is_none(),
            "the field a naive reading would use"
        );
        let spec = ctas_spec(
            &c,
            &schema(&[("id", DataType::Int64), ("dt", DataType::Utf8)]),
        )
        .expect("spec");
        assert_eq!(
            spec.partition_keys
                .iter()
                .map(|k| k.name.as_str())
                .collect::<Vec<_>>(),
            vec!["dt"]
        );
        assert_eq!(
            spec.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id"],
            "the partition column must NOT also be a stored column"
        );
        assert_eq!(spec.stored_as.as_deref(), Some("PARQUET"));
    }

    #[test]
    fn an_untyped_partition_key_takes_its_type_from_the_query() {
        // Spark/Trino spelling: `PARTITIONED BY (dt)` with no type.
        let c = ct(
            "CREATE TABLE lake.sales.o PARTITIONED BY (dt) LOCATION 's3://b/p/' \
             AS SELECT 1 AS id, 'x' AS dt",
        );
        let spec = ctas_spec(
            &c,
            &schema(&[("id", DataType::Int64), ("dt", DataType::Utf8)]),
        )
        .expect("spec");
        assert_eq!(spec.partition_keys[0].sql_type, "string");
        assert_eq!(spec.columns[0].sql_type, "bigint");
    }

    #[test]
    fn a_partition_column_that_is_not_last_is_an_error_not_a_reorder() {
        // The read path splits the schema at `len() - partition_keys.len()`, so
        // a partition column in the middle puts a file column where a partition
        // column is expected. Reordering to "fix" it would silently move the
        // user's data into a different column, so it errors instead.
        let c = ct(
            "CREATE TABLE lake.sales.o PARTITIONED BY (dt) LOCATION 's3://b/p/' \
             AS SELECT 'x' AS dt, 1 AS id",
        );
        let err = ctas_spec(
            &c,
            &schema(&[("dt", DataType::Utf8), ("id", DataType::Int64)]),
        )
        .expect_err("dt is first, not last");
        let msg = err.to_string();
        assert!(msg.contains("must be column 2"), "{msg}");
        assert!(
            msg.contains("`id`"),
            "the message must name what is there: {msg}"
        );
    }

    #[test]
    fn a_missing_location_is_refused_rather_than_invented() {
        let c = ct("CREATE TABLE lake.sales.o AS SELECT 1 AS id");
        let err = ctas_spec(&c, &schema(&[("id", DataType::Int64)])).expect_err("no LOCATION");
        let msg = err.to_string();
        assert!(msg.contains("LOCATION"), "{msg}");
        assert!(
            msg.contains("does not choose data paths"),
            "the message must say WHY it is required: {msg}"
        );
    }

    #[test]
    fn or_replace_is_refused_because_drop_leaves_the_data() {
        let c = ct("CREATE OR REPLACE TABLE lake.sales.o LOCATION 's3://b/p/' AS SELECT 1 AS id");
        let err = ctas_spec(&c, &schema(&[("id", DataType::Int64)])).expect_err("or_replace");
        let msg = err.to_string();
        assert!(
            msg.contains("union"),
            "the message must explain that the result would mix old and new rows: {msg}"
        );
    }

    #[test]
    fn a_type_with_no_hive_equivalent_fails_at_create_not_at_read() {
        // A manifest the read path cannot parse is worse than a rejected CREATE:
        // the table would exist and nobody could select from it.
        let c = ct("CREATE TABLE lake.sales.o LOCATION 's3://b/p/' AS SELECT 1 AS id");
        let err = ctas_spec(
            &c,
            &schema(&[("id", DataType::Duration(arrow_schema::TimeUnit::Second))]),
        )
        .expect_err("duration has no hive type");
        assert!(err.to_string().contains("no Hive equivalent"), "{err}");
    }

    #[test]
    fn declaring_every_column_a_partition_key_leaves_nothing_to_store() {
        let c = ct(
            "CREATE TABLE lake.sales.o PARTITIONED BY (dt) LOCATION 's3://b/p/' \
             AS SELECT 'x' AS dt",
        );
        let err =
            ctas_spec(&c, &schema(&[("dt", DataType::Utf8)])).expect_err("no file columns left");
        assert!(err.to_string().contains("nothing to store"), "{err}");
    }

    // #729: the target's row filter gates the append, not only the source
    // SELECT. Target is a plain DataFusion listing table (the same
    // `ListingTable::insert_into` a `HiveTableProvider` delegates to).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hive_insert_refuses_rows_outside_target_row_filter_and_writes_nothing() {
        use datafusion::logical_expr::{col, lit};
        use sqe_policy::plan_rewriter::PolicyPlanRewriter;
        use sqe_policy::policy_store::InMemoryPolicyStore;
        use sqe_policy::ResolvedPolicy;

        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.row_filters.push(col("region").eq(lit("EU")));
        store.add_table_policy("lake.sales", "employees", pol).await;
        let enforcer = PolicyPlanRewriter::new(Arc::new(store));
        let user = sqe_core::Session::new(
            "alice".to_string(),
            "tok".to_string().into(),
            None,
            chrono::Utc::now(),
            vec![],
        )
        .user;

        let dir = tempfile::tempdir().unwrap();
        // The session's default catalog is the Hive root, and the statements
        // below name the target unqualified: the policy key must still be
        // `lake.sales`, the key the read path resolves the same table under.
        // `datafusion.public` stays registered: the WITH CHECK scratch table
        // lives there.
        use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
        let ctx = SessionContext::new();
        let lake = Arc::new(MemoryCatalogProvider::new());
        lake.register_schema("sales", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        ctx.register_catalog("lake", lake);
        ctx.sql("SET datafusion.catalog.default_catalog = 'lake'")
            .await
            .unwrap();
        ctx.sql("SET datafusion.catalog.default_schema = 'sales'")
            .await
            .unwrap();
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE employees (id BIGINT, region VARCHAR) \
             STORED AS PARQUET LOCATION '{}/'",
            dir.path().display()
        ))
        .await
        .unwrap();
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1_i64, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["EU", "US"])),
            ],
        )
        .unwrap();
        ctx.register_batch("src", src).unwrap();

        let run = |sql: &'static str| {
            let ctx = &ctx;
            let enforcer = &enforcer;
            let user = &user;
            async move {
                let mut summary = None;
                let stmt = Parser::parse_sql(&GenericDialect {}, sql)
                    .unwrap()
                    .remove(0);
                handle_insert(ctx, &stmt, sql, enforcer, user, &mut summary).await
            }
        };
        let files = || std::fs::read_dir(dir.path()).unwrap().count();

        let err = run("INSERT INTO employees SELECT * FROM src")
            .await
            .expect_err("a US row must be refused by the EU row filter");
        assert!(err.to_string().contains("fall outside"), "{err}");
        assert_eq!(
            files(),
            0,
            "nothing may be written when the statement is refused"
        );

        let ok = run("INSERT INTO employees SELECT * FROM src WHERE region = 'EU'")
            .await
            .expect("EU-only rows are admitted");
        let n = ok[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 1);
        assert_eq!(files(), 1);
    }
}
