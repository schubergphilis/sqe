//! The write-statement family: `MERGE INTO`, `INSERT INTO ... SELECT` and
//! `CREATE [OR REPLACE] TABLE ... AS SELECT`. Hive external targets take the
//! `hive_write` path; Iceberg targets go to the `WriteHandler`'s streaming
//! insert, merge and CTAS dispatch, and the policy summaries are merged here.

use super::*;

impl QueryHandler {
    /// Runs `MERGE INTO`: executes the source query, then hands its batches
    /// to the write handler's merge dispatch and merges the policy summaries.
    #[allow(clippy::borrowed_box, clippy::too_many_arguments)]
    pub(super) async fn handle_merge_statement(
        &self,
        session: &Session,
        stmt: &Box<Statement>,
        query_id: uuid::Uuid,
        memory: &crate::query_memory::StatementMemory,
        plan_metrics: &Arc<Mutex<PlanMetrics>>,
        captured_plan: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Extract source SQL from the MERGE statement and execute it
        // to get the source batches, then pass them to the write handler.
        let source_sql = if let Statement::Merge(merge) = stmt.as_ref() {
            merge_source_sql(&merge.source)?
        } else {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                "Expected MERGE statement",
            ));
        };
        // Capture the MERGE source plan from `execute_query`. The
        // lineage for a MERGE event should describe the source's
        // TableScans as inputs and the target as the output, so
        // the write handler re-wraps this source plan as a
        // synthetic INSERT against the target.
        let mut merge_source_plan: Option<sqe_lineage::PlanOrHint> = None;
        // The MERGE source's policy summary is recorded against the
        // outer MERGE statement's audit entry via `policy_summary`.
        let source_batches = self
            .execute_query(
                session,
                &source_sql,
                &query_id,
                memory,
                plan_metrics,
                &mut merge_source_plan,
                &mut *policy_summary,
            )
            .await?;
        // The collected source keeps its reservation in the
        // statement's pool, and the target rewrite reserves from
        // the same pool: one MERGE, one quota (#788).
        let (ctx, session_catalog) = self.write_session_context(session, memory).await?;
        let merge_source_logical = match merge_source_plan {
            Some(sqe_lineage::PlanOrHint::Plan(p)) => Some(*p),
            _ => None,
        };
        // A source that returned zero batches carries no schema, but
        // WHEN NOT MATCHED BY SOURCE still has to run against the
        // whole target (architecture review 501). Hand the write
        // handler the source plan's Arrow schema so it can register
        // an empty but typed source relation.
        let merge_source_schema = merge_source_logical
            .as_ref()
            .map(|p| Arc::new(p.schema().as_arrow().clone()));
        // Dispatch on `write.merge.mode` table property. Default CoW
        // matches prior behaviour; `merge-on-read` routes to the
        // equality-delete path when the target has a declared PK.
        // The target's own policy (row filter / masks, 202) is
        // audited together with the source summary above.
        let mut target_summary: Option<sqe_policy::PolicySummary> = None;
        let merged = self
            .write_handler
            .handle_merge_dispatch(
                session,
                stmt,
                source_batches,
                merge_source_schema,
                session_catalog,
                &ctx,
                merge_source_logical,
                &mut *captured_plan,
                &mut target_summary,
            )
            .await;
        if let Some(target) = target_summary {
            match policy_summary.as_mut() {
                Some(source) => source.merge(&target),
                None => *policy_summary = Some(target),
            }
        }
        merged
    }

    /// Runs `INSERT INTO ... SELECT`: a Hive external table goes through
    /// `hive_write::handle_insert`, an Iceberg one through the write handler's
    /// streaming insert.
    #[allow(clippy::borrowed_box)]
    pub(super) async fn handle_insert_statement(
        &self,
        session: &Session,
        stmt: &Box<Statement>,
        memory: &crate::query_memory::StatementMemory,
        captured_plan: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        if let Statement::Insert(ref ins) = **stmt {
            // A Hive external table takes a different path entirely.
            // The Iceberg one below ends in `commit_written_files`,
            // an atomic metadata swap, and a Hive table has no
            // metadata pointer for that to touch. Routing through
            // the session context is what makes DataFusion call
            // `HiveTableProvider::insert_into`.
            if let sqlparser::ast::TableObject::TableName(ref name) = ins.table {
                let hive_ref = table_reference_of(name);
                if self.is_hive_table(session, &hive_ref) {
                    // A Hive table has no branches: its files are
                    // visible once written, so it cannot hold a
                    // transaction's writes back (#821).
                    if session.transaction_id.is_some() {
                        return Err(autocommit_write_conflict(session));
                    }
                    // Syntax first: an OVERWRITE is refused on the
                    // statement alone, before any table lookup, so
                    // the refusal cannot be masked by a lookup
                    // error on a table that does not exist.
                    crate::hive_write::reject_overwrite(stmt)?;
                    // Gate the target before any object lands (503):
                    // the post-write index refresh repeats the check,
                    // but by then the files are in the bucket.
                    self.hive_ddl_handler
                        .refuse_write_if_denied(session, &hive_ref, "INSERT")
                        .await?;
                    let (ctx, _) = self.write_session_context(session, memory).await?;
                    let written = crate::hive_write::handle_insert(
                        &ctx,
                        stmt,
                        &format!("{stmt}"),
                        self.policy_enforcer.as_ref(),
                        &session.user,
                        &mut *policy_summary,
                    )
                    .await?;

                    // Data first, then the index. The reverse order
                    // makes the index promise files that do not
                    // exist yet; this order can only leave data
                    // that is intact and not yet visible, which
                    // MSCK REPAIR TABLE recovers exactly.
                    if let Err(e) = self
                        .hive_ddl_handler
                        .refresh_index_after_write(session, &hive_ref)
                        .await
                    {
                        // Naming the recovery is the whole point. A
                        // generic error here sends the user looking
                        // for data that is sitting in the bucket.
                        return Err(SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!(
                                "hive_external: the rows were WRITTEN but are not yet                                          visible: refreshing the partition index failed ({e}).                                          The data files are on storage and intact. Run                                          `MSCK REPAIR TABLE {hive_ref}` to make them visible."
                            ),
                        ));
                    }
                    // Drop the cached session context, the way
                    // the Iceberg CTAS and INSERT arms do. Measured:
                    // a SELECT before the write caches the table
                    // provider, and the next SELECT on that same
                    // context returns the PRE-write rows while a
                    // fresh context returns the correct ones. The
                    // index was already refreshed above; without
                    // this the refresh is invisible, which is
                    // indistinguishable from the refresh failing.
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    return Ok(written);
                }
            }
            let select_sql = ins
                .source
                .as_ref()
                .map(|q| sqe_sql::rewrite_named_tvf_args(&format!("{q}")))
                .ok_or_else(|| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        "INSERT without SELECT source",
                    )
                })?;
            let (ctx, _) = self.write_session_context(session, memory).await?;
            self.write_handler
                .handle_insert_streaming(
                    session,
                    stmt,
                    &ctx,
                    &select_sql,
                    &mut *captured_plan,
                    &mut *policy_summary,
                )
                .await
        } else {
            Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                "Expected Insert statement",
            ))
        }
    }

    /// Runs a `CREATE [OR REPLACE] TABLE ... AS SELECT` statement. A target
    /// that is a Hive external table takes its own path: the manifest is
    /// created first, then the rows are loaded through the Hive INSERT path,
    /// and the partition index and session cache are refreshed. Any other
    /// target goes to the write handler's streaming CTAS, which also owns
    /// `OR REPLACE`. A statement without a SELECT, or one that is not a
    /// `CREATE TABLE`, is an error.
    #[allow(clippy::borrowed_box)]
    pub(super) async fn handle_ctas_statement(
        &self,
        session: &Session,
        stmt: &Box<Statement>,
        memory: &crate::query_memory::StatementMemory,
        captured_plan: &mut Option<sqe_lineage::PlanOrHint>,
        policy_summary: &mut Option<sqe_policy::PolicySummary>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        if let Statement::CreateTable(ref ct) = **stmt {
            // A Hive external table takes its own path. CTAS is
            // TWO operations, and the order is the opposite of the
            // INSERT path's:
            //
            // - manifest first, then data. A failure leaves an
            //   empty table: SELECT returns no rows, SHOW CREATE
            //   TABLE works, DROP TABLE cleans up, re-running fixes
            //   it.
            // - data first would leave orphaned files under a path
            //   no table names. Nothing could find them, and
            //   MSCK REPAIR does not apply because repair needs a
            //   manifest to repair.
            //
            // The INSERT path reasons the other way ("data first,
            // the index can only be behind") and that is correct
            // THERE, because the table already exists. When it does
            // not, the failure modes swap.
            if self.is_hive_table(session, &table_reference_of(&ct.name)) {
                let (ctx, _) = self.create_session_context(session).await?;
                let select_sql = crate::hive_write::ctas_select_sql(ct).ok_or_else(|| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, "CTAS without SELECT query")
                })?;

                // Plan the SELECT for its schema only. The column
                // list and the partition-key types both come from
                // the query, because a CTAS need not declare them.
                // Analyzed, so the columns carry the types the
                // load will write (#839).
                let schema = sqe_catalog::file_tvf_prepare::plan_sql(&ctx, &select_sql)
                    .await
                    .and_then(|df| {
                        sqe_catalog::file_tvf_prepare::analyzed_schema(&ctx, df.logical_plan())
                    })
                    .map_err(|e| {
                        SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!("hive_external: CTAS query does not plan: {e}"),
                        )
                    })?
                    .as_arrow()
                    .clone();
                let spec = crate::hive_write::ctas_spec(ct, &schema)?;

                // 1. Manifest.
                self.hive_ddl_handler
                    .create_external_table(session, &spec)
                    .await?;
                crate::session_context::invalidate_session_cache(&session.user.username).await;

                // 2. Data, through the INSERT path, so the fan-out,
                // the index refresh and the cache invalidation are
                // the same code the plain INSERT uses rather than a
                // second copy that could drift.
                let load = crate::hive_write::ctas_load_sql(&ct.name, &select_sql);
                // Parsed with the same dialect the session uses, so
                // the synthesized INSERT cannot parse differently
                // from one a client sent.
                let load_stmt = sqlparser::parser::Parser::parse_sql(
                    &sqlparser::dialect::DuckDbDialect {},
                    &load,
                )
                .map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("hive_external: CTAS load statement does not parse: {e}"),
                    )
                })?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        "hive_external: CTAS load produced no statement".to_string(),
                    )
                })?;
                // Gate the target before any object lands (503).
                self.hive_ddl_handler
                    .refuse_write_if_denied(
                        session,
                        &table_reference_of(&ct.name),
                        "CREATE TABLE AS SELECT",
                    )
                    .await?;
                // A fresh context: the manifest step above
                // invalidated the session cache, so the file
                // functions prepared on the old one are gone.
                // Without this a Hive CTAS from read_parquet
                // failed with "the table function was not
                // prepared before planning".
                let (load_ctx, _) = self.write_session_context(session, memory).await?;
                let written = crate::hive_write::handle_insert(
                    &load_ctx,
                    &load_stmt,
                    &load,
                    self.policy_enforcer.as_ref(),
                    &session.user,
                    &mut *policy_summary,
                )
                .await?;
                if let Err(e) = self
                    .hive_ddl_handler
                    .refresh_index_after_write(session, &table_reference_of(&ct.name))
                    .await
                {
                    return Err(SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!(
                            "hive_external: the table was created and its rows WERE                                      written, but they are not yet visible: refreshing the                                      partition index failed ({e}). Run `MSCK REPAIR TABLE {}`                                      to make them visible.",
                            ct.name
                        ),
                    ));
                }
                crate::session_context::invalidate_session_cache(&session.user.username).await;
                return Ok(written);
            }
            if let Some(ref query) = ct.query {
                // CREATE OR REPLACE TABLE AS SELECT is handled
                // inside the write handler (architecture review
                // 500). It used to drop the target here first —
                // against the DEFAULT warehouse, whatever catalog
                // the name qualified, and before the SELECT had
                // even been planned. The handler resolves the
                // target catalog, keeps the existing table until
                // the new data is written, and only then swaps.
                // The post-commit cache invalidation moved with it
                // (`WriteHandler::after_commit`, 507).
                let select_sql = sqe_sql::rewrite_named_tvf_args(&format!("{query}"));
                let (ctx, _) = self.write_session_context(session, memory).await?;
                self.write_handler
                    .handle_ctas_streaming(
                        session,
                        stmt,
                        &ctx,
                        &select_sql,
                        &mut *captured_plan,
                        &mut *policy_summary,
                    )
                    .await
            } else {
                Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "CTAS without SELECT query",
                ))
            }
        } else {
            Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                "Expected CreateTable statement",
            ))
        }
    }
}
