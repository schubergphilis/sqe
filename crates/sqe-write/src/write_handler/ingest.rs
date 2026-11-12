//! Flight SQL bulk ingest (`DoPut` + `CommandStatementIngest`).
//!
//! What an ingest does depends on two things: whether the target table
//! exists, and the client's `TableDefinitionOptions` ([`IngestMode`]). The
//! combination is decided by [`plan_ingest`], a pure function, so the order
//! of the refusals is unit-testable without a catalog. A child module of
//! `write_handler` so it reaches the CTAS schema helpers, the commit helpers
//! and the policy refusal INSERT OVERWRITE uses, without a second copy of any.

use super::*;

/// What to do when the target table does not exist.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnMissing {
    /// Fail with the catalog's "table not found" (the long-standing default).
    #[default]
    Fail,
    /// Create the table from the uploaded stream's Arrow schema.
    Create,
}

/// What to do when the target table exists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnExisting {
    /// Append the rows (the long-standing default).
    #[default]
    Append,
    /// Refuse with `DUPLICATE_TABLE` before any file is written.
    Fail,
    /// Replace the table's rows in one overwrite commit.
    Replace,
}

/// The Flight SQL `TableDefinitionOptions` as the write path sees them. The
/// coordinator maps the wire enums onto it, so `sqe-write` needs no
/// `arrow-flight`. The default, `(Fail, Append)`, is what a request without
/// options (or with `UNSPECIFIED` values) always did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestMode {
    pub on_missing: OnMissing,
    pub on_existing: OnExisting,
}

impl IngestMode {
    /// Whether the plan depends on the table's existence. `(Fail, Append)`
    /// and `(Fail, Replace)` do not: a missing table is the load's own error.
    fn needs_existence_check(self) -> bool {
        self.on_missing == OnMissing::Create || self.on_existing == OnExisting::Fail
    }
}

/// What the ingest does to the target, once the options and the table's
/// existence are known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IngestPlan {
    /// Create the table from the stream schema, then append to it.
    Create,
    /// Append to the table. A missing table is refused by the load that
    /// follows, exactly as before the options were read.
    Append,
    /// Replace the table's rows.
    Replace,
}

/// The session state a refusal depends on.
#[derive(Clone, Copy, Debug)]
struct IngestSession {
    /// A SQL `BEGIN` transaction: DDL is refused in it (#820). A transaction
    /// the client holds (Flight `BeginTransaction`) is not one: the create
    /// runs autocommit, as every SQL DDL does there (#824).
    in_begin_txn: bool,
    /// `WRITE_BRANCH` is set and no transaction is open (#829).
    on_write_branch: bool,
}

impl IngestSession {
    fn of(session: &Session) -> Self {
        Self {
            in_begin_txn: session.transaction_id.is_some() && !session.in_client_transaction(),
            on_write_branch: session.transaction_id.is_none() && session.write_branch.is_some(),
        }
    }
}

/// Decide what an ingest does. The refusals, in the order they apply:
/// an existing table with `OnExisting::Fail` (`DuplicateTable`, before the
/// stream is read), then a create the session cannot make (`BEGIN`
/// transaction, `WRITE_BRANCH`).
fn plan_ingest(
    mode: IngestMode,
    exists: bool,
    session: IngestSession,
    table: &TableIdent,
) -> sqe_core::Result<IngestPlan> {
    if exists {
        return match mode.on_existing {
            OnExisting::Append => Ok(IngestPlan::Append),
            OnExisting::Replace => Ok(IngestPlan::Replace),
            OnExisting::Fail => Err(SqeError::with_code(
                SqeErrorCode::DuplicateTable,
                format!("Table {table} already exists"),
            )),
        };
    }
    match mode.on_missing {
        OnMissing::Fail => Ok(IngestPlan::Append),
        OnMissing::Create if session.in_begin_txn => Err(SqeError::with_code(
            SqeErrorCode::AutocommitWriteConflict,
            "Catalog only supports writes using autocommit: an ingest that creates \
             a table cannot run inside a BEGIN transaction",
        )),
        // Creating the table would land on main before the append is
        // refused on the branch (#829).
        OnMissing::Create if session.on_write_branch => Err(SqeError::with_code(
            SqeErrorCode::NotSupported,
            format!(
                "Ingest would create {table} on main, but the session writes to a \
                 branch; RESET WRITE_BRANCH first"
            ),
        )),
        OnMissing::Create => Ok(IngestPlan::Create),
    }
}

impl WriteHandler {
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
    /// The target schema comes from table metadata. Per-batch field-id
    /// stamping and type casts are applied by the streaming sink exactly as
    /// they are for INSERT (see `apply_stamped_schema`).
    ///
    /// `mode` says what to do when the table is missing or exists
    /// ([`IngestMode`]). `stream_schema` is the Arrow schema the upload
    /// announced; it is read only to create a missing table, so the caller
    /// peeks the stream for it only when `mode.on_missing` is `Create`.
    ///
    /// `handle_ingest` (the batched signature) is retained for internal callers
    /// and the name-parsing tests.
    ///
    /// `pool` is the pool the fan-out writer's buffers reserve from: the
    /// caller's per-query quota pool (#788). `None` leaves them uncharged.
    pub async fn handle_ingest_streaming<S, E>(
        &self,
        session: &Session,
        table_name: &str,
        pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
        mode: IngestMode,
        stream_schema: Option<arrow_schema::SchemaRef>,
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
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Invalid table name for ingest: {table_name}"),
                ));
            }
        };

        let namespace = iceberg::NamespaceIdent::new(namespace_str.to_string());
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            ?mode,
            "Executing streaming DoPut ingest"
        );

        let session_catalog = self
            .resolve_session_catalog(session, target_catalog)
            .await?;
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();

        // Existence, options and session decide what happens before the
        // stream is read or a file is written. Admission comes before a
        // create, so a refused admission leaves no orphan table.
        let step = self
            .prepare_ingest_target(
                session,
                &session_catalog,
                &table_ident,
                mode,
                stream_schema.as_deref(),
            )
            .await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;
        // A caller who may not write is refused here, before any file (#880).
        session_catalog
            .check_write_access(&table_ident, &table)
            .await?;
        let replace = step == IngestPlan::Replace;
        if replace {
            self.refuse_replace_under_policy(
                session,
                target_catalog.map(str::to_string),
                &table_ident,
                &table,
                "ingest replace",
            )
            .await?;
        }

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
                |e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("Failed to derive ingest Arrow schema: {e}"),
                    )
                },
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
            // Flight DoPut has no session context; the caller hands in the
            // quota pool instead.
            self.fanout_limits(pool),
        )
        .await?;

        if replace {
            // One overwrite snapshot that removes the old files and adds the
            // new ones. An empty upload still commits: it truncates, as an
            // INSERT OVERWRITE of no rows does. A failure leaves the table as
            // it was; the guard deletes the files this ingest wrote.
            self.commit_written_files(session, &session_catalog, &table_ident, data_files, true)
                .await
                .inspect_err(|e| cleanup_guard.keep_if_outcome_unknown(e))?;
            cleanup_guard.mark_committed();
            info!(
                table = %table_ident,
                total_rows,
                "Streaming DoPut ingest replaced the table's rows"
            );
        } else if !data_files.is_empty() {
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
            .inspect_err(|e| cleanup_guard.keep_if_outcome_unknown(e))
            .map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to commit ingest transaction: {e}"),
                )
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

    /// Settle what the ingest does to `table_ident` before the stream is read:
    /// look the table up when the options depend on it, apply [`plan_ingest`],
    /// admit the write, and create the table if that is the plan. Returns
    /// what the write does to the table that now exists.
    async fn prepare_ingest_target(
        &self,
        session: &Session,
        session_catalog: &Arc<SessionCatalog>,
        table_ident: &TableIdent,
        mode: IngestMode,
        stream_schema: Option<&ArrowSchema>,
    ) -> sqe_core::Result<IngestPlan> {
        let state = IngestSession::of(session);
        // A mode that does not depend on existence reads as an existing
        // table: the load that follows reports a missing one, as it always did.
        let exists = if mode.needs_existence_check() {
            session_catalog.table_exists(table_ident).await?
        } else {
            true
        };
        let plan = plan_ingest(mode, exists, state, table_ident)?;
        session_catalog.admit_txn_write(table_ident).await?;
        if plan != IngestPlan::Create {
            return Ok(plan);
        }

        let schema = stream_schema.ok_or_else(|| {
            SqeError::with_code(
                SqeErrorCode::InvalidArguments,
                "The ingest stream announced no schema, so the table cannot be created",
            )
        })?;
        if let Err(created) = self
            .create_table_from_stream_schema(session, session_catalog, table_ident, schema)
            .await
        {
            // Another client may have created it between the check and the
            // create; the table exists then, and `on_existing` applies to it.
            if !session_catalog
                .table_exists(table_ident)
                .await
                .unwrap_or(false)
            {
                return Err(created);
            }
            info!(table = %table_ident, "Lost the create race; ingesting into the existing table");
            return plan_ingest(mode, true, state, table_ident);
        }
        // The table is this request's own and empty: the rows are appended
        // whatever `on_existing` says.
        Ok(IngestPlan::Append)
    }

    /// Create `table_ident` from the uploaded stream's Arrow schema through
    /// the schema conversion CTAS uses, with the caller's catalog credentials.
    /// Runs autocommit even inside a client-held transaction, as SQL DDL does
    /// (#824): a rollback then leaves the new table empty.
    async fn create_table_from_stream_schema(
        &self,
        session: &Session,
        session_catalog: &Arc<SessionCatalog>,
        table_ident: &TableIdent,
        stream_schema: &ArrowSchema,
    ) -> sqe_core::Result<()> {
        if stream_schema.fields().is_empty() {
            return Err(SqeError::with_code(
                SqeErrorCode::InvalidArguments,
                "The ingest stream has no columns, so the table cannot be created",
            ));
        }
        // Same derivation as CTAS: a variant column upgrades to v3, and
        // timestamps narrow to what the format version holds (#773, #849).
        let format_version = if arrow_schema_has_variant(stream_schema) {
            FormatVersion::V3
        } else {
            self.format_version()
        };
        let arrow_schema = timestamps_for_format_version(stream_schema, format_version);
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;
        let rest_catalog = matches!(
            self.config.catalog.backend,
            sqe_core::config::CatalogBackend::Rest
        );
        let mut props = create_table_properties(rest_catalog, format_version, &[]);
        record_json_columns(
            &mut props,
            sqe_core::json_columns::marked_columns(&arrow_schema),
        );

        let namespace = table_ident.namespace().clone();
        let name = table_ident.name().to_string();
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();
        sqe_catalog::txn::scope(None, async {
            ensure_namespace(catalog.as_ref(), &namespace).await?;
            let location = unique_table_location(catalog.as_ref(), &namespace, &name).await;
            let creation = TableCreation::builder()
                .name(name.clone())
                .schema(iceberg_schema)
                .location_opt(location)
                .format_version(format_version)
                .properties(props)
                .build();
            create_table_at_location(catalog.as_ref(), &namespace, creation)
                .await
                .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;
            info!(table = %table_ident, "Table created from the ingest stream schema");
            // A new table changes what the caller's cached session context
            // can resolve (507).
            self.after_commit(session, session_catalog.as_ref(), table_ident)
                .await;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident() -> TableIdent {
        TableIdent::from_strs(["ns", "t"]).unwrap()
    }

    const PLAIN: IngestSession = IngestSession {
        in_begin_txn: false,
        on_write_branch: false,
    };

    fn mode(on_missing: OnMissing, on_existing: OnExisting) -> IngestMode {
        IngestMode {
            on_missing,
            on_existing,
        }
    }

    fn plan(m: IngestMode, exists: bool, s: IngestSession) -> sqe_core::Result<IngestPlan> {
        plan_ingest(m, exists, s, &ident())
    }

    #[test]
    fn default_mode_is_fail_append() {
        assert_eq!(
            IngestMode::default(),
            mode(OnMissing::Fail, OnExisting::Append)
        );
    }

    #[test]
    fn default_mode_appends_and_leaves_a_missing_table_to_the_load() {
        let m = IngestMode::default();
        assert_eq!(plan(m, true, PLAIN).unwrap(), IngestPlan::Append);
        assert_eq!(plan(m, false, PLAIN).unwrap(), IngestPlan::Append);
        assert!(
            !m.needs_existence_check(),
            "the default costs no extra lookup"
        );
    }

    #[test]
    fn every_adbc_mode_on_a_missing_and_an_existing_table() {
        // append = (FAIL, APPEND), create = (CREATE, FAIL),
        // create_append = (CREATE, APPEND), replace = (CREATE, REPLACE).
        let append = mode(OnMissing::Fail, OnExisting::Append);
        let create = mode(OnMissing::Create, OnExisting::Fail);
        let create_append = mode(OnMissing::Create, OnExisting::Append);
        let replace = mode(OnMissing::Create, OnExisting::Replace);

        assert_eq!(plan(append, false, PLAIN).unwrap(), IngestPlan::Append);
        assert_eq!(plan(append, true, PLAIN).unwrap(), IngestPlan::Append);

        assert_eq!(plan(create, false, PLAIN).unwrap(), IngestPlan::Create);
        let refused = plan(create, true, PLAIN).unwrap_err();
        assert_eq!(
            refused.error_code(),
            SqeErrorCode::DuplicateTable,
            "{refused}"
        );

        assert_eq!(
            plan(create_append, false, PLAIN).unwrap(),
            IngestPlan::Create
        );
        assert_eq!(
            plan(create_append, true, PLAIN).unwrap(),
            IngestPlan::Append
        );

        assert_eq!(plan(replace, false, PLAIN).unwrap(), IngestPlan::Create);
        assert_eq!(plan(replace, true, PLAIN).unwrap(), IngestPlan::Replace);
    }

    #[test]
    fn fail_if_exists_is_refused_before_any_session_check() {
        // An existing table with Fail is a DUPLICATE_TABLE even where a
        // create would have been refused for the session.
        let begin = IngestSession {
            in_begin_txn: true,
            on_write_branch: true,
        };
        let err = plan(mode(OnMissing::Create, OnExisting::Fail), true, begin).unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::DuplicateTable, "{err}");
    }

    #[test]
    fn create_in_a_begin_transaction_is_an_autocommit_conflict() {
        let begin = IngestSession {
            in_begin_txn: true,
            on_write_branch: false,
        };
        let err = plan(mode(OnMissing::Create, OnExisting::Append), false, begin).unwrap_err();
        assert_eq!(
            err.error_code(),
            SqeErrorCode::AutocommitWriteConflict,
            "{err}"
        );
        // Appending to an existing table is a DML write: allowed there.
        assert_eq!(
            plan(mode(OnMissing::Create, OnExisting::Append), true, begin).unwrap(),
            IngestPlan::Append
        );
    }

    #[test]
    fn create_on_a_write_branch_is_refused() {
        let branch = IngestSession {
            in_begin_txn: false,
            on_write_branch: true,
        };
        let err = plan(mode(OnMissing::Create, OnExisting::Append), false, branch).unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::NotSupported, "{err}");
        assert!(err.to_string().contains("RESET WRITE_BRANCH"), "{err}");
    }

    #[test]
    fn existence_is_looked_up_only_where_the_plan_depends_on_it() {
        for (m, needs) in [
            (mode(OnMissing::Fail, OnExisting::Append), false),
            (mode(OnMissing::Fail, OnExisting::Replace), false),
            (mode(OnMissing::Fail, OnExisting::Fail), true),
            (mode(OnMissing::Create, OnExisting::Append), true),
            (mode(OnMissing::Create, OnExisting::Fail), true),
            (mode(OnMissing::Create, OnExisting::Replace), true),
        ] {
            assert_eq!(m.needs_existence_check(), needs, "{m:?}");
        }
    }

    #[test]
    fn session_states_map_to_the_refusals() {
        let mut s = test_ingest_session();
        assert!(!IngestSession::of(&s).in_begin_txn);

        // BEGIN: a transaction id the client did not carry.
        s.transaction_id = Some("t".into());
        assert!(IngestSession::of(&s).in_begin_txn);

        // A client-held transaction is not BEGIN.
        let client = s.clone().with_client_transaction_id("t".into());
        assert!(!IngestSession::of(&client).in_begin_txn);

        let mut branch = test_ingest_session();
        branch.write_branch = Some("dev".into());
        assert!(IngestSession::of(&branch).on_write_branch);
    }

    fn test_ingest_session() -> Session {
        Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("t".to_string()),
            None,
            chrono::Utc::now(),
            vec![],
        )
    }
}
