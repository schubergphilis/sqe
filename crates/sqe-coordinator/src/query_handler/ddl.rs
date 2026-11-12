//! The DDL family: `DROP TABLE` (Hive or Iceberg, with the PURGE procedure and
//! lineage hint), `CREATE VIEW` (output schema inferred from the SELECT, then
//! created through the REST catalog) and `COMMENT ON TABLE / COLUMN`, stored as
//! Iceberg table properties.

use super::*;

impl QueryHandler {
    /// Runs `DROP TABLE`: records the lineage hint, routes to the Hive or
    /// Iceberg catalog (or the PURGE procedure) and invalidates the session cache.
    #[allow(clippy::borrowed_box)]
    pub(super) async fn handle_drop_statement(
        &self,
        session: &Session,
        stmt: &Box<Statement>,
        captured_plan: &mut Option<sqe_lineage::PlanOrHint>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // DROP TABLE supports multiple names, but (like
        // `catalog_ops.drop_table` below) only the first is
        // acted on; a mixed Hive/Iceberg multi-name drop is not
        // a new problem this branch introduces.
        // Lineage: the dropped table is the output dataset, with
        // the `lifecycleStateChange: DROP` facet. Recorded before
        // the drop so a failed DROP's FAIL event names it too.
        // `DROP TABLE IF EXISTS` of a missing table still emits
        // it: `drop_table` does not say whether anything existed.
        if let Statement::Drop { names, .. } = stmt.as_ref() {
            if let Some(name) = names.first() {
                *captured_plan = Some(lineage_lifecycle_hint(
                    &self.config,
                    session,
                    name,
                    sqe_lineage::event::lifecycle::DROP,
                ));
            }
        }
        let hive_drop = if let Statement::Drop {
            names,
            if_exists,
            purge,
            ..
        } = stmt.as_ref()
        {
            names
                .first()
                .map(|name| (table_reference_of(name), *if_exists, *purge))
        } else {
            None
        };
        let routed_to_hive = match &hive_drop {
            Some((table_ref, if_exists, purge))
                if matches!(
                    self.hive_ddl_handler.resolve_table_kind(table_ref),
                    crate::hive_ddl_handler::TableKind::Hive { .. }
                ) =>
            {
                match self
                    .hive_ddl_handler
                    .drop_table(session, table_ref, *purge)
                    .await
                {
                    Ok(()) => true,
                    Err(e)
                        if *if_exists && crate::hive_ddl_handler::hive_drop_target_missing(&e) =>
                    {
                        true
                    }
                    Err(e) => return Err(e),
                }
            }
            _ => false,
        };
        let purge = matches!(stmt.as_ref(), Statement::Drop { purge: true, .. });
        if !routed_to_hive && purge {
            // `PURGE` runs `system.drop_table(purge => true)`
            // through the procedure entry (#841), so it takes the
            // same admin gate, shared-location refusal and purge
            // audit as the CALL. A catalog DROP grant alone drops
            // without PURGE.
            if let Some(table) = self.catalog_ops.purge_target(session, stmt).await? {
                let call = sqe_sql::ProcedureCall::DropTable { table, purge: true };
                let purged = self.maintenance_handler.handle(session, &call).await;
                purged.map_err(sql_purge_denial)?;
                if let Some(ref qcache) = self.query_cache {
                    qcache.invalidate_all();
                }
            }
        } else if !routed_to_hive {
            self.catalog_ops.drop_table(session, stmt).await?;
        }
        crate::session_context::invalidate_session_cache(&session.user.username).await;
        Ok(vec![])
    }

    /// Handle CREATE VIEW by inferring the output schema from the SELECT query
    /// and then calling the Polaris REST API to create the view.
    pub(super) async fn handle_create_view(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        // CREATE MATERIALIZED VIEW is unsupported (#324); reject it rather than
        // silently creating a plain view that would never refresh.
        if classify_materialized_view(stmt) == Some(MaterializedViewStmt::Unsupported) {
            return Err(SqeError::NotImplemented(
                "materialized views are not supported".to_string(),
            ));
        }
        let query = match stmt {
            Statement::CreateView(cv) => &cv.query,
            other => {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Expected CREATE VIEW statement, got: {other}"),
                ));
            }
        };

        // Infer the output schema by planning the SELECT query through DataFusion
        let select_sql = format!("{query}");
        let schema = self.get_schema(session, &select_sql).await?;

        // Convert Arrow schema to Iceberg REST API schema format
        let schema_json = sqe_write::view_schema::arrow_schema_to_iceberg_json(&schema);

        self.catalog_ops
            .create_view(session, stmt, &schema_json)
            .await
    }

    /// Handle `COMMENT ON TABLE/COLUMN` by storing the comment as an Iceberg table property.
    ///
    /// - `COMMENT ON TABLE t IS 'text'` → sets property `"comment"` = text
    /// - `COMMENT ON COLUMN t.col IS 'text'` → sets property `"comment.<col>"` = text
    /// - `IS NULL` removes the comment (stores empty string)
    pub(super) async fn handle_comment_on(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        use crate::ident::resolve_table_ident;
        use sqlparser::ast::CommentObject;

        let (object_type, object_name, comment_text) = match stmt {
            Statement::Comment {
                object_type,
                object_name,
                comment,
                ..
            } => (object_type, object_name, comment),
            other => {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Expected COMMENT statement, got: {other}"),
                ));
            }
        };

        // For COLUMN comments the object_name is "table.column" — split off the column part.
        let (table_ref_parts, prop_key) = match object_type {
            CommentObject::Table => {
                // object_name is the table name
                (object_name.clone(), "comment".to_string())
            }
            CommentObject::Column => {
                // object_name is table.column — last ident is the column name
                let parts: Vec<_> = object_name.0.iter().collect();
                if parts.len() < 2 {
                    return Err(SqeError::Execution(
                        "COMMENT ON COLUMN requires table.column format".to_string(),
                    ));
                }
                let col_name = parts
                    .last()
                    .and_then(|p| p.as_ident())
                    .map(|i| i.value.clone())
                    .unwrap_or_default();
                let table_parts =
                    sqlparser::ast::ObjectName(object_name.0[..object_name.0.len() - 1].to_vec());
                (table_parts, format!("comment.{col_name}"))
            }
            other => {
                return Err(SqeError::NotImplemented(format!(
                    "COMMENT ON {other} is not supported; only TABLE and COLUMN are supported"
                )));
            }
        };

        let table_ident = resolve_table_ident(&table_ref_parts, session)?;
        // The catalog the name resolves in (its qualifier, else the session
        // default), as for SHOW CREATE TABLE. The session's default catalog
        // alone made `COMMENT ON COLUMN cat.ns.t.c` fail with "does not
        // exist" for any `cat` but the default.
        let qualifier = crate::ident::catalog_qualifier(&table_ref_parts);
        let catalog = self.show_catalog(session, qualifier.as_deref()).await?;

        let comment_value = comment_text.clone().unwrap_or_default();

        tracing::info!(
            username = %session.user.username,
            table = %table_ident,
            property = %prop_key,
            "COMMENT ON — storing as Iceberg table property"
        );

        let updates = vec![iceberg::TableUpdate::SetProperties {
            updates: std::collections::HashMap::from([(prop_key, comment_value)]),
        }];

        catalog
            .commit_schema_update(&table_ident, updates, vec![])
            .await
    }
}
