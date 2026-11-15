//! `CALL system.*` in embedded mode.
//!
//! The six procedures that only need a catalog run through the same cores as
//! the coordinator (`sqe_write::procedures`), on a `SessionCatalog` over the
//! attached Iceberg catalog. The rest are refused with a message that names
//! the cluster. There is no authorization layer here: the CLI user is the
//! principal and the catalog decides (see the embedded-maintenance-procedures
//! design, D4).

use std::sync::Arc;

use iceberg::Catalog;
use sqe_sql::{ProcedureCall, TableRef};

use super::EmbeddedClient;
use crate::client::QueryResult;

/// The catalog, table identifier and catalog name a procedure runs on.
type ProcedureTarget = (Arc<dyn Catalog>, iceberg::TableIdent, String);

/// The refusal for a procedure that needs a cluster.
fn not_available(name: &str, hint: &str) -> Box<dyn std::error::Error> {
    format!("CALL system.{name} is not available in embedded mode; run it on an SQE cluster{hint}")
        .into()
}

impl EmbeddedClient {
    /// Run one `CALL system.*` statement. The `match` is exhaustive on
    /// purpose: a new procedure does not compile until embedded mode decides
    /// what it does.
    pub(super) async fn embedded_handle_procedure(
        &self,
        call: &ProcedureCall,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        match call {
            ProcedureCall::ExpireSnapshots {
                table,
                older_than,
                retain_last,
            } => {
                let older_than_ms = older_than.map(|t| t.timestamp_millis());
                let (catalog, ident, name) = self.procedure_target("expire_snapshots", table)?;
                let batches = sqe_write::procedures::expire_snapshots(
                    &catalog,
                    &ident,
                    older_than_ms,
                    *retain_last,
                    None,
                )
                .await?;
                self.procedure_result(&name, &ident, &batches, true).await
            }
            ProcedureCall::RewriteManifests { table } => {
                let (catalog, ident, name) = self.procedure_target("rewrite_manifests", table)?;
                let batches =
                    sqe_write::procedures::rewrite_manifests(&catalog, &ident, None).await?;
                self.procedure_result(&name, &ident, &batches, true).await
            }
            ProcedureCall::SetCurrentSnapshot { table, snapshot_id } => {
                let op = "set_current_snapshot";
                let (catalog, ident, name) = self.procedure_target(op, table)?;
                let batches =
                    sqe_write::procedures::move_main_ref(&catalog, &ident, *snapshot_id, false, op)
                        .await?;
                self.procedure_result(&name, &ident, &batches, true).await
            }
            ProcedureCall::RollbackToSnapshot { table, snapshot_id } => {
                let op = "rollback_to_snapshot";
                let (catalog, ident, name) = self.procedure_target(op, table)?;
                let batches =
                    sqe_write::procedures::move_main_ref(&catalog, &ident, *snapshot_id, true, op)
                        .await?;
                self.procedure_result(&name, &ident, &batches, true).await
            }
            ProcedureCall::RegisterTable {
                table,
                metadata_location,
            } => {
                let (catalog, ident, name) = self.procedure_target("register_table", table)?;
                let batches =
                    sqe_write::procedures::register_table(&catalog, &ident, metadata_location)
                        .await?;
                self.procedure_result(&name, &ident, &batches, true).await
            }
            ProcedureCall::TableHealth { table } => {
                let (catalog, ident, name) = self.procedure_target("table_health", table)?;
                let config = self.handler_config();
                let batches = sqe_write::procedures::table_health(
                    &catalog,
                    &ident,
                    &config.maintenance.compaction,
                )
                .await?;
                self.procedure_result(&name, &ident, &batches, false).await
            }
            ProcedureCall::RewriteDataFiles { .. } => Err(not_available("rewrite_data_files", "")),
            ProcedureCall::RemoveOrphanFiles { .. } => {
                Err(not_available("remove_orphan_files", ""))
            }
            ProcedureCall::PurgeOrphanLocations { .. } => {
                Err(not_available("purge_orphan_locations", ""))
            }
            ProcedureCall::RefreshCatalogCache { .. } => {
                Err(not_available("refresh_catalog_cache", ""))
            }
            ProcedureCall::ReprojectColumnTags { .. } => {
                Err(not_available("reproject_column_tags", ""))
            }
            ProcedureCall::SuggestBloomFilterColumns { .. } => {
                Err(not_available("suggest_bloom_filter_columns", ""))
            }
            ProcedureCall::DropTable { .. } => Err(not_available("drop_table", "; use DROP TABLE")),
        }
    }

    /// The catalog, table identifier and catalog name a procedure runs on.
    /// Fails before any catalog call when the catalog is not a writable
    /// Iceberg catalog.
    fn procedure_target(
        &self,
        procedure: &str,
        table: &TableRef,
    ) -> Result<ProcedureTarget, Box<dyn std::error::Error>> {
        let catalog_name = table
            .catalog
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| self.default_catalog());
        let Some(session_catalog) = self.local_session_catalog(&catalog_name) else {
            return Err(format!(
                "CALL system.{procedure}: catalog '{catalog_name}' is not a writable Iceberg catalog"
            )
            .into());
        };
        let catalog: Arc<dyn Catalog> = session_catalog.as_catalog();
        Ok((
            catalog,
            sqe_write::procedures::to_table_ident(table),
            catalog_name,
        ))
    }

    /// Refresh the namespace after a commit, so reads in this session see it,
    /// and turn the core's batches into the client's result.
    async fn procedure_result(
        &self,
        catalog_name: &str,
        ident: &iceberg::TableIdent,
        batches: &[arrow_array::RecordBatch],
        committed: bool,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        if committed {
            let namespace = ident.namespace().as_ref().join(".");
            if let Some(writable) = self.iceberg_catalogs.get(catalog_name) {
                writable.refresh_namespace(&namespace).await?;
            }
        }
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        Ok(QueryResult::from_batches(&schema, batches)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SqlClient;

    /// An attached sqlite catalog `ice` with `ice.s.t` holding the rows of
    /// one CTAS commit plus one INSERT commit per entry in `inserts`.
    async fn client_with_table(inserts: usize) -> (EmbeddedClient, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap().to_string();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        let mut stmts = vec![
            format!("ATTACH '{path}' AS ice (TYPE sqlite)"),
            "CREATE SCHEMA ice.s".to_string(),
            "CREATE TABLE ice.s.t AS SELECT * FROM (VALUES (1), (2)) AS v(id)".to_string(),
        ];
        for i in 0..inserts {
            stmts.push(format!("INSERT INTO ice.s.t VALUES ({})", 100 + i));
        }
        for sql in stmts {
            client
                .execute(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        (client, tmp)
    }

    async fn count(client: &mut EmbeddedClient, table: &str) -> String {
        let r = client
            .execute(&format!("SELECT count(*) FROM {table}"))
            .await
            .expect("count");
        r.rows[0][0].clone()
    }

    /// Snapshot ids of `ice.s.t`, oldest commit first.
    async fn snapshot_ids(client: &EmbeddedClient) -> Vec<i64> {
        let catalog: Arc<dyn Catalog> = client
            .local_session_catalog("ice")
            .expect("catalog")
            .as_catalog();
        let ident = iceberg::TableIdent::from_strs(["s", "t"]).unwrap();
        let table = catalog.load_table(&ident).await.expect("load");
        let mut snaps: Vec<_> = table.metadata().snapshots().cloned().collect();
        snaps.sort_by_key(|s| s.sequence_number());
        snaps.iter().map(|s| s.snapshot_id()).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_expire_snapshots_keeps_the_last() {
        let (mut client, _tmp) = client_with_table(2).await;
        let r = client
            .execute(
                "CALL system.expire_snapshots(table => 'ice.s.t', \
                 older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)",
            )
            .await
            .expect("expire");
        assert_eq!(r.rows.len(), 1);
        let col = |n: &str| r.columns.iter().position(|c| c == n).unwrap();
        assert_eq!(r.rows[0][col("procedure")], "expire_snapshots");
        assert_eq!(r.rows[0][col("input_count")], "3");
        assert_eq!(r.rows[0][col("output_count")], "1");
        assert_eq!(count(&mut client, "ice.s.t").await, "4");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_rollback_then_select_sees_the_earlier_rows() {
        let (mut client, _tmp) = client_with_table(1).await;
        let ids = snapshot_ids(&client).await;
        assert_eq!(ids.len(), 2);
        assert_eq!(count(&mut client, "ice.s.t").await, "3");
        client
            .execute(&format!(
                "CALL system.rollback_to_snapshot(table => 'ice.s.t', snapshot_id => {})",
                ids[0]
            ))
            .await
            .expect("rollback");
        assert_eq!(count(&mut client, "ice.s.t").await, "2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_rollback_refuses_a_non_ancestor() {
        let (mut client, _tmp) = client_with_table(1).await;
        let ids = snapshot_ids(&client).await;
        // Move back to the first snapshot, then commit again: the second
        // snapshot is now off the current history.
        client
            .execute(&format!(
                "CALL system.set_current_snapshot(table => 'ice.s.t', snapshot_id => {})",
                ids[0]
            ))
            .await
            .expect("set_current_snapshot with no roles");
        client
            .execute("INSERT INTO ice.s.t VALUES (500)")
            .await
            .expect("insert");
        let err = client
            .execute(&format!(
                "CALL system.rollback_to_snapshot(table => 'ice.s.t', snapshot_id => {})",
                ids[1]
            ))
            .await
            .expect_err("not an ancestor");
        assert!(err.to_string().contains("is not an ancestor"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_set_current_snapshot_works_without_roles() {
        let (mut client, _tmp) = client_with_table(1).await;
        let ids = snapshot_ids(&client).await;
        let r = client
            .execute(&format!(
                "CALL system.set_current_snapshot(table => 'ice.s.t', snapshot_id => {})",
                ids[0]
            ))
            .await
            .expect("set_current_snapshot");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(count(&mut client, "ice.s.t").await, "2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_register_table_then_select() {
        let (mut client, _tmp) = client_with_table(1).await;
        let catalog: Arc<dyn Catalog> = client
            .local_session_catalog("ice")
            .expect("catalog")
            .as_catalog();
        let ident = iceberg::TableIdent::from_strs(["s", "t"]).unwrap();
        let table = catalog.load_table(&ident).await.expect("load");
        let location = table.metadata_location().expect("location").to_string();
        client
            .execute(&format!(
                "CALL system.register_table(table => 'ice.s.t2', metadata_location => '{location}')"
            ))
            .await
            .expect("register");
        assert_eq!(count(&mut client, "ice.s.t2").await, "3");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_table_health_returns_the_cluster_columns() {
        let (mut client, _tmp) = client_with_table(0).await;
        let r = client
            .execute("CALL system.table_health(table => 'ice.s.t')")
            .await
            .expect("table_health");
        let health = sqe_write::table_health::TableHealth::default();
        let batch = sqe_write::table_health::table_health_batch(&health);
        let expected: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(r.columns, expected);
        assert_eq!(r.rows.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_rewrite_manifests_merges_and_keeps_rows() {
        let (mut client, _tmp) = client_with_table(3).await;
        let before = count(&mut client, "ice.s.t").await;
        let r = client
            .execute("CALL system.rewrite_manifests(table => 'ice.s.t')")
            .await
            .expect("rewrite_manifests");
        let col = |n: &str| r.columns.iter().position(|c| c == n).unwrap();
        let input: i64 = r.rows[0][col("input_count")].parse().unwrap();
        let output: i64 = r.rows[0][col("output_count")].parse().unwrap();
        assert!(output < input, "manifests {input} -> {output}");
        assert_eq!(count(&mut client, "ice.s.t").await, before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_refuses_a_catalog_that_is_not_iceberg() {
        let (mut client, _tmp) = client_with_table(0).await;
        let err = client
            .execute("CALL system.table_health(table => 'nosuch.s.t')")
            .await
            .expect_err("not an Iceberg catalog");
        assert!(
            err.to_string().contains("not a writable Iceberg catalog"),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_call_refuses_the_cluster_only_procedures() {
        let (mut client, _tmp) = client_with_table(0).await;
        let err = client
            .execute("CALL system.rewrite_data_files(table => 'ice.s.t')")
            .await
            .expect_err("refused");
        assert!(
            err.to_string().contains("not available in embedded mode"),
            "{err}"
        );
        let err = client
            .execute("CALL system.drop_table(table => 'ice.s.t')")
            .await
            .expect_err("refused");
        let msg = err.to_string();
        assert!(msg.contains("not available in embedded mode"), "{msg}");
        assert!(msg.contains("DROP TABLE"), "{msg}");
        // The table is untouched.
        assert_eq!(count(&mut client, "ice.s.t").await, "2");
    }
}
