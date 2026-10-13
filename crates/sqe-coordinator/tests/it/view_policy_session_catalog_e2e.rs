//! A `catalog_scoped` policy governs a table read through a view the same as
//! a direct read (#835).
//!
//! The test config registers warehouse `test_warehouse` as catalog `iceberg`.
//! A view body's scans were named by the warehouse, so a mask written for
//! `iceberg.test_ns` matched the direct read and missed the view: a policy
//! that does not match is a mask that does not apply.
//!
//! A body naming the warehouse itself (`test_warehouse.test_ns.t`) cannot be
//! created through SQE, which does not register the warehouse name; one
//! created by another engine is covered by the `qualify_view_scans` unit test.

use std::sync::Arc;

use async_trait::async_trait;
use sqe_core::SessionUser;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{MaskType, PolicyStore, ResolvedPolicy};

/// An in-memory store that keys on the catalog, like a `catalog_scoped` Ranger.
struct CatalogScoped(InMemoryPolicyStore);

#[async_trait]
impl PolicyStore for CatalogScoped {
    fn catalog_scoped(&self) -> bool {
        true
    }
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        self.0.resolve(user, table_name, namespace).await
    }
}

async fn ssn(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    from: &str,
) -> Vec<Option<String>> {
    let sql = format!("SELECT CAST(ssn AS VARCHAR) FROM {from}");
    let batches = handler.execute(session, &sql, None).await.expect(&sql);
    batches
        .iter()
        .flat_map(|b| {
            let col = arrow::compute::cast(b.column(0), &arrow_schema::DataType::Utf8).unwrap();
            let col = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .clone();
            col.iter()
                .map(|v| v.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn a_view_is_governed_by_the_sessions_catalog_name() {
    let store = Arc::new(CatalogScoped(InMemoryPolicyStore::new()));
    let (session, handler) =
        crate::common::setup_handler_with_store(Arc::clone(&store) as Arc<dyn PolicyStore>).await;

    let drop_all = || async {
        let _ = handler
            .execute(
                &session,
                "DROP VIEW IF EXISTS iceberg.test_ns.v835_unqualified",
                None,
            )
            .await;
        let _ = handler
            .execute(
                &session,
                "DROP TABLE IF EXISTS iceberg.test_ns.v835_t",
                None,
            )
            .await;
    };
    drop_all().await;

    for sql in [
        "CREATE TABLE iceberg.test_ns.v835_t AS SELECT CAST(1 AS BIGINT) AS id, '123-45-6789' AS ssn",
        "CREATE VIEW iceberg.test_ns.v835_unqualified AS SELECT * FROM test_ns.v835_t",
    ] {
        handler.execute(&session, sql, None).await.expect(sql);
    }

    let mut mask = ResolvedPolicy::default();
    mask.column_masks
        .insert("ssn".to_string(), MaskType::Nullify);
    store
        .0
        .add_table_policy("iceberg.test_ns", "v835_t", mask)
        .await;

    for from in ["iceberg.test_ns.v835_t", "iceberg.test_ns.v835_unqualified"] {
        assert_eq!(
            ssn(&handler, &session, from).await,
            [None],
            "{from} is masked"
        );
    }

    drop_all().await;
}
