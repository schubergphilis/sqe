//! A table named without its schema is governed as the table it resolved to.
//!
//! DataFusion keeps a scan's `TableReference` as written, so under a session
//! default schema (`USE sales` over Flight, `X-Trino-Schema`, the MCP default)
//! `SELECT * FROM t` used to be governed as `default.t`: a row filter on
//! `sales.t` was skipped and the query returned every row.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, TableProvider};
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};

use sqe_core::SessionUser;
use sqe_policy::plan_rewriter::{resolve_policy_key, HiveCatalogs, PolicyPlanRewriter};
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{PolicyEnforcer, ResolvedPolicy, TableScope};

/// `sales.t` with regions EU, US, EU, in a context whose default schema is
/// `sales`, and a row filter `region = 'EU'` on `sales.t`.
async fn governed() -> (SessionContext, PolicyPlanRewriter) {
    let schema = Arc::new(Schema::new(vec![Field::new("region", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["EU", "US", "EU"]))],
    )
    .unwrap();
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_default_catalog_and_schema("lake", "sales"),
    );
    ctx.register_catalog("lake", Arc::new(datafusion::catalog::MemoryCatalogProvider::new()));
    let sales = Arc::new(MemorySchemaProvider::new());
    sales
        .register_table("t".into(), table as Arc<dyn TableProvider>)
        .unwrap();
    ctx.catalog("lake").unwrap().register_schema("sales", sales).unwrap();

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    store.add_table_policy("sales", "t", policy).await;
    (ctx, PolicyPlanRewriter::new(Arc::new(store)))
}

async fn count(ctx: &SessionContext, rewriter: &PolicyPlanRewriter, sql: &str) -> i64 {
    let user = SessionUser {
        username: "alice".into(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    };
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    let (enforced, _) = rewriter
        .evaluate(&user, &TableScope::of(ctx), plan)
        .await
        .unwrap();
    let batches = ctx.execute_logical_plan(enforced).await.unwrap().collect().await.unwrap();
    batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
}

#[tokio::test]
async fn a_row_filter_holds_however_the_table_is_named() {
    let (ctx, rewriter) = governed().await;
    for sql in [
        "SELECT count(*) FROM lake.sales.t",
        "SELECT count(*) FROM sales.t",
        "SELECT count(*) FROM t",
    ] {
        assert_eq!(count(&ctx, &rewriter, sql).await, 2, "row filter skipped for `{sql}`");
    }
}

#[test]
fn a_catalog_less_name_under_a_hive_root_is_keyed_as_hive() {
    let cats = HiveCatalogs::new();
    cats.insert("lake");
    let scope = TableScope {
        catalog: Some("lake".into()),
        schema: Some("sales".into()),
    };
    for written in [TableReference::bare("t"), TableReference::partial("sales", "t")] {
        assert_eq!(
            resolve_policy_key(&scope.qualify(&written), &cats),
            Some(("lake.sales".to_string(), "t".to_string())),
            "{written}"
        );
    }
}

#[test]
fn a_reserved_catalog_is_never_filled_in() {
    // It would make a user table look like a reserved virtual one and skip
    // its tag lookup.
    let scope = TableScope {
        catalog: Some("datafusion".into()),
        schema: Some("sales".into()),
    };
    assert_eq!(
        scope.qualify(&TableReference::bare("t")),
        TableReference::partial("sales", "t")
    );
    assert_eq!(
        TableScope::default().qualify(&TableReference::bare("t")),
        TableReference::bare("t")
    );
}

/// Live-stack report: `SELECT * FROM customers` with session catalog
/// `workspace1` and schema `default` was denied with "Tag state unknown",
/// because the tag cache is keyed by the real catalog and the lookup ran with
/// no catalog. The rewriter qualifies the reference from the session scope
/// before the tag lookup AND before the policy key, so the unqualified,
/// schema-qualified and fully qualified spellings must all find the tags and
/// all resolve the catalog-scoped key `workspace1.default`.
#[tokio::test]
async fn an_unqualified_reference_uses_the_session_catalog_for_tags_and_the_scoped_key() {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sqe_policy::tag_source::TagSource;
    use sqe_policy::PolicyStore;

    /// Answers only for the fully qualified table, like the metadata cache.
    struct CatalogKeyedTags(Mutex<Vec<Option<String>>>);
    impl TagSource for CatalogKeyedTags {
        fn column_tags(
            &self,
            catalog: Option<&str>,
            namespace: &[String],
            table: &str,
        ) -> Option<HashMap<String, Vec<String>>> {
            self.0.lock().unwrap().push(catalog.map(str::to_string));
            (catalog == Some("workspace1") && namespace == ["default"] && table == "customers")
                .then(HashMap::new)
        }
    }

    struct Scoped(InMemoryPolicyStore);
    #[async_trait]
    impl PolicyStore for Scoped {
        fn catalog_scoped(&self) -> bool {
            true
        }
        async fn resolve(
            &self,
            user: &SessionUser,
            table: &str,
            namespace: &str,
        ) -> sqe_core::Result<ResolvedPolicy> {
            self.0.resolve(user, table, namespace).await
        }
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "region",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["EU", "US", "EU"]))],
    )
    .unwrap();
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_default_catalog_and_schema("workspace1", "default"),
    );
    ctx.register_catalog(
        "workspace1",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );
    let default = Arc::new(MemorySchemaProvider::new());
    default
        .register_table("customers".into(), table as Arc<dyn TableProvider>)
        .unwrap();
    ctx.catalog("workspace1")
        .unwrap()
        .register_schema("default", default)
        .unwrap();

    let inner = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    inner
        .add_table_policy("workspace1.default", "customers", policy)
        .await;
    let tags = Arc::new(CatalogKeyedTags(Mutex::new(Vec::new())));
    let rewriter = PolicyPlanRewriter::new(Arc::new(Scoped(inner))).with_tag_source(tags.clone());

    for sql in [
        "SELECT count(*) FROM customers",
        "SELECT count(*) FROM default.customers",
        "SELECT count(*) FROM workspace1.default.customers",
    ] {
        assert_eq!(
            count(&ctx, &rewriter, sql).await,
            2,
            "`{sql}`: tags found and the scoped filter applied"
        );
    }
    assert!(
        tags.0
            .lock()
            .unwrap()
            .iter()
            .all(|c| c.as_deref() == Some("workspace1")),
        "every tag lookup carried the session catalog"
    );
}
