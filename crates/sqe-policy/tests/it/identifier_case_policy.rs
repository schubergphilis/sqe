//! A catalog declared `identifier_case = "upper"` (Snowflake Horizon) folds
//! unquoted names to upper case in the front door, before planning. The fold
//! must not change what policy does: a statement written with unquoted names
//! is governed exactly like the same statement quoted by hand, and masks and
//! row filters bound to the stored upper-case names still apply.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use sqe_core::SessionUser;
use sqe_policy::plan_rewriter::PolicyPlanRewriter;
use sqe_policy::policy_expr::parse_sql_predicate;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::session_udf::SessionIdentity;
use sqe_policy::{MaskType, PolicyEnforcer, ResolvedPolicy};
use sqe_sql::identifier_case::UpperCaseCatalogs;
use sqe_sql::{SqlFrontDoor, WireDialect};

fn alice() -> SessionUser {
    SessionUser {
        username: "alice".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    }
}

/// `sf."SALES"."EMP"` with upper-case columns, as Snowflake stores them.
fn snowflake_ctx() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, true),
        Field::new("SSN", DataType::Utf8, true),
        Field::new("REGION", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["111", "222", "333"])),
            Arc::new(StringArray::from(vec!["EU", "US", "EU"])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("SALES", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("sf", catalog);
    ctx.register_table(
        TableReference::full("sf", "SALES", "EMP"),
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx
}

/// A Ranger row filter, parsed the way the store parses `filterExpr`.
/// It is not folded, so a Snowflake column is quoted.
fn row_filter(sql: &str) -> sqe_core::Result<datafusion::logical_expr::Expr> {
    let identity = SessionIdentity::new("alice", Vec::<String>::new(), None, None);
    parse_sql_predicate(sql, &identity)
}

async fn rewriter() -> PolicyPlanRewriter {
    rewriter_with(r#""REGION" = 'EU'"#, "SSN").await
}

async fn rewriter_with(filter: &str, masked: &str) -> PolicyPlanRewriter {
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(row_filter(filter).unwrap());
    policy
        .column_masks
        .insert(masked.to_string(), MaskType::Nullify);
    store.add_table_policy("SALES", "EMP", policy).await;
    PolicyPlanRewriter::new(Arc::new(store))
}

fn fold(sql: &str) -> String {
    SqlFrontDoor::new(WireDialect::Embedded)
        .with_upper_case_catalogs(UpperCaseCatalogs::new(["sf"]))
        .normalize(sql)
        .unwrap()
        .sql()
        .to_string()
}

/// Plan the text, rewrite it under `rewriter`, run it.
async fn try_run(
    ctx: &SessionContext,
    rewriter: PolicyPlanRewriter,
    sql: &str,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    let (plan, _) = rewriter
        .evaluate(&alice(), &sqe_policy::TableScope::default(), plan)
        .await?;
    let df = ctx.execute_logical_plan(plan).await;
    Ok(df.unwrap().collect().await.unwrap())
}

/// Run under the quoted `"REGION" = 'EU'` filter and SSN mask.
/// Returns (ids, number of non-NULL SSN values).
async fn run(ctx: &SessionContext, sql: &str) -> (Vec<i64>, usize) {
    let batches = try_run(ctx, rewriter().await, sql).await.unwrap();
    let mut ids = Vec::new();
    let mut ssn_values = 0;
    for b in &batches {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        ids.extend(id.values().iter().copied());
        let ssn = b.column(1);
        ssn_values += ssn.len() - ssn.null_count();
    }
    ids.sort_unstable();
    (ids, ssn_values)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn folded_statement_is_governed_like_the_quoted_one() {
    let ctx = snowflake_ctx();

    let folded = fold("SELECT id, ssn FROM sf.sales.emp WHERE id > 0");
    assert_eq!(
        folded,
        r#"SELECT "ID", "SSN" FROM sf."SALES"."EMP" WHERE "ID" > 0"#
    );
    let quoted = r#"SELECT "ID", "SSN" FROM sf."SALES"."EMP" WHERE "ID" > 0"#;

    for sql in [folded.as_str(), quoted] {
        let (ids, ssn_values) = run(&ctx, sql).await;
        assert_eq!(ids, vec![1, 3], "row filter keeps the EU rows: {sql}");
        assert_eq!(ssn_values, 0, "SSN is masked to NULL: {sql}");
    }
}

/// A user predicate on the folded column cannot widen the row filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn folded_predicate_cannot_reach_filtered_rows() {
    let ctx = snowflake_ctx();
    let (ids, ssn_values) = run(
        &ctx,
        &fold("SELECT id, ssn FROM sf.sales.emp WHERE region = 'US' OR id >= 1"),
    )
    .await;
    assert_eq!(ids, vec![1, 3]);
    assert_eq!(ssn_values, 0);
}

/// A mask authored on `ssn` still binds to the stored `SSN`: mask lookup
/// matches case-insensitively, as Ranger resources do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lower_case_mask_binds_to_the_upper_case_column() {
    let ctx = snowflake_ctx();
    let sql = fold("SELECT id, ssn FROM sf.sales.emp");
    let batches = try_run(&ctx, rewriter_with(r#""REGION" = 'EU'"#, "ssn").await, &sql)
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let ssn_values: usize = batches
        .iter()
        .map(|b| b.column(1).len() - b.column(1).null_count())
        .sum();
    assert_eq!(rows, 2);
    assert_eq!(ssn_values, 0, "SSN is masked through the lower-case key");
}

/// Row-filter expressions are not folded. An unquoted `region` names a
/// column the table does not have, and the query fails: never a result
/// without the filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unquoted_row_filter_fails_closed() {
    let ctx = snowflake_ctx();
    let sql = fold("SELECT id, ssn FROM sf.sales.emp");
    let err = try_run(&ctx, rewriter_with("region = 'EU'", "SSN").await, &sql)
        .await
        .expect_err("an unresolvable row filter must fail the query");
    assert!(err.to_string().contains("region"), "{err}");
}

/// Without the fold the unquoted name resolves to nothing, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unfolded_name_is_not_found() {
    let ctx = snowflake_ctx();
    assert!(ctx
        .state()
        .create_logical_plan("SELECT id FROM sf.sales.emp")
        .await
        .is_err());
}
