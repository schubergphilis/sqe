//! Regression suite for the P0 `view-bypass-policy` bug (WA review batch 5).
//!
//! The reported bug: `PolicyEnforcer::evaluate` runs on the UNOPTIMIZED plan,
//! and SQE views are DataFusion `ViewTable`s, so `SELECT * FROM v` was a single
//! `TableScan(ViewTable)` at evaluate time. The rewriter keyed policy by the
//! view name (ungoverned) and never governed the base table -- row filters,
//! column masks, and column restrictions were ALL skipped.
//!
//! What actually holds on DataFusion 54: view inlining moved out of the
//! `InlineTableScan` analyzer rule (which no longer exists in DF 54) and into
//! `LogicalPlanBuilder::scan_with_filters_inner`, which runs at `ctx.sql` time.
//! So `ctx.sql("SELECT * FROM v")` already produces
//! `Projection -> SubqueryAlias(v) -> TableScan(base)` BEFORE evaluate runs.
//! The rewriter therefore sees and governs the base `TableScan`.
//!
//! These tests pin that guarantee by going through the REAL path
//! (`ctx.sql` -> `rewriter.evaluate` -> `ctx.execute_logical_plan` -> collect)
//! with a registered `ViewTable`, exactly as the coordinator does. They also
//! pin the defense-in-depth fail-closed guard for any view scan that did NOT
//! inline (e.g. a `ViewTable` reached through a directly-constructed
//! `TableScan`, where DF's builder inlining is bypassed).

use std::sync::Arc;

use arrow::array::{
    Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::datasource::{provider_as_source, MemTable, ViewTable};
use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;

use sqe_core::SessionUser;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{
    plan_rewriter::PolicyPlanRewriter, MaskType, PolicyEnforcer, ResolvedPolicy,
};

fn user(name: &str) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: vec![],
    }
}

fn employee_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("ssn", DataType::Utf8, true),
        Field::new("salary", DataType::Decimal128(18, 2), true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

fn employee_batch(schema: Arc<Schema>) -> RecordBatch {
    let salary = Decimal128Array::from(vec![10_000_000_i128, 5_000_000_i128, 7_500_000_i128])
        .with_precision_and_scale(18, 2)
        .unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec![
                "111-11-1111",
                "222-22-2222",
                "333-33-3333",
            ])),
            Arc::new(salary),
            Arc::new(StringArray::from(vec!["EU", "US", "EU"])),
        ],
    )
    .unwrap()
}

/// Register base table `t` (as a MemTable) in a fresh context.
fn ctx_with_base_table() -> (SessionContext, Arc<MemTable>) {
    let schema = employee_schema();
    let batch = employee_batch(schema.clone());
    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let ctx = SessionContext::new();
    ctx.register_table("t", mem.clone()).unwrap();
    (ctx, mem)
}

/// Register a view `view_name` whose body is `view_sql`, planned against the
/// current context (so its body resolves to base `TableScan`s), exactly as
/// SQE's `SqeSchemaProvider::plan_view` does (plan the SQL, wrap in ViewTable).
async fn register_view(ctx: &SessionContext, view_name: &str, view_sql: &str) {
    let body = ctx
        .sql(view_sql)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let view = Arc::new(ViewTable::new(body, Some(view_sql.to_string())));
    ctx.register_table(view_name, view as Arc<dyn TableProvider>)
        .unwrap();
}

/// The full production path: plan `sql` via `ctx.sql`, run the rewriter's
/// `evaluate` on the UNOPTIMIZED logical plan (as the coordinator does), then
/// execute the enforced plan and collect.
async fn enforce_and_run(
    ctx: &SessionContext,
    rewriter: &PolicyPlanRewriter,
    sql: &str,
) -> Vec<RecordBatch> {
    try_enforce_and_run(ctx, rewriter, sql).await.unwrap()
}

/// Like `enforce_and_run` but surfaces errors so a test can assert a query
/// fails closed (e.g. referencing a restricted column).
async fn try_enforce_and_run(
    ctx: &SessionContext,
    rewriter: &PolicyPlanRewriter,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?;
    let plan = df.logical_plan().clone();
    let (enforced, _summary) = rewriter.evaluate(&user("alice"), plan).await?;
    Ok(ctx.execute_logical_plan(enforced).await?.collect().await?)
}

async fn governed_store() -> InMemoryPolicyStore {
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Nullify);
    policy.restricted_columns.push("ssn".to_string());
    // Base table `t` is in the default namespace under the bare name.
    store.add_table_policy("default", "t", policy).await;
    store
}

/// Test 1: base `t` has a row filter (region='EU'), a mask (salary), and a
/// restriction (ssn). `SELECT salary, region FROM v` over `CREATE VIEW v AS
/// SELECT * FROM t` must apply ALL THREE controls to the base table -- same as
/// querying `t` directly. This is the headline: the bypass is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_select_subset_applies_base_row_filter_and_mask() {
    let (ctx, _mem) = ctx_with_base_table();
    register_view(&ctx, "v", "SELECT * FROM t").await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(governed_store().await));
    let batches = enforce_and_run(&ctx, &rewriter, "SELECT salary, region FROM v").await;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "row filter region='EU' must apply through the view");

    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(
        salary.null_count(),
        salary.len(),
        "salary mask (Nullify) must apply through the view"
    );

    // Region values must all be EU (US row filtered out).
    let region = batches[0]
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    for i in 0..region.len() {
        assert_eq!(region.value(i), "EU");
    }
}

/// Test 2: a view that projects/filters a subset of columns. The base row
/// filter and mask still apply; the restricted column is simply not selected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_projecting_subset_still_governed() {
    let (ctx, _mem) = ctx_with_base_table();
    register_view(
        &ctx,
        "v",
        "SELECT customer_id, salary, region FROM t WHERE customer_id > 0",
    )
    .await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(governed_store().await));
    let batches = enforce_and_run(&ctx, &rewriter, "SELECT salary, region FROM v").await;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "base row filter must apply through a projecting view");
    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(
        salary.null_count(),
        salary.len(),
        "base salary mask must apply through a projecting view"
    );
}

/// Test 3: nested view (v2 over v over t). Base policy must still apply fully.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_view_still_governed() {
    let (ctx, _mem) = ctx_with_base_table();
    register_view(&ctx, "v", "SELECT * FROM t").await;
    register_view(&ctx, "v2", "SELECT salary, region FROM v").await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(governed_store().await));
    let batches = enforce_and_run(&ctx, &rewriter, "SELECT salary, region FROM v2").await;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "row filter must apply through a nested view");
    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(
        salary.null_count(),
        salary.len(),
        "salary mask must apply through a nested view"
    );
}

/// Test 4: through a view, selecting the PERMITTED columns is governed (row
/// filter + mask apply, restricted `ssn` not selected). Selecting the restricted
/// column (directly or via `SELECT *`, which expands to include `ssn`) must FAIL
/// CLOSED, not leak it. SQE restriction drops the column from the scan, so an
/// outer reference to it cannot resolve. This matches PostgreSQL column-level
/// security, where `SELECT *` over a column you cannot read is an error, not a
/// silent drop. See issue MED-restricted-column-select-star-ux.md for making
/// this a clean "permission denied" rather than a planner error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_permitted_columns_governed_restricted_fails_closed() {
    let (ctx, _mem) = ctx_with_base_table();
    register_view(&ctx, "v", "SELECT * FROM t").await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(governed_store().await));

    // Permitted columns: governed and returned.
    let batches = enforce_and_run(&ctx, &rewriter, "SELECT salary, region FROM v").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "row filter must apply to permitted columns through a view");
    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(salary.null_count(), salary.len(), "salary mask must apply through a view");

    // Referencing the restricted column (via SELECT *) must not leak it: the
    // rewriter drops ssn from the scan, so the expanded outer ref fails to plan.
    assert!(
        try_enforce_and_run(&ctx, &rewriter, "SELECT * FROM v").await.is_err(),
        "SELECT * over a restricted column must fail closed, not return ssn"
    );
}

/// Test 5 (regression): a plain base-table query (no view) is governed for the
/// permitted columns, and referencing the restricted column fails closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_base_table_permitted_columns_governed() {
    let (ctx, _mem) = ctx_with_base_table();
    let rewriter = PolicyPlanRewriter::new(Arc::new(governed_store().await));

    let batches =
        enforce_and_run(&ctx, &rewriter, "SELECT customer_id, salary, region FROM t").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "base row filter must apply to a plain table query");
    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(salary.null_count(), salary.len(), "salary mask must apply");

    // The restricted column cannot be read, directly or via SELECT *.
    assert!(
        try_enforce_and_run(&ctx, &rewriter, "SELECT ssn FROM t").await.is_err(),
        "selecting a restricted column must fail closed, not leak it"
    );
    assert!(
        try_enforce_and_run(&ctx, &rewriter, "SELECT * FROM t").await.is_err(),
        "SELECT * over a restricted column must fail closed"
    );
}

/// Test 6 (defense-in-depth): a `ViewTable` reached through a directly
/// constructed `TableScan` (NOT via `ctx.sql`'s builder, so DF's plan-time
/// inlining is bypassed) must FAIL CLOSED. Without the guard the rewriter
/// would key policy by the view name `v` (ungoverned) and pass the base table
/// through raw -- the exact reported bypass. The guard denies instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uninlined_view_scan_fails_closed() {
    let schema = employee_schema();
    let batch = employee_batch(schema.clone());
    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

    // View body: SELECT * FROM t  (base scan over `t`).
    let body = LogicalPlanBuilder::scan("t", provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();
    let view = Arc::new(ViewTable::new(body, Some("SELECT * FROM t".to_string())));

    // Build a TableScan node DIRECTLY over the ViewTable source. Unlike
    // `LogicalPlanBuilder::scan` (which inlines when filters are empty), a
    // hand-built `TableScan` with a non-empty filter -- or any path that wraps
    // the provider in a scan node without builder inlining -- leaves the
    // ViewTable un-inlined. We add a filter so even the builder would not
    // inline it, modelling the un-inlined residual case.
    let view_scan = LogicalPlanBuilder::scan_with_filters(
        "v",
        provider_as_source(view.clone() as Arc<dyn TableProvider>),
        None,
        vec![col("customer_id").gt(lit(0_i64))],
    )
    .unwrap()
    .build()
    .unwrap();

    // No policy on `v` (the view name), a row filter on base `t`. Without the
    // guard, the rewriter resolves an empty policy for `v` and passes the
    // ViewTable through, leaking `t` raw. With the guard, it denies.
    let store = InMemoryPolicyStore::new();
    let mut base_policy = ResolvedPolicy::default();
    base_policy.row_filters.push(col("region").eq(lit("EU")));
    store.add_table_policy("default", "t", base_policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let (enforced, summary) = rewriter
        .evaluate(&user("mallory"), view_scan)
        .await
        .unwrap();

    assert!(
        summary.denied,
        "an un-inlined view scan must be reported as denied (fail-closed)"
    );

    // Execute: a deny-all view scan must yield zero rows, never the raw table.
    let ctx = SessionContext::new();
    ctx.register_table("t", mem).unwrap();
    ctx.register_table("v", view as Arc<dyn TableProvider>)
        .unwrap();
    let batches = ctx
        .execute_logical_plan(enforced)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 0,
        "un-inlined view scan must fail closed to zero rows, not leak the base table"
    );
}
