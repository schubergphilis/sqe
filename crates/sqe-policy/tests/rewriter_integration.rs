//! End-to-end integration tests for PolicyPlanRewriter.
//!
//! These tests build real DataFusion LogicalPlans over a MemTable,
//! run them through the rewriter with an InMemoryPolicyStore, and
//! assert both plan shape and executed-batch contents. They are the
//! regression suite for issues #84 (typed NULL masking) and #92
//! (zero e2e coverage of the enforcement engine).

use std::sync::Arc;

use arrow::array::{
    Array, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::{Column, TableReference};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;

use sqe_core::SessionUser;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{
    plan_rewriter::PolicyPlanRewriter, MaskType, PolicyEnforcer, PolicyStore, ResolvedPolicy,
};

fn user(name: &str, roles: &[&str]) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: roles.iter().map(|r| r.to_string()).collect(),
    }
}

fn employee_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("ssn", DataType::Utf8, true),
        Field::new("salary", DataType::Decimal128(18, 2), true),
        Field::new("hired_at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

fn employee_batch(schema: Arc<Schema>) -> RecordBatch {
    // Decimal128 cents-units: $100,000, $50,000, $75,000.
    let salary = Decimal128Array::from(vec![10_000_000_i128, 5_000_000_i128, 7_500_000_i128])
        .with_precision_and_scale(18, 2)
        .unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["111-11-1111", "222-22-2222", "333-33-3333"])),
            Arc::new(salary),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_700_000_000_000_000_i64,
                1_700_000_001_000_000_i64,
                1_700_000_002_000_000_i64,
            ])),
            Arc::new(StringArray::from(vec!["EU", "US", "EU"])),
        ],
    )
    .unwrap()
}

fn build_scan(table_name: &str) -> (Arc<MemTable>, LogicalPlan) {
    let schema = employee_schema();
    let batch = employee_batch(schema.clone());
    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let table_ref = TableReference::bare(table_name.to_string());
    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();
    (mem, plan)
}

async fn execute(plan: LogicalPlan, mem: Arc<MemTable>, table_name: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, mem).unwrap();
    let df = ctx.execute_logical_plan(plan).await.unwrap();
    df.collect().await.unwrap()
}

/// Build a scan over a structured multi-level reference: catalog `cat`,
/// schema `ns1.ns2` (how a multi-level Iceberg namespace renders in
/// DataFusion), table `employees`. Stringified, this is the 4-part name
/// `cat.ns1.ns2.employees` that the old split-on-'.' logic dropped into
/// `_ => continue` and thus passed through unguarded (issue #205).
fn build_multilevel_scan() -> LogicalPlan {
    let schema = employee_schema();
    let batch = employee_batch(schema.clone());
    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let table_ref = TableReference::full("cat", "ns1.ns2", "employees");
    LogicalPlanBuilder::scan(table_ref, provider_as_source(mem), None)
        .unwrap()
        .build()
        .unwrap()
}


/// Like `build_multilevel_scan` but returns the MemTable so the plan can be
/// executed against a registered multi-level catalog/schema.
fn build_multilevel_scan_with_mem() -> (Arc<MemTable>, LogicalPlan) {
    let schema = employee_schema();
    let batch = employee_batch(schema.clone());
    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let table_ref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();
    (mem, plan)
}

/// Execute a plan whose scan is the full `cat.ns1.ns2.employees` reference by
/// registering the matching catalog + schema first.
async fn execute_multilevel(plan: LogicalPlan, mem: Arc<MemTable>) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("ns1.ns2", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("cat", catalog);
    ctx.register_table(TableReference::full("cat", "ns1.ns2", "employees"), mem)
        .unwrap();
    let df = ctx.execute_logical_plan(plan).await.unwrap();
    df.collect().await.unwrap()
}

/// Regression for the live Ranger demo failure: against a real Iceberg-style
/// scan whose fields are FULLY QUALIFIED (`cat.ns1.ns2.employees.col`), the
/// rewriter must normalize the bare-column row filter and mask args to the
/// scan's qualifier. Before the LogicalPlanBuilder normalization fix this
/// failed physical planning with
/// "type_coercion ... No field named cat.ns1.ns2.employees.region". The
/// existing tests used a 1-part bare ref, which DataFusion normalizes
/// leniently, so they never reproduced it. This one executes the plan, so it
/// exercises the analyzer + physical planner that actually broke.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_filter_and_mask_execute_over_qualified_multilevel_scan() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    // Mimic the real user query `SELECT region, salary FROM cat.ns1.ns2.employees`:
    // an outer projection that references the columns by their QUALIFIED names
    // (as the planner does against a real scan). The masked `salary` must keep
    // its qualifier through the rewrite or this outer ref fails to resolve.
    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "region")),
            Expr::Column(Column::new(Some(tref.clone()), "salary")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Nullify);
    // schema "ns1.ns2" -> last dotted component "ns2" is the namespace key
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("alice", &[]), plan).await.unwrap();

    // The step that failed before the fix: analyze + physical-plan + run.
    let batches = execute_multilevel(rewritten, mem).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "row filter should leave only the two EU rows");
    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(
        salary.null_count(),
        salary.len(),
        "masked salary must be entirely NULL"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_filter_injects_filter_above_scan() {
    let (mem, plan) = build_scan("employees");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    store.add_table_policy("default", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter
        .evaluate(&user("alice", &[]), plan)
        .await
        .unwrap();

    let s = format!("{}", rewritten.display_indent());
    assert!(
        s.starts_with("Filter:"),
        "expected Filter at root of rewritten plan, got: {s}"
    );

    let batches = execute(rewritten, mem, "employees").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "row filter should leave only EU rows");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nullify_mask_on_bigint_executes_without_type_error() {
    // Regression test for #84.
    let (mem, plan) = build_scan("employees");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("customer_id".to_string(), MaskType::Nullify);
    store.add_table_policy("default", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter
        .evaluate(&user("bob", &[]), plan)
        .await
        .unwrap();

    let batches = execute(rewritten, mem, "employees").await;
    let id_col = batches[0].column_by_name("customer_id").unwrap();
    assert_eq!(id_col.data_type(), &DataType::Int64);
    assert_eq!(id_col.null_count(), id_col.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nullify_mask_on_decimal_executes_without_type_error() {
    let (mem, plan) = build_scan("employees");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Nullify);
    store.add_table_policy("default", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("c", &[]), plan).await.unwrap();
    let batches = execute(rewritten, mem, "employees").await;
    let salary = batches[0].column_by_name("salary").unwrap();
    assert_eq!(salary.data_type(), &DataType::Decimal128(18, 2));
    assert_eq!(salary.null_count(), salary.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nullify_mask_on_timestamp_executes_without_type_error() {
    let (mem, plan) = build_scan("employees");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("hired_at".to_string(), MaskType::Nullify);
    store.add_table_policy("default", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("d", &[]), plan).await.unwrap();
    let batches = execute(rewritten, mem, "employees").await;
    let ts = batches[0].column_by_name("hired_at").unwrap();
    assert_eq!(
        ts.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(ts.null_count(), ts.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redact_mask_on_string_column_returns_constant() {
    let (mem, plan) = build_scan("employees");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
    store.add_table_policy("default", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("e", &[]), plan).await.unwrap();
    let batches = execute(rewritten, mem, "employees").await;
    let ssn = batches[0]
        .column_by_name("ssn")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    for i in 0..ssn.len() {
        assert_eq!(ssn.value(i), "***");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_column_is_dropped_from_projection() {
    let (mem, plan) = build_scan("employees");

    let store = InMemoryPolicyStore::new();
    let policy = ResolvedPolicy {
        restricted_columns: vec!["ssn".to_string()],
        ..Default::default()
    };
    store.add_table_policy("default", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("f", &[]), plan).await.unwrap();
    let batches = execute(rewritten, mem, "employees").await;
    let schema = batches[0].schema();
    assert!(
        schema.column_with_name("ssn").is_none(),
        "ssn must be absent from rewritten projection"
    );
}

// Fail-closed: a PolicyStore that errors must cause the rewriter to
// inject lit(false), returning zero rows.
struct PoisonStore;

#[async_trait::async_trait]
impl PolicyStore for PoisonStore {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Err(sqe_core::SqeError::Execution(
            "policy backend unreachable".to_string(),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poisoned_policy_store_fails_closed_to_zero_rows() {
    let (mem, plan) = build_scan("employees");

    let rewriter = PolicyPlanRewriter::new(Arc::new(PoisonStore));
    let rewritten = rewriter.evaluate(&user("g", &[]), plan).await.unwrap();

    let batches = execute(rewritten, mem, "employees").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "fail-closed must drop every row");
}

// ── Multi-level namespace policy enforcement (issue #205) ────────────
//
// Before the fix, a 4-part name (catalog.ns1.ns2.table) hit `_ => continue`
// in the rewriter, leaving an empty ResolvedPolicy that passed through. Any
// row filter / mask / restriction on a multi-level-namespace table was
// silently bypassed. The read path now keys by the LAST namespace component
// (matching the write path's `namespace().last()`), so a policy stored under
// `ns2` is found.

// These four assert on rewritten plan shape rather than executing, because a
// 4-part `cat.ns1.ns2.employees` reference cannot be registered in a default
// SessionContext (no catalog `cat`). The rewriter operates on the
// LogicalPlan, so plan shape proves the policy was applied. Execution of the
// injected nodes is already covered by the single-level tests above.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multilevel_namespace_row_filter_is_applied() {
    let plan = build_multilevel_scan();

    // Policy keyed by the LAST namespace component, exactly as the write
    // path stores it (write_handler keys by `namespace().last()`).
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter
        .evaluate(&user("alice", &[]), plan)
        .await
        .unwrap();

    let s = format!("{}", rewritten.display_indent());
    assert!(
        s.starts_with("Filter:"),
        "row filter must be injected over multi-level scan, got: {s}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multilevel_namespace_restriction_is_applied() {
    let plan = build_multilevel_scan();

    let store = InMemoryPolicyStore::new();
    let policy = ResolvedPolicy {
        restricted_columns: vec!["ssn".to_string()],
        ..Default::default()
    };
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("bob", &[]), plan).await.unwrap();

    let s = format!("{}", rewritten.display_indent());
    assert!(
        s.starts_with("Projection:"),
        "restriction must inject a Projection over multi-level scan, got: {s}"
    );
    assert!(
        !s.contains("ssn"),
        "ssn must be dropped from the multi-level namespace projection, got: {s}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multilevel_namespace_no_policy_still_passes_through() {
    // A confidently-resolved 4-part name with no matching policy must keep
    // working (pass through). Fail-closed is reserved for references we
    // cannot resolve to a key, not for resolvable-but-unguarded tables.
    let plan = build_multilevel_scan();

    let store = InMemoryPolicyStore::new();
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("carol", &[]), plan).await.unwrap();

    let s = format!("{}", rewritten.display_indent());
    assert!(
        s.starts_with("TableScan:"),
        "no policy on a resolvable table must pass through unchanged, got: {s}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multilevel_namespace_poisoned_store_fails_closed() {
    // The bypass is gone even when the store errors: a multi-level table
    // must fail closed (lit(false) filter injected), not pass through. The
    // single-level poison test above proves lit(false) -> zero rows on
    // execution; here we prove the filter is injected for a 4-part name.
    let plan = build_multilevel_scan();

    let rewriter = PolicyPlanRewriter::new(Arc::new(PoisonStore));
    let rewritten = rewriter.evaluate(&user("dave", &[]), plan).await.unwrap();

    let s = format!("{}", rewritten.display_indent());
    assert!(
        s.starts_with("Filter:"),
        "multi-level table must fail closed with an injected filter, got: {s}"
    );
}
