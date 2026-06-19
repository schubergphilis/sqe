//! End-to-end integration tests for PolicyPlanRewriter.
//!
//! These tests build real DataFusion LogicalPlans over a MemTable,
//! run them through the rewriter with an InMemoryPolicyStore, and
//! assert both plan shape and executed-batch contents. They are the
//! regression suite for issues #84 (typed NULL masking) and #92
//! (zero e2e coverage of the enforcement engine).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::{Column, TableReference};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;

use sqe_core::SessionUser;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::tag_source::TagSource;
use sqe_policy::{
    plan_rewriter::PolicyPlanRewriter, MaskType, PolicyEnforcer, PolicyStore, ResolvedPolicy,
    TagMaskSpec,
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

// ── Executable mask tests over qualified multilevel scan (Task 5) ─────────
//
// These three tests complement `row_filter_and_mask_execute_over_qualified_multilevel_scan`
// by exercising PartialMask and DateShowYear through the full physical-plan
// pipeline. They catch type_coercion failures that plan-shape-only tests miss.

/// Test A: MASK_SHOW_LAST_4 on a Utf8 column over a qualified multilevel scan.
///
/// Policy masks `ssn` with PartialMask{show_first:0, show_last:4, 'x','x','x'}.
/// The UDF keeps punctuation unchanged and masks digits with 'x', so
/// "111-11-1111" -> "xxx-xx-1111". All three rows are asserted exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_mask_show_last4_on_ssn_over_qualified_multilevel_scan() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "ssn")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.column_masks.insert(
        "ssn".to_string(),
        MaskType::PartialMask {
            show_first: 0,
            show_last: 4,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        },
    );
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("alice", &[]), plan).await.unwrap();

    let batches = execute_multilevel(rewritten, mem).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all three rows must be present (no row filter)");

    let ssn = batches[0]
        .column_by_name("ssn")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    assert_eq!(ssn.value(0), "xxx-xx-1111");
    assert_eq!(ssn.value(1), "xxx-xx-2222");
    assert_eq!(ssn.value(2), "xxx-xx-3333");
}

/// Test B: DateShowYear on a Timestamp(Microsecond, None) column over a qualified
/// multilevel scan.
///
/// Policy masks `hired_at` with DateShowYear. The rewriter emits
/// `CAST(date_trunc('year', hired_at) AS Timestamp(Microsecond, None))`.
/// All three seed timestamps (2023-11-14, ~1_700_000_000_000_000 µs) truncate
/// to 2023-01-01T00:00:00Z = 1_672_531_200_000_000 µs.
/// The test asserts both type preservation (cast-back invariant) and the exact
/// truncated value for all three rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_show_year_on_timestamp_over_qualified_multilevel_scan() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "hired_at")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("hired_at".to_string(), MaskType::DateShowYear);
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("bob", &[]), plan).await.unwrap();

    let batches = execute_multilevel(rewritten, mem).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all three rows must be present (no row filter)");

    let ts_col = batches[0].column_by_name("hired_at").unwrap();
    // Type-preservation invariant: cast-back must keep the original column type.
    assert_eq!(
        ts_col.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "DateShowYear must preserve Timestamp(Microsecond, None) column type"
    );

    // All three seeds are in 2023-11 and truncate to 2023-01-01T00:00:00Z.
    // 2023-01-01T00:00:00Z = 1_672_531_200_000_000 microseconds since Unix epoch.
    let ts_arr = ts_col
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let expected_micros: i64 = 1_672_531_200_000_000;
    assert_eq!(ts_arr.value(0), expected_micros);
    assert_eq!(ts_arr.value(1), expected_micros);
    assert_eq!(ts_arr.value(2), expected_micros);
}

/// Test D: DateShowYear on a Date32 column (the Iceberg DATE wire type).
///
/// Iceberg DATE columns surface as Arrow Date32 (days since Unix epoch).
/// The `apply_mask` DateShowYear arm handles `Date32 | Date64 | Timestamp`, but
/// only Timestamp was previously executed in tests. This test exercises the
/// Date32 path end-to-end through the physical planner.
///
/// Schema: `(id Int64, d Date32)` with seed rows:
///   - 19738 = 2024-01-16
///   - 19800 = 2024-03-18
/// Both are strictly after 2024-01-01, so year-truncation to 19723 (2024-01-01)
/// is observable (the value changes). The expected output is 19723 for all rows.
/// Verified: `python3 -c "import datetime; print((datetime.date(2024,1,1)-datetime.date(1970,1,1)).days)"` -> 19723.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_show_year_on_date32_executes_and_truncates_to_jan1() {
    // Self-contained schema and batch; does not use the shared `employee_schema`.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d", DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            // 19738 = 2024-01-16, 19800 = 2024-03-18 (both > 2024-01-01 = 19723)
            Arc::new(Date32Array::from(vec![19738_i32, 19800_i32])),
        ],
    )
    .unwrap();

    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let tref = TableReference::full("cat", "ns", "t");
    let scan = LogicalPlanBuilder::scan(tref.clone(), provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "id")),
            Expr::Column(Column::new(Some(tref.clone()), "d")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("d".to_string(), MaskType::DateShowYear);
    // namespace last component "ns" matches the catalog schema key
    store.add_table_policy("ns", "t", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("eve", &[]), plan).await.unwrap();

    // Register under catalog "cat", schema "ns" and execute.
    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("ns", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("cat", catalog);
    ctx.register_table(tref, mem).unwrap();
    let batches = ctx
        .execute_logical_plan(rewritten)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "both rows must be present (no row filter)");

    let d_col = batches[0].column_by_name("d").unwrap();
    // Type-preservation invariant: DateShowYear must keep Date32, not leave it as Timestamp.
    assert_eq!(
        d_col.data_type(),
        &DataType::Date32,
        "DateShowYear must preserve Date32 column type after cast-back"
    );

    // All rows must truncate to 2024-01-01 = 19723 days since 1970-01-01.
    let d_arr = d_col.as_any().downcast_ref::<Date32Array>().unwrap();
    let expected_day: i32 = 19723; // 2024-01-01
    assert_eq!(d_arr.value(0), expected_day, "2024-01-16 must truncate to 2024-01-01");
    assert_eq!(d_arr.value(1), expected_day, "2024-03-18 must truncate to 2024-01-01");
}

// ── Tag-based masking: executable end-to-end tests (Phase 3a) ────────────────
//
// These four tests exercise the full TagSource -> resolve_tags -> merge_tag_masks
// -> plan-rewrite pipeline end-to-end over a qualified multilevel scan.
//
// Infrastructure:
//  - `TagTestStore`: a minimal `PolicyStore` for these tests. `resolve` returns
//    a configured `ResolvedPolicy` (defaults to empty = passthrough). `resolve_tags`
//    returns the configured (tag_masks, tag_filters, unmappable) triple.
//  - `FakeTagSource`: a `TagSource` returning a configured column->tags map; also
//    captures every `namespace` arg it receives into an `Arc<Mutex<...>>` log so
//    tests can assert the FULL multi-level namespace was passed, not a truncated form.

/// Configurable `PolicyStore` for tag-masking integration tests.
///
/// `resolve` returns `resource_policy` (cloned); useful for tests that need
/// a resource-level mask pre-loaded (precedence test 3). Defaults to empty.
/// `resolve_tags` returns the configured triple unchanged.
struct TagTestStore {
    resource_policy: ResolvedPolicy,
    tag_masks: HashMap<String, TagMaskSpec>,
    tag_filters: Vec<Expr>,
    unmappable: HashSet<String>,
}

impl TagTestStore {
    fn new() -> Self {
        Self {
            resource_policy: ResolvedPolicy::default(),
            tag_masks: HashMap::new(),
            tag_filters: vec![],
            unmappable: HashSet::new(),
        }
    }

    fn with_resource_policy(mut self, p: ResolvedPolicy) -> Self {
        self.resource_policy = p;
        self
    }

    fn with_tag_mask(mut self, tag: &str, mask: MaskType) -> Self {
        self.tag_masks.insert(tag.to_string(), TagMaskSpec::Ready(mask));
        self
    }

    fn with_unmappable(mut self, tag: &str) -> Self {
        self.unmappable.insert(tag.to_string());
        self
    }
}

#[async_trait::async_trait]
impl PolicyStore for TagTestStore {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(self.resource_policy.clone())
    }

    async fn resolve_tags(
        &self,
        _user: &SessionUser,
        _tags: &HashSet<String>,
    ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
        (self.tag_masks.clone(), self.tag_filters.clone(), self.unmappable.clone())
    }
}

/// A `TagSource` returning a fixed column->tags map.
///
/// Every call to `column_tags` appends the `namespace` argument (as a
/// `Vec<String>`) to `calls`, so tests can assert the exact namespace
/// components received (full multi-level path, not the last component).
struct FakeTagSource {
    col_tags: HashMap<String, Vec<String>>,
    /// Log of namespace args passed to `column_tags`, one entry per call.
    calls: Arc<Mutex<Vec<Vec<String>>>>,
}

impl FakeTagSource {
    fn new(col_tags: HashMap<String, Vec<String>>) -> (Self, Arc<Mutex<Vec<Vec<String>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let source = Self { col_tags, calls: Arc::clone(&calls) };
        (source, calls)
    }
}

impl TagSource for FakeTagSource {
    fn column_tags(
        &self,
        _catalog: Option<&str>,
        namespace: &[String],
        _table: &str,
    ) -> HashMap<String, Vec<String>> {
        self.calls.lock().unwrap().push(namespace.to_vec());
        self.col_tags.clone()
    }
}

/// Test 1 -- tag mask applies end to end.
///
/// FakeTagSource returns `{"ssn": ["PII"]}`.
/// TagTestStore.resolve_tags returns `{"PII": MaskType::Nullify}`.
/// Over the qualified multilevel scan with a user projection selecting
/// `customer_id, ssn`: rewrite (with `.with_tag_source(fake)`) +
/// `execute_multilevel` -> assert `ssn` column is entirely NULL.
/// Proves column_tags -> tag mask -> applied through the physical planner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tag_mask_applies_end_to_end() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "ssn")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let mut col_tags = HashMap::new();
    col_tags.insert("ssn".to_string(), vec!["PII".to_string()]);
    let (fake_source, _calls) = FakeTagSource::new(col_tags);

    let store = TagTestStore::new().with_tag_mask("PII", MaskType::Nullify);

    let rewriter = PolicyPlanRewriter::new(Arc::new(store))
        .with_tag_source(Arc::new(fake_source));
    let rewritten = rewriter.evaluate(&user("alice", &[]), plan).await.unwrap();

    let batches = execute_multilevel(rewritten, mem).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all three rows must be present (no row filter)");

    let ssn = batches[0].column_by_name("ssn").unwrap();
    assert_eq!(
        ssn.null_count(),
        ssn.len(),
        "tag-masked ssn must be entirely NULL (Nullify via PII tag)"
    );
}

/// Test 2 -- multi-level namespace identity: FakeTagSource receives the FULL
/// namespace path `["ns1","ns2"]`, not the last component `["ns2"]` or the
/// dotted string `["ns1.ns2"]`.
///
/// This is the recurring identity bug: the rewriter previously split
/// `table_ref.schema()` ("ns1.ns2") into components but then only passed the
/// last one to the tag source cache, so a multi-level key missed the entry.
/// The fix threads the full split vector through; this test is the regression
/// gate proving that contract holds.
///
/// The test also asserts the mask was applied (ssn is NULL), confirming the tag
/// source was called at all and not silently skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tag_source_receives_full_multilevel_namespace() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "ssn")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let mut col_tags = HashMap::new();
    col_tags.insert("ssn".to_string(), vec!["PII".to_string()]);
    let (fake_source, calls_log) = FakeTagSource::new(col_tags);

    let store = TagTestStore::new().with_tag_mask("PII", MaskType::Nullify);

    let rewriter = PolicyPlanRewriter::new(Arc::new(store))
        .with_tag_source(Arc::new(fake_source));
    let rewritten = rewriter.evaluate(&user("alice", &[]), plan).await.unwrap();

    // --- Identity assertion: namespace arg must be the full ["ns1","ns2"] ---
    // Assert before the async execute call so the MutexGuard is not held
    // across an await point (clippy::await_holding_lock).
    {
        let calls = calls_log.lock().unwrap();
        assert_eq!(calls.len(), 1, "column_tags must be called exactly once");
        assert_eq!(
            calls[0],
            vec!["ns1".to_string(), "ns2".to_string()],
            "column_tags must receive the FULL multi-level namespace components \
             [\"ns1\",\"ns2\"], not [\"ns2\"] or [\"ns1.ns2\"] (identity bug regression)"
        );
    } // MutexGuard dropped here, before the await below.

    // --- Mask assertion: ssn is NULL (tag route worked) ---
    let batches = execute_multilevel(rewritten, mem).await;
    let ssn = batches[0].column_by_name("ssn").unwrap();
    assert_eq!(
        ssn.null_count(),
        ssn.len(),
        "ssn must be NULL confirming the tag source call was used"
    );
}

/// Test 3 -- resource mask wins over tag mask.
///
/// A resource policy (from `resolve`) already carries `ssn -> Redact("***")`.
/// The tag source returns `{"ssn": ["PII"]}` and `resolve_tags` returns
/// `{"PII": MaskType::Hash}`. The resource mask must win: ssn shows "***",
/// not a 64-char SHA-256 hex string.
///
/// Proves the precedence rule: resource mask wins over tag mask (rule 2 of
/// `merge_tag_masks` contract). A Hash result would be a 64-char hex string,
/// so the constant "***" is an unambiguous discriminator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_mask_wins_over_tag_mask() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "ssn")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let mut col_tags = HashMap::new();
    col_tags.insert("ssn".to_string(), vec!["PII".to_string()]);
    let (fake_source, _calls) = FakeTagSource::new(col_tags);

    // Resource policy pre-loaded with Redact("***") on ssn.
    let mut resource_policy = ResolvedPolicy::default();
    resource_policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Redact("***".to_string()));

    // Tag store would apply Hash if resource mask did not win.
    let store = TagTestStore::new()
        .with_resource_policy(resource_policy)
        .with_tag_mask("PII", MaskType::Hash);

    let rewriter = PolicyPlanRewriter::new(Arc::new(store))
        .with_tag_source(Arc::new(fake_source));
    let rewritten = rewriter.evaluate(&user("bob", &[]), plan).await.unwrap();

    let batches = execute_multilevel(rewritten, mem).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all three rows must be present (no row filter)");

    let ssn_col = batches[0]
        .column_by_name("ssn")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    for i in 0..ssn_col.len() {
        assert_eq!(
            ssn_col.value(i),
            "***",
            "resource Redact(\"***\") must win over tag Hash (row {})",
            i
        );
    }
}

/// Test 4 -- unmappable tag fails closed: ssn is DROPPED from the output.
///
/// TagSource returns `{"ssn":["SECRET"]}`. `resolve_tags` returns empty masks
/// + `unmappable={"SECRET"}`. Per the fail-closed contract, a column bearing
/// only an unmappable tag (no resource mask) is RESTRICTED (dropped), not
/// returned raw.
///
/// The plan is a bare scan (no outer projection selecting ssn) to avoid the
/// "No field named ssn" planner error when the inner restriction drops the
/// column before the outer ref can resolve. The test asserts ssn is absent
/// from the output schema after rewrite + execute.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmappable_tag_restricts_column_fail_closed() {
    let (mem, scan) = build_multilevel_scan_with_mem();
    // Use the bare scan (no outer projection) so the restricted ssn column
    // does not appear in a user reference that would fail planning.
    // The assertion is on the output schema: ssn must be absent.

    let mut col_tags = HashMap::new();
    col_tags.insert("ssn".to_string(), vec!["SECRET".to_string()]);
    let (fake_source, _calls) = FakeTagSource::new(col_tags);

    // resolve_tags returns empty masks, SECRET is unmappable.
    let store = TagTestStore::new().with_unmappable("SECRET");

    let rewriter = PolicyPlanRewriter::new(Arc::new(store))
        .with_tag_source(Arc::new(fake_source));
    let rewritten = rewriter.evaluate(&user("carol", &[]), scan).await.unwrap();

    let batches = execute_multilevel(rewritten, mem).await;
    let schema = batches[0].schema();
    assert!(
        schema.column_with_name("ssn").is_none(),
        "ssn must be ABSENT after unmappable-tag fail-closed restriction (got schema: {:?})",
        schema
    );
    // All other columns must still be present.
    assert!(schema.column_with_name("customer_id").is_some());
    assert!(schema.column_with_name("salary").is_some());
}

// ── Session-function row-filter tests (Phase 2B, Task 3) ─────────────────────
//
// These two tests prove that `is_role_in_session('admin') OR region = 'EU'`
// used as a Ranger row filter:
//   (a) works end-to-end: admin sees all 3 rows, analyst sees only 2 EU rows.
//   (b) const-folds to a literal during DataFusion logical optimization on the
//       coordinator, so the plan shipped to workers contains ONLY literals +
//       column references -- no session UDF, no session state.
//
// DISTRIBUTION GATE: session functions are `Volatility::Immutable` and their
// argument is a string literal, so DataFusion const-folds
// `is_role_in_session('admin')` to `true`/`false` during coordinator-side
// logical optimization. The optimized plan sent to workers contains only
// Boolean literals and column predicates. Workers never see, evaluate, or
// carry the UDF. This test is the gate for that property: if the fold does NOT
// happen (the plan string still contains `is_role_in_session` after
// `ctx.state().optimize()`), the test fails hard and this MUST be investigated
// before shipping the distributed path.

/// Build a SessionContext with the catalog/schema/table registration needed
/// to optimize a plan that scans `cat.ns1.ns2.employees`, and return both the
/// context and the optimized plan. Used to assert the const-fold property.
async fn build_optimize_ctx_and_plan(
    plan: LogicalPlan,
    mem: Arc<MemTable>,
) -> (SessionContext, LogicalPlan) {
    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("ns1.ns2", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("cat", catalog);
    ctx.register_table(TableReference::full("cat", "ns1.ns2", "employees"), mem)
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();
    (ctx, optimized)
}

/// Test A: admin role -- `is_role_in_session('admin')` folds to `true`.
/// The OR short-circuits; the row filter disappears entirely. All 3 rows
/// are returned. The optimized plan must contain no residual
/// `is_role_in_session` call (it folded to the literal `true`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_fn_row_filter_admin_sees_all_rows_and_folds_to_literal() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "region")),
        ])
        .unwrap()
        .build()
        .unwrap();

    // Build the filter expression with admin identity baked in.
    let identity = sqe_policy::session_udf::SessionIdentity {
        username: "carol".into(),
        roles: vec!["admin".into()],
        ..Default::default()
    };
    let filter_expr = sqe_policy::policy_expr::parse_sql_predicate(
        "is_role_in_session('admin') OR region = 'EU'",
        &identity,
    )
    .expect("parse_sql_predicate must succeed for admin identity");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(filter_expr);
    store.add_table_policy("ns2", "employees", policy).await;

    // SessionUser roles do not affect InMemoryPolicyStore resolution; the
    // SessionIdentity baked into the UDF is what matters for fold behavior.
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("carol", &["admin"]), plan).await.unwrap();

    // --- (a) end-to-end correctness: admin sees all 3 rows ---
    let mem_for_exec = Arc::clone(&mem);
    let batches = execute_multilevel(rewritten.clone(), mem_for_exec).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "admin: filter must fold to true -> all 3 rows visible");

    // --- (b) const-fold gate: optimized plan must contain no is_role_in_session ---
    let (_ctx, optimized) = build_optimize_ctx_and_plan(rewritten, mem).await;
    let plan_str = format!("{}", optimized.display_indent());
    assert!(
        !plan_str.contains("is_role_in_session"),
        "DISTRIBUTION GATE FAILED: is_role_in_session was NOT const-folded in admin plan.\n\
         Optimized plan:\n{plan_str}"
    );
}

/// Test B: analyst role -- `is_role_in_session('admin')` folds to `false`.
/// The OR reduces to `region = 'EU'`; only 2 EU rows are returned. The
/// optimized plan must contain no residual `is_role_in_session` call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_fn_row_filter_analyst_sees_eu_only_and_folds_to_literal() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "customer_id")),
            Expr::Column(Column::new(Some(tref.clone()), "region")),
        ])
        .unwrap()
        .build()
        .unwrap();

    // Build the filter expression with analyst identity baked in.
    let identity = sqe_policy::session_udf::SessionIdentity {
        username: "dave".into(),
        roles: vec!["analyst".into()],
        ..Default::default()
    };
    let filter_expr = sqe_policy::policy_expr::parse_sql_predicate(
        "is_role_in_session('admin') OR region = 'EU'",
        &identity,
    )
    .expect("parse_sql_predicate must succeed for analyst identity");

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(filter_expr);
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("dave", &["analyst"]), plan).await.unwrap();

    // --- (a) end-to-end correctness: analyst sees only 2 EU rows ---
    let mem_for_exec = Arc::clone(&mem);
    let batches = execute_multilevel(rewritten.clone(), mem_for_exec).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "analyst: filter must reduce to region='EU' -> 2 EU rows");

    // Also verify only EU values appear (US row is excluded).
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    for batch in &batches {
        if let Some(region_col) = batch.column_by_name("region") {
            let regions = region_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("region must be StringArray");
            for i in 0..regions.len() {
                assert_eq!(
                    regions.value(i),
                    "EU",
                    "analyst must only see EU rows, got: {}",
                    regions.value(i)
                );
            }
        }
    }
    let _ = total_rows; // suppress unused warning

    // --- (b) const-fold gate: optimized plan must contain no is_role_in_session ---
    let (_ctx, optimized) = build_optimize_ctx_and_plan(rewritten, mem).await;
    let plan_str = format!("{}", optimized.display_indent());
    assert!(
        !plan_str.contains("is_role_in_session"),
        "DISTRIBUTION GATE FAILED: is_role_in_session was NOT const-folded in analyst plan.\n\
         Optimized plan:\n{plan_str}"
    );
}

/// Test C: PartialMask on a non-string (Int64) column falls back to typed NULL
/// over a qualified multilevel scan.
///
/// A char-class mask is meaningless on an integer. The rewriter must emit a
/// typed NULL (Int64 NULL) rather than coercing to Utf8, which would break
/// downstream predicates and fail physical planning. Both the type and the
/// null-ness are asserted, mirroring `nullify_mask_on_bigint_executes_without_type_error`
/// but for the PartialMask non-string NULL-fallback path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_mask_on_non_string_falls_back_to_typed_null_over_qualified_multilevel_scan() {
    let (mem, scan) = build_multilevel_scan_with_mem();

    let tref = TableReference::full("cat", "ns1.ns2", "employees");
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![Expr::Column(Column::new(
            Some(tref.clone()),
            "customer_id",
        ))])
        .unwrap()
        .build()
        .unwrap();

    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.column_masks.insert(
        "customer_id".to_string(),
        MaskType::PartialMask {
            show_first: 0,
            show_last: 4,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        },
    );
    store.add_table_policy("ns2", "employees", policy).await;

    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let rewritten = rewriter.evaluate(&user("carol", &[]), plan).await.unwrap();

    let batches = execute_multilevel(rewritten, mem).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all three rows must be present (no row filter)");

    let id_col = batches[0].column_by_name("customer_id").unwrap();
    // Type-preservation invariant: non-string NULL-fallback must keep Int64.
    assert_eq!(
        id_col.data_type(),
        &DataType::Int64,
        "PartialMask on Int64 must fall back to Int64 NULL (not Utf8)"
    );
    assert_eq!(
        id_col.null_count(),
        id_col.len(),
        "every customer_id value must be NULL after non-string PartialMask fallback"
    );
}
