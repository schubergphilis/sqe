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
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
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
