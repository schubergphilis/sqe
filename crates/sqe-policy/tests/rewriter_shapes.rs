//! Architecture review finding 211: policy enforcement over realistic plan
//! shapes, not just a single bare `TableScan`.
//!
//! `rewriter_integration.rs` proves the rewriter works over one scan (plus a
//! handful of subquery smoke tests at its tail). This file drives the SAME
//! `PolicyPlanRewriter` through `SessionContext::sql` so DataFusion produces
//! the real plan shapes a user query actually generates: `IN`/`EXISTS`/scalar/
//! correlated subqueries, INNER/LEFT joins with the governed table on either
//! side, a join keyed on a MASKED column, `UNION ALL` with one governed side,
//! a `WITH` CTE that references the governed table twice, aggregates over a
//! masked/restricted column, and `ORDER BY` on a masked column.
//!
//! Every test executes both the policy-enforced plan and an unrestricted
//! ("baseline") plan run through the identical pipeline with an empty
//! `InMemoryPolicyStore`, and asserts the two genuinely differ in the way the
//! policy demands — so a rewriter regression that silently stops enforcing a
//! shape fails the test rather than passing vacuously.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{col, lit, LogicalPlan};
use datafusion::prelude::SessionContext;

use sqe_core::SessionUser;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{plan_rewriter::PolicyPlanRewriter, MaskType, PolicyEnforcer, ResolvedPolicy};

// ── Fixtures ───────────────────────────────────────────────────────────────

fn user(name: &str, roles: &[&str]) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: roles.iter().map(|r| r.to_string()).collect(),
        subject: None,
        email: None,
        groups: vec![],
    }
}

/// `employees(customer_id, ssn, salary, region)`, 4 rows: 2 EU, 2 US. Four
/// (not three) rows so the CTE self-join test has a same-region pair on BOTH
/// sides of the row filter to contrast against (a 1-row US group can never
/// self-join, which would hide a partially-applied filter).
fn employee_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("ssn", DataType::Utf8, true),
        Field::new("salary", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

fn employee_batch() -> RecordBatch {
    RecordBatch::try_new(
        employee_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "111-11-1111",
                "222-22-2222",
                "333-33-3333",
                "444-44-4444",
            ])),
            Arc::new(Int64Array::from(vec![100_000_i64, 50_000, 75_000, 60_000])),
            Arc::new(StringArray::from(vec!["EU", "US", "EU", "US"])),
        ],
    )
    .unwrap()
}

/// `orders(order_id, customer_id, amount)`. `customer_id = 0` (order 100) is
/// a decoy row with no matching employee: it exists so the masked-join-key
/// test can prove a masked employee row lands on the DECOY, not on its own
/// real order.
fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn orders_batch() -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![100_i64, 101, 102, 103])),
            Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![5_000_i64, 999, 888, 777])),
        ],
    )
    .unwrap()
}

fn mem_table(schema: Arc<Schema>, batch: RecordBatch) -> Arc<MemTable> {
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

/// A fresh context with `employees` and `orders` registered bare (no
/// catalog/schema qualification), matching how a user session sees them.
fn new_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table("employees", mem_table(employee_schema(), employee_batch()))
        .unwrap();
    ctx.register_table("orders", mem_table(orders_schema(), orders_batch()))
        .unwrap();
    ctx
}

/// Register `policy` for `employees` under both plausible namespace keys a
/// bare `FROM employees` reference can resolve to, mirroring
/// `rewriter_integration.rs`'s `subqueries_enforce_row_filters_and_masks`:
/// depending on plan shape DataFusion's default catalog resolution surfaces
/// the table as schema `public` (fully resolved) or as an unqualified
/// reference (namespace `default`).
async fn govern_employees(store: &InMemoryPolicyStore, policy: ResolvedPolicy) {
    store
        .add_table_policy("public", "employees", policy.clone())
        .await;
    store.add_table_policy("default", "employees", policy).await;
}

async fn plan_for(ctx: &SessionContext, sql: &str) -> LogicalPlan {
    ctx.state()
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|e| panic!("failed to plan {sql:?}: {e}"))
}

async fn execute(ctx: &SessionContext, plan: LogicalPlan) -> Vec<RecordBatch> {
    ctx.execute_logical_plan(plan)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

/// Run `sql` through `rewriter` as `u` and return the resulting batches.
async fn enforced(
    ctx: &SessionContext,
    rewriter: &PolicyPlanRewriter,
    sql: &str,
    u: &SessionUser,
) -> Vec<RecordBatch> {
    let plan = plan_for(ctx, sql).await;
    let (rewritten, _summary) = rewriter.evaluate(u, plan).await.unwrap();
    execute(ctx, rewritten).await
}

/// Run `sql` through the SAME pipeline but with an empty policy store, i.e.
/// what an unrestricted user sees. Going through `PolicyPlanRewriter` (rather
/// than skipping it) keeps this a true baseline of the identical code path.
async fn baseline(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let rewriter = PolicyPlanRewriter::new(Arc::new(InMemoryPolicyStore::new()));
    enforced(ctx, &rewriter, sql, &user("nobody", &[])).await
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn i64_col(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {name} is not Int64"));
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn str_col(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {name} is not Utf8"));
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            });
        }
    }
    out
}

// ── Subqueries: IN, EXISTS, scalar, correlated ─────────────────────────────

/// Finding 211, shape: `IN (SELECT ...)`.
///
/// Row filter (`region = 'EU'`) on `employees` must shrink the membership set
/// the outer query's `IN` tests against, not just a bare scan of `employees`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_subquery_enforces_row_filter_on_membership_set() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT o.order_id FROM orders o \
               WHERE o.customer_id IN (SELECT customer_id FROM employees)";

    let mut enforced_ids = i64_col(
        &enforced(&ctx, &rewriter, sql, &user("alice", &[])).await,
        "order_id",
    )
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    enforced_ids.sort_unstable();
    assert_eq!(
        enforced_ids,
        vec![101, 103],
        "only orders for the EU-filtered customers (1, 3) must survive"
    );

    let mut baseline_ids = i64_col(&baseline(&ctx, sql).await, "order_id")
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    baseline_ids.sort_unstable();
    assert_eq!(
        baseline_ids,
        vec![101, 102, 103],
        "an unrestricted user must see the US customer's order too (the test must bite)"
    );
}

/// Finding 211, shape: `EXISTS (SELECT ...)` (correlated).
///
/// Same enforcement as the `IN` case, but the subquery is fully correlated on
/// `customer_id` rather than an uncorrelated membership check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exists_subquery_enforces_row_filter_correlated() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT o.order_id FROM orders o \
               WHERE EXISTS (SELECT 1 FROM employees e WHERE e.customer_id = o.customer_id)";

    let mut enforced_ids = i64_col(
        &enforced(&ctx, &rewriter, sql, &user("alice", &[])).await,
        "order_id",
    )
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    enforced_ids.sort_unstable();
    assert_eq!(enforced_ids, vec![101, 103]);

    let mut baseline_ids = i64_col(&baseline(&ctx, sql).await, "order_id")
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    baseline_ids.sort_unstable();
    assert_eq!(baseline_ids, vec![101, 102, 103]);
}

/// Finding 211, shape: uncorrelated scalar subquery.
///
/// `salary` is masked (Nullify). The scalar subquery `(SELECT max(salary)
/// FROM employees)`, repeated once per outer row, must return NULL — never
/// the real maximum.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalar_subquery_enforces_column_mask() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("salary".to_string(), MaskType::Nullify);
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT o.order_id, (SELECT max(salary) FROM employees) AS max_salary FROM orders o";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    assert_eq!(total_rows(&enforced_batches), 4, "one row per order");
    for v in i64_col(&enforced_batches, "max_salary") {
        assert_eq!(v, None, "masked salary must never surface the real max");
    }

    let baseline_batches = baseline(&ctx, sql).await;
    for v in i64_col(&baseline_batches, "max_salary") {
        assert_eq!(
            v,
            Some(100_000),
            "an unrestricted user must see the real max salary (the test must bite)"
        );
    }
}

/// Finding 211, shape: correlated scalar subquery.
///
/// `ssn` is masked (Redact "***"). `(SELECT max(ssn) FROM employees e WHERE
/// e.customer_id = o.customer_id)` must return the redaction constant for
/// every matched order, never the raw SSN. Wrapped in `max()` because
/// DataFusion's analyzer requires a correlated scalar subquery to be
/// provably aggregated to at most one row; `customer_id` is not declared
/// unique in the schema, so a bare (non-aggregated) `SELECT ssn ...` is
/// rejected as "must be aggregated to return at most one row" even though
/// the fixture data guarantees a single match.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn correlated_scalar_subquery_enforces_column_mask() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT o.order_id, \
               (SELECT max(ssn) FROM employees e WHERE e.customer_id = o.customer_id) AS ssn \
               FROM orders o";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    let mut pairs: Vec<(i64, Option<String>)> = i64_col(&enforced_batches, "order_id")
        .into_iter()
        .flatten()
        .zip(str_col(&enforced_batches, "ssn"))
        .collect();
    pairs.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        pairs,
        vec![
            (100, None),                    // decoy order, no matching employee
            (101, Some("***".to_string())), // matched, masked
            (102, Some("***".to_string())),
            (103, Some("***".to_string())),
        ],
        "the raw SSN must never appear in a correlated subquery result"
    );

    let baseline_batches = baseline(&ctx, sql).await;
    let mut baseline_pairs: Vec<(i64, Option<String>)> = i64_col(&baseline_batches, "order_id")
        .into_iter()
        .flatten()
        .zip(str_col(&baseline_batches, "ssn"))
        .collect();
    baseline_pairs.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        baseline_pairs,
        vec![
            (100, None),
            (101, Some("111-11-1111".to_string())),
            (102, Some("222-22-2222".to_string())),
            (103, Some("333-33-3333".to_string())),
        ],
        "an unrestricted user must see the raw SSN (the test must bite)"
    );
}

// ── Joins ───────────────────────────────────────────────────────────────────

/// Finding 211, shape: INNER JOIN, governed table on the LEFT.
///
/// The row filter on `employees` must drop the US customer's row before the
/// join runs, so its order never appears in the join output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inner_join_enforces_row_filter_governed_left_side() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT e.customer_id, o.amount FROM employees e \
               JOIN orders o ON e.customer_id = o.customer_id";

    let mut enforced_pairs: Vec<(i64, i64)> = {
        let b = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
        i64_col(&b, "customer_id")
            .into_iter()
            .flatten()
            .zip(i64_col(&b, "amount").into_iter().flatten())
            .collect()
    };
    enforced_pairs.sort_unstable();
    assert_eq!(
        enforced_pairs,
        vec![(1, 999), (3, 777)],
        "the US customer's order (2, 888) must be excluded by the row filter"
    );

    let mut baseline_pairs: Vec<(i64, i64)> = {
        let b = baseline(&ctx, sql).await;
        i64_col(&b, "customer_id")
            .into_iter()
            .flatten()
            .zip(i64_col(&b, "amount").into_iter().flatten())
            .collect()
    };
    baseline_pairs.sort_unstable();
    assert_eq!(
        baseline_pairs,
        vec![(1, 999), (2, 888), (3, 777)],
        "an unrestricted user must see the US customer's order too (the test must bite)"
    );
}

/// Finding 211, shape: LEFT JOIN, governed table on the (non-preserved) RIGHT
/// side.
///
/// The row filter must still exclude the US employee row from matching, even
/// though a LEFT JOIN would otherwise keep the `orders` row and simply pad
/// unmatched employee columns with NULL — proving enforcement is not
/// bypassed by preserved-side join semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn left_join_enforces_row_filter_governed_right_side() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT o.order_id, e.region FROM orders o \
               LEFT JOIN employees e ON o.customer_id = e.customer_id";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    let mut enforced_pairs: Vec<(i64, Option<String>)> = i64_col(&enforced_batches, "order_id")
        .into_iter()
        .flatten()
        .zip(str_col(&enforced_batches, "region"))
        .collect();
    enforced_pairs.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        enforced_pairs,
        vec![
            (100, None),                   // no employee for customer 0 regardless
            (101, Some("EU".to_string())), // customer 1, EU, passes the filter
            (102, None),                   // customer 2, US, filtered BEFORE the join
            (103, Some("EU".to_string())), // customer 3, EU, passes the filter
        ],
        "order 102's employee row must be hidden by the row filter, not just masked"
    );

    let baseline_batches = baseline(&ctx, sql).await;
    let mut baseline_pairs: Vec<(i64, Option<String>)> = i64_col(&baseline_batches, "order_id")
        .into_iter()
        .flatten()
        .zip(str_col(&baseline_batches, "region"))
        .collect();
    baseline_pairs.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        baseline_pairs,
        vec![
            (100, None),
            (101, Some("EU".to_string())),
            (102, Some("US".to_string())),
            (103, Some("EU".to_string())),
        ],
        "an unrestricted user must see the US employee's region (the test must bite)"
    );
}

/// Finding 211, shape: JOIN keyed on a MASKED column.
///
/// `employees.customer_id` is masked to a constant (`Redact("0")`, cast to
/// Int64). Every employee row must join against the DECOY order
/// (`customer_id = 0`), never against its own real order — proving the join
/// operator sees the masked value, not the raw one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_on_masked_key_uses_the_masked_value() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("customer_id".to_string(), MaskType::Redact("0".to_string()));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT e.customer_id AS masked_id, o.amount FROM employees e \
               JOIN orders o ON e.customer_id = o.customer_id";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    assert_eq!(
        total_rows(&enforced_batches),
        4,
        "all 4 employees must match the single decoy order (customer_id = 0)"
    );
    for id in i64_col(&enforced_batches, "masked_id")
        .into_iter()
        .flatten()
    {
        assert_eq!(id, 0, "the masked join key must be the redacted constant");
    }
    for amount in i64_col(&enforced_batches, "amount").into_iter().flatten() {
        assert_eq!(
            amount, 5_000,
            "every match must be against the decoy order, never a real customer order"
        );
    }

    let baseline_batches = baseline(&ctx, sql).await;
    let mut baseline_pairs: Vec<(i64, i64)> = i64_col(&baseline_batches, "masked_id")
        .into_iter()
        .flatten()
        .zip(i64_col(&baseline_batches, "amount").into_iter().flatten())
        .collect();
    baseline_pairs.sort_unstable();
    assert_eq!(
        baseline_pairs,
        vec![(1, 999), (2, 888), (3, 777)],
        "an unrestricted user must join on the real raw ids (the test must bite; \
         customer 4 has no order and is correctly absent from an INNER JOIN)"
    );
}

// ── UNION ALL and CTE ────────────────────────────────────────────────────────

/// Finding 211, shape: `UNION ALL` with one governed side.
///
/// The row filter on `employees` must apply to its branch of the union while
/// leaving the ungoverned `orders` branch untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn union_all_enforces_row_filter_on_governed_side_only() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql =
        "SELECT customer_id AS id FROM employees UNION ALL SELECT customer_id AS id FROM orders";

    let mut enforced_ids = i64_col(
        &enforced(&ctx, &rewriter, sql, &user("alice", &[])).await,
        "id",
    )
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    enforced_ids.sort_unstable();
    // employees contributes only the EU ids (1, 3); orders contributes all of
    // its raw ids (0, 1, 2, 3) untouched.
    assert_eq!(enforced_ids, vec![0, 1, 1, 2, 3, 3]);

    let mut baseline_ids = i64_col(&baseline(&ctx, sql).await, "id")
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    baseline_ids.sort_unstable();
    assert_eq!(
        baseline_ids,
        vec![0, 1, 1, 2, 2, 3, 3, 4],
        "an unrestricted user must see all 4 employees plus all 4 orders (the test must bite)"
    );
}

/// Finding 211, shape: `WITH cte AS (...)` referencing the governed table
/// twice (self-join).
///
/// `employees` is inlined into the CTE twice by DataFusion (once per
/// reference). Both copies must carry the row filter: with it applied, only
/// the two EU customers (1, 3) can appear on either side of the self-join;
/// without it, the two US customers (2, 4) would also pair off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cte_referenced_twice_enforces_row_filter_on_both_copies() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("region").eq(lit("EU")));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "WITH cte AS (SELECT customer_id, region FROM employees) \
               SELECT a.customer_id AS a_id, b.customer_id AS b_id \
               FROM cte a JOIN cte b ON a.region = b.region \
               WHERE a.customer_id <> b.customer_id";

    let mut enforced_pairs: Vec<(i64, i64)> = {
        let b = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
        i64_col(&b, "a_id")
            .into_iter()
            .flatten()
            .zip(i64_col(&b, "b_id").into_iter().flatten())
            .collect()
    };
    enforced_pairs.sort_unstable();
    assert_eq!(
        enforced_pairs,
        vec![(1, 3), (3, 1)],
        "only the two EU customers may pair off once BOTH cte copies are filtered"
    );

    let mut baseline_pairs: Vec<(i64, i64)> = {
        let b = baseline(&ctx, sql).await;
        i64_col(&b, "a_id")
            .into_iter()
            .flatten()
            .zip(i64_col(&b, "b_id").into_iter().flatten())
            .collect()
    };
    baseline_pairs.sort_unstable();
    assert_eq!(
        baseline_pairs,
        vec![(1, 3), (2, 4), (3, 1), (4, 2)],
        "an unrestricted user must also see the US pair (2, 4) (the test must bite)"
    );
}

// ── Aggregates and ORDER BY ──────────────────────────────────────────────────

/// Finding 211, shape: `count(distinct masked_col)`.
///
/// `ssn` is masked to a single constant, so every row collapses to one
/// distinct value — the aggregate must never see (and thus never count) the
/// 4 distinct raw SSNs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_distinct_enforces_column_mask() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy
        .column_masks
        .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT count(DISTINCT ssn) AS n FROM employees";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    assert_eq!(
        i64_col(&enforced_batches, "n"),
        vec![Some(1)],
        "all 4 SSNs are masked to the same redaction constant"
    );

    let baseline_batches = baseline(&ctx, sql).await;
    assert_eq!(
        i64_col(&baseline_batches, "n"),
        vec![Some(4)],
        "an unrestricted user must see all 4 distinct raw SSNs (the test must bite)"
    );
}

/// Finding 211, shape: `min(restricted_col)`.
///
/// A restricted column is a forced NULL on every row; `MIN` over an
/// all-NULL group is NULL. The raw minimum salary must never leak through
/// the aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn min_aggregate_enforces_column_restriction() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let policy = ResolvedPolicy {
        restricted_columns: vec!["salary".to_string()],
        ..Default::default()
    };
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT min(salary) AS min_salary FROM employees";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    assert_eq!(
        i64_col(&enforced_batches, "min_salary"),
        vec![None],
        "MIN over an entirely-restricted (NULL) column must be NULL, not the real minimum"
    );

    let baseline_batches = baseline(&ctx, sql).await;
    assert_eq!(
        i64_col(&baseline_batches, "min_salary"),
        vec![Some(50_000)],
        "an unrestricted user must see the real minimum salary (the test must bite)"
    );
}

/// Finding 211, shape: `ORDER BY` on a masked column.
///
/// `customer_id` is masked via a `Custom` expression that negates it
/// (`0 - customer_id`). `ORDER BY customer_id ASC` must sort by that negated
/// value, so the output is `[-4, -3, -2, -1]` (raw ids 4, 3, 2, 1 in reverse)
/// — never the raw ascending order `[1, 2, 3, 4]`, and never a raw positive
/// value at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_enforces_column_mask() {
    let ctx = new_ctx();
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.column_masks.insert(
        "customer_id".to_string(),
        MaskType::Custom(lit(0_i64) - col("customer_id")),
    );
    govern_employees(&store, policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));

    let sql = "SELECT customer_id FROM employees ORDER BY customer_id ASC";

    let enforced_batches = enforced(&ctx, &rewriter, sql, &user("alice", &[])).await;
    assert_eq!(
        i64_col(&enforced_batches, "customer_id")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        vec![-4, -3, -2, -1],
        "the sort must run over the masked (negated) value, and no raw positive id may appear"
    );

    let baseline_batches = baseline(&ctx, sql).await;
    assert_eq!(
        i64_col(&baseline_batches, "customer_id")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "an unrestricted user must see the real ascending order (the test must bite)"
    );
}

// ── EXPLAIN literal-redaction: not exercised ────────────────────────────────
//
// The finding asks for an EXPLAIN test proving policy filter literals are not
// printed verbatim, IF the rewriter provides such a guarantee. It does not:
// `PolicyPlanRewriter` injects row-filter/mask expressions as ordinary
// `Filter`/`Projection` nodes built with `LogicalPlanBuilder`, with no
// separate EXPLAIN-redaction path (confirmed by reading `plan_rewriter.rs` in
// full; there is no `Explain`-specific handling at all). `plan.display_indent()`
// on a rewritten plan shows the row filter's own predicate (e.g. `region =
// Utf8("EU")`) exactly as it would show any other filter — that predicate IS
// the enforcement mechanism, not a leaked secret, so there is nothing here to
// assert without inventing behavior that does not exist. Skipped per
// instructions rather than fabricated.
