//! A/B cost of the row-filter guard on TPC-H (#805), run by hand:
//!
//! ```text
//! SQE_GUARD_AB_DIR=/path/to/tpch-parquet cargo test --release -p sqe-policy \
//!     --test it guard_ab -- --ignored --nocapture
//! ```
//!
//! The directory holds one `<table>.parquet` per TPC-H table (DuckDB:
//! `CALL dbgen(sf=1); EXPORT DATABASE 'dir' (FORMAT parquet)`). Each query
//! runs plain (A) and rewritten under a trivially-true row filter on
//! `lineitem` (B); the report says whether B was guarded and the median of
//! interleaved runs of both. Unguarded queries are the control for the filter itself.

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::logical_expr::{col, lit, LogicalPlan};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use sqe_core::SessionUser;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{plan_rewriter::PolicyPlanRewriter, PolicyEnforcer, ResolvedPolicy};

const TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];
const RUNS: usize = 11;

async fn run_once(ctx: &SessionContext, plan: &LogicalPlan) -> (Duration, usize) {
    let started = Instant::now();
    let batches = ctx
        .execute_logical_plan(plan.clone())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    (started.elapsed(), batches.iter().map(|b| b.num_rows()).sum())
}

/// Median times of A and B, run interleaved so drift hits both alike.
async fn medians(ctx: &SessionContext, a: &LogicalPlan, b: &LogicalPlan) -> (Duration, Duration) {
    let (mut ta, mut tb) = (Vec::new(), Vec::new());
    for _ in 0..RUNS {
        let (da, rows_a) = run_once(ctx, a).await;
        let (db, rows_b) = run_once(ctx, b).await;
        assert_eq!(rows_a, rows_b, "a trivially-true filter changed the result");
        ta.push(da);
        tb.push(db);
    }
    ta.sort();
    tb.sort();
    (ta[RUNS / 2], tb[RUNS / 2])
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs SQE_GUARD_AB_DIR with TPC-H parquet; run with --release"]
async fn guard_ab() {
    let dir = std::env::var("SQE_GUARD_AB_DIR").expect("SQE_GUARD_AB_DIR");
    let queries = concat!(env!("CARGO_MANIFEST_DIR"), "/../../benchmarks/queries/tpch");
    let ctx = SessionContext::new();
    for t in TABLES {
        ctx.register_parquet(t, &format!("{dir}/{t}.parquet"), ParquetReadOptions::default())
            .await
            .unwrap();
    }
    let store = InMemoryPolicyStore::new();
    let mut policy = ResolvedPolicy::default();
    policy.row_filters.push(col("l_orderkey").gt_eq(lit(0i64)));
    store.add_table_policy("public", "lineitem", policy).await;
    let rewriter = PolicyPlanRewriter::new(Arc::new(store));
    let user = SessionUser {
        username: "bench".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    };

    println!("query  guarded  plain_ms  policy_ms  ratio");
    let (mut sum_a, mut sum_b) = (Duration::ZERO, Duration::ZERO);
    for q in 1..=22 {
        let sql = std::fs::read_to_string(format!("{queries}/q{q:02}.sql")).unwrap();
        let plain = ctx.state().create_logical_plan(&sql).await.unwrap();
        let (secured, _) = rewriter
            .evaluate(&user, &sqe_policy::TableScope::default(), plain.clone())
            .await
            .unwrap();
        let guarded = format!("{}", secured.display_indent()).contains("CASE WHEN");
        let (a, b) = medians(&ctx, &plain, &secured).await;
        sum_a += a;
        sum_b += b;
        println!(
            "q{q:02}    {:<7}  {:>8}  {:>9}  {:.2}",
            guarded,
            a.as_millis(),
            b.as_millis(),
            b.as_secs_f64() / a.as_secs_f64()
        );
    }
    println!(
        "total           {:>8}  {:>9}  {:.2}",
        sum_a.as_millis(),
        sum_b.as_millis(),
        sum_b.as_secs_f64() / sum_a.as_secs_f64()
    );
}
