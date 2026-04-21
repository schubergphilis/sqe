//! Reproduction for the TPC-E `trade_result_update_holding` stack overflow.
//!
//! Context: `rewrite_in_subquery_where` (see
//! `crates/sqe-coordinator/src/write_handler.rs:1875`) converts
//! `(c1, c2) IN (SELECT ...)` into a balanced OR-of-ANDs AST via
//! `fold_balanced_binary` (line 2089), then collapses the AST back to a
//! `String` with `format!("{expr}")` (line 1981). `sqlparser::ast::Expr`'s
//! `Display` impl emits *flat* text for same-precedence chains
//! (`A OR B OR C OR D`), so the returned `String` no longer carries the
//! balanced structure. When that string is fed back to
//! `SessionContext::sql()`, sqlparser-rs re-parses it via operator-precedence
//! climbing into a left-leaning `Or(Or(Or(A,B),C),D)` tree of depth N.
//! DataFusion's analyzer and optimizer then walk the tree with recursive
//! visitors, overflowing the thread stack at large N.
//!
//! Observed crash from 2026-04-20 SF10 benchmark run:
//! - WHERE clause with 34,496 tuples (~1.44 MB of text)
//! - Coordinator main runtime thread has an 8 MiB stack
//!   (see `crates/sqe-coordinator/src/main.rs:91`)
//! - Abort message:
//!   `thread 'sqe-coordinator' has overflowed its stack`
//!   `fatal runtime error: stack overflow, aborting`
//!
//! This test reproduces the same failure mode without Polaris, Iceberg, or
//! the full `WriteHandler`. A flat `A OR B OR C OR ...` WHERE clause is fed
//! directly to `SessionContext::sql()`, matching what the rewriter emits.
//!
//! Because a stack overflow is an OS-level abort (not a Rust panic), one
//! failing size tears down the whole test binary. Run each size in its own
//! process:
//!
//!   cargo test -p sqe-coordinator --test in_subquery_or_stack_overflow \
//!     prod_stack_32k -- --exact --nocapture
//!
//! To grab a backtrace, run under lldb on the debug binary:
//!
//!   cargo test -p sqe-coordinator --test in_subquery_or_stack_overflow \
//!     --no-run
//!   lldb -- target/debug/deps/in_subquery_or_stack_overflow-<hash> \
//!     prod_stack_32k --exact
//!   (lldb) run
//!   # on overflow:
//!   (lldb) bt 40

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

/// Match the production coordinator runtime: 8 MiB stack, thread name
/// `sqe-coordinator`. See `crates/sqe-coordinator/src/main.rs:85-99`.
fn prod_runtime() -> tokio::runtime::Runtime {
    const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(WORKER_STACK_BYTES)
        .thread_name("sqe-coordinator")
        .build()
        .expect("build tokio runtime")
}

/// Build a flat `(c1 = i AND c2 = 'Si') OR ...` chain with `n` disjuncts.
/// Modeled on the 34,496-tuple WHERE clause from the TPC-E crash log.
fn build_or_chain(n: usize) -> String {
    let mut parts = Vec::with_capacity(n);
    for i in 0..n {
        parts.push(format!("(c1 = {i} AND c2 = 'S{i}')"));
    }
    parts.join(" OR ")
}

async fn try_plan(n: usize) {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0_i64])),
            Arc::new(StringArray::from(vec!["S0"])),
        ],
    )
    .expect("build record batch");
    let mem = MemTable::try_new(schema.clone(), vec![vec![batch]]).expect("build memtable");
    ctx.register_table("t", Arc::new(mem))
        .expect("register memtable");

    let where_sql = build_or_chain(n);
    let sql = format!("SELECT COUNT(*) AS cnt FROM t WHERE {where_sql}");

    eprintln!(
        "[repro] planning SELECT with {n} OR disjuncts (WHERE ~{} bytes)",
        where_sql.len()
    );

    let df = ctx.sql(&sql).await.expect("ctx.sql should not fail");
    let batches = df.collect().await.expect("collect should not fail");
    assert!(!batches.is_empty(), "expected a row back");
    eprintln!("[repro] n={n} planned + collected OK");
}

/// Run `try_plan(n)` on a worker thread of the production-shaped runtime.
/// Spawning onto a worker guarantees the 8 MiB `thread_stack_size` applies
/// (the main thread's stack is set by the OS, not by the tokio builder).
fn run_on_prod_worker(n: usize) {
    let rt = prod_runtime();
    rt.block_on(async move {
        tokio::spawn(try_plan(n))
            .await
            .expect("spawned task panicked or was cancelled");
    });
}

// ---------------------------------------------------------------------------
// Size ladder. Run one at a time with `--exact` because stack overflow aborts
// the entire process.
// ---------------------------------------------------------------------------

#[test]
fn prod_stack_1k() {
    run_on_prod_worker(1_000);
}

#[test]
fn prod_stack_4k() {
    run_on_prod_worker(4_000);
}

#[test]
fn prod_stack_8k() {
    run_on_prod_worker(8_000);
}

#[test]
fn prod_stack_16k() {
    run_on_prod_worker(16_000);
}

#[test]
fn prod_stack_32k() {
    run_on_prod_worker(32_000);
}
