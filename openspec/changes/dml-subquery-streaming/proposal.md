## Why

CoW DML (`UPDATE`, `DELETE`) with an `IN (subquery)` WHERE clause aborts the coordinator process on realistic datalake workloads. TPC-E SF10 `trade_result_update_holding` crashes at 34,496 matching tuples with:

```
thread 'sqe-coordinator' has overflowed its stack
fatal runtime error: stack overflow, aborting
```

The root cause is the current rewriter in `sqe-coordinator/src/write_handler.rs` (`rewrite_in_subquery_where`, lines 1875-1988): it executes the subquery, collects literals, builds a balanced OR-of-ANDs AST, then flattens the tree to text via `format!("{expr}")`. `sqlparser::ast::Expr`'s `Display` impl emits same-precedence chains unparenthesised, so the balanced structure is lost. When the resulting `A OR B OR C OR ...` string is fed back to `SessionContext::sql()`, operator-precedence climbing rebuilds a **left-leaning tree of depth N**. DataFusion's `impl Hash for Expr` (datafusion-expr/src/expr.rs:325) and other recursive visitors then walk the tree and overflow the 8 MiB thread stack at N ~ 34K in release, N ~ 4K in debug.

An lldb backtrace on the TPC-E reproduction confirms the `impl Hash for Expr -> Box<Expr>::hash -> BinaryExpr::hash` recursion triplet dominating the stack. Fix:

1. **Cannot be repaired upstream with a one-line patch.** Every DataFusion pass that walks the expression tree (hash, simplifier, CSE, canonicalizer) is recursive. `stacker::maybe_grow` per pass is defence in depth, not a fix.

2. **Cannot be absorbed by a larger stack.** The coordinator already runs on an 8 MiB stack vs. the 2 MiB tokio default (see `sqe-coordinator/src/main.rs:85-99`). That bought ~4x headroom and defers the crash to higher cardinalities. SF100 (~345K pending trades) and production loads (millions) reach the new ceiling.

3. **Is an architectural mistake.** Materialising an N-row subquery into N-tuple WHERE text produces O(N) plan size for N ∈ [1, ∞). The observed plan at crash time was 1.44 MB. A 1M-row subquery would be ~45 MB. SQE targets TB-scale datalakes where IN subqueries routinely match millions of rows.

## What Changes

Replace literal-inlining with **view-lifted semi-joins**: materialise each `IN (subquery)` result once as a small MemTable keyed on the subquery columns, join it into the outer CoW SELECT, and collapse the WHERE-clause IN expression to a boolean flag check.

Concretely:

- **Remove** `rewrite_in_subquery_where`, `collect_and_replace_in_subqueries`, `substitute_in_subquery_placeholders`, `fold_balanced_binary`. All dead after the fix.
- **Add** `lift_in_subqueries(where_sql, ctx) -> (rewritten_where, joins_sql, InSubqueryCleanup)` in `sqe-coordinator/src/write_handler.rs`. Walks the WHERE AST, executes each subquery as `SELECT DISTINCT ... AS __col0, ..., TRUE AS __matched` against the same session context, materialises the result as a MemTable once, registers it under a unique scratch name, builds a LEFT JOIN clause, and replaces the `InSubquery` node with `COALESCE(__sqN.__matched, FALSE)` (or negation for `NOT IN`).
- **Add** `InSubqueryCleanup` RAII guard. Tracks registered scratch tables; `Drop` calls `ctx.deregister_table` for each. Guard is bound in the DML handler to outlive all per-batch SELECT executions.
- **Modify** `filter_batch_negate`, `filter_batch_match`, `apply_update`, `count_matching_rows` to accept a `joins_sql: &str` argument and inject it into the outer SELECT's FROM clause.
- **Modify** `handle_delete`, `handle_delete_mor`, `handle_update` to call `lift_in_subqueries` instead of `rewrite_in_subquery_where`, bind the guard, and thread `joins_sql` through to each per-batch evaluator.
- **Add** test `crates/sqe-coordinator/tests/in_subquery_view_rewrite.rs` covering CoW UPDATE, CoW DELETE, MoR DELETE with single-column IN, multi-column tuple IN, `NOT IN`, empty subquery result, NULL handling, and a 1M-row subquery stress case that would have produced ~45 MB of WHERE text under the old path.
- **Keep** `crates/sqe-coordinator/tests/in_subquery_or_stack_overflow.rs` as a DataFusion-level regression gate. Once `lift_in_subqueries` is in, no SQE path produces deep OR chains. The test protects against a future regression where someone reintroduces literal inlining.

## Capabilities

### Modified Capabilities

- `write-path`: CoW DML with IN subqueries stops inlining literals. The rewriter is now plan-size O(1) in subquery cardinality; semi-join lowering is delegated to DataFusion.

## Impact

- `sqe-coordinator`: one file touched (`write_handler.rs`), net code reduction (the `fold_balanced_binary` + balanced-tree rewriter is deleted). New RAII guard type and one new helper function.
- `sqe-coordinator/src/main.rs`: the 8 MiB thread stack bump (`WORKER_STACK_BYTES`) is retained as defence in depth. Followup issue to revert once the fix has been in production for a release.
- No public API change. No config change. No spec change for `sql-extensions` or `security-policy`.
- Performance: TPC-E SF10 `trade_result_update_holding` completes instead of aborting. TPC-E SF100 (~345K matching tuples) becomes possible. Memory cost of the scratch MemTable is O(subquery cardinality) instead of O(N^2) for the old plan-text (cardinality × per-tuple text bytes).

## Success Criteria

1. `cargo test -p sqe-coordinator --test in_subquery_or_stack_overflow prod_stack_32k` passes on a release build.
2. `cargo test -p sqe-coordinator --test in_subquery_view_rewrite` passes, including the 1M-row stress case.
3. TPC-E SF10 full benchmark completes end-to-end with `trade_result_update_holding` returning correct row counts.
4. TPC-E SF100 `trade_result_update_holding` returning correct row counts (stretch goal; previously uncrashable).
5. `cargo clippy --all-targets --all-features -- -D warnings` clean.
6. No regression on TPC-H SF1 (22/22, single-node baseline).

## Rollback

The change is localised to one file and adds no dependencies. Revert is a single-commit operation. No data migration, no config migration, no catalog state change.

If a hard production failure surfaces that the regression tests missed, the rollback commit restores the prior rewriter. The 8 MiB stack bump in `main.rs` remains in place (already merged separately) and will continue to absorb the old failure mode up to ~34K tuples, matching prior observed behaviour.
