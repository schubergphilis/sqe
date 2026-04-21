## 1. Core rewriter: `lift_in_subqueries`

- [x] 1.1 Add `InSubqueryCleanup` RAII guard in `sqe-coordinator/src/write_handler.rs` with `Drop` impl that deregisters scratch tables and warn-logs failures
- [x] 1.2 Add module-level `static IN_SUBQUERY_COUNTER: AtomicU64 = AtomicU64::new(0);` for unique scratch names
- [x] 1.3 Implement `async fn lift_in_subqueries(&self, where_sql: &str, ctx: &DFSessionContext) -> sqe_core::Result<(String, String, InSubqueryCleanup)>`
- [x] 1.4 Fast path: if `where_sql.to_uppercase()` does not contain `"SELECT"`, return `(where_sql.to_string(), String::new(), empty_guard)`
- [x] 1.5 Parse WHERE by wrapping in `SELECT * FROM __dummy WHERE {where_sql}`
- [x] 1.6 Walk AST collecting `InSubquery { expr, subquery, negated }` nodes with their LHS column-expression text
- [x] 1.7 For each InSubquery: derive `col_refs: Vec<String>` from either `Expr::Identifier` / `Expr::CompoundIdentifier` (single column) or `Expr::Tuple` (multi-column)
- [x] 1.8 Build scratch-materialiser SQL: `SELECT DISTINCT {sub_c0} AS __col0, {sub_c1} AS __col1, ..., TRUE AS __matched FROM ({subquery}) AS __sq WHERE __col0 IS NOT NULL AND __col1 IS NOT NULL AND ...` — project the subquery's own output columns using positional aliases (`__sq.column_1` etc.) to avoid relying on column name stability
- [x] 1.9 Execute via `ctx.sql(&materialiser_sql).await?.collect().await?`, wrap RecordBatches into `MemTable::try_new(schema, vec![batches])`, register under `__sqe_in_subq_{id}`
- [x] 1.10 Build LEFT JOIN clause: `LEFT JOIN \"__sqe_in_subq_{id}\" AS \"__sqN\" ON \"__sqN\".__col0 = {lhs_0} AND \"__sqN\".__col1 = {lhs_1} AND ...`
- [x] 1.11 Replace the InSubquery AST node with a sentinel `Expr::Identifier` token; after `format!("{expr}")` the sentinel is substituted for `COALESCE(\"__sqN\".\"__matched\", FALSE)` (or `NOT (...)` if `negated`). Sentinel keeps the rewritten AST depth O(1) and avoids re-creating the Display/Parser asymmetry that caused the old stack overflow.
- [x] 1.12 Stringify the rewritten WHERE via `format!("{expr}")` (tree is now O(1) depth)
- [x] 1.13 Return `(rewritten_where, joins_sql_accumulated, InSubqueryCleanup { ctx: ctx.clone(), scratch_tables })`

## 2. Per-batch evaluator updates

- [x] 2.1 Add `joins_sql: &str` parameter to `filter_batch_negate` (line 1614); splice into `FROM datafusion.public.{table_name} AS \"{orig_name}\" {joins_sql}`
- [x] 2.2 Same for `filter_batch_match` (line 1678)
- [x] 2.3 Same for `count_matching_rows` (line 1817)
- [x] 2.4 `apply_update` (line 1733) already accepts a `joins` via decorrelator; extend its `joins_sql` construction to append the IN-subquery joins after the decorrelator joins

## 3. DML handler wiring

- [x] 3.1 `handle_delete` (line 679-683): replace `rewrite_in_subquery_where` call with `lift_in_subqueries`; bind cleanup guard to local that outlives the data-file loop; pass `joins_sql` to `filter_batch_negate`
- [x] 3.2 `handle_delete_mor` (line 823-826): same for `filter_batch_match`
- [x] 3.3 `handle_update` (line 946-952): same for `apply_update` and `count_matching_rows`

## 4. Remove dead code

- [x] 4.1 Delete `rewrite_in_subquery_where` (lines 1861-1989)
- [x] 4.2 Delete `collect_and_replace_in_subqueries` (lines 2036-2074)
- [x] 4.3 Delete `substitute_in_subquery_placeholders` (lines 2135-2238)
- [x] 4.4 Delete `fold_balanced_binary` (lines 2076-2114) and its unit tests
- [x] 4.5 Remove any now-unused imports (`sqlparser::ast::Value`, etc.)

## 5. Regression tests

- [x] 5.1 Create `crates/sqe-coordinator/tests/in_subquery_view_rewrite.rs`
- [x] 5.2 Test: single-column `col IN (SELECT k FROM keyset)` — 10-row outer, 5-row keyset, verify correct row set (`single_column_in_small_keyset`)
- [x] 5.3 Test: multi-column `(c1, c2) IN (SELECT k, label FROM keyset)` — covers both matching and non-matching keyset tuples (`multi_column_tuple_in_small_keyset`)
- [x] 5.4 Test: CoW DELETE-shape `NOT (<rewritten>)` preserves the right rows (`delete_shape_multi_column_not_predicate`)
- [x] 5.5 Test: MoR DELETE-shape single-column IN produces the same row set a user expects (`mor_delete_shape_single_column`)
- [x] 5.6 Test: `NOT IN` with 2-row keyset preserves non-matching rows (`not_in_single_column`)
- [x] 5.7 Test: empty subquery — `IN ()` matches nothing, `NOT IN ()` matches everything (`in_empty_subquery_matches_nothing`, `not_in_empty_subquery_matches_everything`)
- [x] 5.8 Test: NULL handling — NULL subquery rows are dropped from the keyset; NOT IN returns non-matching non-NULL rows (documented deviation) (`null_rows_in_subquery_are_dropped_from_keyset`, `not_in_with_null_subquery_returns_non_matches`)
- [x] 5.9 Stress test: 1,000,000-row subquery, outer 100 rows; release-only `#[ignore]` test asserts `elapsed < 30s` (`stress_one_million_row_keyset`). Measured 38.8 ms on local Apple Silicon release build, 770x under the ceiling.
- [x] 5.10 Marker test asserts the stack-overflow reproduction at `tests/in_subquery_or_stack_overflow.rs` still exists (`stack_overflow_regression_gate_file_exists`)

## 6. Benchmark validation

> Tasks 6.1-6.5 require a live Polaris + S3 stack and the full benchmarking harness. They are deferred to a manual validation pass run before the change is archived. The change can be reviewed and merged on the strength of:
> - the unit test suite (`cargo test -p sqe-coordinator --lib` 289/289 passing),
> - the Phase 5 regression tests (`tests/in_subquery_view_rewrite.rs` 10/10 passing in debug + 1 release-only stress test),
> - the existing stack-overflow gate (`tests/in_subquery_or_stack_overflow.rs::prod_stack_32k`).

- [ ] 6.1 `scripts/integration-test.sh tpch` passes 22/22 (no regression on non-DML paths) — DEFERRED, manual run
- [ ] 6.2 Full TPC-E SF10 run: `BENCH_SCALE=10 ./scripts/benchmark-test.sh tpce` completes; `trade_result_update_holding` returns the expected row count — DEFERRED, manual run
- [ ] 6.3 Stretch: TPC-E SF100 `trade_result_update_holding` completes (previously impossible) — DEFERRED, manual run
- [ ] 6.4 Commit benchmark JSON report to `benchmarks/results/` for historical tracking — DEFERRED, manual run
- [ ] 6.5 TPC-H SF1 single-node + distributed runs pass (from `tpch-sf1-flight-2026-04-02T14:16:27.json` and `tpch-sf1-flight-2026-04-06T20:57:10.json` baselines) — DEFERRED, manual run

## 7. Cleanup

- [x] 7.1 `cargo clippy -p sqe-coordinator --all-targets --all-features -- -D warnings` clean (workspace-wide clippy left for the merge gate)
- [x] 7.2 `cargo test -p sqe-coordinator --lib` clean (289/289), `cargo test -p sqe-coordinator --test in_subquery_view_rewrite` clean (10/10 + 1 release-only `#[ignore]`)
- [x] 7.3 Update `docs/roadmap.md` — added CoW DML scaling entry under Completed
- [x] 7.4 Update `nextsteps.md` — Step 9f line and Upstream watch list now reference `lift_in_subqueries`
- [x] 7.5 Follow-up issue draft: "Revert 8 MiB thread_stack_size in sqe-coordinator/src/main.rs after one release in production" — captured in `followups.md` (this repo lives on a private GitLab; ready to file once the change is merged)
- [x] 7.6 Follow-up issue draft: "Optional `dml.in_subquery.max_materialised_rows` config knob with view-based fallback for 100M+ row subqueries" — captured in `followups.md`
