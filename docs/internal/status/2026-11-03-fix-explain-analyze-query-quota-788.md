**STATUS 2026-11-03: `EXPLAIN ANALYZE` / `EXPLAIN FULL` run under the query's memory cap and join fallback (`fix/explain-analyze-query-quota-788`, #788).**

Found while comparing the embedded and server pools on the engine bench: a
full `ORDER BY price, order_id` of 150M rows at 3 GB completes on
`sqe-server` as a query (18.2s) but failed as `EXPLAIN ANALYZE` with
`Resources exhausted: Additional allocation failed for ExternalSorterMerge[8]
... greedy(used: 3.0 GB, pool_size: 3.0 GB)`.

Cause: the `Explain { analyze }` branch and `ExplainFull` in
`query_handler.rs` handed `ExplainHandler` the plain session context.
`execute_query` and `open_stream` wrap it in the per-query pool
(`query_memory::with_query_quota`: `max_query_memory`, headroom, sort share)
and arm the #784 join fallback; EXPLAIN did neither. So any user could run a
query past `max_query_memory` by prefixing `EXPLAIN ANALYZE`, sorts had no
per-partition share, and a join that the query completes through the
sort-merge retry was not retried: the metrics described a different execution.

Fix: `QueryHandler::explain_execution` builds the same query pool and a
closure that builds the same `JoinFallback` from the enforced plan.
`ExplainHandler::analyze` / `full` take that closure; `drain_capped` wraps
its stream in `FallbackStream` and returns the plan that produced the rows
(the retry's when it replanned), which the per-operator rows are read from.
A memory refusal now carries `ResourceExhausted` instead of
`ExecutionFailed`.

Tests (`crates/sqe-coordinator/tests/it/join_fallback_e2e.rs`, the 1.5M-row
Hive CSV join under a 16 MB cap):
- `explain_analyze_runs_under_the_query_cap`: with the fallback off, both
  `EXPLAIN ANALYZE` and `EXPLAIN FULL` fail with resources exhausted. On
  main they completed (checked by running the test against main's two source
  files: FAILED).
- `explain_analyze_reports_the_fallback_plan`: with the fallback on, both
  complete and report `SortMergeJoinExec`, not the failed `HashJoinExec`
  (FAILED on main: it reported the hash plan).

Gate: `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it --
join_fallback_e2e` (5 passed); `make test-integration FILTER=test_explain`
(4 passed: plan, analyze, full, policy_aware); `cargo test -p
sqe-coordinator --lib -- explain` (5 passed); `cargo clippy -p
sqe-coordinator --all-targets -- -D warnings`.

Measured after the change (release build, fresh `sqe-server`, 3 GB pool and
`max_query_memory`, 150M rows on Polaris/RustFS): `EXPLAIN ANALYZE SELECT
order_id, customer_id, price FROM sales ORDER BY price, order_id` completes
in 16.6s wall; `SortExec` 150M rows, 52 spills, 3.06 GB spilled. Before: the
failure above. Single run on a laptop.

**NEXT:** #788 spill branch (`spill_max_bytes`, ResourcesExhausted
classification, spill compression), then the 150M engine-bench rerun and the
`feat/engine-bench-polaris-readers` MR.
