**STATUS 2026-11-10: #788 one memory pool per statement; DoPut ingest admitted (`fix/ingest-merge-query-quota-788`).**

Stacked on `fix/write-path-query-quota-788`, which put the SQL write
statements under the per-query quota by wrapping each write context. That
left every context with its own fresh quota, so a statement that builds two
held two, and a statement with no context held none:

1. MERGE ran its source through `execute_query` (own quota; the collected
   batches keep their reservation) and then built the target context with a
   second, full quota.
2. Flight `DoPut` ingest (`do_put_statement_ingest`) never passed through
   `execute`: no concurrency permit, no per-user reservation, no refusal on
   a red pool, and `handle_ingest_streaming` called `fanout_limits(None)`,
   so its buffers charged no pool at all.

Fix: `query_memory::StatementMemory` is the statement's quota plus one
`QueryMemoryPool`, created by `admit` and carried in `Admission.memory`.
`StatementMemory::bind(ctx)` gives a context that pool;
`StatementMemory::pool()` is the pool for buffers without a context. Every
quota site uses it: `open_stream`, `execute_query`, `explain_execution` and
`write_session_context` take `&StatementMemory` instead of a `QueryQuota`
and bind it, so the MERGE source and target, a CTAS's SELECT and load, and
an EXPLAIN ANALYZE share one quota and one peak. `with_query_quota` is gone.
`QueryHandler::admit_ingest` runs `admit` plus `refuse_under_pressure` as
for an INSERT; `handle_ingest_streaming` takes the pool as a new
`pool: Option<Arc<dyn MemoryPool>>` argument.

The join fallback (#784) still gives its retry a fresh quota over the
coordinator pool: the first attempt failed before its first batch and has
released what it held, and the retry needs the fair-share budget. Writes
still get no join fallback.

Tests: `query_memory::tests::a_statement_holds_one_quota_across_its_contexts`
(two bound contexts and the bare pool share one cap and one peak; a second
statement has its own; no quota leaves the shared pool),
`query_handler::show_stats_policy_tests::an_ingest_is_admitted_like_a_write`
(green pool admits with a reservation and the pool charges the coordinator
pool; red pool refuses). `cargo test -p sqe-coordinator --lib` 807/807;
`--test it -- join_fallback_e2e` 7/7, `hive_` 105/105;
`make test-integration FILTER=merge` 12/12; clippy
`-p sqe-write -p sqe-coordinator --tests -D warnings` clean.

Not covered: no test drives `DoPut` ingest end to end (none existed before
either); the writer's `Some(pool)` path is the one the SQL writes use.

**NEXT:** the join fallback on the write path (a write whose SELECT needs a
sort-merge join to fit fails cleanly instead of retrying).
