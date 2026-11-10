**STATUS 2026-11-09: #788 write path runs under the per-query quota (`fix/write-path-query-quota-788`).**

Reads wrap their `SessionContext` with `query_memory::with_query_quota`
(`open_stream`, `execute_query`, `explain_execution`). The write arms of
`execute_inner` called `create_session_context` directly, so CTAS, INSERT,
DELETE, UPDATE, MERGE (target side) and TRUNCATE ran on the shared
coordinator pool. Every write buffer in `sqe-write` (`TrackedBatchBuffer`,
`WriteReservation`, `fanout_limits`) reserves from
`ctx.runtime_env().memory_pool`, and so does the SELECT feeding a CTAS or
INSERT, so one write could take the whole pool.

Fix: `QueryHandler::write_session_context(session, quota)` wraps the context
with the admitted quota; the eight write sites use it (Hive and Iceberg
CTAS/INSERT, DELETE, UPDATE, TRUNCATE, MERGE). Admission already refuses
writes under pressure, so a write's quota is always `max_query_memory`.
`hive_write::exec_err` keeps `ResourceExhausted` for a DataFusion memory
refusal; it flattened it to `ExecutionFailed`.

Tests (`tests/it/join_fallback_e2e.rs`, Hive CSV fixture, 16 MB cap):
`a_write_runs_under_the_query_cap` (INSERT ... SELECT over a 1.5M-row hash
join fails with `ResourceExhausted` naming `query.max_query_memory`) and
`the_same_write_completes_without_the_cap` (control: uncapped, all rows
written and read back). Mutation: reverting the Hive INSERT site makes the
INSERT succeed and the first test fail.

Not covered here:
- MERGE runs its source through `execute_query` (own quota) and its target
  rewrite under a second one, so a MERGE can hold two quotas at once.
- Writes get no join fallback: a write whose SELECT needs a sort-merge join
  to fit fails cleanly instead of retrying.
- Flight `DoPut` ingest (`handle_ingest_streaming`) passes `fanout_limits(None)`
  and charges no pool at all.

#784 close-out checked at the same time: the "no single-partition coalesce"
and Exact/Inexact/Absent criteria are covered by
`join_fallback::tests::the_fallback_covers_every_build_estimate` and
`a_join_chain_falls_back_without_a_single_partition_input` on main. What
remains is the SF10/SF30 scaling run.

**NEXT:** DoPut ingest pool charge and one quota per MERGE (#788), then the
#784 SF10 q09 run at a 1 GB cap.
