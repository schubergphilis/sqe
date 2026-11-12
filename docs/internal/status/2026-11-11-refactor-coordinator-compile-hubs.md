**STATUS 2026-11-11: `sqe-coordinator` trait-proof time cut from 20.5 s to about 8.5 s (`refactor/coordinator-compile-hubs`).**

`scripts/compile-profile.sh sqe-coordinator` billed most of the crate's
`evaluate_obligation` time to three methods: `do_get_tables` (4.6 s, through
`session_catalog` into the 750-line `create_session_context` future),
`QueryHistoryTable::scan` (4.3 s, a `MemTable` and `&[Expr]` held across an
await) and `do_get_prepared_statement` (2.9 s). A fourth, `do_put_statement_ingest`,
rose from 0.3 s to 3.5 s once the others were boxed, because the proof moved to
the next unboxed caller of `handle_ingest_streaming`.

Changes, all in `sqe-coordinator`:

- `create_session_context` and `plan_prepared_statement` return `BoxFuture<'a, _>`.
- `do_action_create_prepared_statement` resolves the session, rate limit,
  transaction and client address, then calls the boxed
  `create_prepared_statement`; the body moved unchanged.
- `do_put_statement_ingest` calls the boxed `ingest_flight_stream`, which takes
  the concrete `FlightRecordBatchStream` so the proof stays in this crate.
- `do_get_prepared_statement` goes through `run_resolved_into_flight_response`.
- `QueryHistoryTable::scan` is in `async_trait`'s expanded form and holds the
  table as `Arc<dyn TableProvider>`.

Measured alternating, back to back:

| run | `evaluate_obligation` self | `typeck` self | crate |
|---|---|---|---|
| before 1 | 20.21 s | 1.54 s | 47.1 s |
| after 1 | 8.81 s | 1.43 s | 38.0 s |
| before 2 | 20.79 s | 1.48 s | 49.3 s |
| after 2 | 8.11 s | 1.40 s | 34.5 s |

After the change no single method is billed more than 0.7 s, so no further
hubs were added.

Gates: `cargo check-server`, `cargo clippy -p sqe-coordinator --all-targets -D
warnings`, `cargo test -p sqe-coordinator --lib` (807 passed),
`make test-integration FILTER=prepared` (1 test). The repository has no
end-to-end test for Flight SQL `DoPut` ingest, so the ingest hub is covered by
compile and unit tests only.

**NEXT:** `sqe-catalog` compile hubs (`hive::provider` `scan_with_args`, `scan`,
`table`; `session_tuning` `create_physical_plan`); an ADBC ingest integration test.
