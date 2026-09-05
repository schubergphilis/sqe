# 513: Every committing write path is tested only by `#[ignore]` docker e2e; the in-process sqlite catalog rig exists but only maintenance uses it

- Scope: sqe-coordinator write path and maintenance tests
- Severity: medium
- Category: testing
- Effort: M (days)
- Status: open
- Issue: #605

## Problem

`write_handler.rs` unit tests never reach a catalog. The policy tests build
`WriteHandler::new(write_test_config()).with_policy_enforcer(..)` and a bare
`DFSessionContext::new()` (`crates/sqe-coordinator/src/write_handler.rs:8383-8421`)
and exercise only predicate extraction; the rest are pure helpers
(`sql_type_to_arrow`, `reorder_insert_select`, decorrelator). No unit test
calls `handle_ctas_streaming`, `handle_insert_streaming`, `commit_written_files`,
any DELETE/UPDATE/MERGE handler, or `commit_with_retry` against a table.

Coverage of those lives in `tests/it/*` behind `#[ignore]` and a Polaris +
RustFS stack: `insert_overwrite_e2e.rs` (8 ignored), `mor_update_merge_integration.rs`
(5), `equality_delete_integration.rs` (7), `ctas_write_modes_e2e.rs` (8),
`partition_evolution_e2e.rs` (7), `partition_e2e.rs` (8), `v3_e2e.rs` (15),
`integration_test.rs` (63). CI has no integration job (CLAUDE.md: "LOCAL ONLY,
no CI equivalent"; memory note "dind down = integration CI zero signal").

Meanwhile the maintenance module already has an in-process rig: a sqlite
catalog over a tempdir (`maintenance.rs:3697-3720`, `race_sqlite_catalog`),
fabricated `DataFile`s (`:3956`), and deterministic race seams
(`RewriteRaceSeam`, `:3328`), producing tests like
`concurrent_position_delete_during_local_rewrite_does_not_resurrect_rows`
(`:3753`) that run under plain `cargo test`. Eight `tests/*.rs` files use
`--features test-sqlite` for the ledger and lease.

## Why it matters

Issues 500, 501, 502, 505 and 506 in this review are all reproducible in
process with a sqlite catalog and a local `FileIO`, and none of them would have
been caught by the current default test run. Every write-path MR today merges
on unit tests that cannot commit, or on a developer's local
`make test-integration`. The production-readiness review (P1-10) asked for an
"Iceberg write/commit-conflict matrix in default CI"; the rig to do it is
already in the tree, just not shared.

## Proposed change

1. Extract the maintenance test rig into `crates/sqe-coordinator/src/test_support/sqlite_rig.rs`
   behind `#[cfg(any(test, feature = "test-sqlite"))]`: sqlite catalog factory,
   `local_fs_table(schema, spec)`, `commit_rows(table, batches)`,
   `live_files(table)`, and a `FakeSessionCatalog` wrapper so `WriteHandler`
   methods that take `Arc<SessionCatalog>` can run.
2. Add `write_handler` tests for: INSERT then SELECT round-trip, INSERT
   OVERWRITE dynamic partitions, CoW and MoR DELETE with concurrent append
   (retry), MERGE all clause classes including empty source, CREATE OR REPLACE,
   partition evolution then DELETE, wider INSERT arity error.
3. Run `cargo test -p sqe-coordinator --features test-sqlite` in the default
   CI job (memory note: needs `RUST_MIN_STACK=33554432`).

## Acceptance criteria

- `cargo test -p sqe-coordinator --features test-sqlite -- write_handler::`
  runs at least 12 committing tests and passes in under two minutes.
- CI configuration runs that command on every MR.
- Each of 500, 501, 502, 505, 506 lands with a test in this rig.

## Related

- `docs/internal/reviews/2026-07-25-production-readiness-review.md` P1-10.
- Memory notes "sqlite-gated coordinator test" and "Local integration gate".
