# 804: sqe-coordinator is a God crate with an all-public module surface

- Scope: sqe-coordinator
- Severity: medium
- Category: maintainability
- Effort: L
- Status: open
- Issue: #636

## Problem

sqe-coordinator is 64,268 lines across 57 source files, plus 34,231 lines
in 56 integration test files. Its `lib.rs` declares 55 `pub mod` and 2
private modules, so the entire crate is public API including
`pub mod __test_support` (`crates/sqe-coordinator/src/lib.rs:230`).

The three files flagged as "critical size" in the June review have grown:

| File | June 2026 | Now |
|---|---|---|
| `src/query_handler.rs` | 5,881 | 9,907 |
| `src/write_handler.rs` | 7,505 | 8,679 |
| `src/maintenance.rs` | n/a | 4,697 |
| `src/flight_sql.rs` | n/a | 3,145 |
| `src/bin/sqe_server.rs` | n/a | 3,075 |

The crate mixes at least six concerns that have no reason to share a
compilation unit: Flight SQL transport (`flight_sql*.rs`, `transport.rs`,
`channel_pool.rs`), query lifecycle (`query_handler.rs`, `query_tracker.rs`,
`streaming.rs`, `session_*.rs`), DML and file writing (`write_handler.rs`,
`writer.rs`, `merge_*.rs`, `hive_write.rs`), distributed scheduling
(`scheduler.rs`, `worker_registry.rs`, `distributed_scan.rs`,
`parallel_*.rs`, `codec.rs`), table maintenance (`maintenance*.rs`,
`compaction_dispatch.rs`, `table_health.rs`), and operator UI
(`web_ui.rs`, `web_auth.rs`, `mcp_server.rs`, `quack_executor.rs`).

One consequence is visible in the dependency list: sqe-coordinator depends
on 15 first-party crates, and every one of its 56 integration test files
links all of them.

## Why it matters

- A one-line change anywhere in the crate recompiles 64K lines and relinks
  56 test binaries (the build-time plan measures this).
- Reviewers cannot hold `query_handler.rs` in their head; the review
  history of the JoinStrategyRule regression and the Err-arm fallback hiding
  it is one instance where a regression lived inside a 10K-line file for a
  full MR cycle.
- With everything `pub`, there is no compiler signal for what is an
  internal seam and what is used by `sqe_server.rs`, tests, or sqe-cli.
  Refactoring safety comes only from `cargo build --all-targets`.

## Proposed change

Extract by concern, in order of lowest coupling first:

1. `sqe-maintenance` crate: `maintenance.rs`, `maintenance_lease.rs`,
   `maintenance_log.rs`, `maintenance_principal.rs`, `maintenance_scheduler.rs`,
   `table_health.rs`, `compaction_dispatch.rs`. Depends on sqe-catalog,
   sqe-compaction, sqe-worker's Flight client type.
2. `sqe-scheduler` crate: `scheduler.rs`, `worker_registry.rs`,
   `distributed_scan.rs`, `codec.rs`, `channel_pool.rs`, `transport.rs`,
   `credential_refresh.rs`.
3. `sqe-write` crate: `write_handler.rs`, `writer.rs`, `merge_sql.rs`,
   `merge_target_provider.rs`, `hive_write.rs`, `hive_ddl_handler.rs`.
4. What remains (`query_handler`, `flight_sql`, sessions, streaming, web,
   mcp, quack) stays as sqe-coordinator. Make modules `pub(crate)` by
   default and re-export a curated `pub use` list from `lib.rs`; move
   `__test_support` behind `#[cfg(any(test, feature = "test-support"))]`.
5. Inside `query_handler.rs`, split by phase into a directory module:
   `parse.rs`, `authorize.rs`, `plan.rs`, `execute.rs`, `explain.rs`.

## Acceptance criteria

- No source file in `crates/sqe-coordinator/src` exceeds 3,000 lines.
- `grep -c '^pub mod' crates/sqe-coordinator/src/lib.rs` is below 15.
- `cargo build -p sqe-maintenance` succeeds without linking flight_sql.
- `make test-integration` still reports the same pass count (217 pass / 1
  known red as of the last local gate).

## Related

- 803 (crate layering).
- June 2026 codebase analysis, Medium item 15 and Improvement 1 (still open, worse).
- July 2026 production readiness review, P1-10 "Split orchestration from algorithms".
- `docs/internal/plans/2026-09-04-build-test-time-program.md`.
