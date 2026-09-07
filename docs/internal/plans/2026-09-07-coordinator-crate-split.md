# Plan: split `sqe-coordinator` along its compile seams (build-time item C2)

Date: 2026-09-07. Supersedes the ordering in
`2026-09-04-build-test-time-program.md` items C1 and C2, and re-verifies
`2026-07-13-sqe-write-crate-extraction.md` against today's tree.

## Why C2 and not C1

Measured on the laptop (11-core Mac, warm target, `touch
crates/sqe-core/src/lib.rs && cargo build --workspace --timings`, wall 154 s):

| unit | start (s) | end (s) | duration (s) |
|---|---|---|---|
| workspace-hack + third-party already built | 0 | 16 | |
| sqe-core, sqe-metrics, sqe-trino-functions (parallel) | 16 | 24 | 7 to 8 |
| sqe-sql, sqe-auth, sqe-planner, sqe-policy | 20 | 27 | 4 to 6 |
| sqe-catalog | 23.5 | 30.5 | 7 |
| sqe-compaction | 26 | 28 | 2.5 |
| sqe-worker (lib) | 27 | 33 | 6.5 |
| **sqe-coordinator (lib)** | **28** | **124** | **96** |
| sqe-coordinator bin `sqe-server` | 124 | 154 | 30 |
| everything else (sqe-cli, sqe-worker bin, sqe-bench) | done by 44 | | |

The coordinator library starts 12 s after the shared tier is ready and then
runs alone for 96 s, followed by a 30 s binary link. Untangling the
policy -> catalog and worker -> coordinator edges (C1) can only move the
coordinator's start earlier, so its ceiling is those 12 s. The 126 s that
matter are one crate: 76,219 lines in `crates/sqe-coordinator/src`.

Front-end share, same protocol with `cargo check --workspace --timings`
(wall 82.5 s): the coordinator library checks in **63.7 s** and its binary in
8.8 s, so about two thirds of the 96 s build is type-checking and one third
codegen. Type-checking is serial within a crate and only overlaps across
crates that do not depend on each other; codegen already overlaps with the
next crate's front end through pipelining. The gain therefore comes from
sibling crates, not from a deeper chain.

## What a split can and cannot buy

Cargo compiles one unit per crate, and a dependent crate starts as soon as its
dependency's metadata is written (pipelining), before that dependency's
codegen finishes. Two consequences shape this plan:

- **A chain of crates does not shorten a cold build by itself.** Five crates
  of 15k lines in a line cost roughly the same front-end time as one crate of
  76k. The gain in a chain comes from overlapping codegen of crate N with the
  front end of crate N+1, and from incremental builds: a change in the top
  crate no longer re-checks the bottom ones.
- **Siblings do shorten it.** Two crates that depend on the same base but not
  on each other compile at the same time. The coordinator has exactly one big
  sibling pair today: the write path and the distribution path.

## The coordinator's internal graph (verified 2026-09-07)

Module clusters by `crate::X` references (`src/`, excluding the binary):

| cluster | modules | lines | depends on clusters |
|---|---|---|---|
| base | session_context, session_manager, memory, query_tracker, runtime, runtime_catalog, channel_pool, credential_refresh, transport, tls, worker_registry, rate_limiter, tag_source_impl, catalog_ops, mode, audit_*, auth_session, policy_wiring, metrics_history | ~14k | (none inside the crate) |
| write | write_handler 11,087, merge_sql 1,814, merge_target_provider, writer 962, puffin_carry 757, hive_write 610 | ~15.5k | base (2 symbols, see below) |
| distribution | distributed_scan 3,194, scheduler 801, parallel_scan, parallel_probe_scan, scan_pushdown, adaptive_sort, dim_build_swap, post_planning, explain, streaming | ~9k | base; post_planning and streaming reference query_handler (3 refs, to invert) |
| maintenance | maintenance 5,045, maintenance_scheduler 3,341, maintenance_log 1,784, maintenance_lease 1,046, table_health, compaction_dispatch, maintenance_principal, suggest_bloom | ~13k | base, write |
| query | query_handler 10,469, hive_ddl_handler 911, prepared_statements, query_cache, quack_executor, hive_scan_tasks, hive_tag_source | ~13k | base, write, distribution, maintenance (holds `WriteHandler` and `MaintenanceHandler` as fields) |
| front | flight_sql 4,281, flight_sql_helpers, web_ui 817, web_auth 975, mcp_server | ~7k | query, base |
| binary | bin/sqe_server.rs 3,184 | | everything, plus sqe-worker |

Cross-cluster edges that must be cut before the write cluster can leave the
crate (file:line, today):

- `write_handler.rs:32` uses `catalog_ops::{catalog_qualifier, fold_unquoted_ident, parse_table_ref}`; `query_handler.rs:6706` uses `catalog_ops::resolve_table_ident`. All five identifier helpers (`catalog_ops.rs:1799-1975`) are pure functions over `sqlparser::ast::ObjectName` and `iceberg::TableIdent`. They move to a new `ident` module that travels with the write crate; catalog_ops keeps thin re-exports until MR 4.
- `write_handler.rs:942` calls `session_context::invalidate_session_cache` and `write_handler.rs:6157` calls `session_context::discover_session_catalog`. Both touch coordinator-owned state (the moka session cache, the per-catalog bearer resolution). They become a two-method trait `CatalogDiscovery` defined in the write crate and implemented by the coordinator; `WriteHandler::with_catalog_discovery` installs it. The July plan had one method; the invalidation call is new since then.
- `write_handler.rs:5292-5293` calls `maintenance::{collect_live_delete_files, covered_position_deletes}`, which are re-exports of `sqe_compaction` (`maintenance.rs:34`). Import them from `sqe_compaction` directly.
- `write_handler.rs:10421` (feature `test-sqlite` tests) uses `test_support::sqlite_rig`. The rig drives `WriteHandler`, so it moves with the write crate under the same feature; it needs `maintenance::collect_live_data_files` (`maintenance.rs:3230`, a pure iceberg helper), which moves next to it and is re-exported for maintenance's 18 call sites.
- `puffin_carry.rs:56` uses `write_handler::commit_with_retry`; `maintenance_log.rs:114,141`, `maintenance_lease.rs:117` use `commit_with_retry` and `is_conflict_message`. They move with the write crate as `pub` and the maintenance cluster imports them from there.
- Everything else that reaches into the write modules (`maintenance.rs:32,1318-4228`, `maintenance_log.rs:66-142`, `query_handler.rs:36,1969,1989,2401`, `lib.rs:246-275`, three test files) is a consumer that stays in the coordinator and keeps working through `pub use sqe_write::{...}` re-exports.

## Target shape, in the order the MRs land

```
sqe-coordinator-base?  (not a crate yet; the base cluster stays in sqe-coordinator)
sqe-write              write_handler, writer, merge_sql, merge_target_provider,
                       puffin_carry, hive_write, ident, commit (retry helpers),
                       test_support/sqlite_rig (feature test-sqlite)
sqe-coordinator        base + distribution + maintenance + query + front, re-exports sqe_write
sqe-server             the binary (moves out so the coordinator library stops
                       depending on sqe-worker; also lets `cargo build -p sqe-coordinator`
                       stop linking the 30 s binary)
```

Later, if the numbers justify it: `sqe-distribution` as a sibling of
`sqe-write` (needs the 3 post_planning/streaming -> query_handler references
inverted), then `sqe-maintenance`, then the front. Each step is the same
recipe: cut the edges with a trait or a move, `git mv` the files, re-export.

## MR sequence

Each MR is independently mergeable and revertable. Branch prefix `refactor/`.

### MR A: `refactor/write-path-decouple` (no file moves)

1. `ident.rs`: the five identifier helpers plus their tests move out of
   `catalog_ops.rs`; `catalog_ops`, `write_handler`, `query_handler`,
   `maintenance_log` import from `crate::ident`.
2. `CatalogDiscovery` trait in `write_handler.rs` with `discover(...)` and
   `invalidate_session_cache(&str)`; `PolarisCatalogDiscovery` in
   `session_context.rs`; `QueryHandler::new` installs it. Behaviour when the
   trait is absent equals today's behaviour when discovery finds nothing.
3. `write_handler.rs` imports the two live-delete helpers from
   `sqe_compaction`; `collect_live_data_files` moves to `writer.rs`
   (re-exported from `maintenance`).
4. `commit_with_retry`, `is_conflict_message` become `pub` in a new
   `commit.rs` (moved verbatim from `write_handler.rs:640-730`).
5. Gate: `grep -n 'crate::' src/write_handler.rs src/writer.rs
   src/puffin_carry.rs src/merge_sql.rs src/merge_target_provider.rs
   src/hive_write.rs` names only `ident`, `commit`, `merge_sql`,
   `merge_target_provider`, `puffin_carry`, `writer`, `write_handler`,
   `test_support::sqlite_rig`.

### MR B: `refactor/extract-sqe-write` (the move)

`crates/sqe-write` with the Cargo.toml derived from the files' imports
(sqe-core, sqe-sql, sqe-catalog, sqe-policy, sqe-metrics, sqe-lineage,
sqe-compaction, arrow*, datafusion, iceberg, parquet, sqlparser, tokio,
tracing, uuid, futures, async-trait; `tempfile` behind `test-sqlite`).
One move-only commit (`git mv`), one fix-up commit (paths, `pub`,
re-exports in the coordinator's `lib.rs`), `cargo hakari generate`.
Test-count parity: `cargo test -p sqe-write` must run the 164 tests the six
files carry today (115 + 26 + 16 + 4 + 3 in write_handler, merge_sql,
puffin_carry, merge_target_provider, writer).

### MR C: `refactor/sqe-server-crate`

`crates/sqe-server/src/main.rs` (from `crates/sqe-coordinator/src/bin/sqe_server.rs`),
depending on sqe-coordinator and sqe-worker; `sqe-worker` becomes a
dev-dependency of the coordinator (`tests/hive_diff/mod.rs:337`). Update
`Dockerfile` (`cargo chef cook --package sqe-server`), the nine
`scripts/benchmark-*.sh` lines that pass `-p sqe-coordinator --bin sqe-server`,
and the `glue`/`test-sqlite` feature forwarding.

### MR D: cleanup

Re-point coordinator imports to `sqe_write::`, drop re-exports that no longer
carry a consumer, update the architecture note in the book.

## Verification per MR

`cargo build --workspace`, `cargo test --workspace` (plus
`RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite`
for the rig), `cargo clippy --workspace --all-targets --all-features -- -D
warnings`, `cargo hakari generate --diff`. Timing after MR B and MR C with the
same `touch sqe-core` protocol as the baseline above; record in the progress
log. `make test-integration` before MR B merges (write paths are exercised
only there).

## Progress log

- 2026-09-07: baseline measured (table above). Plan written.
- 2026-09-07, MR A (`refactor/write-path-decouple`): done as specified, with
  three corrections found while cutting. `write_handler.rs` carries its own
  private `resolve_table_ident` (line 490) next to the one it imported from
  `catalog_ops`; both survive this MR, the dedup is for MR D. The two
  live-delete helpers were already `sqe_compaction` re-exports, so that edge
  cost one import. `maintenance_log.rs:649` has a second
  `collect_live_data_files`; `maintenance_lease` imports that one, so only
  the `maintenance.rs` copy moved (to `writer.rs`, re-exported). Lesson for
  the next MRs: `rustfmt lib.rs` follows every `mod` declaration and
  reformats the whole crate, and `rustfmt --check` on stdin does not report
  a nonzero exit; the strip recipe is `patch HEAD < diff(rustfmt(HEAD),
  current)`, verified by `rustfmt(result) == current`.
