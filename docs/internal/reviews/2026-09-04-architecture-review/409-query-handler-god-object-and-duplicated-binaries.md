# 409: QueryHandler is a 9.9K-line God object with a 1,940-line `execute()`, wired twice by two coordinator binaries

- Scope: sqe-coordinator (query_handler.rs, main.rs, bin/sqe_server.rs)
- Severity: medium
- Category: maintainability
- Effort: L (week+)
- Status: open
- Issue: #589

## Problem

**Size.** `crates/sqe-coordinator/src/query_handler.rs` is 9,907 lines (5,881 at the
2026-06-26 review, item 15; it has grown 68 percent since). `QueryHandler::execute` spans
`:870-2807` (about 1,940 lines) and contains a single `match &kind` with roughly forty arms
that inline SHOW, DDL, GRANT, ATTACH, secrets, MERGE source execution, maintenance
procedures, lineage, result-cache, audit emission in four variants, and timeout handling.
The struct has 31 fields (`:126-263`), several of them documented with 30-line comments
explaining why a field must be `Option` (`:184-260`), which is the field's invariant living in
prose because the type cannot express it.

**Two pipelines inside.** `execute` (batch, `Vec<RecordBatch>`) and `execute_stream`
(`:2809-3073`) plus `open_stream` (`:3075-3297`) duplicate admission control (`:880-960` vs
`:2817-2905`), tracker start, span creation and rule application (see 404). The comments
acknowledge it: `:2385-2389` "the streaming path ... still uses `extract_table_names` ...
Upgrading that site is a follow-on task".

**Two binaries wire it.** `src/main.rs` (`async_main`, `:122-608`) and
`src/bin/sqe_server.rs` (`run_coordinator`, `:899-1750`) both construct the auth chain,
session manager, runtime, worker registry, QueryHandler, Flight service and health server.
Both define private adapter impls with identical method names (`authenticate`,
`authenticate_bearer`, `execute`, `describe_prepared`, `build_oauth2_state`; `main.rs:28-107,
637` and `sqe_server.rs:663-760, 2006`). `main.rs:159-160` says a helper is "Shared with
bin/sqe_server.rs so the two coordinator binaries cannot drift", which is an admission that
they otherwise do. `Cargo.toml:26-33` ships both as `sqe-coordinator` and `sqe-server`.

## Why it matters

- Every reviewer in this round had to read the same 2,000-line function to find their
  scope; the 2026-06-26 review already listed it as a critical-size file and the trend is up.
- Behaviour differs by entry point (404, 403) because there is no single place where "run a
  statement" is defined.
- A feature wired in `sqe_server.rs` but not `main.rs` (or the reverse) ships as a latent
  difference between the two binaries. `with_hive_registries` is documented at
  `query_handler.rs:196-214` as exactly this kind of wiring hazard.

## Proposed change

1. Delete `src/main.rs` as a binary (keep `sqe-server` with `--mode coordinator`, which
   already exists via `mode.rs`), or reduce it to `fn main() { sqe_server::run() }`. Move
   shared construction into a `coordinator::bootstrap` module returning a `Coordinator`
   struct so the wiring exists once and is unit-testable.
2. Split `QueryHandler` along `StatementKind`:
   - `statement_router.rs`: classify, admission, tracker/span/audit lifecycle, dispatch to
     a `StatementExecutor` trait.
   - `select_executor.rs`: `execute_query` + `open_stream` merged into one planner that
     returns a stream; the batch API collects from it.
   - `show_handler.rs`, `grant_handler.rs`, `catalog_ddl_handler.rs`, `session_ddl_handler.rs`
     (ATTACH/SECRET/SET), each owning the arms currently inlined.
   - `audit_emitter.rs`: the four audit branches (`:2469-2755`) as one function taking an
     `Outcome` and `StatementKind`.
3. Replace the `Option` fields whose `None` is a programming error (`hive_catalogs`,
   `session_manager`, `worker_registry`) with a typed `CoordinatorDeps` struct built in
   bootstrap so the invariants in the field comments become compile-time.

## Acceptance criteria

- No file in `crates/sqe-coordinator/src` exceeds 3,000 lines; no function exceeds 300.
- One binary target in `Cargo.toml`, or `main.rs` under 30 lines delegating to shared code.
- `execute` and `execute_stream` share one admission function and one audit emitter
  (`grep -c 'try_acquire_owned' query_handler.rs` drops from 4 to 1 call site).
- Existing `tests/it` suite passes unchanged.

## Related

- 2026-06-26 review item 15 still holds (and is worse); item 13 (dual auth config) is the
  same shape in sqe-core.
- 404 (three pipelines), 407 (SessionContext factory home).
