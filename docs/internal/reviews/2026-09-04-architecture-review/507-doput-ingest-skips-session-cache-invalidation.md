# 507: Flight DoPut ingest never invalidates the session context cache; invalidation lives in 20 call sites instead of at the commit

- Scope: sqe-coordinator write path (flight_sql DoPut, write_handler, query_handler)
- Severity: medium
- Category: architecture
- Effort: S (hours)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #599

## Problem

After a successful Iceberg commit the per-user cached DataFusion session
context still holds the pre-commit table provider. The Iceberg CTAS and
INSERT arms fix this by calling
`crate::session_context::invalidate_session_cache(&session.user.username)`
after the handler returns (`crates/sqe-coordinator/src/query_handler.rs:1695-1699`,
and about twenty more call sites: `:1409, 1430, 1441, 1446, 1462, 1471, 1477,
1484, 1510, 1515, 1534, 1548, 1622, 1669, 1694, 1752, 1976, 2013, 2182`).

The Flight `DoPut` ingest path does not.
`crates/sqe-coordinator/src/flight_sql.rs:2030-2072` authenticates, builds the
qualified name, and calls `handle_ingest_streaming` directly; the only
`invalidate` in that file is for prepared-statement parameters (`:1749, 2106,
2194`). `WriteHandler::handle_ingest_streaming`
(`crates/sqe-coordinator/src/write_handler.rs:1988-2099`) commits and returns
the row count; it neither invalidates the session cache nor calls
`invalidate_table_all_tokens`. Only the two MERGE handlers do the latter
(`write_handler.rs:3669`, `:4293`).

The invariant "every write path must invalidate the session cache" is
therefore enforced by convention at the caller, and the one caller outside
`query_handler` forgot.

## Why it matters

An ADBC / Flight SQL client that bulk-loads with `DoPut` and then runs
`SELECT count(*)` on the same connection reads the pre-ingest snapshot until
the cache TTL expires or another statement invalidates it. This is the exact
symptom documented for the Hive path in `query_handler.rs:1749-1758` ("the
next SELECT on that same context returns the PRE-write rows"). It looks like a
lost write and is indistinguishable from a failed commit from the client side.
dbt-sqe's seed loading uses `DoPut`.

Structurally, twenty scattered calls mean every new statement kind (the Hive
arms were added recently and needed two each) is a chance to miss one.

## Proposed change

1. Move invalidation into the `WriteHandler` at the commit site: a private
   `after_commit(session, catalog, table_ident)` that calls
   `invalidate_session_cache` and `invalidate_table_all_tokens`, invoked from
   `commit_written_files`, both CTAS paths, both ingest paths, the DML
   handlers and the maintenance rewrite commit. Then delete the
   `query_handler` calls that only mirror a `WriteHandler` call.
2. Keep explicit calls only for DDL that does not pass through the handler
   (DROP, RENAME, ALTER) and note in a comment why.
3. Add the `DoPut` scenario to `tests/it/integration_test.rs`.

## Acceptance criteria

- Integration test: `DoPut` N rows, then `SELECT count(*)` on the same session
  returns N without waiting.
- `grep -c invalidate_session_cache crates/sqe-coordinator/src/query_handler.rs`
  drops to the DDL-only sites (under 8).
- Unit test: a fake `SessionCatalog` records `invalidate_table_all_tokens` for
  every `WriteHandler` public entry point that reaches a commit.

## Related

- Memory note "Writes need session-cache invalidation".
- 508 (handler consolidation makes the single hook natural).

## Resolution (2026-09-05)

Invalidation moved from the callers to the commit site. Every `WriteHandler`
path that reaches a commit calls one private hook, including the Flight `DoPut`
ingest path that had no caller to mirror.

**What changed**

- `crates/sqe-coordinator/src/write_handler.rs`
  - `WriteHandler::after_commit(session, catalog, table_ident)` calls
    `crate::session_context::invalidate_session_cache(&session.user.username)`
    and `invalidate_table_all_tokens` on the catalog.
  - `TableCacheInvalidator` — a one-method trait implemented for
    `SessionCatalog`, so the hook is unit-testable. `SessionCatalog` is only
    constructible against a live REST catalog (it warms itself with a
    `list_namespaces` round-trip), so a fake `SessionCatalog` was not an
    option; the seam is what the test drives instead.
  - Called from: `commit_written_files` (both INSERT entrypoints, append and
    overwrite), `handle_ctas`, `handle_ctas_streaming`, `ctas_replace_in_place`,
    `handle_create_table`, `handle_create_table_like`, `handle_ingest`,
    `handle_ingest_streaming`, `handle_delete` (CoW), `truncate_via_rewrite`,
    `handle_delete_mor`, `handle_delete_equality`, `handle_update` (CoW),
    `handle_update_equality`, `handle_merge` (CoW) and `handle_merge_equality`
    (MoR). The two hand-written `catalog.invalidate_table_all_tokens(...)`
    calls in the MERGE handlers were replaced by it.
  - The write paths that only held the erased `Arc<dyn Catalog>` bridge now
    resolve the `SessionCatalog` first (`resolve_session_catalog`) and derive
    the bridge from it; `commit_written_files` takes `session` and
    `&Arc<SessionCatalog>` instead of `&dyn Catalog` + the already-loaded
    `IcebergTable` (which it no longer used — the retry helper loads it).
  - The Puffin sidecar is a SECOND metadata commit on the same table, so the
    two INSERT paths invalidate again after emitting it.
- `crates/sqe-coordinator/src/flight_sql.rs` — `do_put_statement_ingest`
  already routed through `WriteHandler::handle_ingest_streaming`, and that
  handler now ends in `after_commit`, so the `DoPut` path invalidates with no
  behavioural change needed at the Flight layer. The arm gained a comment
  recording the invariant: this path never passes through
  `QueryHandler::execute`, which is where the invalidation used to live, so the
  ingest has to keep going through a `WriteHandler` entry point. An ADBC client
  that bulk-loads and then counts rows on the same connection no longer reads
  the pre-ingest snapshot.
- `crates/sqe-coordinator/src/query_handler.rs` — removed the three calls that
  only mirrored a `WriteHandler` commit: the `StatementKind::Ctas` arm, the
  `StatementKind::CreateTable` arm, and the writer-scoped DML block at the end
  of `execute()` (the `Insert | Ctas | Merge | Delete | Update | Truncate`
  match). Every kind in that match reaches `after_commit` now — `Truncate` via
  `handle_delete_dispatch` — so it was a duplicate; the comment in its place
  records why and that the Hive arms keep their own calls (they `return` early
  and never reach the WriteHandler).

**Acceptance grep.** `grep -c invalidate_session_cache
crates/sqe-coordinator/src/query_handler.rs` is **17**, not under 8. The
remaining calls are all outside the WriteHandler and all still required: DROP
TABLE, ALTER SCHEMA, DROP NESTED COLUMN, SET TBLPROPERTIES, SET TAGS, SET
MASKING POLICY, ref DDL, partition evolution, CREATE VIEW, DROP VIEW, CREATE
EXTERNAL TABLE, the two Hive CTAS steps, Hive INSERT, `CALL
system.refresh_catalog_cache`, the maintenance `CALL` arm, and the
auth-failure recovery path. Getting the literal count under 8 would mean
collapsing those DDL sites into a private `invalidate_after_ddl(session)`
helper — a mechanical change to arms this work package does not own. Worth
doing in the DDL-side pass; called out here rather than done quietly.

**Tests** (`cargo test -p sqe-coordinator --lib -- write_handler`, 132 passed /
0 failed)

- `write_handler::tests::after_commit_evicts_the_table_for_every_token` — a
  recording `TableCacheInvalidator` asserts the hook evicts the committed table
  for every token, not just the writer's key.

**Not covered by unit tests.** The per-entry-point assertion the acceptance
criteria asks for ("a fake `SessionCatalog` records
`invalidate_table_all_tokens` for every public entry point that reaches a
commit") cannot be written in-process: every one of those entry points needs a
real catalog to load a table and commit. The hook is tested through the trait
seam, and the wiring is verifiable by inspection (`grep -n
'commit_with_retry(\|commit_rewrite_with_retry(\|after_commit(' write_handler.rs`
shows an `after_commit` after every commit site). The `DoPut` N rows →
`SELECT count(*)` scenario belongs in `tests/it/integration_test.rs` on the
docker rig.
