# 507: Flight DoPut ingest never invalidates the session context cache; invalidation lives in 20 call sites instead of at the commit

- Scope: sqe-coordinator write path (flight_sql DoPut, write_handler, query_handler)
- Severity: medium
- Category: architecture
- Effort: S (hours)
- Status: open
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
