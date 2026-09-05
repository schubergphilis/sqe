# 508: Dead buffered twins of CTAS/INSERT/ingest diverge from the live streaming paths; two field-stamping implementations

- Scope: sqe-coordinator write path (write_handler, writer.rs, sqe-compaction writer.rs)
- Severity: medium
- Category: maintainability
- Effort: M (days)
- Status: partial (branch `fix/config-startup-code-structure`, 2026-09-05)

## Resolution (2026-09-05)

Partial on `fix/config-startup-code-structure`. Deleted unused buffered
`handle_ctas` / `handle_insert` / `handle_ingest`. `stamp_field_ids` vs
compaction stamping still both exist (CoW rewrite still uses the buffered
writer). Follow-up: one stamping path.


## Problem

Production only calls the streaming handlers. `grep` across `crates/` for
`.handle_ctas(`, `.handle_insert(`, `handle_ingest(` finds no caller outside
`write_handler.rs` itself and two lineage test doc comments; the live callers
are `handle_ctas_streaming` (`query_handler.rs:1685`),
`handle_insert_streaming` (`:1768`) and `handle_ingest_streaming`
(`flight_sql.rs:2067`).

The buffered twins remain public and have drifted:

- `handle_insert` (`crates/sqe-coordinator/src/write_handler.rs:1786-1883`)
  ignores `ins.columns`; the streaming path reorders via
  `reorder_insert_select` (`:1478-1486`) because positional stamping "silently
  swaps matching-type columns" (`:45-52`).
- `handle_ctas` (`:1026-1216`) does not run `enforce_source_plan` (it takes
  pre-executed batches) and does not call `ensure_namespace`; the streaming
  path does both (`:1259-1262`, `:1285`).
- `handle_ctas` and `handle_insert` still receive `_or_replace` and drop it
  (see 500).

There are also two independent field-id stamping implementations that
disagree: `stamp_field_ids` (`crates/sqe-coordinator/src/writer.rs:505-579`)
scans all batches for nulls to decide nullability and casts; `build_stamped_schema`
+ `apply_stamped_schema` (`crates/sqe-compaction/src/writer.rs:486-543`) marks
every column nullable and casts per batch. The buffered `write_data_files` is
still live for CoW DELETE/UPDATE per-file rewrites (`write_handler.rs:2259-2268`,
`:2920-2928`), MoR outputs, lease rows and ledger rows, so both paths run in
production and any type-mapping fix must land twice.

## Why it matters

Dead public entry points with subtly different semantics are a trap for the
next feature: someone wiring a new protocol (MCP write, a Trino compat DoPut)
reaches for `handle_insert` and re-introduces the column-swap bug the streaming
path fixed. The extraction plan in
`docs/internal/plans/2026-07-13-sqe-write-crate-extraction.md` proposes to move
all of this verbatim into `sqe-write`, which would freeze the duplication into
a public crate API. The 2026-06-26 review already flagged `write_handler.rs`
size; deleting the twins removes about 400 lines and one of the two writers.

## Proposed change

1. Delete `handle_ctas`, `handle_insert`, `handle_ingest`. Keep
   `parse_ingest_table_name`-style helpers that tests exercise.
2. Make the CoW per-file rewrite and the MoR outputs use
   `write_data_files_streaming_with_metrics` by adapting the surviving
   `Vec<RecordBatch>` into a stream (`RecordBatchStreamAdapter`), then delete
   `write_data_files` / `stamp_field_ids` from `writer.rs`, leaving the
   position/equality delete writers.
3. `maintenance_lease.rs` and `maintenance_log.rs` write one-row batches; they
   can use the same streaming call.
4. Do this before the `sqe-write` extraction so the new crate's API is the
   streaming one only.

## Acceptance criteria

- `grep -n 'pub async fn handle_ctas\b\|pub async fn handle_insert\b\|pub async fn handle_ingest\b' crates/sqe-coordinator/src/write_handler.rs`
  returns nothing.
- `grep -rn 'fn stamp_field_ids' crates/` returns nothing; one stamping
  function remains in `sqe-compaction`.
- Existing `write_handler` unit tests for `reorder_insert_select` and the
  policy predicates still pass; `make test-integration FILTER=ctas` passes.

## Related

- 500, 505, 507.
- `docs/internal/plans/2026-07-13-sqe-write-crate-extraction.md`.
