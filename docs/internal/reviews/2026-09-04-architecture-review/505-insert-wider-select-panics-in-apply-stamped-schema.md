# 505: INSERT never validates source arity against the target; a wider SELECT panics inside the streaming writer

- Scope: sqe-coordinator write path (write_handler INSERT, sqe-compaction writer)
- Severity: medium
- Category: reliability
- Effort: S (hours)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #597

## Problem

The streaming writer stamps field ids by position and indexes the target
schema with the source column index.
`crates/sqe-compaction/src/writer.rs:517-543`:

```rust
.map(|(i, col)| {
    let target = stamped_schema.field(i).data_type();
```

`arrow_schema::Schema::field` is `&self.fields[i]`
(`arrow-schema-59.2.0/src/schema.rs:374-376`) and panics past the end. Nothing
upstream checks arity: `handle_insert_streaming` only rewrites the SELECT when
an explicit column list is present (`crates/sqe-coordinator/src/write_handler.rs:1478-1486`);
without one the plan is executed and streamed straight into the writer. So
`INSERT INTO t2 SELECT a, b, c FROM s` against a two-column `t2` panics inside
the write task after the SELECT has already started executing and the first
parquet part has been opened. A narrower SELECT reaches
`RecordBatch::try_new` and errors late with an Arrow message, after execution.

The buffered stamping twin has its own defect: `stamp_field_ids`
(`crates/sqe-coordinator/src/writer.rs:519-528`) fabricates a field id
`(i + 1) as i32` when the target schema has fewer fields, so a wider batch on
that path would write a parquet column with an invented Iceberg field id instead
of failing. `build_stamped_schema` (`sqe-compaction/src/writer.rs:496-499`) keeps
the same `unwrap_or((i + 1) as i32)` although it iterates the target schema and
can never hit it.

Neither path checks names, so `INSERT INTO t SELECT b, a FROM s` with two
castable columns swaps them silently. That part is SQL-standard positional
semantics and is fine, but it means the only guard against a mis-shaped INSERT
is the panic.

## Why it matters

A panic on a request path inside a tokio task surfaces to the client as a
generic internal error with no SQL-level message, leaves the `WriteCleanupGuard`
to run from `Drop` during unwinding, and shows up in logs as a panic rather than
a user error. The error case is reached by an ordinary typo. The late-arity
error on the narrower side wastes the full SELECT execution before reporting.

## Proposed change

1. In `handle_insert_streaming` (and the ingest path), after planning and
   before `execute_stream`, compare `df.schema().fields().len()` with the
   target Iceberg schema's field count and return
   `SqeError::Execution("INSERT has N columns but target has M")`. Also apply
   the existing `reorder_insert_select` when the counts match but names differ
   only in order? No: keep positional semantics, but log at info when names
   differ so an operator can spot swaps.
2. Make `apply_stamped_schema` return an error when
   `batch.num_columns() != stamped_schema.fields().len()` instead of indexing.
3. Remove the `unwrap_or((i + 1) as i32)` fallbacks; a missing target field is
   an error.

## Acceptance criteria

- Unit test in `sqe-compaction/src/writer.rs`: `apply_stamped_schema` with a
  3-column batch and a 2-field schema returns `Err`, not a panic.
- Unit test: `INSERT INTO t(a,b) SELECT 1,2,3` fails at planning with the
  arity message before any parquet file is opened (assert the tracker is empty).
- `grep -n 'unwrap_or((i + 1) as i32)' crates/` returns nothing.

## Related

- 508 (two stamping implementations).
- `docs/internal/reviews/2026-06-26-codebase-analysis.md` item 9 (panics in
  write handler) covered explicit `panic!` calls; this is an implicit one.

## Resolution (2026-09-05)

The arity mismatch is now a SQL-level error raised at planning time, and no
stamping path invents a field id any more.

**What changed**

- `crates/sqe-coordinator/src/write_handler.rs`
  - New `check_insert_arity(&[String], &IcebergSchema, &TableIdent)`. Arity is
    an error (`SqeError::Execution("INSERT has N columns but target has M")`);
    a positional *name* difference is not, because INSERT is positional per the
    SQL standard — it is logged at info (`positions = "b -> a, a -> b"`) so an
    operator can spot an accidental swap.
  - `handle_insert_streaming` calls it right after `ctx.sql(...)`, before
    policy enforcement, before `execute_stream`, and before the upload tracker
    and `WriteCleanupGuard` exist — so no parquet part can have been opened.
  - `handle_ingest` (batched DoPut) calls it on the first uploaded batch.
    `handle_ingest_streaming` has nothing to check up front (the batches have
    not arrived); its mis-shaped batches are caught per batch by
    `apply_stamped_schema`, which now errors instead of panicking. A comment
    at the call site says so.
- `crates/sqe-compaction/src/writer.rs`
  - `apply_stamped_schema` checks `batch.num_columns() != stamped_schema.fields().len()`
    up front and returns the same "INSERT has N columns but target has M"
    error. This is the panic site: `ArrowSchema::field(i)` is `&self.fields[i]`.
  - `build_stamped_schema` — `unwrap_or((i + 1) as i32)` replaced by an
    `ok_or_else` error.
- `crates/sqe-coordinator/src/writer.rs`
  - `stamp_field_ids` — rejects a batch wider than the target schema with the
    same message, and both fallbacks (`unwrap_or((i + 1) as i32)` for the field
    id, `unwrap_or_else` for the Arrow target type) are now errors. A wider
    batch on this path used to write a parquet column stamped with an Iceberg
    field id belonging to no field.
- `grep -rn 'unwrap_or((i + 1) as i32)' crates/` returns nothing.

**Tests**

- `sqe-compaction --lib writer` (13 passed / 0 failed):
  `apply_stamped_schema_rejects_wider_batch` (3-column batch, 2-field schema →
  `Err`, not a panic), `apply_stamped_schema_rejects_narrower_batch`,
  `apply_stamped_schema_accepts_matching_arity`.
- `sqe-coordinator --lib` (112 passed / 0 failed):
  `check_insert_arity_rejects_wider_select` (asserts the exact message),
  `check_insert_arity_rejects_narrower_select`,
  `check_insert_arity_accepts_matching_and_reordered_names` (a name swap and a
  case difference both stay legal).

**Not covered by unit tests.** The acceptance criterion's "assert the tracker
is empty" needs a loaded Iceberg table, so it needs the docker e2e suite. The
check is placed above every line that creates the tracker or the cleanup
guard, so the property holds by construction; `check_insert_arity` is unit
tested in isolation instead.
