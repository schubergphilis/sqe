# 505: INSERT never validates source arity against the target; a wider SELECT panics inside the streaming writer

- Scope: sqe-coordinator write path (write_handler INSERT, sqe-compaction writer)
- Severity: medium
- Category: reliability
- Effort: S (hours)
- Status: open
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
