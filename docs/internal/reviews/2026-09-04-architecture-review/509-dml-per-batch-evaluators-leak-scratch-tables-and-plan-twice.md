# 509: Per-batch DML evaluators register fixed-name scratch tables in the shared session context, leak them on error, and plan each batch twice

- Scope: sqe-coordinator write path (write_handler filter_batch_negate / filter_batch_match / apply_update / count_matching_rows)
- Severity: medium
- Category: reliability
- Effort: S (hours)
- Status: open
- Issue: #601

## Problem

The CoW and MoR DELETE/UPDATE handlers evaluate the WHERE clause per batch by
registering the batch as a `MemTable` in the session's DataFusion context and
running a generated SELECT. Four helpers do this with deterministic names and
no RAII cleanup:

- `crates/sqe-coordinator/src/write_handler.rs:4751-4760` registers
  `datafusion.public.__delete_<table>`; deregistered at `:4789` with
  `let _ = ctx.deregister_table(...)`, which is only reached on success.
- `:4835-4844` / `:4870` (`__mor_delete_<table>`), `:4910-4919` / `:5005`
  (`__update_<table>`), `:5031-5040` / `:5068` (`__count_<table>`).

Any `?` between register and deregister (planning error, collect error) leaves
the MemTable, holding the whole batch, registered in the per-user cached
session context, which outlives the statement. The next DML on the same table
overwrites the registration (DataFusion `register_table` replaces), so the
leak is bounded by one batch per name, but a batch can be hundreds of MiB and
it survives until then. The MERGE path already solved this correctly with a
UUID suffix and a drop guard (`MergeScratchCleanup`, `:5544-5570`,
`Uuid::now_v7()` at `:3494`); the four helpers predate it and the comment at
`:5498-5501` acknowledges the inconsistency.

Two concurrent DELETEs on the same table in one session (Flight SQL allows
concurrent statements per session) race on the same name.

The CoW UPDATE also plans and executes two SQL statements per batch:
`apply_update` (`:2893-2906`) and `count_matching_rows` (`:2907-2919`) against
the same batch and predicate. On a 10k-file table with 8 batches per file that
is 160k plan+execute cycles for the count alone.

## Why it matters

A wide UPDATE that hits a planning error on batch 500 leaves a large MemTable
pinned in a context that other statements on the session reuse; repeated
failures across tables accumulate one per table name. Concurrent DML on the
same table in one session can evaluate the wrong batch. The double planning
makes CoW UPDATE roughly 2x slower than it needs to be on the evaluator side
and is pure overhead: the match count is the `true_count()` of the mask the
handler already computes in the MoR path (`:3187`).

## Proposed change

1. Introduce `ScratchTable` RAII guard (or reuse `MergeScratchCleanup`) that
   registers `__sqe_dml_<uuid>` and deregisters on drop; use it in all four
   helpers.
2. In CoW UPDATE, compute the match mask once (`filter_batch_match`), derive
   `updated_this_attempt += mask.true_count()`, and pass the mask into
   `apply_update` so the CASE expression can be replaced by a two-way
   `zip`/`take` on the batch when no policy masks are present.
3. Cache the planned `LogicalPlan` for the evaluator across batches of the same
   file where the schema is identical (all batches of a file share it), and
   only swap the MemTable provider.

## Acceptance criteria

- Unit test: a failing predicate leaves no `__sqe_dml_*` tables registered in
  the context (`ctx.catalog("datafusion").schema("public").table_names()`).
- Unit test: two DML evaluations interleaved on the same table name in one
  context produce independent results.
- Benchmark note in the MR: CoW UPDATE on TPC-H `lineitem` SF1 planning count
  halves (log the number of `ctx.sql` calls).

## Related

- `docs/internal/reviews/2026-06-26-codebase-analysis.md` item 5 (swallowed
  `let _ =` results).
