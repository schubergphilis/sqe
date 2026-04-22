## Context

SQE's CoW UPDATE/DELETE path reads each affected data file's parquet batches, rewrites them via a DataFusion SQL round trip, writes the result as a new data file, and commits the swap via a single `Transaction::rewrite_files()` at the end. The `dml-subquery-streaming` change made IN-subquery WHERE clauses plan-size O(1). The SF100 benchmark then exposed three separate scaling problems in the *rest* of the handler.

Research summary (from the three-agent parallel dive that preceded this change):

1. The per-file rewrite loop in `handle_update:1010` and `handle_delete:729` is serial. Every file is read, rewritten, and written before the next file starts. Writers are `Send + 'static` (`vendor/iceberg-rust/crates/iceberg/src/writer/mod.rs:412`); the commit is already batched.

2. The per-batch WHERE clause is evaluated twice. `apply_update` (line 1803) runs a SELECT with `CASE WHEN (where) THEN new ELSE old END`. `count_matching_rows` (line 1897) then runs a second SELECT `WHERE (where)` purely to count matches. Two MemTable registrations, two parser invocations, two planner invocations, two executions per batch.

3. `apply_update` calls `.collect()` then takes `result_batches.into_iter().next()`. For any file whose rewrite produces more than one output batch, rows are silently dropped. DataFusion's default batch size (8192) hides this on small files; large row groups trip it.

Industry comparison confirms the cost model: Trino and Spark both run row-level operations with worker-level parallelism (one file group per task) and never loop serial over a table's files. See `openspec/changes/cow-dml-parallel-streaming/research.md` for the full survey.

## Goals / Non-Goals

**Goals:**

- TPC-E SF100 `trade_result_update_holding` completes inside the 120 s harness cap.
- CoW `UPDATE` and `DELETE` scale close to linearly in total data (file_count × file_size) rather than super-linearly.
- Preserve exactly-matching semantics: same affected rows, same output files, same single atomic commit point.
- Fix the latent multi-batch correctness bug in `apply_update` as a side effect of streaming.

**Non-Goals:**

- Replace `lift_in_subqueries` with a native DataFusion LeftSemi IN-subquery rewrite. That's follow-up #3 in `openspec/changes/dml-subquery-streaming/followups.md`. It targets the IN-subquery path specifically and composes on top of this change.
- Switch the default UPDATE mode from CoW to MoR (Merge-on-Read with position deletes). That's a separate openspec proposal and a much larger surface.
- Physical plan reuse across files (DataFusion has no public prepared-statement API; issue apache/datafusion#13454).
- MERGE INTO support (upstream tracked in apache/datafusion#20746; not yet ready).

## Architecture

Before:

```
handle_update
    load_table
    collect_data_files     (already parallel via buffer_unordered)
    for file in files:
        read_parquet_batches
        for batch in batches:
            apply_update    (SQL round trip #1: CASE WHEN where)
                .collect()
                take first batch (drops rest!)
        for batch in batches:
            count_matching_rows   (SQL round trip #2: COUNT(*) WHERE where)
        write_data_files_with_metrics   (collect-then-write, one file)
    Transaction.rewrite_files().add(new).delete(old).commit()
```

After:

```
handle_update
    load_table
    collect_data_files     (unchanged)
    in_subq_guard = lift_in_subqueries(where, ctx)    (unchanged)

    stream::iter(files)
        .map(file -> async {
            read_parquet_batches
            stream::iter(batches)
                .then(batch -> apply_update_streaming(...))   (SQL round trip, streams output
                                                              with __sqe_matched column)
                .flatten_unordered
                -> write_data_files_streaming_with_metrics    (writes as batches arrive;
                                                              sums __sqe_matched; strips it)
            returns (new_data_files, matched_row_count)
        })
        .buffer_unordered(writer_parallelism)
        .try_collect()                                        (one Result::Err aborts the stream)

    aggregate (all_new_data_files, total_matched)
    Transaction.rewrite_files().add(all_new).delete(all_old).commit()
```

Key shape change: the file-level `for` becomes a `buffer_unordered(N)` stream. The batch-level `for` inside each file becomes a streaming pipeline that writes as it goes. `count_matching_rows` disappears; its output comes from a new projection column in `apply_update`'s SELECT.

## Key Design Decisions

### Bounded concurrency, not unbounded `join_all`

`buffer_unordered(N)` caps the number of in-flight file rewrites at N. Unbounded `join_all` on a 60-file UPDATE would try to open 60 parquet readers and 60 parquet writers simultaneously. Peak memory for one in-flight file rewrite is roughly `file_batches × batch_size` on the read side plus the streaming batch on the write side. At 8 MiB per batch and 8-way parallelism that's ~64 MiB of batch buffers plus whatever DataFusion spills. Safe on a 36 GiB machine; trouble at higher N.

Default `N = min(logical_cpus, 8)`. Capped at 8 because:

- Apple Silicon M-series has 5P + 6E cores. P-cores saturate around 4-5 parallel DF queries (DF itself uses Rayon internally for hash joins; too many outer tasks steal workers from the inner plans).
- S3 writes parallelise fine but are rate-limited per connection. 8 concurrent PUTs is comfortable; more risks per-bucket 429s.
- The upper bound is a safety cap; users with bigger machines set `writer_parallelism` explicitly.

### Count via projected column, not a second query

New SELECT shape in `apply_update`:

```sql
SELECT
    CASE WHEN (where) THEN expr1 ELSE col1 END AS col1,
    ..., colN,
    CAST((where) AS INT) AS __sqe_matched     -- new column
FROM datafusion.public.__update_table AS "table"
<joins_sql>
```

The predicate is evaluated once inside DataFusion's projection. Common subexpression elimination (DataFusion's `CommonSubexprEliminate` rule, enabled by default) should collapse it to one evaluation feeding both the CASE and the CAST. Even if CSE misses, it's in-memory arithmetic on one batch, not a second round trip.

On the Rust side: consume the stream, sum `__sqe_matched` into `total_matched`, drop the column before handing batches to the writer. `RecordBatch::project` does the drop cheaply (array reference bump, no copy).

### Streaming through the existing writer

`crates/sqe-coordinator/src/writer.rs:265` already exposes:

```rust
pub async fn write_data_files_streaming_with_metrics(
    table: &Table,
    stream: SendableRecordBatchStream,
    op: &str,
    metrics: Option<&Metrics>,
    compression: Compression,
) -> Result<Vec<DataFile>>
```

It's used by CTAS and INSERT. The UPDATE/DELETE paths don't call it yet. The wiring is a one-line swap from `.collect() + write_data_files_with_metrics(vec)` to `execute_stream() + write_data_files_streaming_with_metrics(stream)`. The helper already handles rotation at target file size, commits the parquet footer, and emits one `DataFile` descriptor per output file.

### Preserve single-commit atomicity

The `Transaction::rewrite_files()` call stays outside the parallel region. We collect all `(new_data_files, matched_rows)` tuples from the stream via `try_collect()`, aggregate, then run a single commit. Any per-file error cancels the stream and bubbles up; we lose the incomplete rewrite work (no files were added to the table yet, since `add_data_files` hadn't been called) but the table state is untouched.

This matches the current semantics: either all files swap atomically or none do. Partial progress on a failure is impossible by construction.

### Error handling under parallelism

`buffer_unordered` + `try_collect` stops on the first `Err`. In-flight tasks are aborted when the stream is dropped. That matches the current serial behaviour where the first error bails out the loop.

One subtlety: an in-flight parquet write that aborts mid-stream leaves an orphaned object in S3. This already happens in the serial path (the writer creates the object eagerly, the commit never references it, S3 lifecycle policy or manual GC cleans it). The parallel path makes it more visible but not more harmful. Not a regression, logged in the risks table below.

### Config knob

Path: `cow_dml.writer_parallelism`. Type: `usize`. Default: `num_cpus::get().min(8)`. Range: 1..=64. Validated at config load.

Setting `writer_parallelism = 1` reproduces the old serial behaviour exactly (same `buffer_unordered(1)` degenerates to sequential). That's the rollback escape hatch.

### What we do NOT change

- `collect_data_files` already runs with `manifest_concurrency`-way parallelism for metadata scans. Untouched.
- `lift_in_subqueries` (the `dml-subquery-streaming` change) runs once per DML statement, before the file loop. The scratch MemTable is registered once and read by all parallel tasks. DataFusion's TableProvider locking handles concurrent reads safely.
- `decorrelate_scalar_subqueries` runs inside `apply_update` per batch. Moving it outside the loop is a separate optimization (follow-up in this change).
- Commit-time logic, transaction construction, metrics wiring: untouched.

## Rust Shapes

Per-file task function (new private helper):

```rust
/// Rewrite a single data file under UPDATE semantics and stream the output
/// into a new Iceberg data file. Returns (new data files, matched row count).
///
/// The scratch MemTable that `lift_in_subqueries` registered is assumed to be
/// live for the caller's lifetime; this function only reads from it.
async fn rewrite_file_for_update(
    &self,
    ctx: &DFSessionContext,
    table: &Table,
    data_file: &DataFile,
    assignments: &[sqlparser::ast::Assignment],
    where_sql: &str,
    joins_sql: &str,
    table_ident: &TableIdent,
) -> sqe_core::Result<(Vec<DataFile>, usize)>;
```

Outer loop replacement:

```rust
use futures::stream::{self, StreamExt, TryStreamExt};

let parallelism = self.config.cow_dml.writer_parallelism.max(1);

let (new_data_files, total_updated): (Vec<Vec<DataFile>>, Vec<usize>) =
    stream::iter(old_data_files.iter())
        .map(|data_file| async move {
            self.rewrite_file_for_update(
                ctx, &table, data_file, assignments,
                &where_sql, &joins_sql, &table_ident,
            ).await
        })
        .buffer_unordered(parallelism)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .unzip();

let new_data_files: Vec<DataFile> = new_data_files.into_iter().flatten().collect();
let total_updated: usize = total_updated.iter().sum();
```

Per-batch `apply_update` becomes `apply_update_streaming`:

```rust
/// Project the CASE-WHEN rewrite into a RecordBatch stream, emitting an
/// extra `__sqe_matched` INT column so callers can sum matches without a
/// second query. Caller strips the column before writing.
async fn apply_update_streaming(
    &self,
    ctx: &DFSessionContext,
    batch: &RecordBatch,
    assignments: &[sqlparser::ast::Assignment],
    where_sql: &str,
    in_subquery_joins: &str,
    table_ident: &TableIdent,
) -> sqe_core::Result<SendableRecordBatchStream>;
```

The caller strips `__sqe_matched` with `RecordBatch::project(&indices)` where `indices` skips the last column, sums the stripped column's values into a running total, and hands the stripped batch to the streaming writer.

## Data Flow Example

TPC-E `trade_result_update_holding` at SF100 with N=8 parallelism, ~60 affected data files (illustrative; actual count depends on partition layout):

```
t0: open 8 parquet readers; start 8 concurrent rewrite tasks
t0+r1: task 3 finishes first (smallest file), emits 1 DataFile
t0+r2: task 3 begins file 9; tasks 1,4,5 finish around same time
...
t0+T: all 60 tasks complete; aggregate matched_row counts
t0+T: single Transaction.rewrite_files() commit
```

Wall clock drops from `sum(per_file_time)` to roughly `ceil(60 / 8) * avg_per_file_time`. If per-file time averages 1.8 s (derived from SF10 10.94 s / ~6 files), N=8 gives `ceil(60/8) * 1.8 = 8 * 1.8 = 14.4 s`. That's 8x under the 120 s cap with comfortable headroom.

Actual gain will be less than ideal due to S3 write contention and DataFusion's internal parallelism competing for CPU. A realistic target is 4-6x. Still well under the cap.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Memory pressure from N concurrent file rewrites | Streaming (fix D) keeps per-file peak to one batch. `writer_parallelism` is tunable. Default 8 is safe at observed TPC-E file sizes (tested up to 1 GiB files). |
| S3 rate limiting under parallel writes | Default N=8 is comfortably below typical per-bucket PUT limits. Operators on rate-limited paths can cap via config. `reqwest` retries with backoff are already configured. |
| CSE misses the `(where_sql)` in CASE+CAST, doubling predicate cost | CSE is default-on in DataFusion 53 and handles this shape. If a specific predicate trips a CSE miss, the cost is one extra projection (arithmetic on Arrow arrays), not a round trip. Still strictly cheaper than the deleted `count_matching_rows`. |
| Per-file task panic stalls other tasks | `buffer_unordered` propagates errors via the stream; `try_collect` aborts immediately. Panics inside a tokio task are converted to errors by the runtime. |
| Orphaned parquet objects in S3 on mid-stream abort | Already possible in the serial path. S3 lifecycle rules or manual compaction GC them. Same risk envelope, same cleanup story. |
| `decorrelate_scalar_subqueries` non-determinism under parallelism | It's pure (reads AST, emits SQL strings). No shared mutable state. Safe to call concurrently. |
| Changed ordering of new data files in the manifest | Current implementation's order is "first file finished first". We switch to `buffer_unordered`, which is no longer insertion-ordered. Iceberg doesn't depend on data file ordering within a manifest; readers scan all files regardless. No semantic impact. |
| Config knob misconfiguration (N=0 or N=100) | Clamp in the config layer: `max(1, min(writer_parallelism, 64))`. Log a warning on clamp. |
| `apply_update_streaming` needs the scratch MemTable to stay registered | `_in_subq_guard` already binds for the enclosing function's lifetime; parallel tasks inside the same `await` chain are within scope. No change needed. |

## Open Questions

**Q1: Should `writer_parallelism` also apply to `collect_data_files`?**
No. Metadata scan is already parallel via `manifest_concurrency` and has a different cost profile (network-bound, many small requests). Keep the knobs separate so operators can tune independently.

**Q2: Should we measure per-file time and report it in metrics?**
Yes, but as a follow-up. The existing `write_data_files_streaming_with_metrics` emits Prometheus counters for bytes written and files written. Adding a per-file elapsed histogram needs one more counter registration and a wrapping span. Good for observability, not blocking for this change.

**Q3: Does the count of `__sqe_matched` match what `count_matching_rows` reported?**
It should match exactly when the WHERE is deterministic (the common case). A non-deterministic WHERE (e.g., a UDF reading wall clock) could report different counts across the two SELECTs, so dropping the second pass actually makes semantics stricter: the reported affected count now matches the rows the writer saw. Regression test must assert on deterministic predicates only.
