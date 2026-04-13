# Writing Is a Contract {#sec:writes}

> Reading is easy. Writing is where table formats earn their keep.

A query engine that can only read is a glorified report generator. We had proven the read path -- DataFusion parses SQL, Iceberg supplies metadata, Polaris vends credentials, S3 delivers Parquet bytes, and the user gets Arrow batches. Chapters 4 through 6 tell that story. But dbt does not just read. dbt needs to create tables, insert rows, run incremental materializations that merge new data into existing tables. Without a write path, SQE is a demo.

The git log tells the story plainly: March 14, the engine ran its first read query. March 15, CTAS and INSERT INTO worked end to end. One day. But that one day compressed more debugging than the entire read path combined, because writing to Iceberg is not the reverse of reading from it. Reading is a contract between the engine and the storage layer. Writing is a contract between the engine, the storage layer, the catalog, and every other writer who might be committing at the same time. More parties means more places where types can disagree, metadata can diverge, and assumptions can quietly be wrong.


## The Commit Protocol

Before any code, the mental model. Iceberg's write protocol has three stages, and you have to understand all three before the first line of Rust makes sense.

**Stage 1: Write data files.** The engine produces Parquet files and uploads them to S3. These files are orphans -- they exist in storage but are invisible to any reader. No catalog knows about them. No manifest references them. They are bits on disk with no meaning.

**Stage 2: Commit metadata.** The engine sends a commit request to the catalog saying "here are the new data files, add them to this table's current snapshot." The catalog validates the request, creates a new snapshot, updates the manifest list, and returns success.

**Stage 3: Atomicity guarantee.** If the commit fails -- because another writer committed first, because the table was dropped, because the network died -- the data files from Stage 1 are garbage. They sit in S3 until someone cleans them up. No reader will ever see them. This is by design.

::: {.iceberg}
**Iceberg deep dive:** Iceberg's snapshot isolation is implemented through the catalog, not through
storage. S3 historically had no compare-and-swap (though AWS added conditional puts via `If-None-Match`
in August 2024). The catalog provides the atomic swap: it replaces the metadata pointer only if
the base snapshot matches what the writer saw when it started. Optimistic concurrency control.
The writer assumes nobody else is committing and proceeds. If the assumption was wrong, the
commit is rejected and the writer must retry or fail.
:::

This three-stage protocol gives the write path its natural structure: execute the query to produce RecordBatches, write those batches as Parquet data files, then commit the new files through the catalog. Every write operation in SQE follows this shape. The differences are in what happens before Stage 1 and what kind of commit Stage 2 uses.


## CTAS: The Simplest Write

We started with CREATE TABLE AS SELECT. Not because it was the most important write operation, but because it was the simplest to reason about. No existing table to contend with. No concurrent writers. No schema to match. The SELECT defines the schema, the data, and the table all at once.

The coordinator classifies `CREATE TABLE ns.target AS SELECT ...` and routes it to the write handler. The streaming write path processes DataFusion output one batch at a time via `SendableRecordBatchStream`. The `write_data_files_streaming` function in the writer module passes each `RecordBatch` directly to the Iceberg `RollingFileWriter` without buffering. Peak memory drops from O(total rows) to O(batch size) -- typically 8,000 rows. The six-million-row lineorder table at scale factor 1 now loads with the same memory footprint as a fifty-row dimension table. The write handler then converts the Arrow schema to an Iceberg schema, creates the table in Polaris, writes the batches as Parquet data files, and commits them via a fast-append transaction.

The schema conversion is where the first real problem appeared. Arrow schemas from DataFusion queries do not carry Parquet field-ID metadata -- the `PARQUET:field_id` key that Iceberg uses to map columns to schema fields is absent. Without that key, iceberg-rust's `arrow_schema_to_schema` rejects the schema outright. No graceful fallback. No warning. Just a hard error with a message that took some squinting to connect back to the missing metadata.

We wrote our own conversion that walks the Arrow schema and assigns sequential field IDs starting from 1. Sequential IDs work for new tables because there is no existing schema to maintain compatibility with. Iceberg requires field IDs to be unique and never reused within a schema's evolution history. For CTAS, we are creating the initial schema, so any numbering that starts at 1 and increments is valid. The type mapping -- `Int64` to `long`, `Utf8` to `string`, `Float64` to `double` -- comes from iceberg-rust's `arrow_type_to_type` function, which handles the standard conversions but rejects certain Arrow types that have no Iceberg equivalent.

This seemed straightforward. Then we tried a query with a timestamp.


## Four Hours for One Enum Variant

DataFusion produces `Timestamp(Nanosecond, None)` for `CURRENT_TIMESTAMP` and timestamp literals. Iceberg stores timestamps as `Timestamp(Microsecond, None)`. The Parquet writer in iceberg-rust enforces strict type matching. Nanosecond timestamps are rejected.

The error was opaque. The Parquet writer reported a schema mismatch, but the field names and logical types looked identical. We printed both schemas side by side. They matched. We compared them field by field in a loop. They matched. We serialized them to JSON and diffed the output. They matched.

They did not match.

Both `Timestamp(Nanosecond, None)` and `Timestamp(Microsecond, None)` display as "Timestamp" in Arrow's summary formatter. The precision parameter -- the one thing that was different -- does not appear in the default display output. We were staring at two schemas that were genuinely different, rendered identically on screen.

After three hours of this, we added explicit type-level logging that printed the full `Debug` representation of every field's data type, not the display representation. And there it was: `Timestamp(Nanosecond, None)` on the left, `Timestamp(Microsecond, None)` on the right. A single enum variant, buried three layers deep in the Arrow type hierarchy.

The fix was clear once we saw it. The finding-the-problem part took four hours. One line of code. Four hours of debugging. That ratio would become familiar on the write path.

We already had a function called `stamp_field_ids` that added `PARQUET:field_id` metadata to Arrow fields before writing. We extended it to also handle type mismatches. The function now does three things in one pass: stamps field-ID metadata onto each Arrow field, fixes nullable flags by scanning all batches (not just the first), and casts any columns whose Arrow type diverges from what Iceberg expects. The timestamp cast -- nanosecond to microsecond -- is the most common, but the same mechanism handles any type divergence between DataFusion's output and Iceberg's schema.

The nullable flag fix was the second invisible bug. DataFusion sometimes marks a field as non-nullable even when the data contains nulls. This happens with `CAST(NULL AS T)` inside a `UNION ALL` -- the type is inferred as non-nullable from the cast expression, but the actual value is null. The Parquet writer rightfully rejects a null value in a non-nullable column. The first batch in a UNION ALL might have no nulls in that column; the third batch might. If you derive the schema from the first batch alone, you get a non-nullable flag and a crash when the third batch writes.

We scan all batches to detect nulls and upgrade the field to nullable if any are found. Trust the data, not the metadata. This is a pattern that came up again and again on the write path: the metadata says one thing, the bytes say another, and the bytes are always right.


## Writing the Files, Committing the Result

Once the table exists in the catalog and the batches are type-corrected, the physical writing happens through iceberg-rust's writer infrastructure. This part, after all the schema debugging, was refreshingly mechanical. We configure a chain of builders: `ParquetWriterBuilder` for the file format, `RollingFileWriterBuilder` for splitting large writes across multiple files (128 MB default), `DataFileWriterBuilder` for producing the `DataFile` descriptors that Iceberg needs for the commit. Each `DataFile` contains the file path, size, row count, column statistics, and partition values. The builder chain is verbose but honest -- every configuration choice is visible in the code, not hidden in a default.

The `file_prefix` parameter distinguishes data files by origin -- "ctas", "insert", or "ingest". Iceberg does not care about file names. We do, at 2am, when we are staring at a table's storage directory trying to figure out which write operation produced a particular file.

With data files written and their descriptors in hand, the commit itself is four lines:

```rust
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);

let tx = action.apply(tx).map_err(|e| {
    SqeError::Execution(format!("Failed to apply fast append: {e}"))
})?;

tx.commit(catalog.as_ref()).await.map_err(|e| {
    SqeError::Execution(format!("Failed to commit CTAS transaction: {e}"))
})?;
```

`Transaction::new` creates a transaction scoped to the table's current snapshot. `fast_append()` creates a `FastAppendAction` -- the simplest commit type, which only adds data files without modifying or removing existing ones. `apply` prepares the transaction. `commit` sends it to the catalog.

The `fast_append` distinction matters. Iceberg defines several transaction types with different conflict rules:

| Transaction Type | What It Does | Conflict Check |
|---|---|---|
| `fast_append` | Add data files only | Fails if another writer deleted data or changed schema |
| `overwrite` | Replace data files | Fails if another writer modified any affected files |
| `row_delta` | Add data + delete files | Full row-level conflict check |
| `rewrite_files` | Compaction | Fails if any rewritten files were modified |

For INSERT INTO and CTAS, `fast_append` is correct. We are only adding new files. Two writers inserting rows into the same table simultaneously is perfectly safe -- Iceberg's metadata supports concurrent appends without conflict. The conflict scenarios that matter are structural changes: someone drops the table, alters the schema, or performs a compaction between our write and our commit. The engine surfaces the error, and the client can retry.


## INSERT INTO and the Three Entry Points

INSERT INTO follows the same structure as CTAS, with one difference: the table already exists. We load it instead of creating it. The query handler extracts the SELECT source from the INSERT INTO statement, executes it through DataFusion, and passes the batches to the write handler. The write handler loads the existing table, writes data files against its current schema, and commits via fast-append.

The `stamp_field_ids` function becomes critical here. For CTAS, we control the schema -- the Iceberg schema is derived from the Arrow schema, so the types match by construction. For INSERT INTO, the target table's schema was defined previously, possibly with different precision or nullability. The type casting ensures the new data matches the table's expectations.

The same code path also handles Flight SQL's DoPut ingest, where a client streams Arrow batches directly via the Flight protocol instead of sending SQL. Three write entry points -- CTAS, INSERT, DoPut ingest -- all converging on the same `write_data_files_streaming` and `Transaction::fast_append` primitives. One code path for producing Parquet files, one for committing them, regardless of how the data arrived.

::: {.deadend}
**Dead end: letting DataFusion handle the full INSERT.** DataFusion has its own `InsertExec`
plan node. We considered routing INSERT INTO through DataFusion's built-in write path rather
than extracting the SELECT and handling the Iceberg commit ourselves. DataFusion's `InsertExec`
assumes it controls the table provider, including where and how files are written. Iceberg's
commit protocol requires knowledge of the table's metadata location, file naming conventions,
and partition spec. We would have needed to implement DataFusion's `TableProvider` write
interface for Iceberg, which had no upstream support in iceberg-datafusion 0.9. Splitting
query execution from the write commit was simpler and gave us full control over the transaction.
:::


## The Benchmark Surprise

The benchmark suite uses CTAS to load all test data. TPC-H at scale factor 10 means eight tables totaling about 10 GB of Parquet. We had just spent a week understanding Iceberg's commit protocol -- the snapshot isolation, the optimistic concurrency, the catalog round-trips. We expected the commit to be the bottleneck. We instrumented it carefully.

Loading via CTAS took approximately 4 minutes on a single coordinator. We checked the commit timing. Milliseconds. All eight tables. Milliseconds.

The four minutes were Parquet serialization and S3 upload latency. The protocol that we had spent days understanding and debugging was the fastest part of the entire operation, by three orders of magnitude. We had optimized our understanding of the wrong thing.

This reshaped our priorities. The rolling file writer's default chunk size, the Parquet compression settings, the number of concurrent S3 uploads -- these are the knobs that matter for write throughput. The commit protocol is elegant and important and takes almost no time at all. If you are building a write path for Iceberg, optimize the file writing first. The commit is not your problem.


## CREATE OR REPLACE TABLE

dbt's table materialization uses `CREATE OR REPLACE TABLE ... AS SELECT`. The query handler implements this by checking the `or_replace` flag: drop the existing table, then create a new one with the CTAS data.

Drop, then create. Not a single atomic operation -- there is a window between the drop and the create where the table does not exist. For dbt's use case (batch transforms where the table is being rebuilt from scratch), this is acceptable. For a high-availability system where readers must always see either the old or the new version, it would not be. A true atomic replace would require Iceberg's overwrite transaction.


## Statement Classification

Before any write handler runs, the SQL classifier must decide what kind of statement it is looking at. `CREATE TABLE foo AS SELECT 1` and `CREATE TABLE foo (id INT)` both parse as `Statement::CreateTable` in sqlparser-rs. The difference is whether the `query` field is `Some` or `None`:

```rust
Statement::CreateTable(ref ct) => {
    if ct.query.is_some() {
        Ok(StatementKind::Ctas(Box::new(stmt)))
    } else {
        Ok(StatementKind::CreateTable(Box::new(stmt)))
    }
}
```

The classifier routes each statement before DataFusion ever sees it. For write operations, the coordinator extracts and re-executes just the SELECT portion through DataFusion, collects the batches, and hands them to the write handler. The write handler never touches DataFusion directly. It receives already-materialized data. This separation turned out to be one of the better architectural decisions on the write path -- the write handler knows nothing about SQL, and the query handler knows nothing about Iceberg commits. Each does one thing.


## Copy-on-Write vs Merge-on-Read

CTAS and INSERT INTO are append-only. They only add files. The harder operations -- DELETE, UPDATE, MERGE -- require modifying or removing rows that already exist. This is where the engineering gets interesting, and where the two fundamental strategies for row-level mutations diverge.

**Copy-on-Write** rewrites entire data files. Delete one row from a million-row file, and CoW reads that file, writes 999,999 rows to a new one, and commits a replacement. Subsequent reads are clean -- every scan reads only data files, no reconciliation needed. The cost is write amplification. Deleting 100 rows across 100 files means rewriting 100 files.

**Merge-on-Read** writes small delete files -- position deletes marking row positions within specific data files, or equality deletes marking rows by key values. Writes are fast because delete files are tiny. The cost shifts to reads: every subsequent scan must reconcile delete files against data files, filtering out deleted rows at read time. The more deletes you accumulate without compacting, the slower every read gets. It is a debt model -- fast writes now, paid back on every read until you compact.

| Characteristic | Copy-on-Write | Merge-on-Read |
|---|---|---|
| Write cost | High (rewrites entire files) | Low (small delete files) |
| Read cost | None (clean data files) | Per-scan reconciliation |
| Best for | Read-heavy workloads | Write-heavy workloads |
| Compaction need | Low | High (delete files accumulate) |

The practical question: does the delete ratio justify the complexity? If you delete 10 rows out of a million-row table once a day, CoW rewrites a handful of files and you never think about it. If you delete 10% of your data hourly, Merge-on-Read avoids catastrophic write amplification, but you need compaction to keep read performance from degrading.

SQE uses Copy-on-Write. It is simpler, and the RisingWave iceberg-rust fork provides the `rewrite_files()` transaction primitive that makes it work. The RisingWave fork also ships `PositionDeleteFileWriter`, and position delete support is partially implemented in SQE. Full Merge-on-Read with `RowDeltaAction` will follow as the upstream iceberg-rust API stabilizes. The choice of Copy-on-Write as the default is pragmatic, not permanent.


## Row-Level Writes: Copy-on-Write

The SQL classifier parses and classifies MERGE, DELETE, and UPDATE statements. The routing works. And now the handlers deliver.

Upstream iceberg-rust had not shipped `OverwriteAction`, so we found another path: the [RisingWave iceberg-rust fork](https://github.com/risingwavelabs/iceberg-rust) (rev `1978911ec4`), which provides `rewrite_files()` -- a transaction API that atomically replaces a set of data files with new ones. This is exactly the primitive Copy-on-Write needs: read the affected files, rewrite them without the deleted or modified rows, commit the swap.

**DELETE FROM** reads each affected data file, applies the WHERE filter, and rewrites the file without matching rows. If all rows match, the file is simply removed. DELETE without a WHERE clause is a truncate. Cross-table subqueries work in the WHERE clause because DataFusion handles the subquery planning before the CoW rewrite step executes.

**UPDATE** follows the same pattern: read affected files, apply the WHERE filter, apply the SET expressions to matching rows, rewrite the file. CASE WHEN transformations in SET clauses work naturally because DataFusion evaluates them as expressions.

**MERGE INTO** is the most complex. It executes a full outer join between source and target via DataFusion, classifies each result row as matched (UPDATE or DELETE) or not matched (INSERT), then rewrites affected target files and appends new files for INSERT rows. The entire operation commits atomically via `rewrite_files()`.

All three operations are atomic via Iceberg snapshot isolation. If the commit fails -- because another writer modified the same files -- the error surfaces to the client. Retry logic is left to the caller, which is adequate for batch workloads orchestrated by dbt.

Compaction is the remaining thing we have not built. For CoW workloads, file count grows with each mutation -- every DELETE or UPDATE that touches a file produces a new file. A dbt pipeline running incremental models nightly accumulates files steadily. Iceberg's manifest lists handle millions of entries, so the urgency is low, but compaction will eventually matter for scan performance. When we build it, compaction will run as a background task using the same bearer token passthrough model. No ambient credentials. No service account. The user who triggers compaction must have write permission on the table.


## The Bearer Token and Writes

Everything in Chapter 4 about bearer token passthrough applies to writes, with one additional constraint: the write path requires authorization at the catalog level, not just the storage level.

For reads, the user needs read permission on the table and read access to the S3 path. For writes, the user also needs permission to create tables, append data files, and commit transactions. These are separate permissions in Polaris. The write handler creates a `SessionCatalog` with the user's bearer token, and every catalog operation -- `create_table`, `load_table`, `commit` -- is authenticated as that user.

If Alice has read-only access and tries to INSERT, Polaris rejects the commit. The engine does not need its own authorization logic for this. The catalog enforces it. No ambient permissions. No service account.


## Conflict Resolution

Two users insert into the same table at the same time. What happens?

With `fast_append`, both commits succeed. Iceberg's metadata supports concurrent appends because appending new files does not conflict with other appends -- neither writer is modifying or removing files the other cares about. The catalog creates two consecutive snapshots, each adding its own data files. Readers see a consistent view at any snapshot.

The story changes with overwrites. Two writers both try to delete from the same table. Writer A reads the table at snapshot N, identifies files to rewrite, and commits at snapshot N+1. Writer B, who also started at snapshot N, tries to commit at what it thinks is N+1 but is now N+2. The catalog rejects Writer B's commit because the base snapshot no longer matches. This is not a bug. This is the protocol working as intended.

The correct response is to retry: reload the table metadata, re-identify affected files, re-execute the write, re-commit. SQE does not currently implement automatic retry -- the error surfaces to the client. For batch workloads orchestrated by dbt, which serializes model execution, this is adequate. For concurrent OLTP-style mutations, automatic retry with backoff would be necessary.

The retry semantics are where the real complexity hides. A naive retry that re-executes the entire query might produce different results if the source data changed between the first attempt and the retry. An intelligent retry reads the new table state, identifies which of its original changes are still valid, and commits only the delta. This is the complexity that makes row-level write support a multi-week effort rather than a multi-day one. The query execution is the easy part. The conflict resolution and retry logic is where the engineering lives.


## Cache Invalidation on Write

One detail that is easy to miss: when a write operation commits, any cached query results for the affected table become stale. Every INSERT, CTAS, and DROP invalidates all cache entries for the affected table. This is conservative -- it invalidates everything that touches the table, not just the queries whose results actually changed. A more precise approach would check whether the write overlaps with cached query predicates. We took the conservative path because cache precision is a nice-to-have and stale reads are a production incident.


## What We Learned

The write path took one day to implement and three days to debug. The ratio tells you something about writing to Iceberg: the concepts are simple, the protocol is well-designed, and the bugs are invisible. Most of the debugging was in the type system -- the gap between Arrow types as DataFusion produces them and Arrow types as Iceberg expects them. The commit protocol itself was straightforward once we understood the three-stage model.

**Reading is a contract with storage. Writing is a contract with the world.** When you read, you depend on the storage layer delivering bytes. When you write, you depend on the storage layer accepting bytes, the catalog accepting your commit, no other writer conflicting with your commit, and the type system matching across three layers -- DataFusion Arrow, Iceberg schema, Parquet physical. Every additional party in the contract is a potential failure point.

**Start with append-only, then add mutations.** CTAS and INSERT INTO cover 80% of what data pipelines need. dbt's `table` materialization uses CTAS. dbt's `incremental` materialization needs MERGE, but only for the incremental part -- the initial build is still CTAS. Shipping append-only first gave us a useful engine weeks before row-level operations landed. When we added DELETE, UPDATE, and MERGE via the RisingWave fork's `rewrite_files()`, the architecture was already in place -- the handlers slotted into the existing classifier and write handler structure.

**Upstream dependencies gate your timeline, but forks buy you time.** Upstream iceberg-rust had not shipped `OverwriteAction`. Rather than wait, we switched to the RisingWave fork that had the transaction primitive we needed. This is the cost and benefit of building on an ecosystem: you depend on others, but you can also leverage their parallel work. The RisingWave team needed the same primitive for their streaming engine and built it before the upstream community finalized the API. When upstream ships, we migrate back. Until then, the fork works.

**The invisible bugs are the expensive ones.** A timestamp precision mismatch that displays identically in debug output. A nullable flag that is correct for the first batch but wrong for the third. A schema that matches by name and logical type but diverges in a nested enum variant. These are not hard problems. They are invisible problems. The fix is always one line. The debugging is always four hours.

The write path is where a query engine stops being a toy and starts being infrastructure. Reading data is table stakes. Writing data -- correctly, atomically, with snapshot isolation and concurrent writer safety -- is the price of admission to the world of table formats.

We paid that price. And then we moved on to the next problem: making sure users cannot write data they should not be able to see.

::: {.ailog}
**AI Logbook:** The AI implemented CTAS, INSERT INTO, and the `stamp_field_ids` function that assigns sequential Parquet field IDs to Arrow schemas. The timestamp precision bug — `Timestamp(Nanosecond, None)` vs `Timestamp(Microsecond, None)` displaying identically in Arrow's formatter — took four hours of human debugging before the AI was told to add `Debug`-level type logging. The fix was one line of casting. The nullable flag scanner that checks all batches (not just the first) was the human's idea after a UNION ALL crash; the AI implemented it in one pass.
:::
