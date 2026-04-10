# Streaming Writes, Sort Order Safety, and the IN (Subquery) Workaround

*April 10, 2026*

Yesterday we pushed SQE's Trino SQL compatibility from 63% to 95%. Today we turned our attention to performance and correctness: making the engine handle real-world data volumes without running out of memory, fixing a subtle sort order correctness bug, and working around a DataFusion limitation that blocked five benchmark queries.

## The OOM problem: CTAS loads that buffer everything

SQE's CTAS (CREATE TABLE AS SELECT) path had a simple but expensive design flaw. The flow was:

```
1. Execute the SELECT query
2. df.collect().await  <-- buffers ALL rows in memory
3. Pass Vec<RecordBatch> to Parquet writer
4. Write files
5. Commit to Iceberg
```

For small tables this works fine. For the SSB lineorder table (6 million rows, ~500MB in Arrow format) or TPC-DS store_sales (2.8 million rows), step 2 alone consumed more memory than the 2GB query limit. The server got OOM-killed mid-load.

The fix: stream batches directly from DataFusion's execution engine to the Parquet writer, without collecting them first.

```
1. Execute the SELECT query
2. df.execute_stream()  <-- returns a stream, nothing buffered
3. For each batch in stream:
     stamp Iceberg field IDs
     writer.write(batch)   <-- writes to Parquet immediately
4. writer.close()          <-- finishes file(s)
5. Commit to Iceberg
```

Peak memory drops from O(total rows) to O(batch size), typically around 8,000 rows per batch. The 6 million row lineorder load now uses the same amount of memory as a 50-row dimension table.

The Iceberg Parquet writer (`RollingFileWriter`) already supports this pattern. It accepts batches one at a time and automatically rolls to a new file when the current one exceeds the target size. We were just not using it properly.

We applied the same streaming pattern to INSERT INTO SELECT. The old paths remain for DELETE/UPDATE/MERGE, where the full dataset needs to be in memory for predicate evaluation and Copy-on-Write rewriting.

## Sort order: a subtle correctness risk

Iceberg tables can declare a sort order in their metadata. When SQE reads a table, it was propagating this sort order to DataFusion via `EquivalenceProperties`, telling the optimizer "this data is already sorted by these columns." DataFusion then uses `SortPreservingMergeExec` instead of a full sort, which is much faster.

The problem: Iceberg sort order is a hint about how files *should* be written, not a guarantee that they *are* sorted. Different writers handle this differently:

- Spark with sort-on-write: data is physically sorted (safe to trust)
- Trino: sorts within partitions but not globally (partially safe)
- SQE CTAS: does not enforce sort order at all (unsafe to trust)
- External data loads: no guarantees

If SQE declares data as pre-sorted when it is not, DataFusion skips the sort step and returns incorrect results for ORDER BY queries. This is a silent correctness bug: no error, just wrong output.

Our fix: only trust sort order for identity-transform partition columns, which are guaranteed to be clustered by Iceberg's file organization. Non-partition sort columns emit a warning and are ignored by default.

For controlled environments where you know all writers enforce sort order, we added a config option:

```toml
[catalog]
# Default: false (safe for mixed-writer environments)
# Set true when all data files are known to be physically sorted
trust_sort_order = true
```

This is a tradeoff. For terabyte-scale data from mixed writers (the common case in production lakehouses), the safe default prevents silent corruption. For single-writer environments with sort-on-write, the opt-in flag recovers the performance benefit.

## IN (subquery): working around a DataFusion limitation

Five TPC-E benchmark queries were permanently skipped because they use patterns like:

```sql
UPDATE last_trade
SET lt_price = lt_price * 1.01
WHERE lt_s_symb IN (
    SELECT DISTINCT tr.tr_s_symb FROM trade_request tr
);
```

DataFusion's physical planner rejects `InSubquery` expressions in UPDATE and DELETE context with: "Physical plan does not support logical expression InSubquery." This works fine in SELECT but fails for DML.

Rather than rewriting the benchmark queries (which would mean the benchmarks don't test real SQL patterns), we implemented automatic query transformation in SQE's write handler. Before executing an UPDATE or DELETE, SQE now:

1. **Fast path check**: if the WHERE clause does not contain "SELECT", skip (zero overhead for normal queries)
2. **Parse the AST**: find `IN (SELECT ...)` expressions
3. **Execute the subquery**: run it as a standalone SELECT to get the actual values
4. **Rewrite**: replace `IN (SELECT ...)` with `IN ('val1', 'val2', ...)`
5. **Execute the rewritten statement**: DataFusion handles `IN (literal_list)` correctly

This is semantically identical because the subquery is evaluated at the same point in time as the original would have been. The only difference is that the subquery results are materialized as literals, which DataFusion's physical planner can handle.

The transformation is applied to all three DML paths: CoW DELETE, MoR DELETE, and UPDATE.

## Benchmark comparison: SQE vs Trino side by side

We also added a `--compare-trino` flag to the benchmark test script. When enabled, it:

1. Starts a Trino 465 container on the same Polaris + S3 test stack
2. After each benchmark loads data, runs every query against both SQE and Trino
3. Compares row counts and timing
4. Produces a JSON report with per-query speedup ratios

```bash
./scripts/benchmark-test.sh --compare-trino tpch
```

Both engines query the same Iceberg tables via the same Polaris catalog, so any output difference is a real compatibility issue, not a data difference. This is our continuous validation mechanism for Trino SQL compatibility.

## What changed

| Change | Impact |
|---|---|
| Streaming CTAS/INSERT | Memory: O(total rows) to O(batch size). Fixes OOM on SF1+ |
| Sort order safety | Correctness: prevents silent wrong results from untrusted sort metadata |
| `trust_sort_order` config | Performance opt-in for known-sorted environments |
| IN (subquery) rewrite | Unblocks 5 TPC-E queries, zero overhead for normal queries |
| `--compare-trino` benchmarks | Continuous SQE vs Trino validation |
| Int64 date functions | Output: year() returns 2024 not 2024.0, matching Trino |
| 8GB + spill config | Benchmark stability for SF1 loads |

## Looking back at two days of work

Across April 9-10, we went from a query engine with basic Trino function support to one with ~95% SQL compatibility, streaming writes, time travel, metadata introspection, MoR DELETE, and automated Trino comparison testing.

The SQL coverage numbers tell part of the story: String 100%, Math 100%, Date/Time 100%, Regex 100%, URL 100%. But the correctness and performance work matters just as much. A query engine that returns wrong results from bad sort order assumptions, or crashes on large loads, is not production-ready regardless of how many functions it supports.

The remaining ~5% gap is genuinely hard: map-producing aggregates (need Arrow MapBuilder UDAFs), HyperLogLog/TDigest sketch types (Trino-specific), and ORC format support (strategic Parquet-only choice). None of these block typical Iceberg analytics workloads.
