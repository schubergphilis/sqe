---
title: "Building a Comprehensive SQL Benchmark Suite"
description: "Seven benchmark suites, 222 queries, and the infrastructure to measure performance honestly."
pubDate: "2026-03-24"
author: "Jacob Verhoeks"
tags:
  - "benchmarks"
  - "tpch"
  - "tpcds"
  - "performance"
---



*2026-03-24*

When you build a SQL engine from scratch, you need a way to answer a deceptively simple question: does it actually produce correct results? Unit tests and integration tests cover features in isolation. They cannot tell you whether your engine handles the full breadth of analytical SQL that real workloads demand. For that, you need benchmarks. And not just for performance.

This post is about `sqe-bench`, the benchmark suite we built for SQE, and what we learned from running it for the first time.

## Why we built it

The common mental model for SQL benchmarks is performance: TPC-H scores, queries per second, comparative bar charts. That is not why we built this.

SQE is a replacement for a heavily-patched Trino deployment. Users have existing queries, hundreds of them, written against Trino's SQL dialect. Before we can call SQE production-ready, we need confidence that those queries will produce the same results. TPC-H, TPC-DS, SSB, and the others give us a shared, well-understood corpus to test against. If we pass TPC-DS's 99 queries at scale factor 1, we know our joins, window functions, correlated subqueries, and GROUPING SETS all work correctly. Performance is a secondary concern at this stage.

The secondary motivation is regression prevention. Rust refactors are fast, but DataFusion upgrades, iceberg-rust changes, and plan rewriter modifications can silently break query semantics. With JSON benchmark reports committed as CI artifacts, any regression in query results is caught immediately.

## What benchmarks we support and why each matters

We landed on six benchmark suites. Each chosen for a specific reason.

**TPC-H (22 queries, 8 tables)** is the standard entry-level check. Every SQL engine worth using passes TPC-H. Its queries cover joins, aggregates, ORDER BY, date arithmetic, and GROUP BY. It is fast to generate (SF1 takes under 30 seconds) and fast to run. We use it as the CI smoke test on every pull request.

**TPC-DS (99 queries, 24 tables)** is the hard one. Its queries exercise correlated subqueries, CTEs, window functions, GROUPING SETS, ROLLUP, and complex multi-table join patterns across a schema that models a full retail enterprise. If your engine handles TPC-DS well, it can handle real analytical workloads. We run TPC-DS nightly.

**SSB (13 queries, 5 tables)** is a denormalized star schema derived from TPC-H. Its queries are fast and its schema is simple. We use it as a quick smoke test to catch obvious regressions before running the heavier suites.

**TPC-C and TPC-E** cover OLTP read patterns: point lookups, small aggregates, indexed access by key. Their write queries (new order, payment, trade order) are annotated with `-- requires: delete, merge` and are skipped until SQE's DELETE/MERGE support lands. We wanted the OLTP schemas and read queries exercised now, not after a feature gate is cleared.

**TPC-BB (10 queries)** is the SQL-only subset of the Big Bench specification, which extends TPC-DS with web log and product review tables. We skip the ML and UDF queries. SQE is a SQL engine, not a Python runtime. The 10 pure-SQL queries give us coverage of semi-structured data handling and string operations.

## The architecture: generate, load, test

`sqe-bench` follows a three-phase pipeline that maps directly to the three subcommands.

**Generate** produces Parquet files on local disk or S3. Data generation is deterministic. Seeded random number generators ensure results are reproducible across runs. Files are split at 128 MB for parallelism. A TPC-H SF1 generate run takes about 25 seconds and produces 8 directories of Parquet files totalling ~1 GB.

**Load** connects to SQE and creates Iceberg tables from the generated Parquet files. This is where the `read_parquet()` TVF becomes essential. Rather than defining an intermediate import format or writing a custom loader, load sends one CTAS statement per table:

```sql
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/*.parquet');
```

For S3 sources, inline credentials are injected directly into the SQL. The Iceberg table is created in a namespace named `<benchmark>_sf<N>`, so TPC-H at SF1 lands in `tpch_sf1` and TPC-DS at SF10 lands in `tpcds_sf10`. Multiple scale factors can coexist without interference.

**Test** runs the query files in order, compares results against expected CSVs, and emits a terminal summary plus a JSON report. The comparison is strict: schema must match, row count must match, and data must match after sorting by all columns. For floating-point and decimal columns, we apply a configurable epsilon (default 1e-4) to avoid false failures from precision differences between generators.

## The read_parquet TVF as the data loading mechanism

Before writing a single benchmark query, we had to solve the loading problem. The obvious approach, generate CSV files and use COPY FROM, does not exist in SQE's current feature set. The better approach, register a table-valued function that reads Parquet directly, turned out to be straightforward with DataFusion.

`read_parquet()` is a DataFusion `TableFunction` registered on every `SessionContext`. It detects `s3://` paths vs local paths, builds the appropriate `ObjectStore`, expands glob patterns, and returns a `ListingTable`. From the query planner's perspective, it is just another table scan. Predicate pushdown, projection pruning, and partition pruning all work through it.

The key design choice was supporting inline credentials as named SQL parameters:

```sql
SELECT * FROM read_parquet(
  's3://bench-data/tpch/sf1/lineitem/*.parquet',
  access_key => 'AKIA...',
  secret_key => '...',
  endpoint   => 'http://localhost:9000',
  region     => 'us-east-1'
);
```

This makes `sqe-bench load` self-contained. It does not depend on environment variables, instance profiles, or credential files on the coordinator's filesystem. The credentials live in the SQL statement, which is transmitted over an authenticated Flight SQL connection. This is the same pattern users will reach for when they need to pull data from an external bucket during a migration.

As a side effect, `read_parquet()` is immediately useful beyond benchmarking. Users can query Parquet files ad hoc, join them with Iceberg tables, and use CTAS to migrate data from any Parquet source into SQE-managed Iceberg.

## First results

We ran TPC-H at SF1 on the first working build. The results were encouraging.

```
TPC-H SF1 — Flight SQL (localhost:50051)
─────────────────────────────────────────
q01  PASS   1.23s   6001215 rows
q02  PASS   0.45s       460 rows
q03  PASS   0.89s     11620 rows
...
q17  DIFF   2.10s         1 rows  (decimal precision)
q22  PASS   0.33s         7 rows

Results: 20/22 PASS, 1 DIFF, 1 SKIP
Total time: 28.4s
```

20 out of 22 passing on the first run. That is a strong result for an engine that has never been validated against this corpus. The one `DIFF` is a decimal precision mismatch in Q17, a single aggregate value differing in the fourth decimal place. The one `SKIP` is a query that requires a feature not yet implemented.

TPC-DS's 99 queries took longer to diagnose. Our first run cleared 80+ queries on the first attempt. The failures clustered around two areas: DataFusion SQL dialect gaps (a handful of TPC-DS queries use syntax that DataFusion does not yet support) and a Flight SQL stream handling edge case where very large result sets caused the client to miss the final batch.

## What we learned

**Flight SQL stream handling matters more than you think.** Arrow Flight streams results as a sequence of `FlightData` messages. For small result sets, this works transparently. For result sets with millions of rows, the final batch can arrive in a separate `FlightData` message after the apparent end-of-stream signal. Our benchmark client initially missed this, causing row count mismatches on large queries. The fix was straightforward once diagnosed. It would not have surfaced without running full benchmark suites.

**DataFusion's SQL dialect is nearly complete, but not identical to Trino's.** A small number of TPC-DS queries use syntax variants that DataFusion does not currently parse. Specific INTERVAL literal forms and a particular GROUPING SETS notation. These are known gaps, tracked as issues, and none of them affect the common analytical SQL patterns that most workloads use.

**Correctness and performance are separate concerns.** Several queries that return correct results take longer than we would like. This is expected. We have not yet tuned DataFusion's optimizer configuration for the specific join patterns that TPC-DS exercises. Performance tuning comes after correctness. The benchmark suite separates these cleanly: a query is `PASS` regardless of how long it takes, and timing data is recorded separately for trend analysis.

**Skipping is better than failing.** The `-- requires: delete, merge` annotation on write queries is not a workaround. It is a deliberate design choice. Those queries genuinely cannot run yet. Failing them would mask real regressions in the queries that can run. The `SKIP` status lets us track which queries are blocked on upstream features without polluting the pass rate.

## What is next

**Expected results for all scale factors.** We currently ship expected CSVs for SF1 only. For CI at larger scale factors, we need to either generate expected results from a reference implementation or use relative checks (row counts, schema shape, aggregate ranges).

**CI integration.** TPC-H at SF1 runs in under 30 seconds of query time. It will become a required check on every pull request targeting main. TPC-DS and SSB will run in a scheduled nightly pipeline.

**Larger scale factors for performance tracking.** Once correctness is stable, we will run SF10 and SF100 benchmarks to establish performance baselines and catch plan regressions.

**Write queries when DELETE/MERGE land.** TPC-C's new order transaction, TPC-E's trade order. These are the OLTP write patterns that will exercise SQE's Merge-on-Read path. The query files are already written and annotated. They will automatically un-skip when the feature flag is removed.

**Comparison runs.** The same benchmark suite will run against Trino with the identical data, giving us a direct comparison on both correctness and performance. That comparison is the final validation before SQE is considered a production replacement.

---

`sqe-bench` is part of the SQE repository. The benchmark suite, query files, expected results, and generation scripts are all committed alongside the engine code. If you are evaluating SQE, `cargo run -p sqe-bench -- generate tpch --scale 1 --output ./data` followed by the load and test commands will give you a direct picture of where we stand.
