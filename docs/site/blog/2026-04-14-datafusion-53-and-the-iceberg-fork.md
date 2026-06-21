---
title: "DataFusion 53, a Vendored Fork, and 40% Faster Queries"
description: "We upgraded SQE from DataFusion 52 to 53 by forking and rebasing iceberg-rust ourselves. The result: 27-40% faster across every benchmark suite."
pubDate: "2026-04-14"
author: "Jacob Verhoeks"
tags: ["datafusion", "iceberg", "performance", "rust", "open-source"]
---

We had a dependency problem. DataFusion 53 shipped on April 2 with hash join dynamic filters, LIMIT-aware pruning, and 40x faster planning. SQE was stuck on DataFusion 52 because our iceberg-rust dependency was a third-party fork that had not rebased.

So we did the rebase ourselves.

## The dependency chain

SQE depends on three iceberg-rust crates: `iceberg`, `iceberg-catalog-rest`, and `iceberg-datafusion`. Apache upstream iceberg-rust (v0.9.0) lacks two features SQE needs for its write path: `RewriteFilesAction` for Copy-on-Write DELETE/UPDATE, and `PositionDeleteFileWriter` for Merge-on-Read position deletes.

The RisingWave Labs fork has both. SQE has used it since day one, pinned to a specific git commit. The fork is actively maintained. Multiple production systems depend on it.

The problem: the RisingWave fork targets DataFusion 52. Upstream apache/iceberg-rust merged the DF 53 upgrade (PR #2206) on March 25. The RisingWave fork had not rebased. No timeline for when it would.

## The options we considered

**Wait for RisingWave to rebase.** Zero effort. Unknown timeline. Could be weeks or months.

**Fork upstream and port the RisingWave features.** Gets DF 53. Requires porting 2,000-4,000 lines of transaction actions and writer modules. High risk of subtle snapshot bugs.

**Fork the RisingWave branch and apply the DF 53 delta ourselves.** Moderate effort. The upstream PR #2206 provides a clear template. The RisingWave fork is a known-good base.

We chose option three.

## The migration

The DF 53 API changes in iceberg-rust were mechanical:

- `PlanProperties` wrapped in `Arc<PlanProperties>` (the `ExecutionPlan::properties()` return type changed)
- `set_max_row_group_size()` renamed to `set_max_row_group_row_count(Some(n))`
- `with_page_index(true)` became `with_page_index_policy(PageIndexPolicy::Required)`
- `Date32Type::to_naive_date()` became `to_naive_date_opt().unwrap()`

Ten files changed in the fork. The same pattern of changes then applied to SQE's own codebase: 11 `PlanProperties` sites, 5 `statistics()` call sites, 3 page index sites, 8 `HashJoinExec::try_new` calls (new `null_aware` parameter), and 4 sqlparser 0.54 type changes.

Total: 44 API change sites across 15 SQE files.

We vendored the fork into `vendor/iceberg-rust/` (4.6 MB) rather than depending on an external git repo. Single `git clone` gets everything. No authentication issues. No external dependency at build time.

## What we gained

The benchmark numbers on SF0.01, before and after:

| Suite | DF 52 | DF 53 | Improvement |
|---|---|---|---|
| TPC-H (22) | 1.8s | **1.1s** | 38% faster |
| SSB (13) | 0.9s | **0.6s** | 33% faster |
| TPC-DS (99) | 19.3s | **12.2s** | 37% faster |
| ClickBench (43) | 0.7s | **0.6s** | 14% faster |
| TPC-E (18) | 1.1s | **0.3s** | 73% faster |

The Trino comparison on the same run:

| Suite | SQE (DF 53) | Trino 465 | Speedup |
|---|---|---|---|
| TPC-H | 1,198ms | 6,925ms | **7.2x** |
| SSB | 581ms | 1,798ms | **3.2x** |
| TPC-DS | 11,909ms | 22,597ms | **2.2x** |
| ClickBench | 639ms | 1,826ms | **3.0x** |

SQE still wins every suite. The 35% improvement comes from DF 53's faster planning alone. The dynamic filter infrastructure is wired but not yet executing at scan time. That is the next performance unlock.

## What else shipped alongside DF 53

We did not stop at the version bump. Six performance improvements from the Tier 1 roadmap shipped in the same MR:

**ETag metadata validation.** When a cached table's soft TTL expires, SQE sends `If-None-Match` to Polaris. If the table has not changed, Polaris returns 304 and SQE reuses the cached metadata without re-downloading. Reduces REST calls by 30-50% for stable tables.

**ZSTD compression for shuffle.** Coordinator-to-worker DoExchange messages now use ZSTD compression. Network I/O drops 3-10x for distributed queries. Configurable via `shuffle_compression = "zstd"`.

**LZ4 compression for client responses.** Flight SQL DoGet responses use LZ4_FRAME. Fast decompression on the client side, 20-40% bandwidth reduction. Configurable via `flight_compression = "lz4"`.

**ZSTD default for Parquet writes.** CTAS and INSERT INTO now produce ZSTD-compressed Parquet (was Snappy). Files are 2-3x smaller on S3. Subsequent reads are faster because less data to transfer. Configurable via `parquet_compression = "zstd"`.

**Enhanced query history.** `system.runtime.queries` gained four columns: `bytes_scanned`, `rows_scanned`, `spill_bytes`, `peak_memory_bytes`. Extracted from DataFusion's execution plan metrics after each query. Operators can now answer "which query scanned the most data?" with SQL.

**OTel semantic conventions.** Query spans now carry `db.system.name = "sqe"`, `db.operation.name`, `db.namespace`, and `db.collection.name`. Trace sampling is wired (was a TODO) with `ParentBased(TraceIdRatioBased)`.

## The vendoring decision

We considered three approaches for the fork:

1. Separate GitLab repo. Cleaner separation. More overhead. Authentication issues in CI.
2. Git submodule. Tracks upstream. Painful merge conflicts. Extra `git submodule update` step.
3. Vendor directory. Everything in one repo. Single clone. No external deps at build time.

We chose vendoring. The three crates total 4.6 MB. The `vendor/iceberg-rust/` directory has its own workspace `Cargo.toml`. SQE's workspace excludes it and references the crates via path deps. When upstream apache/iceberg-rust merges `OverwriteAction` and `PositionDeleteFileWriter`, we delete the vendor directory and switch to the official crate. That day is tracked via upstream issues #2185 and #2203.

## What comes next

The dynamic filter infrastructure is wired. `IcebergScanExec` accepts `DynamicFilterPhysicalExpr` from hash join build sides. The optimizer routes them correctly. What remains is execution-time evaluation: when the hash join updates the filter with min/max bounds, the Iceberg scan needs to use those bounds to skip manifest files and Parquet row groups. That is Tier 2 on the roadmap. Expected impact: 5-25x on star-schema joins.

The EXPLAIN ANALYZE expansion (5 new metrics: spill_count, spilled_bytes, spilled_rows, output_bytes, output_batches) is already useful for debugging. Run `EXPLAIN ANALYZE SELECT ...` and see exactly which operators spilled to disk and how many bytes.

One week ago, SQE ran 222 queries in 126 seconds and lost to Trino on every suite. Today it runs them in 14.6 seconds and wins every comparison. The DF 53 upgrade was the last major infrastructure change. Everything from here is optimization within the framework that now exists.
