# SQE Performance & Observability Roadmap

**Date:** 2026-04-16 (updated)
**Current:** SQE wins 5 of 7 suites at SF1 vs Trino 465. TPC-H 1.8x, TPC-C 3.4x, TPC-BB 2.3x, ClickBench 2.6x. TPC-DS ~50/99 queries won (1.0x avg, q72 outlier). 214/222 pass.
**Remaining gap:** q72 (15.5s vs 1.4s) -- DataFusion lacks full CBO join enumeration (DF#3843).
**Goal:** Close TPC-DS gap, scale to SF10-SF100.

---

## Tier 1 — Do Now (1-2 weeks each, high ROI)

### 1.1 ETag-Based Metadata Validation with Polaris
**What:** Send `If-None-Match` header on `load_table` REST calls. Polaris returns 304 when table metadata hasn't changed.
**Impact:** 30-50% fewer REST round-trips. Near-zero latency for unchanged tables.
**Effort:** Easy (2-3 days). Store ETag alongside cached entries in `TableMetadataCache`.
**Source:** Polaris 1.3 supports ETag headers on metadata responses.

### 1.2 ZSTD Compression for DoExchange Shuffle
**What:** Enable IPC body compression on Arrow Flight DoExchange between coordinator and workers.
**Impact:** 3-10x network I/O reduction in distributed mode.
**Effort:** Easy (1-2 days). Set `IpcWriteOptions` with ZSTD codec. Add `shuffle_compression = "zstd"` config.

### 1.3 LZ4 Compression for Client Flight SQL Responses
**What:** Enable IPC compression on DoGet responses to Flight SQL clients.
**Impact:** 20-40% bandwidth reduction, especially for WAN clients.
**Effort:** Easy (1 day). Same IPC options, LZ4 for low-latency decompression.

### 1.4 ZSTD Default for Parquet Writes
**What:** Switch CTAS/INSERT Parquet output from Snappy to ZSTD.
**Impact:** 2-3x smaller files on S3. Faster subsequent reads.
**Effort:** Easy (2 days). Make configurable per-table or session.

### 1.5 SQL-Queryable Query History (`system.runtime.queries`)
**What:** In-memory ring buffer (last 10K queries) exposed as a DataFusion TableProvider. Columns: query_id, state, user, SQL, parse_time_ms, plan_time_ms, exec_time_ms, rows_returned, bytes_scanned, spill_bytes, memory_peak.
**Impact:** Must-have for production ops. Enables `SELECT * FROM system.runtime.queries WHERE state = 'RUNNING'`.
**Effort:** Medium (1 week). Query lifecycle states: Queued, Planning, Executing, Blocked, Completed, Failed.

### 1.6 Extract All DataFusion Metrics in EXPLAIN ANALYZE
**What:** Surface `spill_count`, `spilled_bytes`, `output_bytes`, `output_batches`, `selectivity` from DataFusion's `ExecutionPlan::metrics()`. Currently only `elapsed_compute` and `output_rows` are extracted.
**Impact:** Must-have. The data is already there, SQE just doesn't read it.
**Effort:** Easy (2-3 days). Update `walk_analyze()` and `walk_full()` in explain.rs.

### 1.7 OTel Semantic Convention Compliance
**What:** Add `db.system.name = "sqe"`, `db.operation.name`, `db.namespace`, `db.collection.name` to all query spans. Rename spans to `{operation} {schema}.{table}`. Wire `trace_sample_rate` (currently a TODO).
**Impact:** Must-have for multi-service tracing correlation.
**Effort:** Easy-Medium (3-5 days).

---

## Tier 2 — Do When DF 53 Unblocks (highest impact, zero custom code)

### 2.1 Upgrade to DataFusion 53
**What:** DF 53 (released April 2, 2026) includes:
- LIMIT-aware Parquet row group pruning (2-10x for LIMIT queries)
- Hash join dynamic filters (5-25x for star-schema joins)
- 40-50x faster query planning (~100us vs ~4ms)
- Nested field pushdown (struct columns)
- Null-aware anti join
- 42 function performance improvements
**Impact:** 10-50% across the board. The single most impactful change.
**Blocker:** RisingWave iceberg-rust fork pinned to DF 52. Monitor for rebase.
**Effort:** Medium (1-2 weeks once fork rebases). Bump versions, fix API changes, re-benchmark.

### 2.2 Wire Dynamic Filtering to IcebergScanExec
**What:** Implement `DynamicFilterSource` trait on `IcebergScanExec` so hash join build-side min/max values propagate to Iceberg scan planning. Currently dynamic filters only reach DataFusion's built-in ParquetExec.
**Impact:** 3-25x for join-heavy queries with selective build sides.
**Effort:** 2-3 weeks. Need to translate dynamic filters into Iceberg manifest pruning predicates.

### 2.3 Wire Predicate Transfer to Join Execution
**What:** The `PredicateTransfer` building blocks exist in `sqe-planner/src/predicate_transfer.rs`. Wire them into the actual join execution path so build-side distinct values are pushed to probe-side scans.
**Impact:** 13-16x vs semi-join approach (CIDR 2024 paper).
**Effort:** 1-2 weeks.

### 2.4 Iceberg Puffin Bloom Filter Reading
**What:** Read Puffin sidecar files containing per-column bloom filters. Use them to skip data files whose bloom filters exclude the query predicate.
**Impact:** 80-90% file skip rate on point-lookup queries.
**Effort:** Medium (2-3 weeks). Requires Puffin reader support in iceberg-rust.

---

## Tier 3 — Strategic Investments (weeks-months, transformative)

### 3.1 Cost-Based Join Reordering
**What:** Collect table/column statistics (row counts, NDV, histogram buckets) and use them to enumerate join orderings.
**Impact:** 2-5x for multi-join queries. Critical for TPC-DS-style workloads.
**Effort:** Hard (4-6 weeks). Requires statistics collection, caching, and custom optimizer rules.

### 3.2 Local Data File Block Cache
**What:** Cache hot Parquet file ranges on local NVMe (Alluxio-like). LRU eviction.
**Impact:** 20-70% faster for repeated scans (Trino + Alluxio benchmarks).
**Effort:** Medium (2-3 weeks). Wrap `object_store` with a caching layer.

### 3.3 Runtime Broadcast Join Decision
**What:** When one join side fits in memory (detected at runtime), broadcast it instead of shuffling.
**Impact:** 2-10x for star-schema queries in distributed mode.
**Effort:** Medium (2-3 weeks).

### 3.4 Continuous Profiling (Pyroscope)
**What:** Integrate Pyroscope + pprof-rs for CPU profiling, jemalloc backend for heap profiling. Tag with query_id for correlation.
**Impact:** Must-have for production performance debugging.
**Effort:** Medium (1 week). Behind `--features profiling` flag.

### 3.5 Iceberg Table Health System Tables
**What:** `system.iceberg.snapshots`, `system.iceberg.manifests`, `system.iceberg.files` exposing metadata for compaction monitoring.
**Impact:** Must-have for operators managing table lifecycle.
**Effort:** Medium (1-2 weeks).

---

## Observability Gaps (Priority Order)

| # | Gap | Priority | Effort |
|---|---|---|---|
| 1 | SQL-queryable query history with lifecycle states | Must-have | 1 week |
| 2 | Per-query resource attribution (bytes scanned, spill, memory peak) | Must-have | 1 week |
| 3 | EXPLAIN ANALYZE: extract all DF metrics (spill, selectivity, bytes) | Must-have | 2-3 days |
| 4 | OTel semantic conventions (`db.*` attributes, span naming) | Must-have | 3-5 days |
| 5 | Wire trace sampling rate (currently TODO in otel.rs) | Must-have | 1 day |
| 6 | Continuous CPU profiling (Pyroscope) | Must-have | 1 week |
| 7 | Parameterized query hash (top N patterns analysis) | Should-have | 2 days |
| 8 | Iceberg table health tables | Should-have | 1-2 weeks |
| 9 | OTel Baggage propagation (user identity across services) | Should-have | 2 days |
| 10 | Memory profiling via jemalloc | Should-have | 3 days |
| 11 | Per-operator I/O vs compute time breakdown | Nice-to-have | 1 week |
| 12 | Web UI for live query monitoring | Nice-to-have | 2-3 weeks |
| 13 | Tokio runtime metrics (task count, poll duration) | Nice-to-have | 2 days |

---

## What SQE Already Does Well (Confirmed by Research)

These techniques are already implemented and competitive with or ahead of other engines:

| Technique | SQE Status | Notes |
|---|---|---|
| Late materialization | Implemented | `late_materialize.rs` with cost-aware predicate ordering |
| Page-level Parquet filtering | Enabled | `with_page_index(true)` in all scan paths |
| S3 I/O pipeline (prefetch + coalescing) | Implemented | `s3_io.rs` with range coalescing + file prefetch |
| Predicate transfer building blocks | Implemented | `predicate_transfer.rs` (needs wiring to join execution) |
| 5-layer metadata caching | Implemented | RestCatalog, table metadata, manifest, SessionContext, OAuth |
| Spill-to-disk (sort, sort-merge join) | Implemented | FairSpillPool + LZ4 spill compression |
| Adaptive sort stripping | Implemented | Memory-pressure-aware, 3 modes |
| Streaming writes (CTAS/INSERT) | Implemented | O(batch_size) memory, RollingFileWriter |
| StringView support | Via DataFusion 52 | Utf8View is default for VARCHAR |
| Distributed shuffle via DoExchange | Implemented | Worker-side shuffle ingestion |

---

## Recommended Execution Order

**Sprint 1 (this week):** Tier 1.1-1.4 (ETag, compression, Parquet ZSTD) — 4 quick wins
**Sprint 2 (next week):** Tier 1.5-1.7 (query history, EXPLAIN metrics, OTel) — observability foundation
**Sprint 3 (when DF 53 fork lands):** Tier 2.1-2.3 (upgrade, dynamic filters, predicate transfer) — biggest performance leap
**Sprint 4+:** Tier 3 items based on user feedback and production profiling data

---

## Sources

- [DataFusion 53.0.0 Release](https://datafusion.apache.org/blog/2026/04/02/datafusion-53.0.0/)
- [DataFusion Dynamic Filters: 25x Faster Queries](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/)
- [DataFusion LIMIT-Aware Pruning](https://datafusion.apache.org/blog/2026/03/20/limit-pruning/)
- [Predicate Transfer CIDR 2024 Paper](https://www.cidrdb.org/cidr2024/papers/p22-yang.pdf)
- [Trino Dynamic Filtering](https://trino.io/docs/current/admin/dynamic-filtering.html)
- [Trino + Alluxio Cache](https://trino.io/blog/2024/03/08/cache-refresh.html)
- [Polaris 1.3.0 Release](https://polaris.apache.org/releases/1.3.0/)
- [DuckDB Internals (CMU Lecture)](https://15721.courses.cs.cmu.edu/spring2024/notes/20-duckdb.pdf)
- [ClickHouse Data Skipping Indexes](https://clickhouse.com/docs/optimize/skipping-indexes)
- [OTel Database Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/db/database-spans/)
- [Pyroscope Rust SDK](https://grafana.com/docs/pyroscope/latest/configure-client/language-sdks/rust/)
- [Arrow IPC Compression](https://arrow.apache.org/rust/src/arrow_ipc/compression.rs.html)
- [ZSTD vs Snappy for Parquet](https://www.e6data.com/blog/fast-writes-apache-iceberg-snappy-vs-zstd)
