# Iceberg Caching Strategy

## Problem

SQE re-reads everything from Polaris and S3 on every query. Trino caches at 7 layers. On warm queries against the same tables, Trino is 2-3x faster because it skips redundant I/O. At SF0.01 (TPC-H): SQE total 14.2s vs Trino 7.1s.

## Analysis: Trino's 7 Caching Layers

| Layer | What | Default | Correctness risk |
|---|---|---|---|
| 1a. Iceberg Table cache | Loaded Table objects | 30s TTL, soft refs | Low (TTL-bounded stale reads) |
| 1b. REST ETag cache | HTTP conditional loadTable | 5min TTL, 100 entries | None (server validates) |
| 1c. Manifest content cache | Raw manifest bytes | OFF by default | None (files immutable) |
| 2a. Connector table cache | BaseTable per instance | 1000 entries, no TTL | Medium (no expiry) |
| 3. Coordinator memory cache | Entire small files | 2% heap, 1h TTL | None (files immutable) |
| 4. Worker disk cache | All file pages on SSD | OFF, 7d TTL (Alluxio) | None (files immutable) |
| JVM JIT | Compiled hot paths | After ~10k invocations | N/A |

Key insight: **Iceberg metadata and data files are immutable by spec.** A manifest file at `s3://bucket/table/metadata/snap-123-m0.avro` never changes content. Caching by S3 path is inherently safe. Only the "which metadata file is current" pointer changes between snapshots.

## SQE's Current Caching

| Cache | What | Size | TTL | Safe? |
|---|---|---|---|---|
| FooterCache | Parquet metadata (schema, row groups, stats) | 256MB moka LRU | None (immutable files) | Yes |
| ResultCache | Full query results | 256MB, 5MB/entry | 300s | Yes (write-invalidation for SQE writes, 5min stale window for external writes) |
| OPA cache | Policy decisions | moka | Configurable | Yes |
| JWKS cache | JWT validation keys | moka | Configurable | Yes |

Missing: table metadata cache, manifest cache, coordinator file cache, HTTP ETag.

## What to Build

### Cache 1: Table Metadata Cache (highest priority)

**What:** Cache the `Table` object returned by `SessionCatalog::load_table()`. This avoids the Polaris REST round-trip (~30-200ms) for repeated queries to the same table.

**Key:** `(warehouse, namespace, table_name)`
**Value:** `iceberg::table::Table` (contains metadata, schema, snapshots, partition spec)
**TTL:** `metadata_cache_ttl_secs` config (default 30s, already in `CatalogConfig`)
**Max size:** 1000 entries (matches Trino)
**Eviction:** TTL expire-after-write + LRU when full
**Invalidation:** On SQE's own DDL/DML (DROP, ALTER, INSERT, DELETE, UPDATE, MERGE, CTAS)
**Implementation:** moka async cache in `SessionCatalog`

**Correctness:** 30-second stale window for external writes. Acceptable for analytics. The `metadata_cache_ttl_secs` config exists but is not wired -- this connects it.

**Expected savings:** 30-200ms per query per table.

### Cache 2: Manifest File Cache (second priority)

**What:** Cache parsed manifest content (manifest entries with data file paths, column stats, partition values) by S3 path.

**Key:** S3 URI of manifest file (e.g., `s3://warehouse/db/table/metadata/snap-123-m0.avro`)
**Value:** Parsed manifest entries (`Vec<ManifestEntry>`)
**TTL:** None needed (files are immutable by Iceberg spec)
**Max size:** 256MB (configurable, same as FooterCache)
**Eviction:** Size-based LRU (moka weighted cache)
**Implementation:** moka cache in `IcebergScanExec`, alongside existing `FooterCache`

**Correctness:** Zero risk. Iceberg manifest files are append-only write-once. A given S3 path always has the same content. New snapshots create new manifest files with new paths.

**Expected savings:** 50-200ms per query for tables with >10 manifests.

### Cache 3: Coordinator File Cache (third priority)

**What:** Cache entire small files (metadata JSON, manifest lists, manifest files) in memory by S3 path.

**Key:** S3 URI
**Value:** Raw bytes (`Vec<u8>`)
**Max size:** 2% of configured memory limit (matches Trino default)
**Max file size:** 8MB (skip files larger than this)
**TTL:** 1h (matches Trino default)
**Implementation:** moka weighted cache, integrated into the S3 I/O path

**Correctness:** Zero risk for Iceberg files (immutable). The TTL is a memory hygiene measure, not a correctness requirement.

**Expected savings:** Eliminates S3 round-trips for metadata files on warm queries. Combined with Cache 2, this means the scan planning path is fully cached after the first query.

### Cache 5: HTTP ETag Support (low priority, high polish)

**What:** Send `If-None-Match` header with cached ETag when calling `loadTable()` on the Polaris REST catalog. If the table hasn't changed, Polaris returns 304 Not Modified.

**Key:** `(warehouse, namespace, table_name)`
**Value:** `(ETag string, cached Table object)`
**TTL:** 5min (matches Trino's REST table cache)
**Implementation:** Modify `SessionCatalog::load_table()` to store and send ETags

**Correctness:** Perfect (server validates). Requires Polaris to support ETag headers on the loadTable endpoint.

**Expected savings:** ~10-30ms per query when table hasn't changed (skip JSON parsing of unchanged response). Reduces Polaris server load.

## What NOT to Build

**Worker disk cache (Trino Layer 4):** Requires Alluxio or similar, significant engineering effort (~2 weeks). Deferred to post-v1.0.

**Query plan cache:** Trino doesn't have one either. DataFusion re-plans every query.

**JIT equivalent:** Rust AOT compilation means consistent performance. We trade JVM warmup advantage for cold-start advantage.

## Implementation Order

```
Phase 1: Table metadata cache          ~2 hours    30-200ms savings
Phase 2: Manifest file cache           ~3 hours    50-200ms savings
Phase 3: Coordinator file cache        ~4 hours    eliminates remaining S3 round-trips
Phase 5: HTTP ETag support             ~3 hours    reduces Polaris load
                                       ─────────
                                       ~12 hours   expect 2-3x warm query improvement
```

## Configuration

```toml
[catalog]
# Table metadata cache (avoids Polaris REST round-trip)
metadata_cache_ttl_secs = 30     # 0 = disabled. Default 30s.

# Manifest file cache (immutable files, safe to cache indefinitely)
manifest_cache_max_mb = 256      # 0 = disabled. Default 256MB.

# Coordinator file cache (small immutable files in memory)
file_cache_max_mb = 0            # 0 = disabled (default). Set to e.g. 512 for production.
file_cache_ttl_secs = 3600       # 1 hour default, matches Trino.
file_cache_max_file_mb = 8       # Skip files larger than this.
```

## Metrics

Each cache exposes Prometheus metrics:

```
sqe_cache_hits_total{cache="table_metadata"}
sqe_cache_misses_total{cache="table_metadata"}
sqe_cache_evictions_total{cache="table_metadata"}
sqe_cache_size_bytes{cache="table_metadata"}

sqe_cache_hits_total{cache="manifest"}
sqe_cache_misses_total{cache="manifest"}
...

sqe_cache_hits_total{cache="file"}
...
```

## Validation

After implementing each cache:
1. Run `BENCH_SCALE=0.01 ./scripts/benchmark-test.sh --compare-trino tpch`
2. Compare warm-query times vs Trino
3. Verify row counts still match 22/22
4. Run with external writer (Trino writes, SQE reads) to verify cache invalidation

## Expected Results

| Scenario | Before | After | Improvement |
|---|---|---|---|
| Cold TPC-H q01 | SQE 785ms, Trino 1846ms | SQE ~785ms (unchanged) | SQE still 2.4x faster cold |
| Warm TPC-H q06 | SQE 557ms, Trino 113ms | SQE ~200ms | From 0.2x to ~0.6x Trino |
| Warm TPC-H total | SQE 14.2s, Trino 7.1s | SQE ~8-9s | From 0.5x to ~0.8x Trino |
