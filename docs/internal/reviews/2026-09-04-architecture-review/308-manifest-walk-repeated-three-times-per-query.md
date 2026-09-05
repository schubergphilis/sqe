# 308: Every scan walks and clones all manifests up to three times per query with no per-snapshot statistics cache

- Scope: sqe-catalog (table_provider.rs, iceberg_scan.rs, pruning_stats.rs)
- Severity: medium
- Category: performance
- Effort: M (days)
- Status: open

## Problem

`SqeTableProvider::scan_impl` computes full table statistics from manifests on every `scan()` call:

- `crates/sqe-catalog/src/table_provider.rs:321-344`: `compute_table_statistics(&self.table, snapshot_id, projected_schema, manifest_concurrency).await`, unconditionally, once per table per planning.
- `iceberg_scan.rs:2439-2511`: loads the manifest list, fetches every manifest with `buffer_unordered(64)` (default `DEFAULT_MANIFEST_CONCURRENCY`, `iceberg_scan.rs:63`), then `flat_map` clones every live `DataFile` (with its `lower_bounds`/`upper_bounds`/`null_value_counts` maps) into a `Vec<DataFile>` before aggregating.

The same snapshot is then walked again at execution:

- `iceberg_scan.rs:1278-1286`: `collect_data_files_via_plan` (iceberg-rust `plan_files`, one walk).
- `iceberg_scan.rs:1444-1478`: `collect_data_files_for_pruning` re-reads every manifest and clones matching `DataFile`s again for dynamic-filter pruning.
- `iceberg_scan.rs:632-683`: `data_file_info_with_pruning_stats`, used by the distributed planner, walks `plan_files` and `collect_data_files` a further time.

The iceberg-rust `ObjectCache` on the `Table` keeps the raw manifest bytes warm, so repeat walks avoid S3 GETs, but each walk still deserializes and clones every entry (a `DataFile` carries several `HashMap<i32, ..>` per file). Nothing caches the aggregated `Statistics` per `(table uuid, snapshot id)`, even though the result is immutable for a snapshot. A failure in the walk is downgraded to `debug!` (`table_provider.rs:337-343`) so a slow or failing manifest read is invisible.

## Why it matters

On a 50K-file table (SF100 fact tables, or any un-compacted MoR table) planning allocates and drops tens of MB of `DataFile` clones per query, on the coordinator, per concurrent query. It is CPU and allocator pressure in the planning critical path, exactly where the memory notes on SF10 and SF100 place the wall. It also makes `manifest_concurrency = 64` a fan-out multiplier: 64 concurrent manifest decodes per table per query per concurrent session.

## Proposed change

1. Add a `SnapshotStatsCache` (moka, keyed `"{table_uuid}:{snapshot_id}"`, weighted by file count) next to `TableMetadataCache`; `compute_table_statistics` reads through it. Puffin NDV (`table_provider.rs:349-400`) can live in the same entry.
2. Build the `Vec<DataFile>` once per `IcebergScanExec::execute` and share it between planning, pruning and split assignment; drop `collect_data_files_for_pruning`'s second walk by retaining `DataFile` from the first walk keyed by path.
3. Log the statistics failure at `warn!` with the elapsed time, and expose `sqe_catalog_manifest_walk_seconds` so operators see the cost.

## Acceptance criteria

- A unit test with a fake `Table` (the `test_table()` fixture pattern in `rest_catalog.rs:2560-2588`, extended with a manifest list) asserts that two `scan()` calls on the same snapshot decode manifests once.
- TPC-H SF10 planning time (`benchmarks/results/` compare, `explain analyze` planning_time metric) does not regress and manifest reads per query drop, recorded in the MR.

## Related

- 307. Memory notes "Dynamic-filter snapshot cache", "Lakehouse perf roadmap".
- `docs/internal/plans/performance-roadmap.md`.
