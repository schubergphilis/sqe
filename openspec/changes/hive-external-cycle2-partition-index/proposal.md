## Why

Cycle 1 of Hive external tables (MR !884) lists the table's S3 prefix on every query. Only a leading run of equality-pinned partition columns prunes that listing; a range predicate, an `IN` list, or a filter on a non-leading partition column falls back to a full recursive LIST followed by in-memory filtering. On a lake with tens of thousands of partitions the listing dominates the query. Cycle 2 removes this with a persisted partition index stored in the manifest slot cycle 1 reserved, maintained by the two statements Hive users already know (`MSCK REPAIR TABLE`, `ALTER TABLE ... ADD/DROP PARTITION`), and closes the remaining Athena compatibility gaps: `SHOW PARTITIONS` and partition projection.

## What Changes

- `ALTER TABLE ... ADD PARTITION` and `ALTER TABLE ... DROP PARTITION` on Hive external tables: append or remove one index entry, no re-scan. `ADD IF NOT EXISTS` / `DROP IF EXISTS` honored; `DROP PARTITION <location>` deletes only the index entry, never data.
- `MSCK REPAIR TABLE`: full re-list of the table location, rewrite of the whole index. The recovery path when files arrived out of band.
- `SHOW PARTITIONS <table>`: reads the index when present, lists the prefix when absent.
- Persisted partition index in `sqe.partition_index` (slot already reserved in the manifest). Each entry carries partition key values plus location, because a Hive partition may live outside the table prefix. Reads plan from the index instead of listing when an index is present; an absent index keeps cycle-1 discovery behavior.
- Index staleness policy decided and implemented: a stale index that omits a real partition is a missing partition, not a wrong answer, and `MSCK REPAIR` is the documented remedy. Manifest ETag-based invalidation replaces any timer-based cache for the index.
- Caps: new `max_partition_index_entries` config value alongside the existing manifest caps, so a million-partition table cannot produce an unreadable object.
- Athena partition projection: `projection.*` TBLPROPERTIES are implemented instead of refused. Projected partitions are computed from the projection config at plan time and never require storage listing. **Note:** cycle 1 documented these as rejected but the rejection was never implemented (docs/code drift); they currently flow into `parameters` silently ignored, which is a full-scan hazard. This change closes both halves: implement the feature, and until then nothing silently ignores them.
- `ANALYZE TABLE` persists row counts into `sqe.statistics` (slot already reserved), so planning gets accurate cardinality without paying a footer read per query.
- Statement classifier gains arms for `Msck`, `ShowPartitions`, and `AlterTableOperation::AddPartitions`/`DropPartitions` so rejections name `hive_external` precisely instead of "Statement type not supported".

## Capabilities

### New Capabilities
- `hive-partition-index`: The persisted partition index, its DDL maintenance statements (`ADD/DROP PARTITION`, `MSCK REPAIR TABLE`), `SHOW PARTITIONS`, index-driven read planning, staleness semantics, and caps.
- `hive-partition-projection`: Athena partition projection properties, computed partition sets at plan time, validation of projection types and ranges.
- `hive-statistics`: `ANALYZE TABLE` persisting counts into `sqe.statistics` and their use in planning.

### Modified Capabilities

None. No existing spec under `openspec/specs/` covers Hive external tables; cycle 1 predates spec capture for this area.

## Impact

- `crates/sqe-sql/src/classifier.rs` — new statement kinds and arms (`Msck`, `ShowPartitions`, `AddPartitions`/`DropPartitions`)
- `crates/sqe-coordinator/src/query_handler.rs` — replace the `reject_if_hive_table` hooks at ADD PARTITION (:1312) and ANALYZE (:1876) with real handlers; add MSCK and SHOW PARTITIONS dispatch
- `crates/sqe-coordinator/src/hive_ddl_handler.rs` — new handler methods for the four statements
- `crates/sqe-catalog/src/hive/manifest.rs` — index entry serde, validation, caps, ETag handling
- `crates/sqe-catalog/src/hive/provider.rs` — index-driven file list replacing ListingTable prefix listing when an index is present; statistics from the index
- New `crates/sqe-catalog/src/hive/partition.rs` — index model, projection evaluation, repair logic
- `crates/sqe-core/src/config.rs` — `HiveConfig` gains `max_partition_index_entries`
- Docs: `docs/site/book/src/reference/hive-external-tables.md` limitations section rewritten (cycle-2 items move out)
- Performance-sensitive: this is the main fast-query unlock for partitioned Hive tables; benchmark before/after per the design's benchmark plan (partitioned CSV + Parquet fixture, cold/warm, with/without partition-pinning predicate)
