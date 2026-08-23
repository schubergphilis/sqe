## Context

Cycle 1 (MR !884) shipped the Hive external read path: Athena/Glue DDL, sidecar Glue-shaped manifests under an attached warehouse root, coordinator-local scans through DataFusion `ListingTable`, fail-closed `[storage.tvf]` authorization, and `<root>.<database>` policy identity. The manifest reserves four `sqe.*` slots (`partition_index`, `column_tags`, `statistics`, `secret`); only `secret` is populated today.

Every query currently lists the table's S3 prefix. Only a leading run of equality-pinned partition columns prunes the listing (DataFusion `evaluate_partition_prefix`); range predicates, `IN` lists, and non-leading filters fall back to a full recursive LIST plus in-memory filtering. The design doc bounds this cost (finding 7) and names the persisted index as the fix. Cycle 1 also rejects `MSCK REPAIR TABLE`, `SHOW PARTITIONS`, and `ALTER TABLE ADD/DROP PARTITION` with generic not-implemented wording, because none reaches the statement classifier.

One drift item: the cycle-1 design says `projection.*` properties are "rejected outright", but no code rejects them. They flow into `parameters` silently ignored, so a user who asked for a computed partition set gets a full table scan instead. Cycle 2 closes this either way: implement projection, and until its validation runs nothing silently ignores it.

Code seams established by cycle 1 that this change builds on:

- `SqeExtensions.partition_index: Option<serde_json::Value>` (`crates/sqe-catalog/src/hive/manifest.rs:94`) — the reserved slot
- `HiveTableProvider` wrapper intercepting `scan`/`scan_with_args` (`crates/sqe-catalog/src/hive/provider.rs:543-577`) — the seam for replacing the listing-based scan
- `reject_if_hive_table` hooks at `ALTER TABLE ADD PARTITION` (`crates/sqe-coordinator/src/query_handler.rs:1312`) and `ANALYZE` (:1876), both labeled "cycle 2" — the dispatch points this change replaces
- sqlparser 0.62 already parses all three statement forms: `Statement::Msck`, `AlterTableOperation::{AddPartitions, DropPartitions}`, and `SHOW PARTITIONS t` (as `ShowVariable`)
- Manifest caps live in `ManifestLimits` / `[hive]` config (`max_bytes` 1 MiB, `max_columns` 4096, `max_partition_keys` 32)

## Goals / Non-Goals

**Goals:**

- A read plans from the persisted index instead of listing when an index exists; an absent index keeps cycle-1 behavior byte-for-byte
- `ALTER TABLE ADD/DROP PARTITION` maintains the index without touching data or re-listing
- `MSCK REPAIR TABLE` rebuilds the index from storage; the documented recovery path for out-of-band writes
- `SHOW PARTITIONS` reads the index, lists only when absent
- Athena partition projection computes the partition set at plan time; projected tables never list storage for planning
- `ANALYZE TABLE` persists real row counts into `sqe.statistics`
- Every new statement passes the same policy and allowlist gates as cycle-1 statements

**Non-Goals:**

- Automatic background index maintenance; staleness is handled by `MSCK REPAIR`, not by a watcher
- Worker-side execution of Hive scans (cycle 5)
- Tag-based masks/filters (cycle 3)
- Writes to Hive tables beyond index maintenance (cycle 4)
- Nested partition key types; partition keys stay scalar as in cycle 1
- HMS/Glue metastore backends

## Decisions

### D1: Index shape — entries carry values AND location

```json
"partition_index": {
  "format_version": 1,
  "entries": [
    { "values": { "dt": "2026-01-02", "region": "eu" },
      "location": "s3://raw/orders/dt=2026-01-02/region=eu/" }
  ]
}
```

Each entry carries the full location because a Hive partition may live outside the table prefix (`ALTER TABLE ADD PARTITION (dt='2026-01-02') LOCATION 's3://elsewhere/'`), which directory-name discovery cannot express at all. Values are stored as strings and typed on load using the declared partition-key types, exactly as path segments are typed in cycle 1. `__HIVE_DEFAULT_PARTITION__` normalizes to NULL in `values` and renders back to the sentinel in `location`.

Alternative considered: storing only relative paths under the table prefix. Rejected: it makes out-of-prefix partitions unrepresentable, which is one of the two reasons the index exists.

### D2: Staleness policy — a stale index omits, never invents

An index that omits a real partition yields a missing partition in results, not a wrong one. An index entry whose files were deleted out-of-band yields an empty-or-error scan for that partition, same as pointing any reader at a deleted prefix. We do not validate entries against storage at plan time; validating would reintroduce the listing cost the index exists to remove. `MSCK REPAIR TABLE` is the remedy, stated in docs and in the `ANALYZE`/repair error text.

Rationale: the alternative (treat stale as wrong-answer and force re-list) dissolves the feature. Hive itself made this exact trade with metastore partitions; Athena users know the model.

### D3: Index-driven reads replace the ListingTable, not the object store

When an index is present, `HiveTableProvider::scan` builds the file list from index entries after partition-prune filtering, then constructs the scan from an explicit `FileScanConfig` (per-entry file groups keyed by partition values) using the same resolved `FileFormat` from `serde_map.rs`. No `ListingTable`, no prefix LIST at plan time. Per-file sizes come from the index (populated at repair/add time from HEAD or LIST metadata) and feed the existing size-only statistics path, replacing the one-shot statistics listing at `provider.rs:867-881`.

Alternatives considered:

- *Caching object store wrapping ListingTable*: rejected. #363 is the cautionary tale; an infinite-TTL listing cache served a stale size and produced a corrupt-footer error. Store-level caching also cannot express out-of-prefix locations and hides the invalidation question inside infrastructure instead of answering it.
- *Re-enabling DataFusion's list-files cache with a TTL*: rejected for the same reason plus finding 10; TTL invalidation guesses, ETag invalidation knows.

Index invalidation in the process-wide `HiveMetadataCache`: the cached manifest entry is keyed by ETag where the store provides one; a changed ETag reloads the manifest and therefore the index. Where the store has no ETag, the existing cache TTL applies. No new timer-based machinery.

### D4: ADD/DROP PARTITION is a read-modify-write of the whole manifest

One object per table means one write primitive. `ADD PARTITION` appends an entry (HEAD/LIST the given location for file count and bytes, then rewrite the manifest); `DROP PARTITION` removes matching entries and rewrites. Duplicate `ADD` without `IF NOT EXISTS` is an error naming the partition; with it, a no-op. `DROP` of an absent partition without `IF EXISTS` is an error; with it, a no-op. `DROP PARTITION ... PURGE` is rejected: cycle 4 owns data deletion, and pretending otherwise invites silent data loss.

Concurrency: last-writer-wins on the manifest object, same documented semantics as cycle-1 `CREATE` where conditional put is unsupported. Blast radius is one table. Two racing `ADD PARTITION`s on different partitions lose one entry; the remedy is `MSCK REPAIR`, and the docs say so plainly.

### D5: MSCK REPAIR lists once, skips junk, writes once

Repair lists the table location recursively, derives partition values from `k=v/` path segments exactly as cycle-1 discovery does, HEADs nothing (file counts/sizes come free from the LIST result), and rewrites the index wholesale. Hive and Athena skip junk rather than aborting: a file at the table root, a directory that is not `k=v`, an undeclared or missing key, or a value that does not parse to the declared type is omitted, and neighboring valid partitions still land in the index. The earlier fail-closed cut made a single `_SUCCESS` file or a `logs/` dir brick the whole repair; skip-and-continue is the compatibility users expect. Repair still respects `max_partition_index_entries`; exceeding it is an error telling the operator to raise the cap, not a truncated index.

Projection-enabled tables are a successful no-op. Projection owns the partition set at plan time, so a listing would not change what a query scans, and ETL scripts that always run `MSCK REPAIR TABLE` after a load must not fail. ADD/DROP PARTITION on a projected table remain errors.

### D6: Partition projection evaluates at plan time, validates at create AND load

Projection properties (`projection.enabled`, `projection.<col>.type` of `enum`|`integer`|`date`|`injected`, ranges, formats, intervals) are validated when the DDL is parsed and again on every manifest load, mirroring the untrusted-input rule: a planted manifest with a malformed projection config must be refused, not half-interpreted. At plan time the evaluator materializes the partition set (bounded by `max_partition_index_entries`) as virtual index entries with computed locations following the standard `k=v/` layout under the table location. Projected tables never list storage for planning; a filter on a projected column prunes the computed set before any file group is built. `injected` columns take their value from session/query context if provided, else NULL, matching Athena's semantics closely enough to document precisely.

Unknown `projection.*` keys are an error naming the key, consistent with the SerDe-properties rule: a silently ignored projection setting changes query results.

### D7: ANALYZE pays once, planning stops guessing

`ANALYZE TABLE <hive table>` computes row counts per partition entry: Parquet from footer metadata (cheap, reuses the footer cache), CSV/JSON by a full decode pass that ANALYZE explicitly pays for once. Results persist into `sqe.statistics` (`row_count` total and per entry) and flow into the size-only statistics path so `JoinStrategyRule` and `star_schema_reorder` plan from real cardinality. `ANALYZE` requires the same policy gate as reads. Without a prior ANALYZE, estimates stay size-derived as in cycle 1.

### D8: Classifier arms before handlers

New `StatementKind` variants (`MsckRepairTable`, `ShowPartitions`, `AlterTableAddPartitions`, `AlterTableDropPartitions`, `AnalyzeTable` where absent) with classifier arms, then thin coordinator dispatch delegating to new `HiveDdlHandler` methods behind the existing `resolve_table_kind` branch. `SHOW PARTITIONS` needs a pre-scan match because sqlparser delivers it as `ShowVariable`. Each replaced `reject_if_hive_table` hook gets a test asserting the old rejection message is gone and the new behavior holds, per the cycle-1 convention that each dispatch-table row is a test case.

## Risks / Trade-offs

- [Stale index returns incomplete results after out-of-band writes] -> Documented staleness model (D2), `MSCK REPAIR` as the named remedy, and the read-planning path keeps working with no index at all, so dropping the index (delete the manifest slot) is always an escape hatch.
- [A million-partition table makes the manifest unreadable] -> New `max_partition_index_entries` cap enforced at ADD, REPAIR, and load; error names the cap and the config key.
- [Lost updates from racing manifest writers] -> Last-writer-wins documented (D4); blast radius one table; repair recovers. Conditional put used where the store supports it.
- [Projection config evaluated from an untrusted manifest] -> Validation on every load (D6), fail-closed, same rule as SerDe classes and locations.
- [Index-driven scan diverges from ListingTable semantics (NULL sentinel, pruning edge cases)] -> The NULL-partition filter rewriting from cycle 1 (`normalize_default_partition_filters`, `nullify_default_partition_columns`) is reused, not reimplemented; parity tests run identical queries against indexed and non-indexed copies of the same fixture.
- [Mixed engine versions share manifests] -> `partition_index.format_version` inside the already-versioned manifest; unknown versions reject on load per the untrusted-input rule. The outer `manifest_version` stays 1 because the slot was reserved from the first write.
- [`ANALYZE` on large CSV/JSON is a full decode] -> Explicitly paid once and documented; Parquet path is footer-only.

## Migration Plan

Feature work lands additively behind the default-on `hive-external` feature, same as cycle 1. Existing manifests with `partition_index: null` behave exactly as today, so deployment needs no migration. Rollback is reverting the MR: manifests written with a populated index are inert JSON to an older engine only if that engine ignores unknown content inside the known version, which the cycle-1 loader already does (unknown fields inside a known version are ignored). No data is touched by any statement in this change.

## Open Questions

- Should `DROP PARTITION` on a projected table be an error (projected partitions are computed, not stored)? Default answer: yes, with a message saying projection owns the partition set. Confirm during implementation.
- Does `SHOW PARTITIONS` need a LIKE pattern clause for parity with Athena? Trivial to add at the handler if wanted; default is plain output.
