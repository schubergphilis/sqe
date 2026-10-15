**STATUS 2026-10-15: Iceberg column tags come from the planned scan, not the metadata cache (`fix/tag-source-load-on-miss-842`, #842, follow-up to #840 / !1427).**

!1427 made an unknown tag state refuse the statement instead of returning 0 rows. `CacheTagSource` reports unknown on any metadata-cache miss, so after that merge `catalog.metadata_cache_ttl_secs = 0` refused every governed query. An eviction (1000-entry cap, keyed per token and table) or a commit racing the planning load (`insert_if_generation` false) refused single queries.

The issue title says "load on miss", but the fix does no load. `PolicyPlanRewriter::with_scan_tags` takes a hook that reads tags off each `TableScan`'s provider. `tag_source_impl::scan_column_tags` downcasts to `SqeTableProvider` and parses `sqe.column-tags` from `table().metadata().properties()`. Planning built that provider from `load_table` with the caller's token, and a stale cache entry is revalidated first. So the tags match the data being read, there is no extra catalog call, and the sync `TagSource` / `block_on` problem never comes up. The hook returns a three-state `ScanTagState`:
- `NotRecognised` falls back to the `TagSource`. That covers Hive (`CatalogRoutedTagSource`), views and metadata TVFs.
- `Unknown` (malformed JSON) refuses without consulting the cache, which could hold an older readable value.
- `Known(map)`.

If two scans share a name, an unknown state on either wins; otherwise their tag maps are unioned.

The audit writer still uses `CacheTagSource`. On a miss it strips literals and does not error, so it was left unchanged.

Tests:
- sqe-policy: 340 unit tests (3 new on `ScanTagState::merge`) and 91 integration tests. The 3 new integration tests cover:
  - known scan tags masking while the tag source is unknown;
  - unreadable scan tags refusing without asking the tag source;
  - an unrecognised provider falling back.
- sqe-coordinator: 4 `scan_column_tags` tests on a real `SqeTableProvider` over a MemoryCatalog.
- Ranger e2e `unknown_tag_state_denies` was reworked. The cold handler now reads the same masked rows as the warm one. The unknown state is now produced with a malformed `sqe.column-tags` on a separate table, and both handlers must refuse it.
- Full `scripts/access-control-test.sh`: 47 of 48 pass. The one failure, `mcp_reads_are_denied_for_the_ungranted_user_and_valid_for_the_admin`, already fails on main: the test reads a `Table` column that `list_tables` no longer returns (#843).

Known gap: a CDC incremental scan (`FOR INCREMENTAL BETWEEN SNAPSHOT ...`) plans through `IncrementalTableProvider`, which does not carry the table's properties. Such a scan still asks the cache and is still refused when the cache is disabled. The fix is to have the provider carry the properties `apply_incremental_spec` loaded.

No integration test sets `metadata_cache_ttl_secs = 0`. The cold-handler e2e case exercises the same miss.

**NEXT:** #755 cartesian join/aggregate admission; #841 SQL `DROP TABLE ... PURGE`.
