**STATUS 2026-11-14: Hive tables get their own data prefixes; file TVFs refuse Hive data (`feat/hive-data-prefixes-886`, #886, openspec hive-data-prefixes 0.1-6.2).**

`[hive] allowed_data_prefixes` and `[[hive.roots]] data_prefixes` gate where
Hive external tables may live, through `HiveDataPolicy`
(`sqe-core/src/config/hive_data.rs`), in place of the file TVF allow-list. A
process-wide `ProtectedLocations` registry holds the configured Hive prefixes
(registered in `QueryHandler::new`) and every Hive table location loaded
(store `put`/`get`/`check_location` and `EngineHiveDataStoreFactory::store_for`,
the only gate for Glue roots). `TvfPolicy::check_object_store` refuses paths
that overlap an entry after the trusted-local and inline-credential exits, so
admins and `allow_all_object_stores` are refused too. Startup logs an error per
overlapping (TVF prefix, Hive prefix) pair and a deprecation warning while the
fallback is in use.

Decisions (design D4/D5): anyone who may ATTACH may attach a root, each location
still checked per reader; admin roles and allow-all bypass the Hive gate but not
the refusal; with no Hive prefix set anywhere, the Hive gate falls back to the
TVF list for one release. Embedded `sqe-cli` stays on the fallback.

Gate: `make test-integration FILTER=hive_` -> 7 passed (three new `_886` tests,
plus the existing Hive object-store tests). Unit: 12 `hive_data` tests.

Left open: dynamic entries are per process (a restart re-protects a table from
its first load). 5.2 asserts the SELECT still answers; masks are on the
unchanged policy path and not re-asserted there.

**NEXT:** remove the D4 fallback (Hive gate on the TVF list) in the release after this one; then the Iceberg side of the registry (`tvf-prefix-table-overlap`).
