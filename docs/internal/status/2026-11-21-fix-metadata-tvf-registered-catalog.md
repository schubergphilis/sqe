**STATUS 2026-11-21: metadata TVF check keys the registered catalog name; live test for !1889 (`fix/metadata-tvf-registered-catalog`).**

Follow-up to !1889 (metadata TVF access check keys the named catalog).

- The coordinator's catalog resolver matches the third argument case-insensitively, so `table_files('ac', 'orders', 'SALES_WH')` reads catalog `sales_wh`, while !1889 keyed the check on the raw argument `SALES_WH.ac`.
- `MetadataCatalogResolver` now returns the catalog and the name it is registered under. `IcebergMetadataProvider` records that name, and the policy check keys on it. Both resolvers (coordinator, embedded CLI) return it.
- Not exploitable today: Ranger is the only store that keys by catalog (`catalog_scoped`), and it matches resource values case-insensitively (`ranger_store.rs`, `ranger_wildcards_and_case_folding_match_the_servicedef`). A control run with the raw-argument keying put back also passed the live test. The change keeps the key correct for a store that matches case-sensitively.

Gate:
- `cargo test -p sqe-catalog --lib iceberg_metadata_tvf`: exit 0, 29 passed. New: `provider_records_the_registered_catalog_name`.
- `cargo test -p sqe-coordinator --lib metadata_tvf_reads_the_catalog`: exit 0, 1 passed (now asserts the registered name for `probe` -> `Probe`).
- `cargo test -p sqe-policy --lib metadata_tvf`: exit 0, 2 passed.
- `scripts/access-control-test.sh catalog_scoped_metadata_tvf`: exit 0, 1 passed. New live test `catalog_scoped_metadata_tvf_checks_the_catalog_it_names`: Ranger, scoped handler, bob's session in `ops_wh`, deny-all row filter on `sales_wh.ac.orders`. Bob's `table_files('ac','orders','sales_wh')` and `'SALES_WH'` are refused as not found; alice reads the files. This is the live two-catalog test !1889 left open: with the session-catalog keying from before !1889, bob's key is `ops_wh.ac` and the call answers.

Left open:
- The query result cache still indexes metadata TVF results by `ns.table` without the catalog (over-invalidates, never stale; see the !1889 status).

**NEXT:** rebase !1888 (#919) onto main; then #918 write-time half or #920 OpenSpec.
