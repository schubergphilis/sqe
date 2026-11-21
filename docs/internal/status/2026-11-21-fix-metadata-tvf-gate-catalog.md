**STATUS 2026-11-21: metadata TVF access check keys the named catalog (`fix/metadata-tvf-gate-catalog`).**

- Found while working #919. Under `catalog_scoped`, the rewriter's metadata-TVF gate (#750) keyed `table_files('ns', 't', 'cat')` on the session's catalog, because the scan carries the function name, which resolves in the session's catalog. The provider reads `cat`. A caller denied `cat.ns.t` passed the gate from a session in a catalog with no deny on `ns.t`.
- `IcebergMetadataProvider` records the catalog name the call gave (`None` with two arguments, or when the function is bound to one catalog and ignores the argument).
- New `metadata_tvf_target` returns `(catalog, namespace, table)`. The rewriter's `MetadataTarget` takes that shape and falls back to the scope's catalog only when the call named none. `metadata_tvf_target_ident` (query cache indexing) is unchanged.
- With `catalog_scoped` off, nothing changes: the key is the bare namespace either way.

Gate:
- `cargo test -p sqe-policy --lib -- metadata_tvf`: exit 0, 2 passed. New: `plan_rewriter::tests::catalog_scoped_metadata_tvf_is_checked_in_the_catalog_it_names`. With the old keying (session catalog), its first assertion lets the call through.
- `cargo clippy -p sqe-policy -p sqe-catalog -p sqe-coordinator -p sqe-cli --all-targets -- -D warnings`: exit 0.
- `make test-integration FILTER=snapshot`: exit 0, 12 passed (2 access-control tests skip there). These read `table_snapshots` through the policy-wired test handler: flight ingest, incremental scan, maintenance procedures.

Left open:
- No live Ranger test with two catalogs holding the same table. The unit test drives the rewriter with a catalog-scoped store.
- The query result cache indexes metadata TVF results by `ns.table` without the catalog (`metadata_tvf_targets` in `query_handler.rs`). A write to `cat_a.ns.t` also invalidates `cat_b.ns.t`'s cached TVF rows. That over-invalidates; it never serves stale rows.

**NEXT:** #920 part 1 (`sqe.column-tags-by-id` readers).
