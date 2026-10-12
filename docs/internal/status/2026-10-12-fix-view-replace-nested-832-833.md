**STATUS 2026-10-12: views: replace with an unchanged schema, and view bodies that name the session catalog (`fix/view-replace-nested-832-833`, #832, #833).**

#832: `CREATE OR REPLACE VIEW` with a schema identical to one the view already has failed with "Cannot set last added schema: no schema has been added". The server's view builder reuses the id of an identical schema or version, and then records "last added" only for one added in the same commit, so the commit's `-1` references had nothing to point at. Replacing a view with the definition it already has (a dbt view model re-run) would fail the same way at `set-current-view-version`. `view_replace_updates` now resolves both against the existing metadata, using the server's own equality:
- An identical schema is referenced by its id, and `add-schema` is not sent.
- An identical version becomes `set-current-view-version <id>` alone.
- `-1` is still used for a schema or version that is actually new (#770).

#833: a view body planned in a context that registered only the warehouse name (`test_warehouse`) could not resolve the session's catalog name (`iceberg.ns.t`). This hit a view over a view and a view over a table alike. Two-part names worked. `SqeCatalogProvider::with_catalog_name` records the configured catalog name, and view planning registers it next to the warehouse, for nested views as well. The sibling-warehouse fallback skips it.

A view created after a session had listed its namespace was missing from `information_schema.tables` and `columns` until the listing expired. Table commits invalidate every token's listing of the namespace; `create_view` and `drop_view` now do too.

Tests:
- `sqe-catalog` unit tests: an unchanged schema skips `add-schema`; a changed schema is added as `-1`; the same definition reuses the version.
- Integration test `view_replace_nested_e2e`.
- Verified live on the distributed rig.

Found on the way: unqualified names in a view body are named by the warehouse in the plan, while a direct query names them by the session catalog. Under `catalog_scoped` Ranger policies, the two keys differ (#835).

**NEXT:** #835 (fail-open or closed under `catalog_scoped`), #816 (token refresh-ahead), #803.
