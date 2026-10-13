**STATUS 2026-10-13: view body scans are keyed by the session's catalog name (`fix/view-scan-session-catalog-835`, #835, security).**

Under `[policy.ranger] catalog-scoped = true` with a config catalog named differently from its warehouse (`[catalogs.iceberg]` over `test_warehouse`), `qualify_view_scans` named a view body's unqualified tables `test_warehouse.ns.t`, while a direct read keys `iceberg.ns`. The open question in #835 is answered fail-OPEN: for an Iceberg table Ranger requires no access grant (only Hive roots do), so an unmatched key resolves to an empty policy and the view returned raw values. Confirmed live: on main the e2e reads the masked `ssn` in clear through the view; with the fix it is NULL. `qualify_view_scans` now takes the session's name, and also renames a scan whose body names the warehouse explicitly (a view created by another engine; SQE itself refuses that name in `CREATE VIEW`). Sibling catalogs are left alone.

Tags: `properties_for` misses the exact `|iceberg|` suffix (cache keys hold the warehouse) and falls back to the suffix-only match; one warehouse match is used, two deny. That is how direct reads already behave, so no change there.

Migration: a Ranger policy written for `<warehouse>.<namespace>` to cover views stops matching. CHANGELOG and the fine-grained access control page say so. Tests: `view_scans_take_the_sessions_catalog_name` (unit), `view_policy_session_catalog_e2e` (`make test-integration FILTER=a_view_is_governed`, fails on main, passes here). Not run: `make test-access-control` (its rig does not alias a catalog, so it cannot exercise this).

Also found, filed as #839: `views_over_views_and_same_schema_replace` and `test_view_with_aggregation` fail on main with a fresh stack, independent of this change.

**NEXT:** #839 (nested view stale after replace); native audit log rotation (#838); #803 when it recurs.
