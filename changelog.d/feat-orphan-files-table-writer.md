### Changed

- A table writer can run a deleting `system.remove_orphan_files` without an SQE admin role

    The deleting call moved from the admin tier to the table-write tier, next to `rewrite_data_files` and `expire_snapshots`. Before it deletes anything it commits the table property `sqe.maintenance.remove-orphan-files.last-run-ms` as the caller, so the catalog decides: a reader gets the catalog's 403 and nothing is deleted. Every deleting run, an admin's included, now adds one metadata version (no snapshot). The dry run stays read-only. `purge_orphan_locations` stays admin-only.

### Fixed

- `SHOW CREATE TABLE` printed no `TBLPROPERTIES` for a table outside the config default catalog

    The properties were read from the config default's catalog, so `SHOW CREATE TABLE sales_wh.ns.t` (or `ns.t` under `USE sales_wh.ns`) found no table, or a same-named table in the default catalog, and printed its properties. `SHOW SCHEMAS` / `SHOW TABLES` with no `FROM` under a session default other than the config default read the config default the same way, as did time travel and `SHOW STATS` / `ANALYZE` on a 2-part name. All now resolve the session's catalog by name. A session default that config and Polaris cannot build (a Hive root, an ATTACHed catalog) keeps the old answer.

- `expire_snapshots` now evicts the table from the coordinator's table cache after its commit, so the next query does not plan against expired snapshots
