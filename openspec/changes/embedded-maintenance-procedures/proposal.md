## Why

`CALL system.*` is the last statement in the embedded support table (`docs/site/book/src/features/embedded.md`) that reads "no" for a reason other than design. The procedures live in `sqe-coordinator`'s `MaintenanceHandler`, which `sqe-cli` does not depend on. Its catalog bridge builds a Polaris `SessionCatalog` from an OIDC session, and the handler carries the worker fleet, the ticket signer and the coordinator metrics. So an embedded user who writes to an Iceberg table cannot expire its snapshots, compact its manifests or roll it back without a cluster.

Depending on `sqe-coordinator` from `sqe-cli` would add the largest crate to the CLI's build graph for six small functions. Moving the whole handler out is the size of the coordinator split (#636). The procedures an embedded user needs are already catalog-only: each loads a table, builds one iceberg-rust transaction and commits it.

## What Changes

- **Catalog-only procedure cores move to `sqe-write`.** `expire_snapshots`, `rewrite_manifests`, `set_current_snapshot`, `rollback_to_snapshot`, `table_health` and `register_table` get a free function that takes `&Arc<dyn Catalog>` and a `TableIdent` and returns the result batches. `MaintenanceHandler` keeps its authorization, catalog bridge, cache invalidation and audit, and calls the cores. The cluster's behaviour and result rows do not change.
- **Embedded mode runs those six.** `sqe-cli --embedded` routes `StatementKind::Procedure` for them to the cores, against the writable Iceberg catalog the table name resolves to (`writable_target`), and refreshes the namespace listing after a commit.
- **The rest stay cluster-only and say so.** `rewrite_data_files` (worker dispatch, spill runtime), `remove_orphan_files`, `purge_orphan_locations` (storage deletes outside a commit), `drop_table` (embedded already has `DROP TABLE`; purge deletes outside a commit), `refresh_catalog_cache`, `reproject_column_tags` (policy) and `suggest_bloom_filter_columns` (query history) are refused by name with "is not available in embedded mode".
- **Authorization.** Embedded mode has no policy engine and no roles: the CLI user runs with their own catalog credentials, as for `DROP TABLE` and `ALTER TABLE` there. The catalog's commit check is the gate for every procedure it runs. The cluster's admin tier for `set_current_snapshot`, `rollback_to_snapshot` and `register_table` exists because one coordinator serves many users; it has no embedded counterpart.

## Capabilities

### New Capabilities
- `embedded-maintenance-procedures`: which `CALL system.*` procedures embedded mode runs, and what it answers for the rest.

### Modified Capabilities
None. `iceberg-procedures` keeps every requirement: the coordinator calls the moved cores and its result rows, authorization tiers and cache invalidation are unchanged.

## Impact

- `sqe-write`: new `procedures` module (cores, `summary_batch`, `classify_commit_error`, `load_table`, `reload_committed`, `current_manifest_count`, `collect_health_inputs`, `last_compaction_snapshot_ms`); `table_health.rs` moves from `sqe-coordinator`.
- `sqe-compaction`: `delete_triggered_files` moves next to `delete_heavy_files` and `delete_ratio_heavy_files`, which it combines.
- `sqe-coordinator/src/maintenance.rs`: the six handlers shrink to bridge + core call. No new crate edge; `sqe-coordinator` already depends on `sqe-write`. Old paths stay as re-exports.
- `.config/budgets.json`: the `sqe-write` and `sqe-compaction` ceilings rise by the lines moved in and `sqe-coordinator`'s falls by the same amount.
- `sqe-cli/src/embedded.rs`: one dispatch arm and a `handle_procedure` function.
- Docs: the embedded support table row, and `sql-reference` procedures page.
