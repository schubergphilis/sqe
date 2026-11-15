## Context

`MaintenanceHandler::handle` (`crates/sqe-coordinator/src/maintenance.rs`) authorizes a `ProcedureCall`, builds a catalog bridge from the session (`create_catalog_bridge`), and dispatches. Six of its procedures only use that bridge. Each one loads the table, applies one iceberg-rust action, commits it, and builds a result batch:

| Procedure | Handler | Extra inputs |
|---|---|---|
| `expire_snapshots` | `expire_snapshots` | `sqe_catalog::txn::drop_stale_branches`; post-commit `cleanup_expired_files` through `reload_committed` |
| `rewrite_manifests` | `rewrite_manifests` | `carry_rewrite_statistics` (`sqe_write::puffin_carry`, optional metrics) |
| `set_current_snapshot`, `rollback_to_snapshot` | `move_main_ref` | none |
| `register_table` | `register_table` | none |
| `table_health` | `table_health` | `collect_health_inputs`, `last_compaction_snapshot_ms`, `table_health::analyze_table_health` with `MaintenanceCompactionConfig` |

What the handler adds around them, and what stays in the coordinator:

- authorization (`authorize_or_deny`, `procedure_tier`) and the audit entries;
- the user-scoped bridge;
- cache invalidation: `invalidate_table_cache` for expiry, and `sqe_catalog::invalidate_rest_catalog_cache_all` after a ref move or a registration;
- log lines that name the user.

`sqe-cli` depends on `sqe-write`, `sqe-catalog` and `sqe-sql`, but not on `sqe-coordinator`. `sqe-write` already depends on `sqe-catalog`, `sqe-compaction` and `sqe-metrics-core`, so every input in the table above is reachable from it. Embedded mode already commits to Iceberg through `sqe-write`, using CTAS and INSERT on a `SessionCatalog::local` from `writable_target`.

## Goals / Non-Goals

**Goals:**
- Embedded `CALL system.*` runs the six catalog-only procedures with the cluster's result rows.
- One implementation of each procedure, shared by both callers.
- The cluster's behaviour is unchanged: result rows, tiers, audit and cache invalidation.

**Non-Goals:**
- `rewrite_data_files` in embedded mode. It needs the spill runtime, the worker fleet and the race seam; that is a later change, if anyone asks for it.
- Orphan removal or purge in embedded mode: they delete outside a commit, so there is nothing for the catalog to refuse.
- An embedded authorization model.

## Decisions

### D1. Free functions in `sqe-write::procedures`, not a trait object

Each core is a non-generic `pub async fn`. It takes `&Arc<dyn Catalog>`, a `&TableIdent` and its arguments, and returns `sqe_core::Result<Vec<RecordBatch>>`:

```rust
pub async fn expire_snapshots(catalog: &Arc<dyn Catalog>, ident: &TableIdent,
    older_than_ms: Option<i64>, retain_last: Option<usize>,
    table_cache: Option<&TableMetadataCache>,
) -> Result<Vec<RecordBatch>>;
pub async fn rewrite_manifests(catalog, ident, metrics: Option<&MetricsRegistry>) -> Result<Vec<RecordBatch>>;
pub async fn move_main_ref(catalog, ident, snapshot_id: i64, require_ancestor: bool, op_name: &'static str) -> Result<Vec<RecordBatch>>;
pub async fn register_table(catalog, ident, metadata_location: &str) -> Result<Vec<RecordBatch>>;
pub async fn table_health(catalog, ident, cfg: &MaintenanceCompactionConfig) -> Result<Vec<RecordBatch>>;
```

Cache invalidation moves with the code that does it, since both caches live in `sqe-catalog`, which `sqe-write` already depends on:

- **`expire_snapshots`** invalidates *between* the commit and the file cleanup, so that no cached handle lists snapshots whose files are being deleted. It therefore takes `table_cache: Option<&TableMetadataCache>`, and the body of `invalidate_table_cache` moves inside it. The coordinator passes its cache; embedded passes `None`.
- **`move_main_ref` and `register_table`** keep their `sqe_catalog::invalidate_rest_catalog_cache_all()` call inside the core. This also covers an embedded REST catalog, if one reads through that cache.

The helpers move with the cores and keep their names:
- `summary_batch`, `classify_commit_error`, `load_table`, `reload_committed` and `current_manifest_count`;
- `to_table_ident` and `nested_namespace`, which embedded needs to turn a `TableRef` into a `TableIdent`;
- `collect_health_inputs`, `last_compaction_snapshot_ms`, `MAINTENANCE_SNAPSHOT_PROPERTY_PREFIX` and `carry_rewrite_statistics`.

`maintenance.rs` imports them back with `pub(crate) use sqe_write::procedures::{...}`, so `maintenance_scheduler.rs` and the rewrite paths keep calling `crate::maintenance::*` unchanged.

Two log lines change. `move_main_ref`'s info line loses the `user` field inside the core, so the coordinator handler logs the user after the call. `register_table`'s request log stays in the handler, together with `location_for_log`, which does not move.

*Alternative rejected:* a `ProcedureBackend` trait implemented by both callers. It adds a generic or `dyn` seam for six functions with one catalog argument, and it does not remove any code.

### D2. `table_health.rs` moves whole; `delete_triggered_files` goes to `sqe-compaction`

`table_health.rs` depends only on `MaintenanceCompactionConfig` (`sqe-core`) and on three maintenance helpers:
- `pack_file_groups_partition_aware` and `delete_heavy_files`, which are already in `sqe-compaction`;
- `delete_triggered_files`, which `maintenance.rs` defines and both rewrite paths use.

`delete_triggered_files` only combines `delete_heavy_files` and `delete_ratio_heavy_files`, so it moves into `sqe-compaction` beside them, and `maintenance.rs` re-exports it.

`table_health.rs` moves to `sqe-write/src/table_health.rs` with its unit tests. Only three lines (129, 136, 470) change in the move: their `crate::maintenance::` paths become `sqe_compaction::`. `sqe-coordinator/src/lib.rs` replaces `pub mod table_health;` with `pub use sqe_write::table_health;`, so `crate::table_health::*` and `sqe_coordinator::table_health::TableHealth` (used by `tests/it/maintenance_log_test.rs`) keep resolving.

### D3. Embedded dispatch

`execute` gets one arm: `StatementKind::Procedure(call) => return self.embedded_handle_procedure(&call).await`. The handler and its tests live in a new `crates/sqe-cli/src/embedded/procedures.rs`. `embedded.rs` has 370 lines of headroom, which is not enough for both. The sqlite test helpers in `embedded.rs`'s `mod tests` are widened to `pub(super)` rather than copied. A one-part name resolves to namespace `default`, as on the cluster; that is also embedded mode's default schema. The handler's `match` on `ProcedureCall` is flat and exhaustive, with no wildcard arm, so a new procedure fails to compile until embedded mode decides what it does.

- **Catalog resolution.** The catalog is `table_ref.catalog`, or `default_catalog()` when that is empty. If it is not in `iceberg_catalogs`, the call fails: `CALL system.<name>: catalog '<c>' is not a writable Iceberg catalog`. The `SessionCatalog::local` is built the same way `writable_target` builds it; that construction is factored into a helper that takes the catalog name, and `writable_target` calls it. Nested namespaces come from `TableRef::namespace_parts`, as on the coordinator.
- **The six run.** After a committing procedure (every one except `table_health`), `refresh_namespace` runs on the table's namespace, as after CTAS. A registered table then appears in the listing, and a rolled-back or expired table is re-read from the catalog. `table_health` reads `self.handler_config().maintenance.compaction`, which is the default in embedded mode.
- **The rest are refused.** These answer `CALL system.<name> is not available in embedded mode; run it on an SQE cluster`:
  - `rewrite_data_files`, `remove_orphan_files`, `purge_orphan_locations` and `drop_table`;
  - `refresh_catalog_cache`, `reproject_column_tags` and `suggest_bloom_filter_columns`.

  `drop_table` names `DROP TABLE` as the embedded alternative.
- **Results.** The core's batches become a `QueryResult` the same way the CTAS path builds one from its batches.

### D4. Authorization in embedded mode

There is no policy engine and no role set: embedded mode registers only the `user_sha256` UDF from `sqe-policy`, and `write_session` builds a session with no roles. The CLI user is the principal, using the catalog credentials they attached. This is already how embedded `DROP TABLE`, `ALTER TABLE ... EXECUTE` and CTAS behave.

Every procedure that runs is a catalog commit or a metadata read, so the catalog decides. `expire_snapshots` deletes files only after its commit succeeds, through the reloaded table's credentials, which matches the cluster's TableWrite reasoning. The cluster's Admin tier (`set_current_snapshot`, `rollback_to_snapshot`, `register_table`) protects a shared coordinator from its other users; a single-user process has no counterpart to that. The procedures page states this explicitly, so nobody reads the embedded behaviour as a bypass of the cluster tiers.

### D5. Budgets

This is a move, so the line totals shift and do not grow:
- `sqe-write` rises by the lines that move in (`table_health.rs` plus the cores and helpers, about 1,000);
- `sqe-compaction` rises by about 25;
- `sqe-coordinator` falls by the sum of the two.

The MR states the before and after numbers from `cargo xtask budgets --check`.

`embedded.rs` gains only the dispatch arm and the factored catalog helper; the new code is in `embedded/procedures.rs`.

No crate edge is added.

## Risks / Trade-offs

- **Silent drift between the callers.** This is why the cores are shared rather than copied. The coordinator's existing maintenance tests run through the moved code unchanged.
- **Embedded freshness after a ref move.** A cached embedded table provider can keep serving the pre-rollback snapshot. `refresh_namespace` replaces the schema's providers, and a test asserts that `SELECT` after `rollback_to_snapshot` returns the earlier rows.
- **The pure move hides an edit.** The move lands as its own commit before any embedded code. It is checked with `git diff --color-moved=zebra --color-moved-ws=allow-indentation-change`. The only code that is not moved is the `table_cache` parameter, the three `table_health.rs` paths and the coordinator's call sites.
