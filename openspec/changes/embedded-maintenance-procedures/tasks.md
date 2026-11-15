## 1. Move (stage 1, its own commit; checked with `--color-moved`)

- [ ] 1.1 `delete_triggered_files` to `sqe-compaction` beside `delete_heavy_files`; `maintenance.rs` re-exports it
- [ ] 1.2 `table_health.rs` to `sqe-write/src/table_health.rs` with its tests (three `crate::maintenance::` paths become `sqe_compaction::`); `sqe-coordinator/src/lib.rs` `pub use sqe_write::table_health;`
- [ ] 1.3 New `sqe-write/src/procedures.rs`: `summary_batch`, `classify_commit_error`, `to_table_ident`, `nested_namespace`, `load_table`, `reload_committed`, `current_manifest_count`, `collect_health_inputs`, `last_compaction_snapshot_ms`, `MAINTENANCE_SNAPSHOT_PROPERTY_PREFIX`, `carry_rewrite_statistics`; `maintenance.rs` re-imports them

## 2. Cores (stage 1)

- [ ] 2.1 `procedures::{expire_snapshots, rewrite_manifests, move_main_ref, register_table, table_health}` taking `&Arc<dyn Catalog>` + `&TableIdent` (design D1); `expire_snapshots` takes `Option<&TableMetadataCache>`
- [ ] 2.2 `MaintenanceHandler`'s six handlers become bridge + core call + user log
- [ ] 2.3 Coordinator maintenance unit tests and `cargo test -p sqe-coordinator --features test-sqlite` maintenance tests pass unchanged

## 3. Embedded (stage 2)

- [ ] 3.1 Factor the `SessionCatalog::local` construction out of `writable_target` into a by-name helper
- [ ] 3.2 `StatementKind::Procedure` arm and `embedded_handle_procedure` in `embedded/procedures.rs`, with a flat exhaustive `ProcedureCall` match (D3)
- [ ] 3.3 `refresh_namespace` after every committing procedure
- [ ] 3.4 Tests (sqlite catalog): expire with `older_than` + `retain_last`, rollback then `SELECT`, rollback to a non-ancestor, `set_current_snapshot` with no roles, `register_table` then `SELECT`, `table_health` columns, `rewrite_manifests` counts, non-Iceberg catalog refused, `rewrite_data_files` and `drop_table` refused

## 4. Docs and housekeeping

- [ ] 4.1 `docs/site/book/src/features/embedded.md` table row: `CALL system.*` "partly", naming the six; procedures page states the embedded authorization (D4)
- [ ] 4.2 `.config/budgets.json` ceilings moved, not raised (D5); `cargo xtask budgets --check`
- [ ] 4.3 `changelog.d/` and `docs/internal/status/` fragments
