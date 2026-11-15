**STATUS 2026-11-15: embedded mode runs six `CALL system.*` procedures (`feat/embedded-call-procedures-875`).**

OpenSpec change `embedded-maintenance-procedures`. Stage 1 moved the
catalog-only procedure cores (`expire_snapshots`, `rewrite_manifests`,
`move_main_ref`, `register_table`, `table_health`) and their helpers from
`sqe-coordinator/src/maintenance.rs` to `sqe-write::procedures`;
`table_health.rs` moved to `sqe-write` and `delete_triggered_files` to
`sqe-compaction`. The coordinator handlers keep authorization, audit and the
user log lines and call the cores. Stage 2 routes `StatementKind::Procedure`
in `sqe-cli` embedded mode to the cores (`embedded/procedures.rs`); the other
seven procedures are refused by name. No crate edge added; budget ceilings
moved, not raised.

Gates: `cargo test -p sqe-cli --bin sqe-cli embedded_call_` 9 passed;
`embedded` 80 passed; sqe-write/sqe-coordinator/sqe-compaction unit and
sqlite tests 149 + 16 passed; clippy and `cargo xtask budgets --check` clean.
Integration regression: see the MR.

**NEXT:** `rewrite_data_files` in embedded mode only if someone asks (spill
runtime, no worker fleet).
