**STATUS 2026-11-18: all crates inherit the workspace lints; pre-push budgets green again (`fix/pre-commit-findings`).**

22 manifests (20 `sqe-*` crates, `workspace-hack`, `xtask`) gained `[lints] workspace = true`, so `await_holding_lock = "deny"` now covers the whole workspace and the cargo `missing_lints_inheritance` warning is gone. Workspace clippy with `-D warnings` passes with the lint active; no code change was needed.

`.config/budgets.json`: `cargo xtask budgets --update` dropped the per-file entry for `sqe-trino-functions/src/trino_functions.rs` (now under the default). Hand-raised, because this week's merges grew them: `query_handler.rs` 10300 -> 10350 (10303 lines), `sqe-catalog` 64400 -> 64600 (64501), `sqe-coordinator` 69200 -> 69600 (69474), `sqe-trino-functions` 14250 -> 21600 (21490, the Trino parity function branches). `pre-commit run --all-files --hook-stage pre-push` exits 0.

Left as is: `workspace-hack` unused-dependency warnings (hakari by design), unused `workspace.package` keys `authors`/`homepage`, and the deprecated `BehaviorVersion::v2026_01_12` in the vendored REST catalog.

**NEXT:** `sqe-trino-functions` is close to its new ceiling; split `trino_functions.rs` registration by family before the next function batch.
