### Fixed

- Every workspace crate now inherits the workspace lint table, so `await_holding_lock = "deny"` applies everywhere

    22 crates (and `xtask`) had no `[lints] workspace = true`, so the deny on holding a lock across an `.await` only reached the crates that opted in, and cargo warned `missing_lints_inheritance` on every build. Workspace clippy stays clean with the lint now active. The size budgets in `.config/budgets.json` are raised to cover the Trino parity functions merged this week (`sqe-trino-functions` 14250 -> 21600 lines), the matching catalog and coordinator growth, and `query_handler.rs` (10300 -> 10350), so the pre-push hooks pass again without `SKIP=budgets`.
