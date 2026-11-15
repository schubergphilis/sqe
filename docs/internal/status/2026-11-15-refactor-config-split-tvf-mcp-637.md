**STATUS 2026-11-15: config split, first slice: TVF path policy and MCP config leave `config/mod.rs` (`refactor/config-split-tvf-mcp-637`).**

#637 item 1. A pure move: `TvfPolicy`, `TvfCaller` and the object-URL helpers go to `config/tvf_policy.rs` (1,326 lines with their tests), and `McpConfig` with its auth types to `config/mcp.rs` (351). `mod.rs` re-exports every public item, so no caller outside `config/` changed. The only edits in the moved code are imports, seven `pub(super)` widenings for `mod.rs` and `hive_data.rs`, and rustfmt wrapping in the new files. Tests that go through `SqeConfig::validate`/`load` or `ENV_LOCK` stay in `mod.rs`. `mod.rs` falls from 12,201 to 10,579 lines; its budget ceiling is lowered from 12,500 to 11,000.

Gate: `cargo test -p sqe-core --lib config` 323 passed before and after, with identical test-name lists; `cargo check-server`, clippy on `sqe-core` and `cargo xtask budgets --check` are clean. A token-count comparison of the removed and added code shows nothing dropped.

**NEXT:** the next slices of item 1: storage/Hive, auth, catalog, coordinator/worker, one MR each, with each section's tests moving with it. Item 3 (legacy auth fields and `[catalog]`) waits until auth and catalog live in their own files.
