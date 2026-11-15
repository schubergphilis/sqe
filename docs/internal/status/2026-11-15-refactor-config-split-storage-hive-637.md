**STATUS 2026-11-15: config split, second slice: storage and Hive config leave `config/mod.rs` (`refactor/config-split-storage-hive-637`).**

#637 item 1. A pure move, following !1803:
- `StorageConfig`, its `Default` and redacting `Debug` impls and its `default_*` fns go to `config/storage.rs` (168 lines), with `test_storage_config_s3_allow_http_defaults_false`.
- `HiveConfig`, `HiveRootConfig`, `HiveRootKind` and the `default_hive_*` fns go to `config/hive.rs` (157 lines).

`mod.rs` re-exports the four public types, so no file outside `config/` changed and `hive_data.rs` is untouched. No visibility changed. `mod.rs` falls from 10,579 to 10,269 lines, and its budget ceiling is lowered from 11,000 to 10,750.

Gate:
- `cargo test -p sqe-core --lib config`: 323 passed before and after, with identical test-name lists.
- `cargo check -p sqe-core --all-targets`, `cargo check-server`, clippy on `sqe-core`, `cargo xtask budgets --check` and the rustfmt ratchet: exit 0.
- Token check of the removed against the added code: nothing dropped.

**NEXT:** the auth slice (`AuthConfig`, `AuthProviderConfig`, `ExternalAuthConfig`), then catalog. #637 item 3 (the legacy flat auth fields) builds on the auth slice.
