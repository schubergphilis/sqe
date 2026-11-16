**STATUS 2026-11-16: config split, third slice: auth config leaves `config/mod.rs` (`refactor/config-split-auth-637`).**

#637 item 1. A pure move, following the storage and Hive slice:
- `AuthProviderConfig` (with its impl and redacting `Debug`), `AuthConfig` (with its impl and redacting `Debug`), `ExternalAuthConfig` and their `default_*` fns go to `config/auth.rs` (473 lines).

`mod.rs` re-exports the three public types, so no file outside `config/` changed. Visibility:
- `default_roles_claim` and `default_admin_roles` are `pub(super)` in `auth.rs`, imported under `#[cfg(test)]` for the tests that stay in `mod.rs`.
- `default_true` and `default_refresh_buffer` stay in `mod.rs` as `pub(super)`.

`mod.rs` falls from 10,269 to 9,805 lines. Its budget ceiling is lowered from 10,750 to 10,000.

Gate:
- `cargo test -p sqe-core --lib config`: 323 passed before and after, with identical test-name lists.
- `cargo check -p sqe-core --all-targets`, `cargo check-server`, clippy on `sqe-core`, `cargo xtask budgets --check` and the rustfmt ratchet: exit 0.
- Token check of old `mod.rs` against new `mod.rs` plus `auth.rs`: nothing dropped. The only additions are `use`/`mod` wiring and four `pub(super)`.

**NEXT:** #637 item 3 (the legacy flat auth fields) can now work inside `config/auth.rs`. Then the catalog slice.
