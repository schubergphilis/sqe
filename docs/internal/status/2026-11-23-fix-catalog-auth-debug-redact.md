**STATUS 2026-11-23: a catalog's configured token and client secret no longer print in `Debug` output (`fix/catalog-auth-debug-redact`).**

`CatalogAuthConfig` derived `Debug` over plain `String` fields, so `{:?}` on a catalog's auth, or on any `CatalogConfig` or `SqeConfig` holding one, printed a `static` token or an inline `client_credentials` secret in clear. No code path logged it, so this is hardening, not a found leak. `Static.token` and `ClientCredentials.client_secret` are now `SecretString`, which renders `<set>` / `<unset>` and is read with `expose()`. Found while reviewing !1950 (#937); tracked as #944.

Gates: `cargo clippy -p sqe-auth -p sqe-core -p sqe-catalog -p sqe-coordinator --tests -- -D warnings`; `cargo test -p sqe-auth --lib` (new `debug_output_redacts_token_and_client_secret`); `cargo test -p sqe-core --lib config::`; `cargo test -p sqe-catalog --lib`; `cargo check-server`; IT `engine_credential_read_gate_and_writes`; `cargo xtask budgets --check`; `make leaks`.

**NEXT:** none for this change. The aikido-reputation job on Cargo.lock MRs fails on 72 pre-existing VERY-LOW findings and an expired `ctutils` suppression in `.asset`; that needs its own review.
