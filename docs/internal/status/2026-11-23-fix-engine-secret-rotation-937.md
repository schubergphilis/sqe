**STATUS 2026-11-23: the engine client secret rotates without a restart (`fix/engine-secret-rotation-937`, #937).**

A `client_credentials` block (`auth` or `credentials`, on `[catalog]` and every `[catalogs.<name>]`) takes its secret from `client_secret` or `client_secret_file`, exactly one. The file is read on every token request; the token cache key carries a fingerprint of the secret, so a rotated secret mints a new token at once and the old entry is dropped. The first token on a new engine secret writes an `Auth` audit event (`engine_secret_rotated`, catalog and client id only). A missing, empty or wrong file fails the token request, with no fallback. Startup refuses both sources set, or an unreadable or empty file. The environment variable source is still a startup read.

Gates: `cargo test -p sqe-core --lib config::`; `cargo test -p sqe-auth --lib` (wiremock rotation tests); `cargo check-server`; `cargo clippy -p sqe-auth -p sqe-core -p sqe-coordinator --tests -- -D warnings`; IT `engine_credential_read_gate_and_writes` on the local stack (rotates a principal's secret at Polaris, rewrites the file, keeps reading); `openspec validate --strict`; `cargo xtask budgets --check`; `make leaks`.

Not covered by a live test: the coordinator's rotation audit event (the IT drives `resolve_bearer_tracked` directly, so the wiring sees no second rotation). `CatalogAuthConfig` derives `Debug` and prints an inline `client_secret`; nothing logs it today, filed as a follow-up.

**NEXT:** follow-up for a redacting `Debug` on `CatalogAuthConfig`.
