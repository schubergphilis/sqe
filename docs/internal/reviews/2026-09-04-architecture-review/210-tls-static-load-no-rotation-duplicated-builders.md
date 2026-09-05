# 210: TLS material is read once at startup, in three separate builders, with no rotation or minimum version

- Scope: sqe-coordinator tls.rs + mcp_server.rs, sqe-worker tls.rs
- Severity: medium
- Category: operability
- Effort: M (days)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
- Issue: #563

## Problem

The gRPC (Flight) server config reads cert and key from disk into a static
`Identity`:

```rust
// crates/sqe-coordinator/src/tls.rs:16-24
let cert = std::fs::read(&config.cert_file)...;
let key = std::fs::read(&config.key_file)...;
let identity = Identity::from_pem(cert, key);
```

`crates/sqe-worker/src/tls.rs:22` is a stated mirror of the same function.
The MCP HTTPS listener builds a third, unrelated config:

```rust
// crates/sqe-coordinator/src/mcp_server.rs:67-72
let _ = rustls::crypto::ring::default_provider().install_default();
let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_file, &tls.key_file).await?;
```

None of the three:

- reloads on file change (cert-manager rotates Kubernetes TLS secrets every
  60 to 90 days; the pod must restart to pick them up, dropping every
  Flight session);
- sets a minimum protocol version or cipher policy (tonic/rustls defaults
  allow TLS 1.2; fine today but not pinned);
- requires client certificates consistently. `client_ca_root` is set only
  when `ca_file` is non-empty (`tls.rs:29-40`), and the MCP listener ignores
  `ca_file` entirely, so an mTLS deployment has an mTLS Flight port and a
  server-only MCP port on the same cert.

`start_mcp_server` also installs the ring crypto provider as a side effect
and discards the result, which will silently no-op if another provider was
installed first.

## Why it matters

Bearer tokens, worker secrets and vended S3 credentials cross these hops
(`config.rs:3786-3793`). Rotation-by-restart means operators either accept
periodic outages or let certificates run long. Three builders mean an mTLS
requirement or a min-version bump is applied in one and forgotten in
another, and there is no test that the three agree.

## Proposed change

1. Create one `sqe_core::tls::ServerTlsMaterial` loader that returns a
   `rustls::ServerConfig` (with `min_protocol_version = TLS1_2`, optional
   client-cert verifier from `ca_file`) and derive both the tonic
   `ServerTlsConfig` and the axum `RustlsConfig` from it. Delete the worker
   mirror; the worker can depend on `sqe-core`.
2. Add a watcher (`notify` crate, or poll `mtime` every 60 s as
   `api_key.rs:129-160` already does for keys) that calls
   `RustlsConfig::reload_from_pem_file` for axum and swaps the tonic acceptor
   via `tonic::transport::server::TlsAcceptor` rebuild, or document that
   Flight requires restart and gate that in Helm with a `reloader` sidecar.
3. Apply `ca_file` to the MCP listener when set.

## Acceptance criteria

- Test: overwrite `cert_file` on disk; a new HTTPS connection to `/mcp`
  presents the new certificate without restart.
- Test: with `ca_file` set, an MCP request without a client cert is refused.
- `grep -rn "Identity::from_pem" crates/` returns one production hit.

## Related

- docs/internal/reviews/2026-07-25-production-readiness-review.md P1 "TLS
  depends on engine or ingress configuration".

## Resolution (2026-09-05)

**One loader.** `crates/sqe-core/src/tls.rs` holds `ServerTlsMaterial::load`,
which reads cert, key and optional CA once, parses them once, and derives every
downstream shape: `tonic_server()` (Flight), `rustls_server_config()` (axum:
MCP and Quack), and `cert_fingerprint()`. `crates/sqe-worker/src/tls.rs` is
deleted and `crates/sqe-coordinator/src/tls.rs` is a re-export plus the
reloader. `grep -rn "Identity::from_pem" crates/` returns one production hit,
`sqe_core::tls::tonic_identity`, which both the server and the client path call.

**Minimum version.** The rustls config is built with
`builder_with_provider(ring).with_protocol_versions(&[TLS12, TLS13])`, so TLS
1.1 and below are never offered on any port. The explicit provider also means
the listeners no longer depend on whatever happened to install the process
default first.

**mTLS consistency.** `ca_file` now produces a `WebPkiClientVerifier` on the
rustls path as well as `client_ca_root` on the tonic path, so the MCP and Quack
listeners require a client certificate exactly when the Flight port does. The
"Quack TLS is server-side only" warning in `bin/sqe_server.rs` is gone because
the statement is no longer true.

**Crypto provider.** `sqe_core::tls::ensure_crypto_provider()` replaces the two
`let _ = rustls::crypto::ring::default_provider().install_default();` calls. It
checks `CryptoProvider::get_default()` first and logs at `info` when another
component already installed one, instead of discarding the result silently.

**Reload.** `crates/sqe-coordinator/src/tls.rs::spawn_cert_reload_watch` polls
the cert, key and CA mtimes every `DEFAULT_CERT_RELOAD_INTERVAL` (60 s), the
same shape as `sqe-auth`'s `spawn_reload_watcher`. On change it reloads through
`ServerTlsMaterial` (so the new pair is validated before anything is swapped)
and then calls `RustlsConfig::reload_from_pem_file`, or `reload_from_config`
when `ca_file` is set, because `reload_from_pem_file` would rebuild a default
server config and silently drop the client verifier. The mtime stamp only
advances on success, so a half-written cert-manager pair is retried on the next
tick and the listener keeps serving the previous certificate. Both axum
listeners (MCP in `mcp_server.rs`, Quack in `bin/sqe_server.rs`) spawn it.

**Rotation documented.** The tonic Flight listener is not hot-reloadable:
tonic 0.14 builds its acceptor inside `Server::tls_config` and exposes no way
to replace it on a live server. That is stated in the doc comment on
`TlsConfig::cert_file`, in the module docs of `sqe_coordinator::tls`, in the
`hot_reload = false` field on every "listening (TLS)" log line, and in
`docs/site/book/src/deployment/configuration.md#tls` together with the
`reloader.stakater.com/auto` sidecar annotation that covers it in Kubernetes.

Tests (all green): `sqe-core` `tls::tests::tls_server_config_floor_is_tls12`,
`tls_ca_file_requires_a_client_certificate`,
`tls_server_without_ca_does_not_ask_for_client_cert`,
`tls_material_reports_a_stable_cert_fingerprint`,
`tls_material_debug_redacts_the_private_key`,
`tls_ensure_crypto_provider_is_idempotent`; `sqe-coordinator`
`tls::tests::tls_mcp_listener_requires_client_cert_when_ca_set`,
`tls_reload_picks_up_an_overwritten_cert`,
`tls_reload_preserves_the_client_ca_requirement`,
`tls_reload_keeps_the_old_cert_when_the_new_pair_is_broken`,
`tls_builder_is_shared_with_sqe_core`. Fixture PEMs (a test CA certificate and two leaf
pairs, generated once with openssl) live in
`crates/sqe-core/tests/fixtures/tls/`, allowlisted in `.gitignore` against the
repo-wide `*.crt` / `*.key` guard (that guard still covers every other path).

Remaining items, deliberately not done here:

- **No tonic hot-reload.** The acceptance criterion "overwrite `cert_file`; a
  new HTTPS connection presents the new certificate without restart" holds for
  the MCP and Quack ports, not for the Flight port. Doing it would mean either
  a tonic upgrade that exposes the acceptor or serving Flight through a
  hand-rolled `TlsAcceptor` on top of `tokio-rustls`, which is a larger change
  than this finding warrants. The Helm reloader sidecar is the documented
  workaround.
- **The mTLS assertion is on the rustls config, not a live handshake.** The
  test asserts `ServerTlsMaterial::requires_client_cert()` and that the axum
  `RustlsConfig` is built from that material, rather than driving a real TCP
  TLS handshake without a client certificate. A handshake-level test belongs
  with the TLS variant of `make test-distributed`, which this branch does not
  add.
