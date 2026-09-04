# 210: TLS material is read once at startup, in three separate builders, with no rotation or minimum version

- Scope: sqe-coordinator tls.rs + mcp_server.rs, sqe-worker tls.rs
- Severity: medium
- Category: operability
- Effort: M (days)
- Status: open

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
