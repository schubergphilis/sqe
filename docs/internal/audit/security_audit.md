# SQE Security Audit

**Date:** 2026-03-19 (audit); 2026-04-01 (fixes applied)
**Scope:** `crates/` -- all non-test Rust source code
**Auditor:** Static analysis via Claude Code
**Status:** 12 findings remediated (see OSS security hardening, Step 3 in nextsteps.md)

> **Follow-up audits.** A second pass (`docs/issues.md`, 2026-04-13) found 43 issues, all fixed in MR !61. A third pass (2026-05-15) filed 130 issues against the full workspace; ~110 were closed across 19 themed MRs (!195 -> !213). See the campaign write-up at `docs/blog/2026-05-15-nineteen-mrs-four-waves.md` and the wave-by-wave summary in `nextsteps.md`.

---

## Summary

| Severity | Count | Fixed |
|---|---|---|
| Critical | 1 | 1 |
| High | 3 | 3 |
| Medium | 4 | 4 |
| Low | 3 | 3 |
| Info / Positive | 8 | — |

**Fixes applied:** TLS support added (`[coordinator.tls]`), JWT validation hardened, constant-time comparison for auth, PII-safe logging (query hash alongside full text), K8s securityContext in Helm templates, configurable scan timeout, rate limiter on Trino HTTP path, error sanitisation (`client_message()` + debug mode toggle), startup WARN on `ssl_verification = false`, query size limit enforced, `unwrap()` replaced with proper error handling on hot path.

---

## Critical

### C1 — TLS not enforced on Flight SQL listener

**Files:**
- `crates/sqe-coordinator/src/bin/sqe_server.rs:315-319`
- `crates/sqe-core/src/config.rs` (no TLS fields in `CoordinatorConfig`)

The Flight SQL server (`tonic`) is started without any `.tls_config()` call. Bearer tokens and Basic auth credentials are transmitted in plaintext unless TLS is terminated externally (proxy, Kubernetes ingress).

`ssl_verification: false` is also configurable on the Keycloak and OAuth2 HTTP clients (`crates/sqe-auth/src/keycloak.rs:30-31`, `crates/sqe-auth/src/oauth.rs:33-34`), which disables certificate validation entirely when set.

**Fix:**
1. Add `tls_enabled: bool`, `tls_cert_path: String`, `tls_key_path: String` to `CoordinatorConfig`
2. Conditionally apply `.tls_config(ServerTlsConfig::new()..)` in `sqe_server.rs`
3. Default `ssl_verification` to `true`; document that `false` must never be used in production

---

## High

### H1 — Internal error details leaked to Flight SQL clients

**Files:** `crates/sqe-coordinator/src/flight_sql.rs:98,131,164,221,262,266,292,298,314,325`
**File:** `crates/sqe-coordinator/src/query_handler.rs:271`

All error returns use `Status::internal(format!("{e}"))`, passing raw error messages from DataFusion, reqwest, iceberg-rust, and tonic directly to the client. This can expose:
- Internal file paths (from Rust `std::io::Error`)
- Library version strings
- SQL planning internals (table names, column names from error context)
- Catalog structure details

Examples:
```
"Invalid authorization header: {e}"       // flight_sql.rs:164
"Auth type not supported: expected Basic" // flight_sql.rs:169
"SQL planning failed: {e}"               // query_handler.rs:271
"Query execution failed: {e}"            // flight_sql.rs:262
```

**Fix:** Return generic client-facing messages; log details server-side with a request ID:
```rust
tracing::error!(request_id = %req_id, error = %e, "Query planning failed");
Status::internal(format!("Query failed [{}]", req_id))
```

### H2 — `unwrap()` in error-handling paths (can crash server on user input)

**Files:**
- `crates/sqe-coordinator/src/catalog_ops.rs:460,468,480` — `parse_table_ref(&name).unwrap()` called inside error handlers; if the table name is malformed, this panics during error recovery
- `crates/sqe-coordinator/src/write_handler.rs:421,436` — `arrow_schema_to_iceberg(&arrow_schema).unwrap()` during CTAS/INSERT; panics if schema conversion fails

Total non-test `unwrap()`/`expect()` occurrences: **123**. Most are acceptable (startup, CLI, tests), but the above two locations are on the query execution hot path and are reachable by user input.

**Fix:**
```rust
// catalog_ops.rs — replace:
parse_table_ref(&name).unwrap()
// with:
parse_table_ref(&name)
    .map_err(|e| SqeError::Execution(format!("Invalid table reference: {e}")))?
```

### H3 — No query size limit

**File:** `crates/sqe-coordinator/src/query_handler.rs:73-77`

Query length is logged (`sql_length`) but no upper bound is enforced. A client can submit an arbitrarily large SQL string, consuming memory and CPU in the parser. This is a denial-of-service vector.

**Fix:** Reject queries above a configurable threshold (e.g., 1 MB default) before parsing:
```rust
if sql.len() > config.max_query_size_bytes.unwrap_or(1_048_576) {
    return Err(Status::invalid_argument("Query exceeds maximum allowed size"));
}
```

---

## Medium

### M1 — Auth scheme information disclosure

**File:** `crates/sqe-coordinator/src/flight_sql.rs:169`

```rust
"Auth type not supported: expected Basic, got: {first_10_chars}"
```

Reveals to the client what auth scheme the server expects. Prefer a generic message.

### M2 — Policy-enforced plan logged at DEBUG

**File:** `crates/sqe-coordinator/src/query_handler.rs:280`

```rust
debug!("Policy-enforced plan: {:?}", enforced_plan)
```

If debug logging is enabled in production, this emits the full logical plan (including any injected row-filter predicates) to the log file. An operator with log access could reconstruct which policy rules fired and potentially infer policy configuration.

**Fix:** Reduce to `trace!` or remove entirely; policy details are not useful operational information.

### M3 — `ssl_verification: false` has no warning

**Files:** `crates/sqe-auth/src/keycloak.rs:30-31`, `crates/sqe-auth/src/oauth.rs:33-34`

The `danger_accept_invalid_certs(true)` path is silently accepted. There is no startup warning when certificate verification is disabled.

**Fix:** Log a `WARN` at startup when `ssl_verification = false` is configured:
```rust
if !config.ssl_verification {
    tracing::warn!("TLS certificate verification is DISABLED — do not use in production");
}
```

### M4 — Audit log contains full SQL query text

**File:** `crates/sqe-metrics/src/audit.rs:11`

The audit log writes the full SQL query string. For queries that embed literal values (e.g., `WHERE ssn = '123-45-6789'`), this stores PII in the log file. Tokens are correctly excluded.

**Fix (optional):** Consider logging a SHA256 hash of the query alongside the full text, and document the PII risk in the ops guide.

---

## Low

### L1 — Credential size not validated after extraction

**File:** `crates/sqe-catalog/src/credential_vending.rs:66,70,90`

Extracted AWS access keys, secret keys, and session tokens from Polaris responses have no size bounds checked before being stored in the credential cache. Polaris should enforce these upstream, but defensive validation is cheap.

### L2 — CLI disables TLS verification in "test mode"

**File:** `crates/sqe-cli/src/http.rs:46-47`

`danger_accept_invalid_certs(accept_invalid_certs)` — the CLI accepts an `accept_invalid_certs` flag. This is appropriate for local dev but could be accidentally used against production endpoints.

### L3 — No rate limiting on authentication attempts

Authentication (ROPC grant, token validation) is performed per-request with no throttle on failed attempts. Brute-force protection is deferred to the upstream IdP (Keycloak/OIDC), which is acceptable but should be documented as a deployment assumption.

---

## Positive Findings (Well-Implemented)

| Area | Detail |
|---|---|
| **Credential redaction in Debug** | `AuthConfig`, `StorageConfig`, and `Session` all implement custom `fmt::Debug` that redact tokens and secret keys (`crates/sqe-core/src/config.rs:65-76,105-114`, `crates/sqe-core/src/session.rs:14-24`) |
| **No SQL injection risk** | SQL is parsed via DataFusion / sqlparser-rs; identifiers are extracted from the AST, not interpolated from raw user strings |
| **No hardcoded secrets** | All credentials loaded from TOML config or environment; no defaults embedded in source |
| **Token expiry management** | Background refresh task with proactive refresh before expiry; expired sessions evicted automatically (`crates/sqe-auth/src/authenticator.rs:206-305`) |
| **Session IDs are UUID v4** | `Uuid::new_v4()` — cryptographically random, client cannot predict or influence session ID (`crates/sqe-core/src/session.rs:36`) |
| **No session fixation** | Every authentication creates a new session; no client-supplied session ID is accepted |
| **Tokens not in audit log** | Audit log includes session ID and username but not the bearer token value |
| **SSRF protection in CLI** | `next_uri` same-origin validation in `crates/sqe-cli/src/http.rs:127` |
| **Basic auth split is correct** | `splitn(2, ':')` used for Basic auth parsing — handles passwords containing `:` correctly |

---

## Action Plan

| Priority | Item | Effort | Status |
|---|---|---|---|
| **C1** | Add TLS config + enforcement to Flight SQL server | Medium | ✅ Done — `[coordinator.tls]` with optional mTLS |
| **H1** | Wrap all `Status::internal(format!("{e}"))` with request-ID-keyed generic messages | Small | ✅ Done — `client_message()` + debug mode toggle |
| **H2** | Replace `unwrap()` in `catalog_ops.rs:460,468,480` and `write_handler.rs:421,436` | Small | ✅ Done |
| **H3** | Add configurable query size limit before parsing | Small | ✅ Done |
| **M1** | Generic auth-scheme error message | Trivial | ✅ Done |
| **M2** | Downgrade policy plan log to `trace!` | Trivial | ✅ Done |
| **M3** | Startup `WARN` when `ssl_verification = false` | Trivial | ✅ Done |
| **M4** | Document PII risk in audit log; optionally add query hash alongside text | Small | ✅ Done — query hash added |

All findings addressed in the OSS security hardening pass (Step 3, 51/51 tasks complete). See `openspec/changes/oss-security-hardening/` for full details.
