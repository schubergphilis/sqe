## Why

SQE's original auth design is hardwired to one pattern: a client sends username+password, SQE exchanges them via OIDC ROPC against Keycloak, and the resulting token is forwarded to the catalog. This works for interactive BI tools (DBeaver, Tableau) but fails for:

- **Service accounts / CI pipelines**: these have pre-obtained bearer tokens (k8s ServiceAccount, Workload Identity, CI OIDC); they cannot do ROPC
- **Embedded and edge deployments**: no IdP running; API key or anonymous access is sufficient
- **mTLS-heavy environments** (service meshes, zero-trust): certificate CN is the identity; password is redundant
- **Multi-tenant SaaS**: different tenants use different IdPs; SQE must route auth per-request

The fix is a `AuthProvider` trait that decouples "how SQE validates the client" from "what identity it produces". The OIDC ROPC path becomes one implementation; others are added without touching the coordinator core.

Note: ROPC (Resource Owner Password Credentials) is deprecated in OAuth 2.1 but is the only viable non-browser option for JDBC clients. It remains supported but is no longer the sole method.

## What Changes

- New `AuthProvider` trait in `sqe-auth` with five implementations:
  1. `OidcPasswordProvider` — generalised ROPC (was Keycloak-specific); works with any OIDC IdP
  2. `BearerTokenProvider` — validate pre-obtained JWT via JWKS endpoint, extract identity from claims
  3. `ApiKeyProvider` — SQE-issued opaque keys mapped to a fixed identity + role set (SQLite store)
  4. `AnonymousProvider` — fixed identity, no credentials checked (local dev / trusted network)
  5. `MtlsProvider` — TLS client certificate CN → identity (requires TLS from oss-security-hardening)
- Coordinator `SessionManager` depends only on `AuthProvider` trait — zero Keycloak-specific code
- Config selects which provider(s) are active; multiple providers chain (first match wins)
- Token refresh and catalog passthrough logic moves to `OidcPasswordProvider` and `BearerTokenProvider` implementations (not shared code)

## Capabilities

### New Capabilities
- `auth-bearer-token`: validate pre-obtained JWT, no IdP call required at query time
- `auth-api-key`: opaque API key → identity mapping with SQLite-backed key store
- `auth-anonymous`: fixed identity for dev / internal / trusted-network deployments
- `auth-mtls`: client certificate CN → identity extraction
- `auth-chain`: first-match provider chain for multi-tenant deployments

### Modified Capabilities
- `auth-oidc` (was `auth-passthrough`): generalised OIDC ROPC, no Keycloak imports

## Impact

- `sqe-auth` crate: trait-driven, old Keycloak module replaced by `OidcPasswordProvider`
- `sqe-coordinator`: `SessionManager` now takes `Arc<dyn AuthProvider>` — zero breaking behaviour change for OIDC users
- Config: new `auth.providers` array (ordered); default is `[{ type = "oidc_password" }]`
- API key store: new SQLite file, managed via `sqe-cli key create/list/revoke` subcommands

## Rollback

Default config is backwards compatible. Removing the `auth.providers` array falls back to OIDC password, identical to current behaviour.
