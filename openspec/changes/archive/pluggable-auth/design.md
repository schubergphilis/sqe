## Context

Replacing hardwired Keycloak ROPC with a pluggable `AuthProvider` trait. Five implementations cover all realistic deployment scenarios for an Apache 2.0 open-source project.

## Goals / Non-Goals

**Goals:**
- `AuthProvider` trait decouples client credential validation from identity production
- Five providers: OIDC password, bearer token, API key, anonymous, mTLS
- First-match provider chain for multi-tenant deployments
- API key store is externally managed (config file or environment variable); no built-in CLI key management

**Non-Goals:**
- OAuth2 Authorization Code / Device Code flows (no browser in JDBC context)
- Built-in web UI for key management
- Per-user quota or billing (use rate limiting from oss-security-hardening)

## Architecture

```
Flight Handshake
  │  username + password  (Basic auth)
  │  OR Bearer token      (token property)
  │  OR TLS client cert   (mTLS)
  ▼
AuthChain  [provider_1, provider_2, ...]
  │  try each provider in order
  │  first Ok(Identity) wins
  │  all Err → authentication failed
  ▼
Identity { user_id, display_name, roles, catalog_token }
  │
  ▼
SessionManager → Session
```

### AuthProvider Trait

```rust
/// Attempt to authenticate from raw Flight credentials.
/// Returns Ok(Identity) on success, Err(AuthError::NotMyCredentials)
/// if this provider does not handle this credential type,
/// or Err(AuthError::AuthFailed) on a definitive rejection.
#[async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError>;

    /// Optional: return a fresh catalog token for an existing identity.
    /// Called by SessionManager before each catalog request.
    async fn refresh_catalog_token(&self, identity: &Identity) -> Result<Option<String>, AuthError> {
        Ok(None)  // default: no refresh needed (bearer / api key / anonymous)
    }
}

pub enum AuthError {
    NotMyCredentials,   // pass to next provider in chain
    AuthFailed(String), // definitive rejection, stop chain
    Internal(anyhow::Error),
}
```

### Provider 1: OidcPasswordProvider

Generalised ROPC. Works with any OIDC IdP that supports the password grant: Keycloak, Okta (enterprise), Entra ID (legacy mode), Zitadel, Authentik, Auth0 (enterprise).

```toml
[[auth.providers]]
type         = "oidc_password"
token_url    = "https://idp.example.com/realms/myapp/protocol/openid-connect/token"
client_id    = "sqe"
# client_secret is optional (public client):
client_secret = "changeme"
# JWT claim that carries roles (default: "realm_access.roles"):
roles_claim  = "realm_access.roles"
```

Flow:
1. Receive `username + password` from Flight Basic auth
2. POST `grant_type=password` to `token_url`
3. Validate JWT signature via JWKS (derived from `token_url` → `/.well-known/openid-configuration`)
4. Extract `roles` from `roles_claim` path (dot-separated JSON pointer)
5. Return `Identity { user_id: sub, roles, catalog_token: access_token }`
6. Background refresh via `refresh_catalog_token()` using stored `refresh_token`

### Provider 2: BearerTokenProvider

Client pre-obtains a JWT (k8s ServiceAccount token, Workload Identity, CI OIDC, service account PAT). Passes it as the "password" field in Flight Basic auth, or via the Flight `Authorization: Bearer` header.

```toml
[[auth.providers]]
type       = "bearer_token"
jwks_url   = "https://idp.example.com/.well-known/jwks.json"
audience   = "sqe"        # optional JWT `aud` claim check
# claim mapping:
user_claim = "sub"
roles_claim = "groups"    # or "roles", or any string array claim
```

Flow:
1. Detect credential is a JWT (starts with `eyJ`) or Authorization header is Bearer
2. Fetch JWKS (cached, refreshed on key rotation)
3. Validate signature + expiry + audience
4. Map claims → `Identity`
5. `refresh_catalog_token()` returns the same JWT (catalog must accept it directly, i.e. passthrough)

### Provider 3: ApiKeyProvider

Opaque keys mapped to a group-based identity. Keys are defined externally — in a config file or environment variable. No built-in key generation or rotation UI.

```toml
[[auth.providers]]
type    = "api_key"
# Path to a TOML file containing key definitions, OR inline:
keys_file = "/etc/sqe/api-keys.toml"
```

`api-keys.toml` format:
```toml
[[keys]]
key         = "sqe_k_abc123def456"  # opaque string, prefix convention only
description = "dbt production pipeline"
groups      = ["data-engineering", "writer"]

[[keys]]
key         = "sqe_k_xyz789"
description = "Tableau read-only service"
groups      = ["bi-reader"]
```

Roles for the identity are derived from the union of groups' role mappings (configured in `[auth.role_mappings]`).

Flow:
1. Receive `password` field; if it matches a known key prefix pattern — try lookup
2. Constant-time compare against loaded keys
3. Map groups → roles via `role_mappings`
4. `catalog_token`: use service credential from catalog auth (not user token passthrough)

Keys file is watched for changes (inotify/kqueue) and hot-reloaded without restart.

### Provider 4: AnonymousProvider

Fixed identity for dev / single-user / trusted-network deployments.

```toml
[[auth.providers]]
type   = "anonymous"
user   = "anonymous"
groups = ["public"]
```

Accepts any credentials (or none). Should be last in the chain in production if used at all.

### Provider 5: MtlsProvider

TLS client certificate — extract Common Name (CN) as user identity.
Requires TLS enabled (oss-security-hardening).

```toml
[[auth.providers]]
type        = "mtls"
# Optional: map cert OU or SAN to groups
groups_from = "ou"   # "ou" | "san_dns" | "san_email"
```

Flow:
1. Check TLS peer certificate present
2. Extract CN → `user_id`
3. Optionally extract OU or SAN fields → groups → roles

### Role Mappings

Groups map to roles globally in `[auth.role_mappings]`:

```toml
[auth.role_mappings]
"data-engineering" = ["writer", "reader", "admin-tables"]
"bi-reader"        = ["reader"]
"public"           = []
```

Roles feed into the `PolicyEnforcer` (OPA/Cedar, Phase 5). Until then, roles are carried in the session and logged.

### Provider Chain

```toml
# Production: bearer first (CI/services), then OIDC password (humans)
[[auth.providers]]
type = "bearer_token"
jwks_url = "..."

[[auth.providers]]
type = "oidc_password"
token_url = "..."
client_id = "sqe"
```

Chain stops on first `Ok` or on `AuthFailed`. `NotMyCredentials` advances to next.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| ROPC kept | yes, as OidcPasswordProvider | Only non-browser option for JDBC |
| API key store | external config file | Simpler, no DB dependency, hot-reloadable |
| API keys: group-based | yes | Maps cleanly to existing role_mappings |
| JWT as password field | accepted in BearerTokenProvider | Flight Basic auth is the only hook; `eyJ` prefix is unambiguous |
| Catalog token for API key | service credential (not passthrough) | API keys don't have IdP tokens to forward |

## Risks

| Risk | Mitigation |
|---|---|
| ROPC deprecated in OAuth2.1 | Documented; provider remains; IdPs will support it for years |
| JWKS cache staleness on key rotation | Refresh on validation failure (try twice before rejecting) |
| API keys in config file | Recommend file permissions 0600; document secrets management options |
