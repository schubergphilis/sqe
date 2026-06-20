# Device Authorization Grant + Trino External Auth

**Date:** 2026-03-30
**Status:** Draft
**Scope:** Add OIDC Device Authorization Grant (RFC 8628) for CLI and Trino external authentication (Authorization Code + PKCE) for Trino JDBC browser-based SSO.

## Motivation

SQE currently supports 8 authentication methods (ROPC, bearer token, client credentials, token exchange, API key, AWS IAM, mTLS, anonymous). All are non-interactive — they require credentials upfront.

Two gaps remain:

1. **CLI users** need a way to authenticate without embedding passwords. The OIDC Device Authorization Grant lets them visit a URL, log in via browser, and the CLI picks up the token automatically. This is the standard pattern (`gh auth login`, `aws sso login`, `gcloud auth login`).

2. **Trino JDBC users** (DBeaver, DataGrip, etc.) expect browser-based SSO via `externalAuthentication=true`. The Trino protocol defines a 401 challenge → browser redirect → poll flow that the JDBC driver handles natively.

Both flows share underlying OIDC infrastructure: discovery, token exchange, JWT validation.

## Architecture

```
                        ┌──────────────────┐
                        │  OidcDiscovery   │
                        │  (well-known)    │
                        └────────┬─────────┘
                 ┌───────────────┼───────────────┐
                 │               │               │
          ┌──────▼──────┐ ┌─────▼──────┐ ┌──────▼──────┐
          │ DeviceCode  │ │ AuthCode   │ │ Existing    │
          │ Service     │ │ Service    │ │ AuthChain   │
          │ (RFC 8628)  │ │ (PKCE)     │ │ providers   │
          └──────┬──────┘ └─────┬──────┘ └──────┬──────┘
                 │              │               │
    ┌────────────┤       ┌──────┤        ┌──────┤
    │            │       │      │        │      │
  CLI        (future)  Trino  (future)  Flight  Trino
  sqe query           ExtAuth          SQL      HTTP
                      endpoints        Handshake Basic/Bearer
```

### New Components

| Component | Crate | Purpose |
|---|---|---|
| `OidcDiscovery` | `sqe-auth` | Fetch + cache `.well-known/openid-configuration`; resolve endpoints |
| `DeviceCodeService` | `sqe-auth` | RFC 8628 device flow: start → poll → complete |
| `AuthCodeService` | `sqe-auth` | Authorization Code + PKCE: generate auth URL → exchange code → tokens |
| `PendingAuthStore` | `sqe-auth` | In-memory map of pending interactive auth sessions (moka TTL cache) |
| Trino external auth endpoints | `sqe-trino-compat` | `/oauth2/token/initiate/{id}`, `/oauth2/callback`, `/oauth2/token/{id}` |

### What Already Exists (No Changes Needed)

The existing `AuthChain` and all 8 providers are unchanged. The new components are additive — they produce an `Identity` that feeds into the existing `SessionManager`.

## OIDC Discovery

### `OidcDiscovery` struct

Fetches `{issuer}/.well-known/openid-configuration` at startup and caches the result. Exposes discovered endpoints:

```rust
pub struct OidcDiscovery {
    config: OidcDiscoveryConfig,
    endpoints: tokio::sync::OnceCell<DiscoveredEndpoints>,
    http: reqwest::Client,
}

pub struct DiscoveredEndpoints {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub device_authorization_endpoint: Option<String>,
    pub jwks_uri: String,
    pub issuer: String,
    pub userinfo_endpoint: Option<String>,
}

pub struct OidcDiscoveryConfig {
    pub issuer: String,
    /// Manual overrides — if set, skip discovery for that endpoint.
    pub authorization_endpoint_override: Option<String>,
    pub token_endpoint_override: Option<String>,
    pub device_authorization_endpoint_override: Option<String>,
}
```

Discovery is lazy (first use) and cached for the process lifetime. If the IdP doesn't advertise `device_authorization_endpoint`, the device code flow is unavailable (logged at startup as a warning).

## Device Authorization Grant (RFC 8628)

### Flow

```
1. CLI → DeviceCodeService::start(scopes)
2. DeviceCodeService → IdP:  POST /device/authorize
                              client_id, scope=openid+profile
3. IdP → DeviceCodeService:  { device_code, user_code, verification_uri,
                                verification_uri_complete, expires_in, interval }
4. DeviceCodeService → CLI:  DeviceAuthSession { user_code, verification_uri,
                              verification_uri_complete, expires_in }
5. CLI:                      Opens browser / prints URL + code
6. User:                     Visits URL, enters code, authenticates in browser
7. CLI → DeviceCodeService::poll(device_code)
8. DeviceCodeService → IdP:  POST /token
                              grant_type=urn:ietf:params:oauth:grant-type:device_code
                              device_code, client_id
9. IdP → DeviceCodeService:  { "error": "authorization_pending" }  (repeat)
                              OR { access_token, id_token, refresh_token }
10. DeviceCodeService → CLI: Identity (user_id, roles, catalog_token)
```

### `DeviceCodeService` API

```rust
pub struct DeviceCodeService {
    discovery: Arc<OidcDiscovery>,
    client_id: String,
    scopes: Vec<String>,
    http: reqwest::Client,
}

pub struct DeviceAuthSession {
    pub device_code: String,      // opaque, for polling
    pub user_code: String,        // human-readable, e.g. "ABCD-1234"
    pub verification_uri: String, // e.g. "https://idp.example.com/device"
    pub verification_uri_complete: Option<String>, // with code pre-filled
    pub expires_in: u64,
    pub interval: u64,            // poll interval in seconds
}

pub enum DevicePollResult {
    Pending,
    SlowDown,
    Complete(TokenSet),
    AccessDenied,
    ExpiredToken,
}

pub struct TokenSet {
    pub access_token: String,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
}

impl DeviceCodeService {
    pub async fn start(&self) -> Result<DeviceAuthSession, AuthError>;
    pub async fn poll(&self, device_code: &str) -> Result<DevicePollResult, AuthError>;
}
```

### Error handling

- `authorization_pending` → return `Pending`, caller retries after `interval` seconds
- `slow_down` → return `SlowDown`, caller increases interval by 5 seconds (per RFC)
- `expired_token` → return `ExpiredToken`, flow must restart
- `access_denied` → return `AccessDenied`, user rejected
- Network errors → `AuthError::Internal`

### Config

```toml
[[auth.providers]]
type = "oidc_device"
client_id = "sqe-cli"    # may differ from server client_id (public client)
scopes = ["openid", "profile"]
# Endpoints resolved via [auth.external] discovery
```

The `DeviceCodeProvider` is registered in the `AuthProviderConfig` enum but is **not used in the chain** for Handshake/HTTP auth (it's interactive). Instead, the CLI calls `DeviceCodeService` directly, gets a `TokenSet`, and then uses the access token as a bearer token for subsequent Flight SQL connections.

## Authorization Code + PKCE (Trino External Auth)

### Flow (from Trino JDBC driver's perspective)

```
1. JDBC → POST /v1/statement (no auth header)
2. SQE  → 401 Unauthorized
          WWW-Authenticate: Bearer x_redirect_server="{initiate_url}",
                                   x_token_server="{poll_url}"
3. JDBC → Opens browser to {initiate_url}
4. Browser → GET /oauth2/token/initiate/{auth_id}
5. SQE   → 302 redirect to IdP authorization endpoint
            (client_id, redirect_uri=/oauth2/callback, state, code_challenge, scope)
6. User  → Authenticates at IdP in browser
7. IdP   → 302 redirect to /oauth2/callback?code=...&state=...
8. SQE   → Exchanges code for tokens, stores in PendingAuthStore
9. Browser ← HTML success page ("You can close this tab")
10. JDBC → GET /oauth2/token/{auth_id}  (polling since step 3)
11. SQE  → { "token": "<session_id>" }  (or { "nextUri": "..." } if pending)
12. JDBC → DELETE /oauth2/token/{auth_id}
13. JDBC → POST /v1/statement  (Authorization: Bearer <session_id>)
```

### `AuthCodeService` API

```rust
pub struct AuthCodeService {
    discovery: Arc<OidcDiscovery>,
    client_id: String,
    client_secret: Option<String>,
    redirect_uri: String,
    scopes: Vec<String>,
    http: reqwest::Client,
}

pub struct AuthCodeChallenge {
    pub auth_id: String,
    pub authorization_url: String,  // full URL to redirect browser to
    pub code_verifier: String,      // PKCE, kept server-side
    pub state: String,              // CSRF token
}

impl AuthCodeService {
    /// Generate an authorization URL + PKCE challenge for a new auth attempt.
    pub fn start_challenge(&self) -> AuthCodeChallenge;

    /// Exchange the authorization code (from IdP callback) for tokens.
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenSet, AuthError>;
}
```

### `PendingAuthStore`

In-memory store for auth sessions that are in-flight (user is authenticating in browser).

```rust
pub struct PendingAuthStore {
    store: moka::future::Cache<String, PendingAuth>,
}

enum PendingAuth {
    /// Browser redirect sent, waiting for callback.
    AwaitingCallback {
        code_verifier: String,
        state: String,
        created_at: Instant,
    },
    /// Callback received, tokens available.
    Complete(TokenSet),
    /// Auth failed.
    Failed(String),
}

impl PendingAuthStore {
    pub fn new(challenge_timeout: Duration) -> Self;
    pub fn insert_pending(&self, auth_id: &str, code_verifier: String, state: String);
    pub fn complete(&self, auth_id: &str, tokens: TokenSet);
    pub fn fail(&self, auth_id: &str, error: String);
    pub fn poll(&self, auth_id: &str) -> Option<PendingAuth>;
    pub fn remove(&self, auth_id: &str);
}
```

TTL defaults to 15 minutes (matching Trino's `challenge-timeout`).

### Trino External Auth Endpoints

Added to the existing axum router in `sqe-trino-compat`:

| Route | Method | Handler |
|---|---|---|
| `/oauth2/token/initiate/{auth_id_hash}` | GET | Generate auth URL, redirect browser to IdP |
| `/oauth2/callback` | GET | Receive IdP callback, exchange code, store tokens |
| `/oauth2/token/{auth_id}` | GET | Poll: return `{"nextUri":...}` or `{"token":...}` |
| `/oauth2/token/{auth_id}` | DELETE | Cleanup pending auth session |

The existing `submit_query` handler is modified: when no `Authorization` header is present, instead of returning a flat 401, it generates an `auth_id`, inserts a pending session, and returns:

```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer x_redirect_server="https://sqe:8080/oauth2/token/initiate/{hash}",
                         x_token_server="https://sqe:8080/oauth2/token/{auth_id}"
```

### Token handling

When the auth code exchange completes, SQE:
1. Validates the resulting JWT via the existing `BearerTokenProvider` to extract `Identity`
2. Creates a `Session` via `SessionManager`
3. Returns the session ID as the token to the Trino JDBC driver

The JDBC driver then sends `Authorization: Bearer <session_id>` on subsequent requests, which the existing Trino HTTP handler already supports.

## Config

```toml
[auth.external]
# OIDC discovery — resolves all endpoints automatically
issuer = "https://idp.example.com/realms/sqe"
client_id = "sqe"
client_secret = "secret"              # omit for public clients
redirect_uri = "https://sqe.example.com/oauth2/callback"
scopes = ["openid", "profile"]
challenge_timeout_secs = 900          # 15 min, for pending auth sessions

# Optional manual overrides (skip discovery for these)
# authorization_endpoint = "https://..."
# token_endpoint = "https://..."
# device_authorization_endpoint = "https://..."

[auth.external.device]
# Device code flow config (CLI)
client_id = "sqe-cli"                 # often a separate public client
scopes = ["openid", "profile", "offline_access"]
```

When `[auth.external]` is absent, both device code and Trino external auth are disabled. The existing provider chain works as before.

## Testing

### Unit tests

- `OidcDiscovery`: mock `.well-known` endpoint, verify endpoint extraction, verify overrides take precedence
- `DeviceCodeService`: mock device authorization + token endpoints, test full flow (start → poll pending → poll complete), test error cases (expired, denied, slow_down)
- `AuthCodeService`: verify PKCE generation (S256), verify authorization URL construction, mock token exchange
- `PendingAuthStore`: insert/poll/complete/expire/remove lifecycle

### Integration tests

- Device code flow against quickstart Keycloak (if device grant is enabled in realm)
- Trino external auth endpoints: simulate the 401 → initiate → callback → poll → token cycle with HTTP client
- Trino JDBC driver with `externalAuthentication=true` against SQE (manual/CI)

## File Plan

| File | Action |
|---|---|
| `crates/sqe-auth/src/oidc_discovery.rs` | New — `OidcDiscovery` + `DiscoveredEndpoints` |
| `crates/sqe-auth/src/device_code.rs` | New — `DeviceCodeService` + `DeviceAuthSession` + `DevicePollResult` |
| `crates/sqe-auth/src/auth_code.rs` | New — `AuthCodeService` + `AuthCodeChallenge` |
| `crates/sqe-auth/src/pending_auth.rs` | New — `PendingAuthStore` |
| `crates/sqe-auth/src/lib.rs` | Add module declarations + re-exports |
| `crates/sqe-core/src/config.rs` | Add `ExternalAuthConfig`, `DeviceAuthConfig`, `AuthProviderConfig::OidcDevice` |
| `crates/sqe-trino-compat/src/server.rs` | Add OAuth2 endpoints, modify 401 response |
| `crates/sqe-trino-compat/src/oauth2.rs` | New — Trino external auth route handlers |
| `crates/sqe-auth/Cargo.toml` | Add `rand` (for PKCE code_verifier) if not already present |
| `openspec/changes/pluggable-auth/tasks.md` | Add sections 10–13 for new work |
