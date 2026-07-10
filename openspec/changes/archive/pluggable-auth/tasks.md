## 1. AuthProvider Trait + Error Types

- [x] 1.1 Define `AuthProvider` trait, `AuthError` enum, `FlightCredentials` struct, `Identity` struct in `sqe-auth/src/provider.rs`
- [x] 1.2 Define `AuthChain` struct (Vec of providers, first-match logic)
- [x] 1.3 Update `SessionManager` to accept `Arc<dyn AuthProvider>` instead of hardwired Keycloak client
- [x] 1.4 Unit test: chain with two providers — first returns `NotMyCredentials`, second returns `Ok`; result is second's identity

## 2. OidcPasswordProvider (generalise existing)

- [x] 2.1 Move `sqe-auth/src/oidc_password.rs` (renamed from keycloak.rs in oss-security-hardening) to implement `AuthProvider` trait
- [x] 2.2 Add configurable `roles_claim` (dot-separated JSON pointer, default `realm_access.roles`)
- [x] 2.3 Add configurable `token_url` (no more hardwired realm path pattern)
- [x] 2.4 Move token refresh into `refresh_catalog_token()` on the provider (not on SessionManager)
- [x] 2.5 Unit test: ROPC flow — credential detection, roles extraction, skip logic for non-OIDC credentials
- [x] 2.6 Integration test: authenticate against quickstart OIDC with test users (covered by e2e test script)

## 3. BearerTokenProvider

- [x] 3.1 Create `sqe-auth/src/bearer_token.rs` implementing `AuthProvider`
- [x] 3.2 JWKS fetch + cache (moka, 15-min TTL); retry on key rotation (`kid` mismatch → refetch once)
- [x] 3.3 JWT validation: signature, expiry, optional `aud` check
- [x] 3.4 Claim extraction: `user_claim` → `user_id`; `roles_claim` → `roles` (string array claim)
- [x] 3.5 Detect credential type: starts with `eyJ` or `Authorization: Bearer` header → this provider
- [x] 3.6 `refresh_catalog_token()` → return same JWT (passthrough to catalog)
- [x] 3.7 Unit test: valid JWT passes; expired JWT returns `AuthFailed`; unknown `kid` triggers JWKS refetch; audience/issuer validation

## 4. ApiKeyProvider

- [x] 4.1 Create `sqe-auth/src/api_key.rs` implementing `AuthProvider`
- [x] 4.2 Load keys from TOML file (`keys_file` config); parse `key`, `description`, `user`, `groups` per entry
- [x] 4.3 Constant-time comparison (`subtle` crate) to prevent timing attacks
- [x] 4.4 Map groups → roles via `[auth.role_mappings]` config
- [x] 4.5 Hot-reload: background task polls keys file mtime; reload without restart
- [x] 4.6 `refresh_catalog_token()` → return `None` (API keys have no catalog token)
- [x] 4.7 Unit test: correct key authenticates; wrong key returns `AuthFailed`; file reload picks up new key

## 5. AnonymousProvider

- [x] 5.1 Create `sqe-auth/src/anonymous.rs` implementing `AuthProvider`
- [x] 5.2 Returns configured `user` + `roles` for any credentials (or no credentials)
- [x] 5.3 Unit test: any input → fixed identity; refresh returns None

## 6. MtlsProvider

- [x] 6.1 Create `sqe-auth/src/mtls.rs` implementing `AuthProvider`
- [x] 6.2 Extract CN from TLS peer certificate via `client_cert_cn` field in `FlightCredentials`
- [x] 6.3 Optional OU/SAN extraction → groups, mapped to roles via `[auth.role_mappings]`
- [x] 6.4 Return `NotMyCredentials` if no client cert present (chain falls through)
- [x] 6.5 Unit test: cert with known CN → correct identity; no cert → `NotMyCredentials`; structured CN parsing

## 7. TokenExchangeProvider (added — not in original spec)

- [x] 7.1 Create `sqe-auth/src/token_exchange.rs` implementing `AuthProvider` (RFC 8693)
- [x] 7.2 Exchange incoming credential (bearer token or username+password) for user-scoped JWT
- [x] 7.3 Configurable `audience`, `user_claim`, `roles_claim`
- [x] 7.4 JWT payload decoding and claim extraction
- [x] 7.5 Unit test: credential detection, claim extraction, config parsing

## 8. AwsIamProvider (added — not in original spec)

- [x] 8.1 Create `sqe-auth/src/aws_iam.rs` implementing `AuthProvider`
- [x] 8.2 Detect AWS access key IDs (`AKIA`/`ASIA` prefix, 20+ alphanumeric chars)
- [x] 8.3 STS `GetCallerIdentity` validation with AWS Signature Version 4 (inline, no AWS SDK)
- [x] 8.4 Config-only mode (`validate_with_sts = false`) for fast key-mapped auth
- [x] 8.5 ARN glob pattern matching for role mappings
- [x] 8.6 Unit test: credential detection, ARN parsing, glob matching, SigV4 helpers, role resolution

## 9. Config + Wiring

- [x] 9.1 Define `AuthProviderConfig` enum (one variant per provider type) in `sqe-core`
- [x] 9.2 `[[auth.providers]]` array in TOML; deserialise into `Vec<AuthProviderConfig>`
- [x] 9.3 Factory function: `build_auth_chain(configs) -> AuthChain`
- [x] 9.4 Default config (no `[auth.providers]` key) builds single legacy `Authenticator` — backwards compat
- [x] 9.5 Role mappings: `[auth.role_mappings]` deserialized as `HashMap<String, Vec<String>>`, shared to providers
- [x] 9.6 Unit test: default config produces legacy chain; explicit multi-provider config produces chain in order; factory tests for each provider type

## 10. OIDC Discovery

- [x] 10.1 Create `sqe-auth/src/oidc_discovery.rs` with `OidcDiscovery`, `DiscoveredEndpoints`, `OidcDiscoveryConfig`
- [x] 10.2 Fetch + cache `.well-known/openid-configuration` via `tokio::sync::OnceCell`
- [x] 10.3 Manual endpoint overrides take precedence over discovery
- [x] 10.4 Warn if `device_authorization_endpoint` is not advertised
- [x] 10.5 Unit test: JSON parsing, override precedence

## 11. Device Authorization Grant (RFC 8628)

- [x] 11.1 Create `sqe-auth/src/device_code.rs` with `DeviceCodeService`, `DeviceAuthSession`, `DevicePollResult`
- [x] 11.2 `start()` → POST to device authorization endpoint → return user_code + verification_uri
- [x] 11.3 `poll()` → POST to token endpoint with device_code grant type
- [x] 11.4 Handle `authorization_pending`, `slow_down`, `expired_token`, `access_denied`
- [x] 11.5 Unit test: response parsing, error mapping

## 12. Authorization Code + PKCE

- [x] 12.1 Create `sqe-auth/src/auth_code.rs` with `AuthCodeService`, `AuthCodeChallenge`
- [x] 12.2 PKCE S256 code_challenge generation from random code_verifier
- [x] 12.3 `start_challenge()` → authorization URL with PKCE + state
- [x] 12.4 `exchange_code()` → POST to token endpoint with code + verifier
- [x] 12.5 Unit test: PKCE generation, URL construction, token parsing

## 13. Trino External Auth Endpoints

- [x] 13.1 Create `sqe-trino-compat/src/oauth2.rs` with endpoint handlers
- [x] 13.2 `GET /oauth2/token/initiate/{hash}` → 302 redirect to IdP
- [x] 13.3 `GET /oauth2/callback?code=&state=` → exchange code, store tokens
- [x] 13.4 `GET /oauth2/token/{auth_id}` → poll (pending/complete/error)
- [x] 13.5 `DELETE /oauth2/token/{auth_id}` → cleanup
- [x] 13.6 Modify `submit_query` → 401 with `WWW-Authenticate: Bearer x_redirect_server, x_token_server`
- [x] 13.7 Integration test: full external auth cycle

## 14. Config + Wiring

- [x] 14.1 Add `ExternalAuthConfig` + `DeviceAuthConfig` to `sqe-core/src/config.rs`
- [x] 14.2 Add `[auth.external]` section to `sqe.toml.example`
- [x] 14.3 Construct services in coordinator startup from config
- [x] 14.4 Unit test: config parsing with and without `[auth.external]`

## 15. PendingAuthStore

- [x] 15.1 Create `sqe-auth/src/pending_auth.rs` with `PendingAuthStore`, `PendingAuth`, `TokenSet`
- [x] 15.2 Insert/poll/complete/fail/remove lifecycle
- [x] 15.3 Moka TTL-based expiry (default 15 min)
- [x] 15.4 Unit test: full lifecycle, missing key returns None
