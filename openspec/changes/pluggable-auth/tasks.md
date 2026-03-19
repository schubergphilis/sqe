## 1. AuthProvider Trait + Error Types

- [ ] 1.1 Define `AuthProvider` trait, `AuthError` enum, `FlightCredentials` struct, `Identity` struct in `sqe-auth/src/provider.rs`
- [ ] 1.2 Define `AuthChain` struct (Vec of providers, first-match logic)
- [ ] 1.3 Update `SessionManager` to accept `Arc<dyn AuthProvider>` instead of hardwired Keycloak client
- [ ] 1.4 Unit test: chain with two providers — first returns `NotMyCredentials`, second returns `Ok`; result is second's identity

## 2. OidcPasswordProvider (generalise existing)

- [ ] 2.1 Move `sqe-auth/src/oidc_password.rs` (renamed from keycloak.rs in oss-security-hardening) to implement `AuthProvider` trait
- [ ] 2.2 Add configurable `roles_claim` (dot-separated JSON pointer, default `realm_access.roles`)
- [ ] 2.3 Add configurable `token_url` (no more hardwired realm path pattern)
- [ ] 2.4 Move token refresh into `refresh_catalog_token()` on the provider (not on SessionManager)
- [ ] 2.5 Unit test: ROPC flow with mock token endpoint; wrong password returns `AuthFailed`
- [ ] 2.6 Integration test: authenticate against quickstart OIDC with test users

## 3. BearerTokenProvider

- [ ] 3.1 Create `sqe-auth/src/bearer_token.rs` implementing `AuthProvider`
- [ ] 3.2 JWKS fetch + cache (moka, 15-min TTL); retry on key rotation (`kid` mismatch → refetch once)
- [ ] 3.3 JWT validation: signature, expiry, optional `aud` check
- [ ] 3.4 Claim extraction: `user_claim` → `user_id`; `roles_claim` → `roles` (string array claim)
- [ ] 3.5 Detect credential type: starts with `eyJ` or `Authorization: Bearer` header → this provider
- [ ] 3.6 `refresh_catalog_token()` → return same JWT (passthrough to catalog)
- [ ] 3.7 Unit test: valid JWT passes; expired JWT returns `AuthFailed`; unknown `kid` triggers JWKS refetch

## 4. ApiKeyProvider

- [ ] 4.1 Create `sqe-auth/src/api_key.rs` implementing `AuthProvider`
- [ ] 4.2 Load keys from TOML file (`keys_file` config); parse `key`, `description`, `groups` per entry
- [ ] 4.3 Constant-time comparison (`subtle` crate) to prevent timing attacks
- [ ] 4.4 Map groups → roles via `[auth.role_mappings]` config
- [ ] 4.5 Hot-reload: watch keys file for changes (notify crate); reload without restart
- [ ] 4.6 `refresh_catalog_token()` → return `None` (API keys use service credential for catalog auth)
- [ ] 4.7 Unit test: correct key authenticates; wrong key returns `AuthFailed`; file reload picks up new key

## 5. AnonymousProvider

- [ ] 5.1 Create `sqe-auth/src/anonymous.rs` implementing `AuthProvider`
- [ ] 5.2 Returns configured `user` + `groups` for any credentials (or no credentials)
- [ ] 5.3 Unit test: any input → fixed identity

## 6. MtlsProvider

- [ ] 6.1 Create `sqe-auth/src/mtls.rs` implementing `AuthProvider`
- [ ] 6.2 Extract CN from TLS peer certificate via tonic interceptor
- [ ] 6.3 Optional OU/SAN extraction → groups
- [ ] 6.4 Return `NotMyCredentials` if no client cert present (chain falls through)
- [ ] 6.5 Unit test: cert with known CN → correct identity; no cert → `NotMyCredentials`

## 7. Config + Wiring

- [ ] 7.1 Define `AuthProviderConfig` enum (one variant per provider type) in `sqe-core`
- [ ] 7.2 `[auth.providers]` array in TOML; deserialise into `Vec<AuthProviderConfig>`
- [ ] 7.3 Factory function: `build_auth_chain(configs) -> AuthChain`
- [ ] 7.4 Default config (no `[auth.providers]` key) builds single `OidcPasswordProvider` — backwards compat
- [ ] 7.5 Role mappings: `[auth.role_mappings]` deserialized as `HashMap<String, Vec<String>>`
- [ ] 7.6 Unit test: default config produces OIDC-only chain; explicit multi-provider config produces chain in order
