## ADDED Requirements

### Requirement: AuthProvider trait — provider chain
The system SHALL support an ordered chain of auth providers; the first to return a valid identity wins.

#### Scenario: First provider succeeds
- **GIVEN** a chain of [BearerTokenProvider, OidcPasswordProvider]
- **WHEN** a client sends a valid JWT as credential
- **THEN** BearerTokenProvider validates the JWT and returns an identity
- **AND** OidcPasswordProvider is never called

#### Scenario: First provider declines, second succeeds
- **GIVEN** a chain of [BearerTokenProvider, OidcPasswordProvider]
- **WHEN** a client sends username+password (not a JWT)
- **THEN** BearerTokenProvider returns `NotMyCredentials`
- **AND** OidcPasswordProvider performs ROPC and returns an identity

#### Scenario: All providers fail
- **GIVEN** a chain of [BearerTokenProvider, OidcPasswordProvider]
- **WHEN** a client sends invalid username+password
- **THEN** both providers return failure
- **AND** authentication fails with `UNAUTHENTICATED`

### Requirement: OidcPasswordProvider — generalised OIDC
The system SHALL authenticate clients via any OIDC-compliant IdP using the ROPC grant.

#### Scenario: Successful authentication
- **GIVEN** `token_url`, `client_id` are configured
- **WHEN** a client sends valid username+password
- **THEN** ROPC grant is performed against `token_url`
- **AND** the identity contains `user_id` from JWT `sub` claim
- **AND** roles are extracted from the configured `roles_claim` path

#### Scenario: Configurable roles claim
- **GIVEN** `roles_claim = "groups"` is configured
- **WHEN** a JWT contains `{ "groups": ["analyst", "reader"] }`
- **THEN** the identity has `roles = ["analyst", "reader"]`

### Requirement: BearerTokenProvider — pre-obtained JWT
The system SHALL accept a pre-obtained JWT without performing ROPC.

#### Scenario: Valid JWT in password field
- **GIVEN** BearerTokenProvider with valid JWKS endpoint
- **WHEN** a client sends a valid, unexpired JWT in the password field
- **THEN** the JWT is validated against JWKS
- **AND** the identity is returned without any IdP call

#### Scenario: Expired JWT
- **GIVEN** BearerTokenProvider is configured
- **WHEN** a client sends an expired JWT
- **THEN** authentication fails with `UNAUTHENTICATED`

#### Scenario: JWKS key rotation
- **GIVEN** an IdP rotates its signing keys
- **WHEN** a JWT signed with the new key is presented
- **THEN** BearerTokenProvider detects the unknown `kid`
- **AND** fetches updated JWKS once
- **AND** validates successfully with the new key

### Requirement: ApiKeyProvider — group-based opaque keys
The system SHALL authenticate clients using opaque API keys defined in an external config file.

#### Scenario: Valid API key
- **GIVEN** `api-keys.toml` contains a key with `groups = ["bi-reader"]`
- **WHEN** a client authenticates with that key
- **THEN** the identity has roles derived from `bi-reader`'s role mappings

#### Scenario: Invalid API key
- **GIVEN** ApiKeyProvider is configured
- **WHEN** a client sends an unknown key
- **THEN** authentication fails with `UNAUTHENTICATED`
- **AND** response time is constant (timing-safe comparison)

#### Scenario: Hot reload
- **GIVEN** a running coordinator with ApiKeyProvider
- **WHEN** a new key is added to the keys file
- **THEN** the new key is accepted without restart within one reload cycle

### Requirement: AnonymousProvider
The system SHALL support a no-credentials mode for trusted network / dev deployments.

#### Scenario: Anonymous access
- **GIVEN** AnonymousProvider is the sole provider
- **WHEN** any client connects (with or without credentials)
- **THEN** the identity is the configured fixed user + groups

### Requirement: Role mappings
The system SHALL map groups to roles via global configuration.

#### Scenario: Group-to-role mapping
- **GIVEN** `[auth.role_mappings] "data-engineering" = ["writer", "reader"]`
- **WHEN** a user authenticates with group `data-engineering`
- **THEN** the identity has roles `["writer", "reader"]`

#### Scenario: Unknown group
- **GIVEN** a user authenticates with a group not in role_mappings
- **WHEN** the identity is constructed
- **THEN** roles for that group default to `[]` (empty, no error)
