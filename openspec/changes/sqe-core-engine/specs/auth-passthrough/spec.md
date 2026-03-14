## ADDED Requirements

### Requirement: Keycloak token acquisition via ROPC
The system SHALL authenticate Flight SQL clients using username/password by exchanging credentials with Keycloak's token endpoint via Resource Owner Password Credentials grant.

#### Scenario: Successful authentication
- **GIVEN** a running Keycloak with realm `iceberg` and client `sqe-client`
- **WHEN** a client connects with valid username/password
- **THEN** an access_token and refresh_token are obtained from Keycloak
- **AND** a Session is created carrying the user identity and tokens

#### Scenario: Invalid credentials
- **GIVEN** a running Keycloak
- **WHEN** a client connects with invalid username/password
- **THEN** authentication fails with an error
- **AND** no session is created

### Requirement: Token caching and refresh
The system SHALL cache access tokens per session and refresh them before expiry using the refresh token.

#### Scenario: Token refresh before expiry
- **GIVEN** an active session with a token expiring in less than 60 seconds
- **WHEN** the background refresh task runs
- **THEN** the token is refreshed using the refresh_token
- **AND** the session carries the new access_token

#### Scenario: Refresh token expired
- **GIVEN** an active session whose refresh_token has expired
- **WHEN** a refresh is attempted
- **THEN** the session is marked expired
- **AND** the client receives an authentication error prompting reconnection

### Requirement: Bearer token propagation to Polaris
The system SHALL include the user's access_token as a Bearer token in every Polaris REST catalog call.

#### Scenario: Catalog call with bearer token
- **GIVEN** an authenticated session
- **WHEN** a catalog operation is performed (list tables, load table, etc.)
- **THEN** the HTTP request includes `Authorization: Bearer {access_token}`
- **AND** Polaris authenticates the request as the end user

### Requirement: Token propagation to workers
The system SHALL propagate bearer tokens and vended S3 credentials to workers via Arrow Flight metadata headers.

#### Scenario: Distributed query credential propagation
- **GIVEN** a distributed query assigned to workers
- **WHEN** the coordinator sends plan fragments to a worker
- **THEN** the Flight metadata includes the bearer_token and vended_s3_creds
- **AND** the worker uses these credentials for Polaris and S3 access

#### Scenario: Long-running query token refresh
- **GIVEN** a distributed query outliving the access_token lifetime
- **WHEN** the coordinator refreshes the token
- **THEN** updated credentials are pushed to workers
- **AND** workers use the new credentials for subsequent operations
