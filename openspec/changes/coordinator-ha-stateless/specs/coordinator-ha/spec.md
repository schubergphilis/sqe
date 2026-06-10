## ADDED Requirements

### Requirement: Stateless session validation
The system SHALL validate the user's OIDC JWT directly on each request so that any coordinator replica can serve any client without shared session state.

#### Scenario: Any replica validates any client
- **GIVEN** `session.validation = "jwt"` and three coordinator replicas
- **AND** a client holding a valid IdP-issued JWT
- **WHEN** the client's requests are load-balanced across all three replicas
- **THEN** every replica validates the JWT and serves the request
- **AND** no replica returns `session not found`

#### Scenario: Invalid JWT rejected without leak
- **GIVEN** `session.validation = "jwt"`
- **WHEN** a request carries an expired, wrong-issuer, or bad-signature JWT
- **THEN** the request is rejected with an authentication error
- **AND** the error does not reveal which check failed

#### Scenario: Verification cache hit
- **GIVEN** a JWT validated once on a replica within its TTL
- **WHEN** the same JWT is presented again to that replica
- **THEN** the cached identity is used and the JWKS is not re-fetched

### Requirement: Result routing to the owning replica
The system SHALL route a `DoGet` for an in-flight result to the replica that planned the query.

#### Scenario: DoGet lands on a non-owning replica
- **GIVEN** a query planned on replica A, with a ticket encoding `owning_replica = A`
- **WHEN** a `DoGet` with that ticket is load-balanced to replica B
- **THEN** replica B proxies to or redirects toward replica A
- **AND** the client receives the correct result stream

#### Scenario: Owning replica fails before result is drained
- **GIVEN** an in-flight query owned by replica A
- **WHEN** replica A terminates before the result is fully drained
- **THEN** the next `DoGet` returns a retryable error
- **AND** the client may re-submit the query against any healthy replica

### Requirement: Multi-replica deployment
The system SHALL support running N coordinator replicas behind a Kubernetes Service with a PodDisruptionBudget.

#### Scenario: Rolling restart preserves availability
- **GIVEN** three coordinator replicas behind a Service and a PDB with `minAvailable = 2`
- **WHEN** one replica is restarted
- **THEN** the other two continue serving new queries
- **AND** only queries owned by the restarted replica are lost (client retries)

### Requirement: Global rate limiting across replicas
The system SHALL, when a shared rate-limit store is configured, enforce per-user and global limits across all replicas (Phase 2).

#### Scenario: Shared store enforces one global limit
- **GIVEN** two replicas configured with a shared rate-limit store
- **AND** a per-user limit of R queries per minute
- **WHEN** a user sends 2R queries per minute split across both replicas
- **THEN** roughly R succeed and the rest are rate-limited
- **AND** the limit is not effectively doubled

#### Scenario: Shared store unreachable falls open
- **GIVEN** a configured shared rate-limit store that becomes unreachable
- **WHEN** requests continue to arrive
- **THEN** the coordinator falls back to per-replica limiting and logs a warning
- **AND** authenticated requests are not failed closed
