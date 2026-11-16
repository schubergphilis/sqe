## ADDED Requirements

### Requirement: Sessions of the configured client require a valid scope
When `[session.catalog_scope]` is configured, the system SHALL refuse a session whose bearer token has `azp` equal to `client_id` unless it presents a catalog scope that is signed by a key of the configured issuer, has `sub` and `jti` equal to the bearer's, and has not expired. The refusal SHALL carry a reason and SHALL be audited with the catalog names only. Tokens of other clients SHALL behave as before.

#### Scenario: Missing scope
- **WHEN** a token of the configured client opens a session without a scope
- **THEN** the session is refused with a reason and an audit record

#### Scenario: Wrong jti, expired or bad signature
- **WHEN** the scope is for another `jti`, has expired, or is signed by an unknown key
- **THEN** the session is refused the same way

#### Scenario: Other clients unaffected
- **WHEN** a token of another client opens a session without a scope
- **THEN** it behaves as before this change

### Requirement: A scoped session sees only its catalogs
In a scoped session the system SHALL treat every catalog not in the scope as not existing, SHALL list only scoped catalogs in `SHOW CATALOGS`, SHALL let `USE` and `X-Trino-Catalog` select only scoped catalogs, and SHALL refuse `ATTACH CATALOG`. Grants SHALL still apply within the scope.

#### Scenario: Out-of-scope catalog
- **GIVEN** a scope of catalogs A and B
- **WHEN** the session reads catalog C, which its grants would allow
- **THEN** it gets "catalog not found"
- **AND** `SHOW CATALOGS` lists A and B only

### Requirement: Read-only catalogs refuse every write
In a scoped session the system SHALL refuse at plan time any write into a catalog marked `read_only`, including INSERT, UPDATE, DELETE, MERGE, DDL, CALL, COPY, CTAS, write branches and transaction commits.

#### Scenario: Read-write and read-only
- **GIVEN** a scope with A `read_write` and B `read_only`
- **WHEN** the session reads A and B and writes A
- **THEN** all three succeed
- **AND** `INSERT INTO B...`, `CREATE TABLE B...` and `CALL B.system.<proc>` are refused

### Requirement: The scope follows the token
The system SHALL re-check the scope on every request and when the session's credentials rotate; a rotated token SHALL need a scope for its own `jti`. The session cache SHALL key on the scope.

#### Scenario: Token rotation
- **GIVEN** a Flight SQL session whose bearer rotates to a new `jti`
- **WHEN** the next request carries the old scope
- **THEN** it is refused until a scope for the new `jti` is sent
