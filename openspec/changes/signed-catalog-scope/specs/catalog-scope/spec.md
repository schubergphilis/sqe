## ADDED Requirements

### Requirement: Sessions of the configured client require a valid scope
When `[session.catalog_scope]` is configured, the system SHALL refuse a session whose bearer token has `azp` equal to `client_id` unless it presents a catalog scope that meets all of these conditions:
- it is signed by a key of the configured issuer;
- its `sub` and `jti` equal the bearer's;
- it has not expired.

The refusal SHALL carry a reason and SHALL be audited with the catalog names only. The requirement SHALL hold on every protocol, including those that cannot carry a scope. Tokens of other clients SHALL behave as before.

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
In a scoped session the system SHALL:
- treat every catalog not in the scope as not existing;
- list only scoped catalogs in `SHOW CATALOGS`;
- let `USE` and `X-Trino-Catalog` select only scoped catalogs;
- refuse `ATTACH`, `DETACH`, `CREATE SECRET`, `DROP SECRET` and DCL.

Grants SHALL still apply within the scope.

#### Scenario: Out-of-scope catalog
- **GIVEN** a scope of catalogs A and B
- **WHEN** the session reads catalog C, which its grants would allow
- **THEN** it gets "catalog not found"
- **AND** `SHOW CATALOGS` lists A and B only

### Requirement: Read-only catalogs refuse every write
In a scoped session the system SHALL refuse, before any commit, every write into a catalog marked `read_only`: INSERT, UPDATE, DELETE, MERGE, DDL, CALL, COPY, CTAS, write branches and transaction commits.

#### Scenario: Read-write and read-only
- **GIVEN** a scope with A `read_write` and B `read_only`
- **WHEN** the session reads A and B and writes A
- **THEN** all three succeed
- **AND** `INSERT INTO B...`, `CREATE TABLE B...` and `CALL B.system.<proc>` are refused

### Requirement: The scope follows the token
The system SHALL check the scope on every request. A rotated token SHALL need a scope for its own `jti`. The session context cache and the query result cache SHALL key on the scope when one is present, and SHALL keep their keys unchanged when none is.

#### Scenario: Token rotation
- **GIVEN** a Flight SQL session whose bearer rotates to a new `jti`
- **WHEN** the next request carries the old scope
- **THEN** it is refused until a scope for the new `jti` is sent

### Requirement: The user stays the identity end to end
A scoped session SHALL pass the same bearer (or the user's exchanged token) to Polaris and S3 as an unscoped session of the same user. The scope SHALL NOT introduce a service identity.

#### Scenario: Polaris sees the person
- **GIVEN** a scoped session of user U through the configured client
- **WHEN** it reads catalog A
- **THEN** Polaris receives U's token and authorizes U
