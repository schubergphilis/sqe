## ADDED Requirements

### Requirement: Engine fetches read keys after the person's load
With `[catalogs.<name>.credentials]` set, SQE SHALL load a table with the person's token and no delegation header, and SHALL then fetch storage keys from Polaris `/credentials` with the engine's token. Unset, SQE SHALL behave as before.

#### Scenario: Metadata-only person reads through SQE
- **WHEN** a person holds only metadata privileges in Polaris and the policy engine allows `select` on `ns.t`
- **THEN** `SELECT * FROM ns.t` returns the rows, with masks and row filters applied
- **AND** the person's `loadTable` carries no `X-Iceberg-Access-Delegation` header

#### Scenario: Setting unset
- **WHEN** the catalog has no `credentials` block
- **THEN** SQE sends the delegation header with the person's token and makes no engine `/credentials` call

### Requirement: No keys without the person's load and select
With the identity set, a table SHALL be readable only through a Ranger access policy that grants the person `select` (default deny). SQE SHALL call `/credentials` only after the person's own load succeeded and the policy engine allows the person `select` on the table. A denied person SHALL get the schema and no rows, and SQE SHALL make no `/credentials` call and no object-store call for that table.

#### Scenario: Load refused
- **WHEN** the person's `loadTable` answers 403 or 404
- **THEN** the statement fails as today and no `/credentials` call is made

#### Scenario: Select denied
- **WHEN** the policy engine denies the person `select` on `ns.t`
- **THEN** `SELECT * FROM ns.t`, `SELECT count(*) FROM ns.t` and a query with a fallible predicate on `ns.t` return zero rows
- **AND** no `/credentials` call and no object-store request is made for `ns.t`

#### Scenario: No access policy at all
- **WHEN** the identity is set and no Ranger policy mentions `ns.t`
- **THEN** the person is denied `select` and reads zero rows
- **AND** a table in a catalog without the identity is read as before

#### Scenario: DML by a denied person
- **WHEN** a person without write-data in Polaris runs UPDATE or DELETE on `ns.t`
- **THEN** the statement fails on the person's delegated load (Polaris 403), as without the identity

#### Scenario: No select gate wired
- **WHEN** the identity is set and the session catalog has no select gate
- **THEN** the load fails and no `/credentials` call is made

### Requirement: Keys match the person's table
SQE SHALL take the storage credential whose prefix is the longest prefix of the location in the metadata the person loaded, and only its key entries. No match SHALL fail the statement.

#### Scenario: No prefix matches
- **WHEN** `/credentials` returns credentials for another location only
- **THEN** the statement fails and the table is not read

### Requirement: Engine failure fails closed
A failed engine fetch (token error, 403, 5xx) SHALL fail the statement. SQE SHALL NOT fall back to the person's delegation header.

#### Scenario: Engine refused
- **WHEN** `/credentials` answers 403 to the engine
- **THEN** the statement fails and no delegated `loadTable` with the person's token follows

### Requirement: Writes use the person's delegation
INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CTAS and commits SHALL load the target with the person's delegated `loadTable`. Engine keys SHALL NOT be used to write.

#### Scenario: Reader cannot write
- **WHEN** a person holds read privileges only, the engine holds write-data, and the person runs INSERT, UPDATE, DELETE, MERGE, TRUNCATE or CTAS on `ns.t`
- **THEN** each statement is refused before any data file is written

#### Scenario: Engine-keyed table reaches a writer
- **WHEN** a table loaded with engine keys is passed to the writer or the storage probe
- **THEN** the write is refused

### Requirement: Reload follows the same rule
The table reloader SHALL reload as the person and fetch read keys as the engine.

#### Scenario: Credential refresh
- **WHEN** a long scan refreshes its keys
- **THEN** the reload sends no delegation header and the keys come from an engine `/credentials` call

### Requirement: Audit and redaction
Each engine key fetch SHALL write an audit event naming the person as actor and the engine client id as `app_name`, with the table and the outcome. Keys SHALL NOT appear in logs, audit events or `Debug` output.

#### Scenario: Debug output
- **WHEN** a table's engine keys are formatted with `{:?}`
- **THEN** the output holds no key or token value

### Requirement: Startup refuses unsafe settings
Startup SHALL refuse a `credentials` block whose mode is not `client_credentials`, on a catalog with `require_vended_credentials = false`, when the policy backend has no default-deny `select` (only Ranger has one), or when `[policy.ranger] catalog_scoped` is off.

#### Scenario: Bare Ranger keys
- **WHEN** the identity is set and `catalog_scoped = false`
- **THEN** the server does not start and names the setting

#### Scenario: Passthrough policy
- **WHEN** the identity is set and the policy engine is passthrough
- **THEN** the server does not start and names the setting

### Requirement: Secret rotation without restart
A `client_credentials` block SHALL take its secret from exactly one of `client_secret` and `client_secret_file`. A secret file SHALL be read on every token request, so a rotated secret is used from the next request on without a restart, and the first token minted with a rotated engine secret SHALL write one audit event that holds no secret material. A wrong or unreadable secret file SHALL fail the token request with no fallback to an earlier secret.

#### Scenario: Secret rotated at the identity provider
- **WHEN** the operator rotates the engine principal's secret and replaces the secret file
- **THEN** the next token request sends the new secret, reads keep working, and one `engine_secret_rotated` audit event is written

#### Scenario: Both sources or an unreadable file
- **WHEN** a block sets both `client_secret` and `client_secret_file`, or the file is missing or empty at startup
- **THEN** the server does not start and names the block and the file
