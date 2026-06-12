## ADDED Requirements

### Requirement: Namespace names are scoped to the caller's grants
For REST/Polaris-backed catalogs, the system SHALL only include a namespace in metadata listings when the calling user's bearer is authorized to load that namespace's metadata (Polaris `LOAD_NAMESPACE_METADATA` allows). A 403 from the catalog SHALL hide the name; other probe failures SHALL fail open (name stays visible, contents remain protected by per-operation checks).

#### Scenario: SHOW SCHEMAS hides ungranted namespaces
- **GIVEN** user `team-a-dev` holds grants in `team_a_data.public` and none in `team_a_data.limited`
- **WHEN** the user runs `SHOW SCHEMAS FROM team_a_data`
- **THEN** the result contains `public` and `information_schema`
- **AND** does not contain `limited`

#### Scenario: information_schema agrees with SHOW SCHEMAS
- **GIVEN** the same user
- **WHEN** the user queries `SELECT schema_name FROM team_a_data.information_schema.schemata`
- **THEN** the rows match the `SHOW SCHEMAS` result exactly

#### Scenario: Flight SQL metadata agrees
- **GIVEN** the same user connected over Flight SQL
- **WHEN** the client issues `GetDbSchemas` for the catalog
- **THEN** the returned schemas match the `SHOW SCHEMAS` result exactly

#### Scenario: Privileged callers are unaffected
- **GIVEN** user `team-a-admin` holds a grant covering `team_a_data.limited`
- **WHEN** the user runs `SHOW SCHEMAS FROM team_a_data`
- **THEN** the result contains both `public` and `limited`

#### Scenario: Catalog hiccup fails open
- **GIVEN** the visibility probe for a namespace fails with a timeout or 5xx (not 403)
- **WHEN** the session's catalog provider is built
- **THEN** that namespace name remains listed
- **AND** access to its contents is still enforced per operation

### Requirement: Visibility filtering is configurable and backend-aware
The system SHALL gate the filter behind a catalog config flag `namespace_visibility_filter` (default `true`) and SHALL skip filtering for single-identity backends (Glue/HMS/JDBC/Hadoop), where no per-caller identity reaches the catalog.

#### Scenario: Flag off restores listing behavior
- **GIVEN** `namespace_visibility_filter = false` for a REST catalog
- **WHEN** any user runs `SHOW SCHEMAS`
- **THEN** the full `listNamespaces` result is shown (pre-change behavior)

#### Scenario: Single-identity backend skips probes
- **GIVEN** a Glue-backed catalog
- **WHEN** a session's catalog provider is built
- **THEN** no visibility probes are issued and all namespaces are listed
