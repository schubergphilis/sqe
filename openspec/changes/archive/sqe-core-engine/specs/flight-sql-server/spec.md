## ADDED Requirements

### Requirement: Arrow Flight SQL server
The system SHALL expose an Arrow Flight SQL server on the coordinator for JDBC client connectivity.

#### Scenario: Flight SQL connection with authentication
- **WHEN** a JDBC client connects via Arrow Flight SQL with username/password
- **THEN** the coordinator authenticates via Keycloak ROPC
- **AND** returns a session token for subsequent requests

#### Scenario: Execute query via Flight SQL
- **GIVEN** an authenticated Flight SQL session
- **WHEN** a client submits a SQL query
- **THEN** the query is executed and results are returned as Arrow RecordBatches

### Requirement: Flight SQL metadata calls
The system SHALL support Flight SQL metadata operations: getCatalogs, getSchemas, getTables, getTableTypes.

#### Scenario: Client browses schema
- **GIVEN** an authenticated Flight SQL session
- **WHEN** a client calls getTables for a schema
- **THEN** a list of tables accessible to the user is returned
