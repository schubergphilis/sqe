## ADDED Requirements

### Requirement: Interactive SQL REPL
The `sqe-cli` binary SHALL provide an interactive SQL REPL that connects to an `sqe-server` coordinator via Flight SQL.

#### Scenario: Start interactive session
- **WHEN** `sqe-cli --host <coordinator> --port <port>` is run without a query argument
- **THEN** it SHALL present an interactive prompt where the user can type SQL statements and see results

#### Scenario: Execute single query
- **WHEN** `sqe-cli --host <coordinator> --port <port> -e "SELECT 1"` is run
- **THEN** it SHALL execute the query, print the result to stdout, and exit

### Requirement: Connection target
The `sqe-cli` binary SHALL accept the coordinator address via `--host` and `--port` flags, with defaults of `localhost` and `8080`.

#### Scenario: Default connection
- **WHEN** `sqe-cli` is run without `--host` or `--port`
- **THEN** it SHALL attempt to connect to `localhost:8080`

#### Scenario: Custom connection
- **WHEN** `sqe-cli --host sqe-coordinator.svc --port 9090` is run
- **THEN** it SHALL connect to `sqe-coordinator.svc:9090`

### Requirement: Authentication support
The `sqe-cli` SHALL support passing a bearer token or prompting for Keycloak credentials for authenticated connections.

#### Scenario: Token flag
- **WHEN** `sqe-cli --token <jwt>` is provided
- **THEN** it SHALL use the token for Flight SQL authentication

#### Scenario: Username/password prompt
- **WHEN** `sqe-cli --user <username>` is provided without `--token`
- **THEN** it SHALL prompt for a password and perform Keycloak OIDC password grant to obtain a token

### Requirement: Output formatting
The `sqe-cli` SHALL format query results as aligned ASCII tables by default, with optional output formats.

#### Scenario: Default table output
- **WHEN** a query returns results and no `--format` flag is set
- **THEN** results SHALL be displayed as an aligned ASCII table with column headers

#### Scenario: CSV output
- **WHEN** `--format csv` is specified
- **THEN** results SHALL be printed as comma-separated values

#### Scenario: JSON output
- **WHEN** `--format json` is specified
- **THEN** results SHALL be printed as newline-delimited JSON objects

### Requirement: Version display and mismatch warning
The `sqe-cli` SHALL display its own version and the server version on connect.

#### Scenario: Version match
- **WHEN** `sqe-cli` connects and client and server versions match
- **THEN** it SHALL display the version in the connection banner

#### Scenario: Version mismatch
- **WHEN** `sqe-cli` connects and client version differs from server version
- **THEN** it SHALL display a warning indicating the version mismatch
