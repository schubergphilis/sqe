## ADDED Requirements

### Requirement: CLI is invocable by any agent framework
The CLI MUST be the primary AI agent interface — usable without MCP, without protocol negotiation, by any framework that can invoke a subprocess.

#### Scenario: Agent invokes sqe query via subprocess
- **GIVEN** an AI agent with subprocess execution capability
- **WHEN** it runs `sqe query "SELECT * FROM orders LIMIT 5" --output json`
- **THEN** output is newline-delimited JSON rows, one per result row
- **AND** exit code is 0 on success, non-zero on error
- **AND** errors go to stderr, results go to stdout

#### Scenario: Agent introspects command capabilities
- **GIVEN** an AI agent exploring available tools
- **WHEN** it runs `sqe schema search --describe`
- **THEN** a JSON object is returned describing: command name, description, parameters with types, example invocations, example output
- **AND** the agent can use this description without reading documentation

#### Scenario: Piped output auto-selects machine format
- **GIVEN** `sqe query "SELECT ..." | jq .`
- **WHEN** stdout is not a TTY (piped)
- **THEN** output defaults to newline-delimited JSON without `--output` flag

### Requirement: Schema search returns agent-usable results
The system SHALL return semantic search results in a structured, agent-usable format.

#### Scenario: Search for concept
- **GIVEN** tables exist with ontology metadata in rdf.triples
- **WHEN** `sqe schema search "customer purchasing behaviour"` is run
- **THEN** output lists matching tables/columns with: name, description (from ontology), relevance score, example query
- **AND** output is valid JSON with `--output json`

### Requirement: REST API is self-describing via OpenAPI
The REST API MUST expose a complete OpenAPI 3.1 specification that an LLM can read and use.

#### Scenario: Agent fetches API spec
- **GIVEN** sqe-rest server running
- **WHEN** `GET /api/v1/openapi.json` is called
- **THEN** a valid OpenAPI 3.1 document is returned
- **AND** every endpoint has: description, parameter types, request body schema, response schema, example

#### Scenario: Multi-dialect query via REST
- **GIVEN** the REST API
- **WHEN** `POST /api/v1/query` with `{"query": "SELECT ?c WHERE { ?c rdf:type :Customer }", "dialect": "auto"}`
- **THEN** SPARQL is detected, executed, result returned as JSON or Arrow IPC (per `Accept` header)

### Requirement: MCP is a thin REST wrapper
MCP tools MUST be generated from the OpenAPI spec, not hand-coded.

#### Scenario: MCP tool list matches OpenAPI operations
- **GIVEN** an MCP client connecting to sqe-mcp
- **WHEN** `tools/list` is called
- **THEN** tools returned correspond 1:1 to REST API endpoints documented in OpenAPI
