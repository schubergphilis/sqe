## 1. RDF Triple Store on Iceberg (sqe-semantic)

- [ ] 1.1 Define `rdf.triples` table schema as a documented convention; add `CREATE TABLE` template to quickstart
- [ ] 1.2 Add `sqe-semantic` crate to workspace with `spargebra` + `rdf-fusion` dependencies
- [ ] 1.3 Implement `SparqlCompiler::compile(query: &str) -> Result<LogicalPlan>` via spargebra + rdf-fusion bridge
- [ ] 1.4 Wire SPARQL dialect detection into `QueryRouter`: if input starts with `SELECT ?` or `CONSTRUCT` or `ASK` or `DESCRIBE` → SPARQL path
- [ ] 1.5 Register compiled LogicalPlan into coordinator's existing execution path (same as SQL)
- [ ] 1.6 Unit tests: BGP with 1 triple pattern; BGP with 3 patterns; FILTER; LIMIT/OFFSET
- [ ] 1.7 Integration test: populate rdf.triples via SQL INSERT, query with SPARQL, verify results match

## 2. Property Graph / ISO GQL (sqe-semantic)

- [ ] 2.1 Define `graph.nodes` + `graph.edges` Iceberg table schemas as documented convention
- [ ] 2.2 Add `graphlite` dependency (embedded ISO GQL engine)
- [ ] 2.3 Implement `GqlBridge::detect(input: &str) -> bool`: detect ISO GQL syntax (`MATCH`, `RETURN`, `CREATE GRAPH`)
- [ ] 2.4 Mode A: load graph.nodes + graph.edges into graphlite in-memory, execute GQL, return Arrow
- [ ] 2.5 Mode B: compile MATCH patterns to DataFusion recursive CTEs (single-hop and multi-hop)
- [ ] 2.6 Mode selection: check `graph.nodes` row count vs `[semantic] gql_memory_threshold` config
- [ ] 2.7 Unit tests: MATCH single hop; MATCH multi-hop (depth 3); MATCH with property filter; cycle guard fires
- [ ] 2.8 Integration test: ingest graph data, run GQL MATCH, verify traversal result

## 3. Vector Search (sqe-vector)

- [ ] 3.1 Create `sqe-vector` crate with `lance` + `lance-datafusion` dependencies (feature flag `vector`)
- [ ] 3.2 Implement `LanceScanExec`: DataFusion ExecutionPlan reading Lance datasets from object storage
- [ ] 3.3 Register `lance_scan(path)` TVF in SessionContext (same pattern as `iceberg_scan`)
- [ ] 3.4 Implement `vec_distance(embedding_col, query_vec, metric)` DataFusion scalar UDF (cosine, l2, dot)
- [ ] 3.5 Implement `embed(text)` async UDF: HTTP POST to configured `[vector] embedding_url`, cache by SHA256(text)
- [ ] 3.6 Unit tests: `vec_distance` returns correct values for known vectors; `lance_scan` reads test dataset
- [ ] 3.7 Integration test: ingest text + embeddings to Lance, run hybrid SQL+vector query with LATERAL join

## 4. Semantic Search (sqe-semantic)

- [ ] 4.1 Implement `SemanticIndex`: on startup, load `rdf.triples` + table/column metadata into in-memory BM25 index (tantivy crate)
- [ ] 4.2 Implement `SemanticIndex::search(query: &str) -> Vec<SemanticHit>` (table names, column names, ontology concepts, relevance score)
- [ ] 4.3 Refresh index on `SHOW TABLES` or configurable interval (default 5 min)
- [ ] 4.4 Unit test: search over synthetic metadata returns expected tables ranked correctly

## 5. Rich CLI (sqe-cli)

- [ ] 5.1 Add `sqe schema search <query>` subcommand → calls SemanticIndex, outputs JSON array of hits
- [ ] 5.2 Add `sqe schema describe <table>` subcommand → columns, sample values, ontology links from rdf.triples
- [ ] 5.3 Add `sqe schema relationships <table>` → graph.edges where src or dst matches table name
- [ ] 5.4 Add `sqe schema ontology <concept>` → SPARQL query on rdf.triples for concept
- [ ] 5.5 Add `sqe explore "<question>"` → calls SemanticIndex + ontology + returns suggested SQL/SPARQL templates
- [ ] 5.6 Add `--output json|arrow|csv|table` flag to `sqe query`; default `table` for TTY, `json` when piped
- [ ] 5.7 Add `--describe` flag to all commands: outputs JSON tool description (name, description, parameters, example)
- [ ] 5.8 Ensure all `--help` text is agent-readable: concrete examples, output format descriptions, no jargon
- [ ] 5.9 Unit tests: each subcommand produces valid output in each `--output` format

## 6. REST / OpenAPI Server (sqe-rest)

- [ ] 6.1 Create `sqe-rest` crate (axum); optional binary `sqe-server-http` (separate from Flight SQL binary)
- [ ] 6.2 Implement `POST /api/v1/query` → detect dialect, route to SQL/SPARQL/GQL executor, return JSON or Arrow IPC
- [ ] 6.3 Implement `GET /api/v1/schema/tables` → list all tables with description from rdf.triples
- [ ] 6.4 Implement `GET /api/v1/schema/tables/{name}` → columns, sample, ontology links
- [ ] 6.5 Implement `GET /api/v1/schema/search?q=...` → SemanticIndex search results
- [ ] 6.6 Implement `POST /api/v1/explore` → question → suggested queries (uses SemanticIndex + LLM prompt template)
- [ ] 6.7 Implement `GET /api/v1/openapi.json` → serve OpenAPI 3.1 spec (generated via `utoipa` crate)
- [ ] 6.8 Auth: same bearer token as Flight SQL (shared SessionManager)
- [ ] 6.9 Integration test: POST query via REST, verify Arrow IPC response decodes correctly

## 7. MCP Server (sqe-mcp)

- [ ] 7.1 Create `sqe-mcp` crate: thin MCP protocol handler (stdio transport per MCP spec)
- [ ] 7.2 Map MCP `tools/call` → REST API calls to `sqe-rest`
- [ ] 7.3 Expose tools: `execute_query`, `search_schema`, `describe_table`, `explore`
- [ ] 7.4 Generate tool descriptions from OpenAPI spec (`/api/v1/openapi.json`) at startup
- [ ] 7.5 Integration test: MCP client sends tool call, receives correct response

## 8. TypeScript Client (@sqe/client)

- [ ] 8.1 Create `packages/sqe-client/` npm package (TypeScript, ESM + CJS dual output)
- [ ] 8.2 Implement `RestTransport`: fetch-based, JSON→Apache Arrow decoding via `apache-arrow` npm
- [ ] 8.3 Implement `FlightTransport`: `@grpc/grpc-js` + `@grpc/proto-loader` loading Arrow Flight SQL proto files; returns Arrow RecordBatches
- [ ] 8.4 Implement `SqeClient`: auto-selects FlightTransport (Node.js) or RestTransport (browser) based on env
- [ ] 8.5 Expose `client.query(sql, options)`, `client.searchSchema(q)`, `client.describeTable(name)`, `client.explore(question)`
- [ ] 8.6 Type-safe return: `RecordBatch[]` from `apache-arrow` for query results
- [ ] 8.7 Unit tests (Vitest): REST transport against mock server; Arrow decoding of known IPC bytes
- [ ] 8.8 Integration test (Node.js): connect to running SQE, run query, verify result schema

## 9. Config

- [ ] 9.1 Add `[semantic]` config section: `enabled`, `triples_table`, `nodes_table`, `edges_table`, `gql_memory_threshold`
- [ ] 9.2 Add `[vector]` config section: `enabled`, `embedding_url`, `embedding_cache_size`, `default_metric`
- [ ] 9.3 Add `[rest_api]` config section: `bind`, `enabled`, `cors_origins`
- [ ] 9.4 Startup: if `rdf.triples` table exists and `[semantic]` not explicitly disabled → auto-enable semantic search
- [ ] 9.5 Unit test: each new config section deserialises from TOML example; unknown keys log WARN
