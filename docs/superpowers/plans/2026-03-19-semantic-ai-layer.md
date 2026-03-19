# Semantic AI Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a unified semantic layer to SQE: SPARQL on Iceberg triple tables, ISO GQL property graph queries, Lance vector search, a rich agent-friendly CLI, REST/OpenAPI server, MCP wrapper, and a TypeScript npm client.

**Architecture:** All query dialects (SQL/SPARQL/GQL) compile to DataFusion LogicalPlan. Storage stays Iceberg + Lance on object storage. CLI is the primary AI agent interface (subprocess-invocable by any framework). REST/OpenAPI is secondary. MCP is a thin REST wrapper — not a separate implementation. TypeScript client auto-selects Flight SQL (Node) or REST (browser).

**Tech Stack:** Rust — spargebra (SPARQL parser), rdf-fusion (SPARQL→DataFusion), graphlite (ISO GQL embedded), lance + lance-datafusion (vector), tantivy (BM25 semantic index), axum (REST), utoipa (OpenAPI gen). TypeScript — apache-arrow (npm), @grpc/grpc-js (Flight SQL transport).

**Spec:** `openspec/changes/semantic-ai-layer/`

**Prerequisites:** pluggable-catalogs (StorageConfig), oss-security-hardening (TLS/auth), pluggable-auth (AuthProvider)

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `crates/sqe-semantic/src/sparql.rs` | create | SPARQL→DataFusion compiler |
| `crates/sqe-semantic/src/gql.rs` | create | ISO GQL bridge (graphlite + DataFusion) |
| `crates/sqe-semantic/src/router.rs` | create | dialect detection + routing |
| `crates/sqe-semantic/src/search.rs` | create | BM25 semantic index (tantivy) |
| `crates/sqe-vector/src/lance.rs` | create | LanceScanExec + lance_scan TVF |
| `crates/sqe-vector/src/udf.rs` | create | vec_distance(), embed() UDFs |
| `crates/sqe-rest/src/routes.rs` | create | axum routes for REST API |
| `crates/sqe-rest/src/openapi.rs` | create | utoipa OpenAPI spec generation |
| `crates/sqe-mcp/src/server.rs` | create | MCP stdio transport + tool mapping |
| `crates/sqe-cli/src/schema.rs` | create | `sqe schema` subcommands |
| `crates/sqe-cli/src/explore.rs` | create | `sqe explore` subcommand |
| `crates/sqe-cli/src/output.rs` | modify | --output json|arrow|csv|table |
| `packages/sqe-client/src/rest.ts` | create | TypeScript REST transport |
| `packages/sqe-client/src/flight.ts` | create | TypeScript Flight SQL transport |
| `packages/sqe-client/src/index.ts` | create | SqeClient unified entry point |

---

### Task 1: SPARQL compiler (spargebra → DataFusion)

**Files:**
- Create: `crates/sqe-semantic/Cargo.toml` (new crate)
- Create: `crates/sqe-semantic/src/sparql.rs`
- Test: `crates/sqe-semantic/tests/sparql_test.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-semantic/tests/sparql_test.rs
use sqe_semantic::sparql::SparqlCompiler;
use datafusion::prelude::*;

#[tokio::test]
async fn single_bgp_pattern_compiles_to_join() {
    // Create a DataFusion context with a mock rdf.triples table
    let ctx = SessionContext::new();
    register_mock_triples(&ctx, vec![
        ("http://alice", "rdf:type", "http://Customer", "default"),
        ("http://bob",   "rdf:type", "http://Order",    "default"),
    ]).await;

    let compiler = SparqlCompiler::new(ctx.clone(), "rdf.triples");
    let plan = compiler.compile("SELECT ?s WHERE { ?s <rdf:type> <http://Customer> }").unwrap();
    let result = ctx.execute_logical_plan(plan).await.unwrap();
    let batches = result.collect().await.unwrap();
    let rows: Vec<_> = batches.iter().flat_map(|b| arrow::array::as_string_array(b.column(0)).iter()).collect();
    assert_eq!(rows, vec![Some("http://alice")]);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-semantic sparql_test 2>&1 | head -20`
Expected: compile error — crate does not exist

- [ ] **Step 3: Create crate + add dependencies**
```toml
# crates/sqe-semantic/Cargo.toml
[dependencies]
spargebra = "0.4"
rdf-fusion = { git = "https://github.com/tobixdev/rdf-fusion" }
datafusion = { workspace = true }
async-trait = { workspace = true }
```

- [ ] **Step 4: Implement SparqlCompiler**
```rust
// crates/sqe-semantic/src/sparql.rs
use spargebra::{Query, algebra::*};
use datafusion::logical_expr::*;

pub struct SparqlCompiler {
    ctx: SessionContext,
    triples_table: String,
}

impl SparqlCompiler {
    pub fn compile(&self, sparql: &str) -> Result<LogicalPlan> {
        let query = Query::parse(sparql, None)?;
        match query {
            Query::Select { pattern, .. } => self.compile_pattern(pattern),
            _ => Err(SqeError::UnsupportedSparqlForm),
        }
    }

    fn compile_pattern(&self, pattern: GraphPattern) -> Result<LogicalPlan> {
        // Use rdf-fusion's LogicalPlan builder or hand-compile BGP → joins
        rdf_fusion::compile_pattern(&self.ctx, &self.triples_table, pattern)
    }
}
```

- [ ] **Step 5: Run test**

Run: `cargo test -p sqe-semantic sparql_test 2>&1`
Expected: passes

- [ ] **Step 6: Test multi-pattern BGP**
```rust
#[tokio::test]
async fn multi_pattern_bgp_intersects_correctly() {
    // register triples: alice is Customer + VIP; bob is Customer only
    // SPARQL: ?s rdf:type Customer ; hasSegment VIP
    // expect: only alice
}
```

- [ ] **Step 7: Run both tests, commit**
```bash
git add crates/sqe-semantic/
git commit -m "feat(semantic): SPARQL 1.1 compiler to DataFusion LogicalPlan"
```

---

### Task 2: Dialect router

**Files:**
- Create: `crates/sqe-semantic/src/router.rs`
- Modify: `crates/sqe-coordinator/src/executor.rs`
- Test: `crates/sqe-semantic/tests/router_test.rs`

- [ ] **Step 1: Write failing test**
```rust
use sqe_semantic::router::{QueryDialect, detect_dialect};

#[test]
fn detects_sparql_select() {
    assert_eq!(detect_dialect("SELECT ?s WHERE { ?s rdf:type :X }"), QueryDialect::Sparql);
}
#[test]
fn detects_gql_match() {
    assert_eq!(detect_dialect("MATCH (n:Customer) RETURN n"), QueryDialect::Gql);
}
#[test]
fn detects_sql_select() {
    assert_eq!(detect_dialect("SELECT id, name FROM customers"), QueryDialect::Sql);
}
```

- [ ] **Step 2: Implement detect_dialect**
```rust
pub fn detect_dialect(input: &str) -> QueryDialect {
    let trimmed = input.trim_start();
    // SPARQL: SELECT ?var or CONSTRUCT { or ASK { or DESCRIBE
    if trimmed.starts_with("SELECT ?") || trimmed.starts_with("select ?")
       || ["CONSTRUCT", "ASK", "DESCRIBE"].iter().any(|k| trimmed.starts_with(k)) {
        return QueryDialect::Sparql;
    }
    // ISO GQL: MATCH keyword before FROM/WHERE (simplified heuristic)
    if trimmed.to_uppercase().starts_with("MATCH ") {
        return QueryDialect::Gql;
    }
    QueryDialect::Sql
}
```

- [ ] **Step 3: Wire into coordinator executor**

In `executor.rs`: before SQL parsing, call `detect_dialect()`. Route to `SparqlCompiler` or `GqlBridge` accordingly.

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-semantic router_test 2>&1`
Expected: passes

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-semantic/src/router.rs crates/sqe-coordinator/src/executor.rs
git commit -m "feat(semantic): dialect detection router (SQL/SPARQL/GQL)"
```

---

### Task 3: ISO GQL bridge (graphlite)

**Files:**
- Create: `crates/sqe-semantic/src/gql.rs`
- Test: `crates/sqe-semantic/tests/gql_test.rs`

- [ ] **Step 1: Write failing test**
```rust
#[tokio::test]
async fn simple_match_returns_nodes() {
    let ctx = SessionContext::new();
    register_mock_graph(&ctx).await; // graph.nodes + graph.edges with test data

    let bridge = GqlBridge::new(ctx.clone(), GqlMode::Auto { threshold: 10_000_000 });
    let result = bridge.execute("MATCH (c:Customer) RETURN c.properties->>'name' AS name").await.unwrap();
    let batches = result.collect().await.unwrap();
    assert!(!batches.is_empty());
}
```

- [ ] **Step 2: Add graphlite dependency**
```toml
graphlite = "0.1"  # check current version on crates.io
```

- [ ] **Step 3: Implement GqlBridge Mode A (in-memory)**
```rust
pub struct GqlBridge { ctx: SessionContext, mode: GqlMode }

impl GqlBridge {
    pub async fn execute(&self, gql: &str) -> Result<SendableRecordBatchStream> {
        let node_count = self.count_nodes().await?;
        if node_count < self.mode.threshold() {
            self.execute_in_memory(gql).await  // Mode A: graphlite
        } else {
            self.compile_to_datafusion(gql).await  // Mode B: recursive CTE
        }
    }

    async fn execute_in_memory(&self, gql: &str) -> Result<SendableRecordBatchStream> {
        // Load graph.nodes + graph.edges into graphlite::Graph
        // Run gql via graphlite, convert result to Arrow RecordBatches
        todo!()
    }
}
```

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-semantic gql_test 2>&1`
Expected: passes (Mode A path)

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-semantic/src/gql.rs
git commit -m "feat(semantic): ISO GQL bridge via graphlite (Mode A in-memory)"
```

---

### Task 4: Lance vector search

**Files:**
- Create: `crates/sqe-vector/Cargo.toml`
- Create: `crates/sqe-vector/src/lance.rs`
- Create: `crates/sqe-vector/src/udf.rs`
- Test: `crates/sqe-vector/tests/vector_test.rs`

- [ ] **Step 1: Write failing tests**
```rust
#[test]
fn vec_distance_cosine_known_vectors() {
    // identical vectors → distance 0.0 (or 1.0 similarity depending on convention)
    let a = vec![1.0_f32, 0.0, 0.0];
    let b = vec![1.0_f32, 0.0, 0.0];
    let dist = cosine_distance(&a, &b);
    assert!((dist - 0.0).abs() < 1e-6);
}

#[test]
fn vec_distance_orthogonal_vectors() {
    let a = vec![1.0_f32, 0.0];
    let b = vec![0.0_f32, 1.0];
    let dist = cosine_distance(&a, &b);
    assert!((dist - 1.0).abs() < 1e-6); // fully dissimilar
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-vector vector_test 2>&1`
Expected: crate not found

- [ ] **Step 3: Add dependencies**
```toml
# crates/sqe-vector/Cargo.toml
[features]
default = []
vector = ["lance", "lance-datafusion"]

[dependencies]
lance = { version = "0.18", optional = true }
lance-datafusion = { version = "0.18", optional = true }
datafusion = { workspace = true }
```

- [ ] **Step 4: Implement vec_distance UDF**
```rust
// crates/sqe-vector/src/udf.rs
pub fn vec_distance_udf() -> ScalarUDF {
    create_udf(
        "vec_distance",
        vec![DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
             DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
             DataType::Utf8], // metric: "cosine" | "l2" | "dot"
        Arc::new(DataType::Float32),
        Volatility::Immutable,
        Arc::new(|args| {
            // extract arrays, compute distance
            todo!()
        }),
    )
}
```

- [ ] **Step 5: Implement lance_scan TVF**
```rust
// crates/sqe-vector/src/lance.rs
pub struct LanceScanFunction;
impl TableFunctionImpl for LanceScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = extract_string_literal(&exprs[0])?;
        Ok(Arc::new(LanceTableProvider::new(path)?))
    }
}
```

- [ ] **Step 6: Register in coordinator SessionContext**

In coordinator startup: `ctx.register_udf(vec_distance_udf()); ctx.register_udtf("lance_scan", Arc::new(LanceScanFunction));`

- [ ] **Step 7: Run tests**

Run: `cargo test -p sqe-vector 2>&1`
Expected: passes

- [ ] **Step 8: Commit**
```bash
git add crates/sqe-vector/
git commit -m "feat(vector): Lance scan TVF and vec_distance UDF"
```

---

### Task 5: Semantic index (BM25 over ontology + schema)

**Files:**
- Create: `crates/sqe-semantic/src/search.rs`

- [ ] **Step 1: Write failing test**
```rust
#[tokio::test]
async fn semantic_search_finds_relevant_table() {
    let mut index = SemanticIndex::new();
    index.add_table("customers", "Stores information about registered customers including name, email, and segment");
    index.add_table("orders", "Records of purchases made by customers");
    index.build();

    let hits = index.search("customer purchasing data");
    assert_eq!(hits[0].name, "customers"); // most relevant
    assert!(hits.iter().any(|h| h.name == "orders"));
}
```

- [ ] **Step 2: Add tantivy dependency**
```toml
tantivy = "0.22"
```

- [ ] **Step 3: Implement SemanticIndex**
```rust
pub struct SemanticIndex {
    index: tantivy::Index,
    reader: Option<tantivy::IndexReader>,
}
impl SemanticIndex {
    pub fn add_table(&mut self, name: &str, description: &str) { ... }
    pub fn add_from_rdf(&mut self, triples: &[Triple]) { ... }  // extract rdfs:comment, rdfs:label
    pub fn build(&mut self) { ... }  // commit index
    pub fn search(&self, query: &str) -> Vec<SemanticHit> { ... }  // BM25 ranked results
}
```

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-semantic search 2>&1`
Expected: passes

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-semantic/src/search.rs
git commit -m "feat(semantic): BM25 schema search index over ontology and table metadata"
```

---

### Task 6: Rich CLI — sqe schema + sqe explore

**Files:**
- Modify: `crates/sqe-cli/src/main.rs` (add subcommands)
- Create: `crates/sqe-cli/src/schema.rs`
- Create: `crates/sqe-cli/src/explore.rs`
- Modify: `crates/sqe-cli/src/output.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-cli/tests/cli_test.rs
use assert_cmd::Command;

#[test]
fn schema_search_outputs_json() {
    let mut cmd = Command::cargo_bin("sqe").unwrap();
    cmd.args(["schema", "search", "customer data", "--output", "json"])
       .env("SQE_ENDPOINT", "http://localhost:58080");
    // with a running test server or mocked:
    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.is_array());
}

#[test]
fn describe_flag_returns_tool_description() {
    let mut cmd = Command::cargo_bin("sqe").unwrap();
    cmd.args(["schema", "search", "--describe"]);
    let output = cmd.output().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json["name"].is_string());
    assert!(json["description"].is_string());
    assert!(json["parameters"].is_array());
    assert!(json["example"].is_string());
}
```

- [ ] **Step 2: Implement schema subcommands**

In `schema.rs`, add:
- `search(query, output_format)` → calls `GET /api/v1/schema/search?q=` → formats output
- `describe(table, output_format)` → calls `GET /api/v1/schema/tables/{name}`
- `relationships(table, output_format)` → SQL query on `graph.edges`
- `ontology(concept, output_format)` → SPARQL query on `rdf.triples`

Each command: add `--describe` flag that prints static JSON description instead of executing.

- [ ] **Step 3: Implement output.rs --output flag**
```rust
pub enum OutputFormat { Table, Json, Arrow, Csv }

pub fn write_batches(batches: &[RecordBatch], fmt: OutputFormat, writer: &mut dyn Write) {
    match fmt {
        OutputFormat::Table => arrow::util::pretty::print_batches(batches),
        OutputFormat::Json  => write_ndjson(batches, writer),
        OutputFormat::Arrow => write_arrow_ipc(batches, writer),
        OutputFormat::Csv   => write_csv(batches, writer),
    }
}

pub fn default_format_for_tty() -> OutputFormat {
    if atty::is(atty::Stream::Stdout) { OutputFormat::Table } else { OutputFormat::Json }
}
```

- [ ] **Step 4: Run CLI tests**

Run: `cargo test -p sqe-cli 2>&1`
Expected: passes (integration tests need server, mark `#[ignore]` if so)

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-cli/
git commit -m "feat(cli): add 'sqe schema' and 'sqe explore' commands with --output and --describe flags"
```

---

### Task 7: REST / OpenAPI server

**Files:**
- Create: `crates/sqe-rest/Cargo.toml`
- Create: `crates/sqe-rest/src/routes.rs`
- Create: `crates/sqe-rest/src/openapi.rs`
- Test: `crates/sqe-rest/tests/rest_test.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-rest/tests/rest_test.rs
use axum_test::TestServer;
use sqe_rest::build_router;

#[tokio::test]
async fn openapi_endpoint_returns_valid_spec() {
    let server = TestServer::new(build_router(mock_engine())).unwrap();
    let resp = server.get("/api/v1/openapi.json").await;
    resp.assert_status_ok();
    let spec: serde_json::Value = resp.json();
    assert_eq!(spec["openapi"], "3.1.0");
    assert!(spec["paths"].is_object());
}

#[tokio::test]
async fn query_endpoint_executes_sql() {
    let server = TestServer::new(build_router(mock_engine())).unwrap();
    let resp = server.post("/api/v1/query")
        .json(&serde_json::json!({"query": "SELECT 1 AS n", "dialect": "sql", "format": "json"}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert_eq!(body[0]["n"], 1);
}
```

- [ ] **Step 2: Add dependencies**
```toml
axum = { workspace = true }
utoipa = { version = "4", features = ["axum_extras"] }
utoipa-swagger-ui = { version = "6" }
```

- [ ] **Step 3: Implement routes with utoipa annotations**
```rust
// Each handler annotated with #[utoipa::path(...)] for spec generation
#[utoipa::path(
    post, path = "/api/v1/query",
    request_body = QueryRequest,
    responses((status = 200, description = "Query results", body = Vec<serde_json::Value>))
)]
async fn handle_query(/* ... */) -> impl IntoResponse { ... }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-rest 2>&1`
Expected: passes

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-rest/
git commit -m "feat(rest): OpenAPI 3.1 REST API server with utoipa spec generation"
```

---

### Task 8: MCP server (thin REST wrapper)

**Files:**
- Create: `crates/sqe-mcp/Cargo.toml`
- Create: `crates/sqe-mcp/src/server.rs`

- [ ] **Step 1: Implement MCP stdio transport**
```rust
// MCP protocol: JSON-RPC 2.0 over stdio
// Read from stdin, write to stdout, one message per line
pub struct McpServer { rest_client: SqeRestClient }

impl McpServer {
    pub async fn run(&self) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut lines = BufReader::new(stdin).lines();
        while let Some(line) = lines.next_line().await? {
            let request: McpRequest = serde_json::from_str(&line)?;
            let response = self.handle(request).await;
            println!("{}", serde_json::to_string(&response)?);
        }
        Ok(())
    }
}
```

- [ ] **Step 2: Map tools to REST endpoints**

`execute_query` → `POST /api/v1/query`
`search_schema` → `GET /api/v1/schema/search?q=`
`describe_table` → `GET /api/v1/schema/tables/{name}`
`explore` → `POST /api/v1/explore`

- [ ] **Step 3: Generate tool descriptions from OpenAPI spec**

On startup: fetch `/api/v1/openapi.json`, extract path descriptions → MCP tool list.

- [ ] **Step 4: Commit**
```bash
git add crates/sqe-mcp/
git commit -m "feat(mcp): MCP stdio server as thin wrapper over REST API"
```

---

### Task 9: TypeScript client (@sqe/client)

**Files:**
- Create: `packages/sqe-client/src/rest.ts`
- Create: `packages/sqe-client/src/flight.ts`
- Create: `packages/sqe-client/src/index.ts`
- Create: `packages/sqe-client/package.json`
- Test: `packages/sqe-client/tests/rest.test.ts`

- [ ] **Step 1: Set up package**
```json
{
  "name": "@sqe/client",
  "version": "0.1.0",
  "type": "module",
  "dependencies": {
    "apache-arrow": "^17.0.0",
    "@grpc/grpc-js": "^1.10.0",
    "@grpc/proto-loader": "^0.7.0"
  },
  "devDependencies": {
    "vitest": "^2.0.0",
    "typescript": "^5.0.0"
  }
}
```

- [ ] **Step 2: Write failing test**
```typescript
// packages/sqe-client/tests/rest.test.ts
import { describe, it, expect, vi } from 'vitest';
import { RestTransport } from '../src/rest.js';

describe('RestTransport', () => {
  it('posts query and returns Arrow batches', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: { get: () => 'application/json' },
      json: () => Promise.resolve([{ id: 1, name: 'Alice' }]),
    } as any);

    const transport = new RestTransport('http://localhost:8080', 'test-token');
    const result = await transport.query('SELECT 1');
    expect(result).toHaveLength(1);
  });
});
```

- [ ] **Step 3: Implement RestTransport**
```typescript
// packages/sqe-client/src/rest.ts
import { tableFromJSON, RecordBatch } from 'apache-arrow';

export class RestTransport {
  constructor(private endpoint: string, private token: string) {}

  async query(sql: string, dialect = 'auto'): Promise<RecordBatch[]> {
    const resp = await fetch(`${this.endpoint}/api/v1/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.token}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify({ query: sql, dialect, format: 'json' }),
    });
    if (!resp.ok) throw new Error(`Query failed: ${resp.statusText}`);
    const rows = await resp.json() as Record<string, unknown>[];
    return tableFromJSON(rows).batches;
  }

  async searchSchema(q: string): Promise<SchemaHit[]> {
    const resp = await fetch(`${this.endpoint}/api/v1/schema/search?q=${encodeURIComponent(q)}`, {
      headers: { 'Authorization': `Bearer ${this.token}` },
    });
    return resp.json();
  }
}
```

- [ ] **Step 4: Implement FlightTransport (Node.js)**
```typescript
// packages/sqe-client/src/flight.ts
// NOTE: Arrow Flight SQL over gRPC requires assembling from parts:
// - @grpc/grpc-js for transport
// - @grpc/proto-loader for Arrow Flight SQL proto files
// - apache-arrow for RecordBatch deserialization
// The Arrow Flight SQL proto files are at:
//   https://github.com/apache/arrow/blob/main/format/FlightSql.proto
//   https://github.com/apache/arrow/blob/main/format/Flight.proto
// Vendor these proto files into packages/sqe-client/proto/

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { RecordBatchReader } from 'apache-arrow';

export class FlightTransport {
  private client: any; // generated FlightServiceClient

  constructor(endpoint: string, token: string) {
    const packageDef = protoLoader.loadSync(['proto/Flight.proto', 'proto/FlightSql.proto']);
    const proto = grpc.loadPackageDefinition(packageDef) as any;
    this.client = new proto.arrow.flight.protocol.FlightService(
      endpoint,
      grpc.credentials.createSsl(),
      { 'grpc.default_authority': endpoint }
    );
  }

  async query(sql: string): Promise<RecordBatch[]> {
    // 1. GetFlightInfo with CommandStatementQuery
    // 2. DoGet with returned ticket
    // 3. Deserialize Arrow IPC stream → RecordBatches
    // Full implementation follows Arrow Flight SQL wire protocol
    throw new Error('TODO: implement Flight SQL query');
  }
}
```

- [ ] **Step 5: Implement SqeClient (unified)**
```typescript
// packages/sqe-client/src/index.ts
export class SqeClient {
  private transport: RestTransport | FlightTransport;

  constructor(opts: { endpoint: string; token: string; transport?: 'auto' | 'rest' | 'flight' }) {
    const mode = opts.transport ?? 'auto';
    const useRest = mode === 'rest' || (mode === 'auto' && typeof window !== 'undefined');
    this.transport = useRest
      ? new RestTransport(opts.endpoint, opts.token)
      : new FlightTransport(opts.endpoint, opts.token);
  }

  async query(sql: string, opts?: { dialect?: string }): Promise<RecordBatch[]> {
    return this.transport.query(sql, opts?.dialect);
  }

  async searchSchema(question: string): Promise<SchemaHit[]> {
    return (this.transport as RestTransport).searchSchema(question);
  }
}
```

- [ ] **Step 6: Run tests**

Run: `cd packages/sqe-client && npm test`
Expected: REST transport test passes; Flight transport marked TODO

- [ ] **Step 7: Commit**
```bash
git add packages/sqe-client/
git commit -m "feat(client): @sqe/client TypeScript package with REST transport and Flight scaffold"
```
