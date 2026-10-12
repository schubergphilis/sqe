## Context

Phase 6 turns SQE into a unified query engine for structured + unstructured + semantic data. Four sub-systems are introduced, all sharing the DataFusion execution engine and Iceberg/object-store storage. The AI agent interface is CLI-first because CLIs are invocable by any agent framework without protocol-specific support.

## Goals / Non-Goals

**Goals:**
- SPARQL 1.1 on Iceberg triple tables via DataFusion (not a separate RDF engine)
- ISO GQL property graph queries bridged to DataFusion for mixed graph+SQL queries
- Vector similarity search via Lance format + DataFusion UDFs
- CLI interface agents can invoke without MCP — excellent descriptions, machine-readable output
- OpenAPI REST API: universal HTTP interface, introspectable by LLMs
- TypeScript/JavaScript npm client for browser and Node.js

**Non-Goals:**
- Full SPARQL 1.2 (1.1 subset covers 95% of ontology use cases)
- SPARQL UPDATE / SPARQL endpoint compliance (write path separate)
- Full Cypher (ISO GQL is the standard; Cypher is Neo4j-specific)
- Training or fine-tuning AI models (inference only)
- Managed embedding generation (users bring their own embedding models/vectors)

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Query Interfaces                              │
│                                                                  │
│  CLI (primary)      REST/OpenAPI      Arrow Flight SQL           │
│  sqe query          /api/v1/query     gRPC (existing)            │
│  sqe schema search  /api/v1/schema    TypeScript client          │
│  sqe explore        /api/v1/explore                              │
│                          │                                       │
│                     MCP server (thin wrapper, tertiary)          │
└──────────────────────────┬───────────────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────────────┐
│                   sqe-semantic (new crate)                       │
│                                                                  │
│  QueryRouter                                                     │
│    detect_dialect(input) → SQL | SPARQL | GQL                   │
│                                                                  │
│  SparqlCompiler                                                  │
│    spargebra::parse() → SPARQL algebra                          │
│    → rdf_fusion compile → DataFusion LogicalPlan                │
│    triples table: rdf.triples (subject, predicate, object, graph)│
│                                                                  │
│  GqlBridge                                                       │
│    graphlite embedded engine (ISO GQL)                          │
│    graph tables: graph.nodes, graph.edges (Iceberg)             │
│    multi-hop: WITH RECURSIVE on DataFusion                      │
│                                                                  │
│  SemanticSearch                                                  │
│    embed rdf.triples metadata → search index (in-memory)        │
│    "find tables about customer churn" → ranked table+column list │
└──────────────────────────┬───────────────────────────────────────┘
                           │  DataFusion LogicalPlan
┌──────────────────────────▼───────────────────────────────────────┐
│              DataFusion + sqe-vector (new crate)                 │
│                                                                  │
│  vec_distance(col, query_vec, metric) UDF                       │
│  embed(text) UDF  → calls configured embedding endpoint          │
│  LanceScanExec  → reads Lance format alongside Iceberg           │
└──────┬───────────────────────────────────────────────────────────┘
       │
┌──────▼──────────────────────────────┐
│  Storage                            │
│  Iceberg tables  (structured data)  │
│  rdf.triples     (ontology)         │
│  graph.nodes/edges (property graph) │
│  lance://...     (vector data)      │
└─────────────────────────────────────┘
```

## Data Conventions

### RDF Triple Store on Iceberg

```sql
CREATE TABLE rdf.triples (
    subject     VARCHAR NOT NULL,   -- IRI or blank node
    predicate   VARCHAR NOT NULL,   -- IRI
    object      VARCHAR NOT NULL,   -- IRI, blank node, or literal
    graph_name  VARCHAR NOT NULL    -- named graph IRI (use 'default' for default graph)
) USING ICEBERG
PARTITIONED BY (predicate);  -- partition by predicate = property table layout
```

Partition by `predicate` so `SELECT * WHERE predicate = 'rdf:type'` hits one partition. Fast for schema-shaped ontology queries. Scales to billions of triples.

Time travel on the ontology is automatic (Iceberg snapshots). "What did we know about Customer six months ago?" = Iceberg `FOR SYSTEM_TIME AS OF`.

### Property Graph on Iceberg

```sql
CREATE TABLE graph.nodes (
    id          VARCHAR NOT NULL,
    labels      VARCHAR[] NOT NULL,     -- ['Customer', 'VIP']
    properties  JSON
) USING ICEBERG;

CREATE TABLE graph.edges (
    src_id      VARCHAR NOT NULL,
    dst_id      VARCHAR NOT NULL,
    label       VARCHAR NOT NULL,       -- 'PLACED_ORDER', 'BELONGS_TO'
    properties  JSON
) USING ICEBERG
PARTITIONED BY (label);
```

### Vector Data (Lance)

Lance format stored on the same object storage as Iceberg, under a parallel path:

```
s3://my-lake/
  iceberg/orders/          ← Iceberg table
  iceberg/customers/       ← Iceberg table
  lance/doc_embeddings/    ← Lance dataset (Arrow + vector index)
```

Access via `iceberg_scan()` pattern extended to `lance_scan()`:
```sql
SELECT text, vec_distance(embedding, embed('order dispute')) AS score
FROM lance_scan('s3://my-lake/lance/doc_embeddings/')
ORDER BY score
LIMIT 10;
```

## SPARQL Compiler Design

```
SPARQL query string
       │
       ▼ spargebra::Query::parse()
SPARQL Algebra Tree
  BGP (basic graph pattern) → equi-joins on rdf.triples
  FILTER                    → DataFusion Expr
  JOIN                      → DataFusion Join
  UNION                     → DataFusion Union
  PROJECT                   → DataFusion Projection
  DISTINCT                  → DataFusion Distinct
  LIMIT/OFFSET              → DataFusion Limit
       │
       ▼ SparqlCompiler::to_logical_plan()
DataFusion LogicalPlan
       │
       ▼ DataFusion optimizer + executor
Arrow RecordBatches
```

BGP triple pattern `(s, p, o)` where variables = wildcards:
```sparql
?customer rdf:type :Customer ;
          :hasSegment :VIP .
```
Compiles to:
```sql
SELECT t1.subject AS customer
FROM rdf.triples t1
JOIN rdf.triples t2 ON t1.subject = t2.subject
WHERE t1.predicate = 'rdf:type'    AND t1.object = 'ontology:Customer'
  AND t2.predicate = 'hasSegment'  AND t2.object = 'ontology:VIP'
```

Each additional triple pattern in the BGP = one more join on `rdf.triples`. DataFusion's optimizer reorders joins by selectivity.

## ISO GQL Bridge Design

`graphlite` is embedded as a library. Graph queries can be run in two modes:

**Mode A — Pure GQL (small graphs, ontology-sized)**
Load `graph.nodes` + `graph.edges` into graphlite in-memory on query start. Run ISO GQL. Return results.

```
MATCH (c:Customer {segment:'VIP'})-[:PLACED_ORDER]->(o:Order)
RETURN c.id, count(o) AS orders
```

**Mode B — GQL→DataFusion (large graphs, billion-edge scale)**
Compile MATCH patterns to DataFusion joins + recursive CTEs. No graphlite involvement at execution time.

```sql
-- compiled output
WITH RECURSIVE traversal(src_id, dst_id, depth, path) AS (
  SELECT e.src_id, e.dst_id, 1, ARRAY[e.src_id, e.dst_id]
  FROM graph.edges e
  WHERE e.label = 'PLACED_ORDER'
  UNION ALL
  SELECT t.src_id, e.dst_id, t.depth + 1, array_append(t.path, e.dst_id)
  FROM traversal t JOIN graph.edges e ON t.dst_id = e.src_id
  WHERE t.depth < 5 AND NOT e.dst_id = ANY(t.path)  -- cycle guard
)
SELECT n.properties->>'id', count(*) FROM traversal
JOIN graph.nodes n ON n.id = traversal.src_id ...
```

Mode selection: `graph.nodes` row count < configurable threshold (default 10M) → Mode A. Above → Mode B.

## AI Agent Interface Design

### CLI (primary — works with any agent)

```bash
# Execute any query (auto-detects SQL / SPARQL / GQL)
sqe query "SELECT * FROM orders LIMIT 10"
sqe query "SELECT ?c WHERE { ?c rdf:type :Customer }"
sqe query "MATCH (c:Customer)-[:PLACED_ORDER]->(o) RETURN c, count(o)"
sqe query --file my_query.sparql

# Schema discovery (agent-friendly semantic search)
sqe schema search "customer purchasing behaviour"
sqe schema describe orders
sqe schema relationships orders    # show graph edges from/to this table
sqe schema ontology Customer       # show all RDF triples for a concept

# Exploration (returns structured guidance for agents)
sqe explore "why do VIP customers churn?"
# → returns: relevant tables, suggested joins, sample SPARQL, sample SQL

# Output formats
sqe query "SELECT ..." --output json   # newline-delimited JSON rows
sqe query "SELECT ..." --output arrow  # Arrow IPC binary (pipe to other tools)
sqe query "SELECT ..." --output csv
sqe query "SELECT ..." --output table  # human-readable (default)
```

Every command has a `--describe` flag returning a JSON description of what the command does, its parameters, and example output — making it self-documenting for agent frameworks that introspect tool capabilities.

### REST / OpenAPI (secondary — universal HTTP)

```
GET  /api/v1/schema/catalogs
GET  /api/v1/schema/tables
GET  /api/v1/schema/tables/{name}          → columns, sample values, ontology links
GET  /api/v1/schema/search?q=...           → semantic search
POST /api/v1/query                         → execute SQL / SPARQL / GQL
     body: { "query": "...", "dialect": "auto|sql|sparql|gql", "format": "json|arrow" }
POST /api/v1/explore                       → natural language → suggested queries
GET  /api/v1/openapi.json                  → full OpenAPI 3.1 spec (LLM-readable)
```

The OpenAPI spec is the key artifact. An AI agent can fetch `/api/v1/openapi.json` and immediately understand every available operation without any additional documentation.

### MCP (tertiary — MCP-capable agents only)

Thin axum handler that maps MCP tool calls to REST API calls. Not a separate implementation. Tools exposed:
- `execute_query` → POST /api/v1/query
- `search_schema` → GET /api/v1/schema/search
- `describe_table` → GET /api/v1/schema/tables/{name}
- `explore` → POST /api/v1/explore

## TypeScript / JavaScript Client

```
@sqe/client (npm package)
├── src/
│   ├── flight.ts        — Arrow Flight SQL transport (gRPC via @grpc/grpc-js + arrow proto)
│   ├── rest.ts          — REST fallback (fetch API, works in browser)
│   ├── arrow.ts         — apache-arrow integration (RecordBatch handling)
│   └── index.ts         — unified client (auto-selects flight vs rest)
```

Arrow Flight SQL in TypeScript requires:
- `apache-arrow` (npm) — Arrow IPC format, RecordBatch
- `@grpc/grpc-js` — gRPC transport for Node.js
- `@grpc/proto-loader` — load Arrow Flight SQL proto files at runtime
- Generated types from `arrow/format/Flight.proto` and `arrow/format/FlightSql.proto`

The Flight SQL proto files are in the Apache Arrow repo. The TypeScript Arrow ecosystem (`apache-arrow` npm) handles data format but does NOT yet ship a Flight SQL gRPC client — that must be assembled from the parts above.

For browser environments: REST fallback (fetch-based, JSON response decoded to Arrow via `apache-arrow`).

```typescript
import { SqeClient } from '@sqe/client';

const client = new SqeClient({
  endpoint: 'https://sqe.example.com',
  token: 'my-bearer-token',
  // transport: 'flight' | 'rest' | 'auto' (default: auto - flight for Node, rest for browser)
});

// Returns Apache Arrow Table
const result = await client.query('SELECT * FROM orders LIMIT 100');
for (const batch of result.batches) {
  console.log(batch.toArray());
}

// Schema search
const tables = await client.searchSchema('customer churn indicators');

// SPARQL
const ontology = await client.query(
  'SELECT ?type WHERE { :Customer rdf:type ?type }',
  { dialect: 'sparql' }
);
```

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| CLI-first agent interface | yes | Works with any agent framework, shell, Claude Code skills — not locked to MCP |
| MCP as thin REST wrapper | yes | Avoid duplicate implementation; REST is the real API |
| SPARQL via rdf-fusion | yes | Proven architecture; same DataFusion path as SQL |
| ISO GQL not Cypher | yes | ISO standard, not Neo4j-proprietary |
| graphlite for GQL | dual-mode | In-memory for small graphs, compile-to-SQL for large |
| Lance for vectors | yes | Deep DataFusion integration; Arrow-native |
| embed() UDF | configurable endpoint | SQE doesn't generate embeddings; users bring their model |
| TypeScript client | REST+Flight hybrid | REST for browser compat; Flight for Node.js perf |
| Ontology in Iceberg | yes | Time travel, per-user auth, no separate RDF server |

## Risks

| Risk | Mitigation |
|---|---|
| rdf-fusion experimental | Contribute fixes upstream; SparqlCompiler in sqe-semantic as fallback |
| GQL→DataFusion recursive CTE perf | Cycle guard + depth limit (default 10); configurable |
| embed() UDF latency | Async UDF with configurable timeout; results cached by text hash |
| Lance + Iceberg path collision | Separate `lance://` scheme in StorageConfig; no overlap |
| TypeScript Flight SQL gRPC — no prebuilt client | REST fallback is feature-complete; Flight is performance opt-in |
