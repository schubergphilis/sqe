# When Your SQL Engine Understands Meaning

*SQL engines know table shapes. They don't know what the data means. We're changing that — with ontologies on Iceberg, property graphs, vector search, and AI-native interfaces. All in one engine.*

---

## The schema introspection problem

An AI agent connects to your data lake. It runs `SHOW TABLES` and gets back 400 tables. It picks one and runs `DESCRIBE orders` — 47 columns. `ord_typ_cd`. `cust_sgmt_flg`. `rev_adj_amt`. The agent has no idea what any of this means.

This is the fundamental gap between a SQL engine and a useful data tool. SQL engines are excellent at executing queries. They're terrible at helping you figure out which query to write.

Schema introspection tells you the shape of the data: column names, types, nullability. It doesn't tell you the meaning: "this column is the customer lifetime value," "this table is the primary source for churn analysis," "these two tables are related through the customer_id foreign key but only for orders placed after 2024."

Today, that knowledge lives in documentation wikis, tribal knowledge, dbt YAML files, and the heads of senior data engineers. AI agents can't access any of it.

---

## The idea: meaning as data

What if semantic knowledge — ontologies, relationships, descriptions — lived alongside the data, in the same Iceberg tables, versioned by the same snapshots, secured by the same access controls?

That's what SQE's semantic layer does. It's not a separate metadata service. It's not an external knowledge graph. It's data in Iceberg, queryable in SQL, SPARQL, and ISO GQL, accessible through the same auth and policy enforcement as everything else.

Three types of semantic data, all stored in Iceberg:

### 1. Ontologies as RDF triples

An ontology is a formal description of concepts and their relationships. RDF (Resource Description Framework) is the standard for representing ontologies as triples: subject, predicate, object.

In SQE, ontologies live in a conventional Iceberg table:

```sql
CREATE TABLE rdf.triples (
    subject    VARCHAR NOT NULL,
    predicate  VARCHAR NOT NULL,
    object     VARCHAR NOT NULL,
    graph_name VARCHAR NOT NULL DEFAULT 'default'
) PARTITIONED BY (predicate);
```

Partitioning by predicate is the key design choice. Most ontology queries ask "what is related by this predicate?" — all `rdf:type` triples in one partition, all `schema:description` triples in another. Fast scan, no full-table read.

You populate it with standard SQL:

```sql
INSERT INTO rdf.triples VALUES
    ('sales:orders', 'rdf:type', 'schema:Table', 'default'),
    ('sales:orders', 'schema:description', 'Primary order transactions', 'default'),
    ('sales:orders.customer_id', 'schema:references', 'customers:customers.id', 'default'),
    ('sales:orders.ord_typ_cd', 'schema:name', 'Order Type Code', 'default'),
    ('sales:orders.ord_typ_cd', 'schema:description', 'R=Return, S=Sale, X=Exchange', 'default'),
    ('sales:orders.rev_adj_amt', 'schema:name', 'Revenue Adjustment Amount', 'default');
```

Then you query it in SPARQL:

```sparql
SELECT ?table ?description
WHERE {
    ?table rdf:type schema:Table .
    ?table schema:description ?description .
    FILTER(CONTAINS(?description, "customer"))
}
```

SQE compiles SPARQL to DataFusion's `LogicalPlan` via the `rdf-fusion` library. No separate triple store, no SPARQL endpoint to maintain. The SPARQL query becomes a regular DataFusion query against the `rdf.triples` table — with predicate pushdown, partition pruning, and the same query optimizer as SQL.

And because it's Iceberg: **time travel works.** "What did we know about the `orders` table six months ago?" is just a snapshot query. The ontology evolves with the data.

### 2. Property graphs on Iceberg

Some relationships are better expressed as graphs than triples. "Which customers bought products that were also bought by customers who churned?" is a multi-hop graph traversal that's painful in SQL and awkward in SPARQL.

SQE stores graph data in two Iceberg tables:

```sql
-- Nodes: entities with labels and properties
CREATE TABLE graph.nodes (
    id         VARCHAR NOT NULL,
    labels     VARCHAR[] NOT NULL,    -- ['Customer', 'VIP']
    properties JSON
);

-- Edges: directed relationships between entities
CREATE TABLE graph.edges (
    src_id     VARCHAR NOT NULL,
    dst_id     VARCHAR NOT NULL,
    label      VARCHAR NOT NULL,       -- 'PLACED_ORDER', 'BOUGHT_PRODUCT'
    properties JSON
) PARTITIONED BY (label);
```

Queried with ISO GQL (the new international standard, ISO 39075:2024 — not Cypher, which is proprietary to Neo4j):

```
MATCH (c:Customer {segment: 'VIP'})-[:PLACED_ORDER]->(o:Order)
RETURN c.id, count(o) AS total_orders
ORDER BY total_orders DESC
```

The execution is dual-mode:
- **Small graphs** (under 10 million nodes): load into `graphlite` (an embedded graph engine) in memory, execute GQL natively, return Arrow results.
- **Large graphs**: compile MATCH patterns to DataFusion recursive CTEs. The graph traversal becomes a SQL query plan — joining `graph.edges` against itself for each hop, with cycle detection.

The threshold is configurable. The choice is automatic.

### 3. Vector search alongside structured data

Unstructured content — documents, support tickets, product descriptions — often lives alongside structured tables. SQE adds vector search via Apache Lance, an Arrow-native columnar format designed for ML workloads.

Lance datasets sit on the same object storage as Iceberg tables:

```
s3://my-lake/
    iceberg/orders/            ← structured data (Iceberg)
    lance/support_embeddings/  ← vector embeddings (Lance)
```

Two new DataFusion functions make vector search feel like SQL:

```sql
-- embed() calls your configured embedding endpoint
-- vec_distance() computes similarity

SELECT text, vec_distance(embedding, embed('order dispute resolution'), 'cosine') AS score
FROM lance_scan('s3://my-lake/lance/support_embeddings/')
WHERE score < 0.3
ORDER BY score
LIMIT 10;
```

`embed()` is an async UDF that sends text to a configurable HTTP endpoint (your embedding model — OpenAI, Cohere, local model, anything) and caches results by SHA-256 hash. `vec_distance()` computes cosine, L2, or dot product similarity.

The real power is hybrid queries — combining structured SQL, ontology SPARQL, and vector search in one query:

```sql
-- Find support tickets about order disputes for VIP customers
WITH dispute_docs AS (
    SELECT text, vec_distance(embedding, embed('order dispute'), 'cosine') AS score
    FROM lance_scan('s3://my-lake/lance/support_embeddings/')
    WHERE score < 0.3
)
SELECT d.text, d.score, o.order_id, c.name
FROM dispute_docs d
JOIN orders o ON d.order_id = o.id
JOIN customers c ON o.customer_id = c.id
WHERE c.segment = 'VIP'
ORDER BY d.score
```

One query. Three data modalities. One engine.

---

## AI-native interfaces

Having semantic data is useless if AI agents can't access it. SQE is designed for four interface modalities, ordered by priority:

### CLI-first (works with any agent)

The CLI is the primary agent interface — not an afterthought. Every command has structured output and a `--describe` flag that returns a JSON description of what the command does, its parameters, and example output.

```bash
# Semantic search across all tables and ontology
$ sqe schema search "customer purchasing behaviour"
{
  "results": [
    {"table": "sales.orders", "relevance": 0.92, "description": "Primary order transactions"},
    {"table": "customers.segments", "relevance": 0.87, "description": "Customer segmentation model"},
    {"ontology": "schema:CustomerLifetimeValue", "relevance": 0.84}
  ]
}

# Describe a table with semantic context
$ sqe schema describe orders --output json
{
  "columns": [...],
  "ontology": {"description": "Primary order transactions", "related_tables": [...]},
  "sample_queries": ["SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"]
}

# Explore a question (returns suggested queries, not answers)
$ sqe explore "why do VIP customers churn?"
{
  "relevant_tables": ["orders", "customers", "churn_events"],
  "suggested_sql": "SELECT ...",
  "suggested_sparql": "SELECT ?reason WHERE { ... }",
  "suggested_gql": "MATCH (c:Customer {segment:'VIP'})-[:CHURNED]->..."
}
```

This works with Claude Code, GPT function calling, LangChain, or a simple shell script. No MCP SDK required. No special protocol. Just stdin/stdout.

### REST + OpenAPI (universal HTTP)

An OpenAPI 3.1 spec at `/api/v1/openapi.json` that any AI agent can fetch and immediately understand every operation:

```
POST /api/v1/query         — execute SQL, SPARQL, or GQL (auto-detected)
GET  /api/v1/schema/search — semantic search across tables and ontology
GET  /api/v1/schema/tables/{name} — table metadata with ontology context
POST /api/v1/explore       — natural language → suggested queries
```

The OpenAPI spec is the key artifact. AI agents that understand OpenAPI (which is most of them) can self-discover capabilities without documentation.

### MCP (for MCP-capable agents)

A thin wrapper over the REST API. MCP tool calls map directly to HTTP requests. Tool descriptions are auto-generated from the OpenAPI spec at startup — no hand-written tool definitions that drift from the actual API.

### TypeScript client (for web and Node.js)

```typescript
import { SqeClient } from '@sqe/client';

const client = new SqeClient({
  endpoint: 'https://sqe.example.com',
  token: 'my-bearer-token',
});

// Semantic search
const tables = await client.searchSchema('customer churn indicators');

// SPARQL
const ontology = await client.query(
  'SELECT ?desc WHERE { sales:orders schema:description ?desc }',
  { dialect: 'sparql' }
);

// Returns Apache Arrow Tables — zero-copy in Node.js
const result = await client.query('SELECT * FROM orders LIMIT 100');
```

Auto-selects Arrow Flight (gRPC, high performance) in Node.js and REST (fetch API) in the browser. Same API surface, different transports.

---

## Why this matters

The data industry has spent a decade building storage (data lakes), catalogs (Iceberg, Delta), and query engines (Trino, Spark, DataFusion). What's missing is the semantic layer — the part that knows what the data means, not just where it lives.

Today, that semantic layer is either:
- **Manual** — documentation wikis, tribal knowledge, dbt YAML descriptions
- **Proprietary** — vendor-specific knowledge graphs separate from the data
- **Absent** — AI agents brute-force their way through schema introspection

SQE's approach is different: **the semantic layer is data.** Ontologies are Iceberg tables. Graph relationships are Iceberg tables. Vector embeddings are Lance datasets on the same storage. Everything is versioned, secured, and queryable through the same engine.

This means:
- **No separate infrastructure.** No triple store to maintain, no graph database to sync, no vector database to keep in sync with your data lake.
- **Unified security.** The same bearer token passthrough and policy enforcement that protects your sales data also protects your ontology. Row-level security on triples works the same as row-level security on tables.
- **Time travel for knowledge.** When someone asks "what did we understand about customer churn last quarter?" the answer is an Iceberg snapshot query, not an archaeology expedition through wiki edit history.
- **One query, multiple modalities.** SQL for structured data, SPARQL for ontologies, GQL for graphs, vector search for unstructured content — all in one query plan, one engine, one result set.

---

## The shape of the roadmap

The semantic AI layer is Step 6 in SQE's roadmap — the most ambitious and the most additive. It doesn't change existing SQL functionality. It adds new capabilities on top:

| Component | What | How |
|-----------|------|-----|
| RDF/SPARQL | Ontologies as Iceberg tables | `spargebra` + `rdf-fusion` compiled to DataFusion |
| ISO GQL | Property graphs on Iceberg | `graphlite` (small) or recursive CTEs (large) |
| Vector search | Embeddings on Lance | `lance-datafusion` + custom UDFs |
| Semantic search | "Find tables about X" | BM25 index over ontology + metadata |
| CLI | Agent-native commands | Structured output, `--describe` for self-documentation |
| REST/OpenAPI | Universal HTTP API | `utoipa` for OpenAPI 3.1 spec generation |
| MCP | MCP-capable agents | Thin wrapper over REST, auto-generated tools |
| TypeScript | Web and Node.js | Arrow Flight (Node) + REST (browser) |

89 tasks across 9 phases. Fully additive — new crates (`sqe-semantic`, `sqe-vector`, `sqe-rest`, `sqe-mcp`), no existing code broken.

The design is intentionally layered: you can use SQE as a pure SQL engine and ignore everything above. Or you can populate an ontology and get semantic search. Or you can go all-in with graphs, vectors, and AI interfaces. Each layer adds value without requiring the others.

---

## The bet

We're betting that the next generation of data tools won't just execute queries — they'll understand what the data means and help users (human and AI) find and combine the right information.

The foundation is in place: a fast Rust query engine with Iceberg-native data access, bearer token security, and distributed execution. The semantic layer is the capability that turns it from infrastructure into intelligence.

Structured data. Ontologies. Graphs. Vectors. One engine. One security model. One query.

---

*SQE is open-source under Apache 2.0. The semantic AI layer (Step 6) is in design. The specs are at `openspec/changes/semantic-ai-layer/`. Contributions welcome.*
