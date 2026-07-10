## Why

SQL query engines understand table shapes, not data meaning. AI agents querying a data lake via SQE face the classic schema introspection problem: column names like `ord_typ_cd` and `cust_sgmt_flg` are opaque. Without semantic context, agents either hallucinate table relationships or need exhaustive hand-holding to write correct queries.

SQE's architecture — Iceberg as the storage layer, DataFusion as the execution engine, per-user auth throughout — makes it uniquely positioned to solve this:

1. **Ontology and graph data live IN Iceberg** — versioned, time-travelled, secured like all other data
2. **Vector embeddings for unstructured content** alongside structured data, same storage, same query engine
3. **AI-native query interface**: rich CLI (invocable by any agent framework) + OpenAPI REST (introspectable by agents) — not just MCP (which only works with MCP-capable agents)

This is Phase 6. It turns SQE from a fast query engine into a unified semantic data platform: structured + unstructured + ontology, queryable in SQL, SPARQL, and ISO GQL, accessible by AI agents through any interface.

## What Changes

### Ontology / RDF on Iceberg
- Convention: `rdf.triples (subject, predicate, object, graph_name)` Iceberg table
- SPARQL 1.1 queries compiled to DataFusion via `spargebra` + `rdf-fusion`
- Ontology is versioned Iceberg data — time travel, snapshot diff, per-user access

### Property Graph / ISO GQL
- Convention: `graph.nodes (id, labels[], properties jsonb)` + `graph.edges (src, dst, label, properties jsonb)`
- ISO GQL (ISO 39075:2024) via embedded `graphlite` + bridge to DataFusion for joins with business data
- Cypher subset compiled to DataFusion recursive CTEs for multi-hop traversal

### Vector Search (unstructured data)
- Apache Lance format alongside Iceberg on same object storage (`lance://` paths)
- `vec_distance(embedding, query_vec)` and `embed(text)` DataFusion scalar UDFs
- `LATERAL` join pattern enables hybrid: SQL filter + vector similarity + ontology filter in one query

### AI Agent Interface
- **Rich CLI** (primary): `sqe query`, `sqe schema search`, `sqe schema describe`, `sqe explore` — machine-readable output (`--output json|arrow|table`), excellent help text. Invocable by any agent framework, Claude Code skills, shell scripts.
- **OpenAPI REST** (secondary): HTTP API with OpenAPI 3.1 spec. Agents can introspect the schema and call endpoints directly. More universal than MCP.
- **MCP server** (tertiary): for MCP-capable agents (Claude Desktop etc.). Thin wrapper over the REST API.
- **TypeScript/JavaScript client**: `sqe-client` npm package for browser and Node.js over Arrow Flight SQL + REST fallback

## Capabilities

### New Capabilities
- `ontology-rdf`: RDF triple store on Iceberg; SPARQL 1.1 queries via rdf-fusion→DataFusion
- `graph-gql`: ISO GQL property graph queries; graphlite embedded bridge
- `vector-search`: Lance vector embeddings; vec_distance() and embed() UDFs; hybrid SQL+vector queries
- `cli-agent-interface`: Rich `sqe` CLI with semantic commands, machine-readable output
- `rest-api`: OpenAPI 3.1 HTTP API for schema introspection and query execution
- `typescript-client`: npm package `@sqe/client` over Arrow Flight SQL with REST fallback
- `mcp-server`: MCP wrapper over REST API (thin, tertiary priority)

### Modified Capabilities
- `information-schema`: extended with semantic metadata from rdf.triples
- `storage-only` (pluggable-catalogs): `lance://` paths treated as vector table locations

## Impact

- Two new Iceberg table conventions (`rdf.triples`, `graph.nodes/edges`) — opt-in, no impact on existing deployments
- New `sqe schema` and `sqe explore` CLI subcommands alongside existing `sqe query`
- New `sqe-rest` crate (axum HTTP API server, optional binary)
- New `crates/sqe-vector` crate (Lance integration, `vec_distance` UDF)
- New `crates/sqe-semantic` crate (SPARQL compiler, GQL bridge)
- New `packages/sqe-client` (TypeScript npm package)

## Rollback

All new capabilities are opt-in. Existing SQL+Iceberg users see no change. Semantic features activate only when `rdf.triples` / `graph.nodes` tables exist or `[semantic]` config is enabled.
