# Sovereign Query Engine (SQE)

**Goal:** Replace patched Trino with a purpose-built, distributed SQL query engine for Iceberg REST Catalog (Apache Polaris) with Keycloak OIDC auth passthrough, OPA-based fine-grained security, and petabyte-scale execution.

---

## 1. Core Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Layer                           │
│  JDBC (Arrow Flight SQL)  ·  Trino Wire Compat  ·  HTTP    │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                   Coordinator Node                          │
│  SQL Parser (w/ extensions) → Analyzer → Optimizer →        │
│  Distributed Planner → Scheduler                            │
│                                                             │
│  Session Manager (Keycloak token lifecycle)                 │
│  PolicyEnforcer trait (no-op now, OPA/Cedar later)          │
│  Metrics Collector (Prometheus)                             │
└──────────────────────────┬──────────────────────────────────┘
                           │  Arrow Flight (plan fragments)
              ┌────────────┼────────────┐
┌─────────────▼──┐  ┌──────▼───────┐  ┌─▼──────────────┐
│   Worker Node   │  │  Worker Node  │  │  Worker Node   │
│   DataFusion    │  │  DataFusion   │  │  DataFusion    │
│   Execution     │  │  Execution    │  │  Execution     │
└────────┬────────┘  └──────┬───────┘  └──┬─────────────┘
         │                  │              │
┌────────▼──────────────────▼──────────────▼─────────────────┐
│              Data Access Layer                              │
│  iceberg-rust (table provider)                              │
│  → Iceberg REST Catalog (Polaris) w/ user OIDC bearer      │
│  → S3 / MinIO (object storage)                             │
│  Iceberg v3 support · Views · Manifest caching             │
└────────────────────────────────────────────────────────────┘
```

## 2. Component Breakdown

### 2.1 SQL Frontend & Custom Extensions

**Parser:** Fork or extend `datafusion-sql` (based on `sqlparser-rs`) to support custom statements:

```sql
-- Catalog-aware (Phase 1-2)
SHOW CATALOGS;
SHOW SCHEMAS IN catalog;
CREATE VIEW ... AS SELECT ...;  -- persisted via Iceberg REST

-- Security DDL (deferred to security phase)
GRANT SELECT (col1, col2) ON table TO role_x;
GRANT ROWS WHERE region = 'EU' ON table TO role_eu;
REVOKE ...;
SHOW GRANTS ON table;
SHOW EFFECTIVE POLICY FOR CURRENT_USER ON table;
```

**Implementation path:**
- `sqlparser-rs` already supports `GRANT`/`REVOKE` AST nodes
- Add custom `Statement` variants for policy inspection
- Register a `CustomStatementHandler` trait in the coordinator that intercepts non-query statements and routes them to the policy backend or Polaris catalog
- Security DDL handlers are stub/unimplemented until the policy phase

### 2.2 Authentication — Keycloak OIDC Passthrough

This mirrors your Trino DCAF branch logic, ported to Rust:

```
Client (JDBC) ──► Coordinator
  │  Credentials: username + password (or refresh token)
  │
  ▼
Coordinator: SessionManager
  │  POST /realms/{realm}/protocol/openid-connect/token
  │  grant_type=password, client_id=sqe-public
  │  → receives access_token, refresh_token, expires_in
  │
  │  Stores token in Session (per-connection, in-memory)
  │  Spawns refresh task (token_lifetime - 30s buffer)
  │
  ▼
On every catalog/S3 call:
  │  Authorization: Bearer {session.access_token}
  │  Forwarded to: Polaris REST, S3 (STS or presigned)
```

**Key design decisions:**
- No fixed service account — every query runs as the authenticated user
- Token is propagated to workers via plan fragment metadata (Arrow Flight headers)
- Workers attach the bearer token to their own iceberg-rust catalog calls
- Refresh is coordinator-side only; workers get fresh tokens per-fragment

**Rust crate:** Thin `keycloak-oidc` module (~300 lines), wrapping `reqwest` + `jsonwebtoken` for validation. Same scope as your earlier JWT interceptor estimate.

### 2.3 Distributed Execution (Ballista-derived)

**Why not vanilla Ballista:** Ballista gives you the scaffolding (scheduler, executor, Arrow Flight transport) but needs significant extension for:
- Per-query auth context propagation
- OPA-aware plan rewriting before scheduling
- Iceberg-specific partition pruning at the scheduler level
- Custom resource management for PB-scale scans

**Approach: Fork Ballista scheduler, keep executor model.**

```
Coordinator (Scheduler)
  ├── Receives LogicalPlan
  ├── Applies OPA row/column filters (plan rewrite)
  ├── Runs DataFusion optimizer (predicate pushdown, projection pruning)
  ├── Converts to PhysicalPlan
  ├── Partitions by Iceberg manifest/data file groups
  ├── Assigns fragments to workers (locality-aware if on-prem)
  └── Streams results back via Arrow Flight

Worker (Executor)
  ├── Receives PhysicalPlan fragment + session context (bearer token)
  ├── Opens iceberg-rust TableProvider with user's token
  ├── Executes scan → filter → project → aggregate
  ├── Streams Arrow RecordBatches back to coordinator
  └── Reports metrics (rows scanned, bytes read, duration)
```

**Scaling model:**
- Workers are stateless, horizontally scalable (K8s Deployment)
- Coordinator can run HA with leader election (etcd or K8s lease)
- For PB queries: coordinator splits by Iceberg partition spec → manifest → data file groups
- Backpressure via Arrow Flight flow control

### 2.4 Iceberg Integration (iceberg-rust)

**Table Provider:**
```rust
struct SovereignIcebergProvider {
    catalog_url: String,       // Polaris REST endpoint
    bearer_token: String,      // From user session
    table_ident: TableIdent,
    // Cached metadata
    schema: Arc<Schema>,
    partition_spec: PartitionSpec,
    // Config
    s3_config: S3Config,       // endpoint, region, path-style
}

impl TableProvider for SovereignIcebergProvider {
    // Schema from Iceberg metadata
    // scan() → IcebergScan with predicate pushdown to manifest filtering
    // supports_filters_pushdown() → leverages partition pruning
}
```

**Iceberg v3 support: ✅ Shipped**
- iceberg-rust 0.8.0 (Jan 2026) includes V3 metadata format support
- V3 manifests with delete file content (Puffin-based deletion vectors)
- Row lineage tracking for data governance
- Default values for NULL handling (initial-default + write-default)
- No blockers — build directly on 0.8.0+

**Views:**
- Iceberg REST catalog supports `POST /v1/namespaces/{ns}/views`
- Implement as `CREATE VIEW` → serialize SQL to Polaris view representation
- On read: resolve view SQL, parse, and inline into the query plan

### 2.5 Security — Future Phase (OPA or similar)

Column-level and row-level security via OPA plan rewriting is a planned future extension. The architecture will support a `PolicyEnforcer` trait on the coordinator that rewrites the `LogicalPlan` before optimization (injecting row filters, column masks, projection stripping). This is intentionally deferred to keep the initial scope focused on the core query path.

**Design hook for later:**
```rust
/// Trait for pluggable security policy enforcement.
/// OPA, Cedar, or custom implementations can be swapped in.
trait PolicyEnforcer: Send + Sync {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan>; // Returns rewritten plan with security filters
}

/// No-op implementation for Phase 1-3
struct PassthroughEnforcer;
```

The custom SQL extensions (`GRANT`, `REVOKE`, `SHOW GRANTS`) will also be deferred until this phase, as they depend on having a policy backend to write to.

### 2.6 JDBC Access — Arrow Flight SQL

**Primary interface:** Arrow Flight SQL (JDBC driver already exists: `org.apache.arrow.flight.sql.FlightSqlClient`)

- Standard JDBC apps (DBeaver, Tableau, dbt) connect via Arrow Flight SQL JDBC driver
- Wire format is Arrow IPC — zero-copy where possible, columnar-native
- Supports `getTables`, `getSchemas`, `getCatalogs` metadata calls
- Prepared statements map to DataFusion's `LogicalPlan` caching

**Trino wire compatibility (optional, lower priority):**
- Trino uses a custom HTTP REST protocol (v1/statement)
- Implement as a thin HTTP adapter that translates Trino wire → internal DataFusion plan
- Scope: `POST /v1/statement`, `GET /v1/statement/{id}/{token}`, `DELETE`
- Enables existing Trino clients/dashboards to connect without driver changes
- Consider: is this worth the maintenance cost vs. migrating clients to Flight SQL?

### 2.7 Observability & Metrics

```
Coordinator / Workers
  │
  ├── Prometheus /metrics endpoint
  │   ├── sqe_queries_total{status, user}
  │   ├── sqe_query_duration_seconds{quantile}
  │   ├── sqe_rows_scanned_total{table}
  │   ├── sqe_bytes_read_total{table}
  │   ├── sqe_active_sessions
  │   ├── sqe_catalog_requests_total{endpoint, status}
  │   ├── sqe_s3_requests_total{operation, status}
  │   └── sqe_worker_tasks_active{worker}
  │
  ├── OpenTelemetry traces
  │   └── Per-query span tree: parse → auth → opa → optimize → schedule → execute
  │
  └── Query audit log (structured JSON)
      └── {timestamp, user, query_text, tables_accessed, opa_decision, duration, rows_returned}
```

## 3. Project Structure

```
sovereign-query-engine/
├── Cargo.toml (workspace)
├── crates/
│   ├── sqe-core/           # Shared types, config, errors
│   ├── sqe-sql/            # Extended SQL parser (sqlparser-rs fork/extension)
│   ├── sqe-auth/           # Keycloak OIDC, session manager, JWT validation
│   ├── sqe-policy/         # PolicyEnforcer trait, no-op impl (OPA/Cedar later)
│   ├── sqe-catalog/        # Iceberg REST catalog client (wraps iceberg-rust)
│   ├── sqe-planner/        # LogicalPlan → PhysicalPlan, partition-aware splitting
│   ├── sqe-coordinator/    # Scheduler, Flight SQL server, session management
│   ├── sqe-worker/         # Executor, DataFusion runtime, Flight client
│   ├── sqe-trino-compat/   # Optional Trino wire protocol adapter
│   └── sqe-metrics/        # Prometheus exporter, OTel integration
├── docker/
│   ├── Dockerfile.coordinator
│   ├── Dockerfile.worker
│   └── docker-compose.yml  # Local dev: coordinator + 2 workers + Polaris + Keycloak + MinIO
├── helm/                   # K8s deployment
├── tests/
│   ├── integration/        # End-to-end: JDBC → query → Iceberg → S3
│   └── tpc/                # TPC-H / TPC-DS benchmarks at scale
└── docs/
    └── architecture.md
```

## 4. Technology Choices

| Concern | Choice | Rationale |
|---|---|---|
| Query engine | DataFusion | Extensible, Rust-native, Arrow-native, active community |
| Distribution | Ballista (forked) | Arrow Flight transport, scheduler model, but needs auth extension |
| Iceberg | iceberg-rust 0.8.0+ | Rust-native, V3 metadata shipped, REST catalog, DataFusion integration |
| Auth | Keycloak OIDC | Your existing IdP, password grant → bearer passthrough |
| Fine-grained security | Deferred (OPA/Cedar) | Architecture has `PolicyEnforcer` trait hook; plug in later |
| JDBC | Arrow Flight SQL | Standard driver, columnar wire format, metadata API |
| Storage | S3 / MinIO | Your existing object store |
| Catalog | Apache Polaris | Your existing REST catalog |
| Metrics | Prometheus + OTel | Standard observability stack |
| Deployment | K8s (Helm) | Stateless workers, HA coordinator |
| License | Apache 2.0 | Matches your stack constraints |

## 5. Implementation Phases

### Phase 1 — Single-node proof of concept (4-6 weeks)
- DataFusion + iceberg-rust 0.8.0 reading tables via Polaris REST (v3 native)
- Keycloak OIDC token acquisition and passthrough
- Arrow Flight SQL server (single node, no distribution)
- Basic JDBC connectivity (DBeaver test)
- Goal: `SELECT * FROM iceberg_table WHERE x = 1` works end-to-end with user auth

### Phase 2 — Views & write path (3-4 weeks)
- Iceberg views (CREATE VIEW → Polaris REST, inline resolution on read)
- INSERT INTO via iceberg-rust 0.8.0 partitioned writer
- Manifest caching for metadata performance
- Audit logging (structured JSON query log)

### Phase 3 — Distributed execution (4-6 weeks)
- Ballista-derived scheduler + worker model
- Auth context propagation via Flight metadata
- Partition-aware query splitting (Iceberg manifest-level)
- Multi-worker execution with result aggregation
- Backpressure and failure handling

### Phase 4 — Production hardening (4-6 weeks)
- Prometheus metrics + OTel tracing
- TPC-H benchmarking
- Helm chart + CI/CD
- Trino wire compat (if needed)

### Phase 5 — Security & policy (future)
- OPA (or Cedar) integration with plan rewriting
- Column masks, row filters
- Custom SQL: `GRANT`, `REVOKE`, `SHOW GRANTS`
- Policy-based column redaction

### Phase 6 — Scale validation
- TPC-DS at TB/PB scale
- Concurrent query workloads
- Worker auto-scaling

## 6. Key Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| iceberg-rust v3 | ✅ Resolved | Shipped in v0.8.0 (Jan 2026) — V3 metadata, manifests, delete file content |
| Ballista maintenance uncertain | Fork divergence | Keep fork minimal; upstream what you can |
| Trino compat complexity | Scope creep | Make it optional; Flight SQL is the primary interface |
| PB-scale partition planning | Coordinator bottleneck | Stream manifest processing; hierarchical planning |
| DataFusion write path maturity | May need contrib | v0.8.0 added INSERT INTO partitioned + fanout writers — evaluate gaps |

## 7. Relation to Existing Trino Patches (DCAF branch)

Your Trino DCAF branch proves three things that directly inform this design:
1. **User-scoped token passthrough works** with Polaris — the catalog respects per-user bearer tokens
2. **Keycloak password grant → OIDC → catalog** is a viable auth flow
3. **The catalog + S3 access pattern** is well-understood and tested

SQE is essentially a clean-room rebuild of this proven pattern in a Rust-native, DataFusion-based engine where you control the full stack — no more patching a Java monolith to get the auth model you need. With iceberg-rust 0.8.0 shipping V3 metadata support and improved DataFusion integration (partitioned inserts, fanout writers), the Rust ecosystem is now mature enough to build this on.