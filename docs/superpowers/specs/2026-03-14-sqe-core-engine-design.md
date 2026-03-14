# SQE Core Engine — Design Spec

## Goal

Rebuild the Chameleon Trino fork (DCAF branch) as a purpose-built distributed SQL query engine in Rust. The core requirement: every query runs as the authenticated user — the user's Keycloak token is passed through to Polaris (Iceberg REST catalog) and S3 (via credential vending). No service accounts.

Built on DataFusion for query execution, with a custom Ballista-inspired coordinator/worker architecture for distributed scale from single-node to petabyte.

## Scope

- Distributed query execution (coordinator + workers) from day one
- Full write path: CTAS, CREATE OR REPLACE, INSERT INTO, MERGE INTO, DELETE FROM, DROP TABLE, ALTER TABLE RENAME
- Keycloak OIDC auth with token passthrough to Polaris and S3 credential vending
- Arrow Flight SQL as primary client protocol
- Trino v1/statement HTTP wire compatibility for existing clients
- Basic `information_schema` virtual schema (tables, columns, schemata) — required for Trino compat and DBeaver
- CREATE VIEW / DROP VIEW support (Iceberg views via Polaris REST)
- PassthroughEnforcer stub for future OPA/Cedar policy integration
- Connects to existing quickstart stack (Polaris, Keycloak, MinIO at data-platform/quickstart/full/)

## Non-Scope

- OPA/Cedar policy enforcement (trait stubbed, implementation deferred)
- Custom GRANT/REVOKE/SHOW GRANTS SQL (parser stubs only, returns "not configured")
- dbt-sqe Python adapter (separate project)
- Helm charts / production deployment
- Coordinator high-availability / leader election (deferred; session state must be designed to allow future HA)

---

## 1. Workspace & Crate Structure

```
sovereign-query-engine/          # this repo root
├── Cargo.toml                   # workspace
├── sqe.toml.example             # default config
├── crates/
│   ├── sqe-core/                # shared types, config, errors, Arrow helpers
│   ├── sqe-auth/                # Keycloak OIDC client, session manager, token cache + refresh
│   ├── sqe-catalog/             # Iceberg REST catalog (wraps iceberg-rust), bearer passthrough, S3 cred vending
│   ├── sqe-sql/                 # extended SQL parser (sqlparser-rs), statement classification + routing
│   ├── sqe-policy/              # PolicyEnforcer trait + PassthroughEnforcer stub
│   ├── sqe-planner/             # LogicalPlan → PhysicalPlan, Iceberg partition-aware fragment splitting
│   ├── sqe-coordinator/         # scheduler, Flight SQL server, session mgmt, statement routing, local exec
│   ├── sqe-worker/              # DataFusion executor, receives fragments + tokens via Flight
│   ├── sqe-trino-compat/        # Trino v1/statement HTTP adapter → internal execution
│   └── sqe-metrics/             # Prometheus /metrics, OpenTelemetry spans, query audit log
├── docker/
│   ├── Dockerfile.coordinator
│   ├── Dockerfile.worker
│   └── docker-compose.yml       # SQE nodes only (connects to existing quickstart network)
└── tests/
    └── integration/             # end-to-end tests against quickstart stack
```

### Binaries

- `sqe-coordinator` — main entry point (Flight SQL + Trino HTTP + scheduling + local execution)
- `sqe-worker` — stateless executor (0..N for scale)

### Dependency Flow (crate-level)

```
sqe-core ← sqe-auth ← sqe-catalog ← sqe-sql ← sqe-policy ← sqe-planner
                                                                ↑
sqe-worker depends on: sqe-core, sqe-catalog, sqe-planner
```

**Binary composition** (not crate dependencies — these are linked into the coordinator binary):
- `sqe-coordinator` binary links: all crates above + `sqe-trino-compat` + `sqe-metrics`
- `sqe-worker` binary links: `sqe-core`, `sqe-catalog`, `sqe-planner`, `sqe-metrics`

---

## 2. Auth Flow — Keycloak Token Passthrough

Ported from the trino-fork's `KeycloakAuthenticator` + `CredentialCarryingPrincipal` + `TrinoRestCatalog` session token pattern.

### sqe-auth Crate

1. **Token acquisition** — Client connects via Flight SQL with username/password. Coordinator exchanges with Keycloak via ROPC grant: `POST /realms/iceberg/protocol/openid-connect/token`, client_id=`sqe-client` (new client, same config as trino-client).

2. **Token caching** — Cache keyed by `(user, session_id)` stores `CachedToken { access_token, refresh_token, expiry }`. Passwords are NOT stored — only tokens. If the refresh token also expires, the client must re-authenticate (new Flight SQL handshake). This is safe because Flight SQL connections are short-lived compared to Trino sessions.

3. **Token refresh** — Background task refreshes tokens 60s before expiry using refresh_token. If refresh fails, the session is marked expired and the client receives an auth error prompting reconnection.

4. **Session struct** — Each connection gets a `Session` carrying user identity + live access_token. Equivalent of the trino-fork's `CredentialCarryingPrincipal`.

### ROPC Grant Justification

ROPC (password grant) is deprecated in OAuth 2.1 but is intentionally used here: JDBC/Flight SQL clients have no browser redirect capability, and this mirrors the proven trino-fork flow. Future path: support device authorization flow or token injection (client provides a pre-obtained bearer token directly) as an alternative auth method.

### Token Propagation

```
Client (username/password)
  → sqe-auth: Keycloak ROPC → access_token + refresh_token
  → Session { user, access_token, roles }
  → sqe-catalog: every Polaris REST call includes Authorization: Bearer {session.access_token}
  → sqe-catalog: S3 access via credential vending (Polaris vends scoped STS creds per table)
  → Fallback: static S3 keys from config if Polaris doesn't vend (dev/MinIO)
```

### Distributed Token Propagation

Coordinator sends bearer token + vended S3 credentials to workers in Arrow Flight metadata headers alongside plan fragments. Workers attach credentials to their own iceberg-rust catalog/S3 calls. Workers do not cache or refresh tokens independently.

**Long-running query token refresh:** For queries that outlive the access_token lifetime, the coordinator refreshes the session token and pushes updated credentials to workers via a lightweight Flight metadata update channel. Workers check for credential updates between RecordBatch processing cycles. This prevents token expiry failures on petabyte-scale queries.

### Token Fingerprinting

Ported from `TrinoRestCatalog.java`: session IDs include a hash of the token tail. This invalidates iceberg-rust's internal REST catalog session cache when a token refreshes, ensuring the REST client re-authenticates with Polaris rather than reusing a stale HTTP session. In the trino-fork, this was needed because Trino's catalog session cache was keyed on user identity alone; here it serves the same purpose for iceberg-rust's `RestCatalog` internal session state.

### Keycloak Client Configuration

New `sqe-client` registered in Keycloak realm `iceberg`:
- Type: Confidential
- Grant type: Resource Owner Password Credentials
- Same roles/scopes as existing `trino-client`

---

## 3. Catalog Integration — Iceberg REST + S3 Credential Vending

### sqe-catalog Crate

Wraps iceberg-rust's REST catalog client. Implements DataFusion's `CatalogProvider` / `SchemaProvider` / `TableProvider` traits.

**Per-session catalog instances** — each user session gets its own catalog handle with their bearer token. Not shared across users.

### S3 Credential Vending Flow

```
1. User authenticates → bearer token
2. sqe-catalog calls Polaris: GET /v1/namespaces/{ns}/tables/{table}
   → Authorization: Bearer {user_token}
3. Polaris response includes:
   - Table metadata (schema, partitions, manifests)
   - Vended S3 credentials: { access_key, secret_key, session_token, expiry }
     (Polaris calls STS assume-role scoped to table's S3 prefix)
4. sqe-catalog uses vended creds for all S3 reads/writes for that table
5. Distributed: coordinator passes vended creds to workers in Flight metadata
6. Workers use vended creds — no static S3 keys needed on workers
```

**Security model:** End-to-end. User token → Polaris authorizes → Polaris vends scoped S3 creds → workers can only touch what the user is allowed to. No static S3 keys in production.

**Fallback:** If Polaris doesn't vend credentials (e.g., MinIO dev setup without STS), fall back to static S3 keys from `[storage]` config section.

**Credential caching:** Vended creds cached per `(session, table)` with TTL matching the credential expiry from Polaris. For long-running queries that outlive vended credential TTL (typical STS: 15min–1hr), the coordinator re-vends by calling Polaris again with the refreshed bearer token and pushes updated S3 credentials to workers alongside the token refresh channel.

### Table Reads

- `IcebergTableProvider` from iceberg-rust handles scan planning, manifest filtering, partition pruning
- Predicate pushdown via DataFusion's `supports_filters_pushdown()` → Iceberg manifest-level pruning
- Bearer token forwarded on every Polaris REST call; vended S3 creds on every data file read

### Table Writes

| Operation | Flow |
|---|---|
| **CTAS** | Execute SELECT → infer Iceberg schema from Arrow → create table in Polaris (`POST /v1/namespaces/{ns}/tables`) → write Parquet via iceberg-rust DataFileWriter → commit snapshot |
| **CREATE OR REPLACE** | Same as CTAS but commits a new snapshot replacing the current. Old snapshots retained for Iceberg time-travel. Data file GC is catalog-side (Polaris expireSnapshots). |
| **CREATE VIEW** | Serialize SQL to Polaris REST view representation (`POST /v1/namespaces/{ns}/views`). On read: resolve view SQL, parse, inline into query plan. |
| **DROP VIEW** | Polaris REST: `DELETE /v1/namespaces/{ns}/views/{view}` |
| **INSERT INTO** | Execute SELECT → write new data files → append snapshot commit |
| **MERGE INTO** | Scan target + source → join in DataFusion → classify matched/unmatched → write position deletes + new data files → atomic snapshot commit (Merge-on-Read) |
| **DELETE FROM** | Scan with predicate → write position delete files → commit |
| **DROP TABLE** | Polaris REST: `DELETE /v1/namespaces/{ns}/tables/{table}` |
| **ALTER TABLE RENAME** | Polaris REST rename |

**Write strategy:** Merge-on-Read with position deletes (simpler write path, iceberg-rust supports reading position + equality deletes). Compaction deferred to later.

### Metadata Caching

Table metadata cached per-session with short TTL (30s). Cache key includes token fingerprint.

---

## 4. SQL Layer & Statement Routing

### sqe-sql Crate

Extends `sqlparser-rs` — wraps and post-transforms, does NOT fork.

### Statement Classification

```
SQL input
  → sqlparser-rs parse
  → classify:
      Query (SELECT)                → DataFusion logical planning → optimize → execute
      DDL (CREATE TABLE AS...)      → Write path handler (coordinator-only, not distributed)
      DML (INSERT, MERGE, DELETE)   → Write path handler (coordinator-only, not distributed)
      View (CREATE/DROP VIEW)       → Catalog view handler
      Catalog (SHOW CATALOGS/...)   → Metadata handler
      InfoSchema (information_schema queries) → Virtual schema provider
      Policy (GRANT, REVOKE, SHOW)  → PolicyManager (stub: "not configured")
      Utility (SET, USE, EXPLAIN)   → Session/coordinator handler
```

**Write operations execute on the coordinator only** — they are not distributed to workers. The SELECT portion of CTAS/INSERT INTO may be distributed for reads, but the actual Parquet file writing and Iceberg snapshot commits happen on the coordinator. This avoids write idempotency issues: if a worker dies mid-write, there are no orphan data files to clean up.

### Write Path SQL

All parsed natively by sqlparser-rs (no custom grammar): CTAS, CREATE OR REPLACE TABLE AS, INSERT INTO SELECT, MERGE INTO, DELETE FROM, DROP TABLE [IF EXISTS], ALTER TABLE RENAME TO.

### Statement Routing (sqe-coordinator)

```rust
match classify(parsed_statement) {
    StatementKind::Query(plan)     => plan_rewrite → optimize → distribute → execute,
    StatementKind::Ctas(ctas)      => execute_ctas(session, ctas),
    StatementKind::Insert(ins)     => execute_insert(session, ins),
    StatementKind::Merge(merge)    => execute_merge(session, merge),
    StatementKind::Delete(del)     => execute_delete(session, del),
    StatementKind::Drop(drop)      => execute_drop(session, drop),
    StatementKind::Rename(rename)  => execute_rename(session, rename),
    StatementKind::Catalog(meta)   => handle_metadata(session, meta),
    StatementKind::Policy(policy)  => policy_manager.handle(session, policy),
    StatementKind::Utility(util)   => handle_utility(session, util),
}
```

---

## 5. Distributed Execution

### Why Custom Instead of Ballista

The architecture doc considered forking Ballista. We chose custom instead because:
- **Auth passthrough is the core design constraint**, not a feature to bolt on. Ballista has no concept of per-query user credentials or token propagation to executors.
- **Iceberg-aware scheduling** (split by manifest/data file groups) requires custom partition planning that Ballista's scheduler doesn't support.
- **The distribution layer is thin** — Arrow Flight for transport, DataFusion for execution. The custom parts are: fragment serialization with credentials, worker registry with heartbeat, adaptive splitting. This is less code than fighting Ballista's extension model.
- **Ballista's maintenance status is uncertain** ([discussion #30](https://github.com/apache/datafusion-ballista/issues/30)) and there's a persistent version gap with DataFusion.

### Architecture

```
Coordinator (single process)
  ├── Flight SQL server (port 50051)
  ├── Trino HTTP adapter (port 8080)
  ├── Local DataFusion runtime (single-node mode / small queries)
  ├── Worker registry (heartbeat-based)
  └── Fragment scheduler

Worker (0..N stateless processes)
  ├── Flight server (receives plan fragments + tokens in metadata)
  ├── DataFusion runtime (executes fragments)
  └── Heartbeat to coordinator (5s interval)
```

### Query Execution Flow

1. Coordinator: parse SQL → LogicalPlan → policy rewrite (passthrough) → DataFusion optimizer
2. Coordinator decides: **local or distributed?**
   - Small query / no workers → execute locally on coordinator's DataFusion
   - Large query / workers available → distribute
3. Distributed path: PhysicalPlan → split by Iceberg partition/manifest groups → assign to workers
4. Coordinator sends fragments to workers via Arrow Flight `do_exchange`:
   - Flight metadata: `bearer_token`, `vended_s3_creds`, `fragment_id`, `session_id`
   - Payload: serialized `PhysicalPlan` fragment via `datafusion-proto` codec with custom extensions for iceberg-rust plan nodes (IcebergTableScan etc. require registered codec extensions — this is a known requirement, same issue Ballista solves with custom codecs)
5. Worker: deserialize fragment → create local DataFusion context with user's credentials → execute against Polaris/S3
6. Worker streams Arrow RecordBatches back to coordinator
7. Coordinator collects/merges results → streams to client

### Scaling: Small to Petabyte

- **Adaptive splitting:** Single-manifest tables → single fragment (stays local). Large tables → split across manifests → further split by data file groups if a single manifest is too large. Granularity adapts to data size automatically.
- **Streaming throughout:** Coordinator never materializes full result sets in memory. Workers stream RecordBatches back via Arrow Flight flow control. Backpressure propagates from client → coordinator → workers.
- **Spill-to-disk:** DataFusion's built-in memory manager with disk spill for sorts, joins, aggregations exceeding memory limits. Configurable per-worker memory budget.

### Worker Lifecycle

- Workers register with coordinator on startup via Flight call
- Periodic heartbeat (5s). Coordinator removes workers after 3 missed beats (15s)
- Coordinator tracks worker load for scheduling decisions

### Single-Node Mode

No workers registered → coordinator runs everything locally. No config change needed — just don't start worker processes. Development and testing require only the coordinator binary.

### Fragment Assignment

- Iceberg-aware: split by manifest file groups, not arbitrary row counts
- Round-robin across workers with load weighting
- Locality-aware placeholder for future on-prem data-local workers

### Failure Handling

- Worker dies mid-read-fragment → coordinator re-assigns to another worker (reads are idempotent)
- Token/credential expiry mid-query → coordinator refreshes and pushes updated creds to workers
- No workers available → falls back to local execution
- Client sees a single error if the query ultimately fails
- Write operations are coordinator-only, so worker failure cannot cause orphan data files

---

## 6. Virtual information_schema

### Why In-Scope

Trino clients (DBeaver, Tableau, Superset) issue `information_schema` queries on connection for schema browsing. Without it, the Trino compat layer is unusable for interactive tools.

### Implementation (sqe-catalog)

Register virtual `information_schema` schema with TableProviders that pull metadata from Polaris REST:

- **`information_schema.schemata`** → Polaris `listNamespaces` → `(catalog_name, schema_name, schema_owner)`
- **`information_schema.tables`** → Polaris `listTables` per namespace → `(table_catalog, table_schema, table_name, table_type)`
- **`information_schema.columns`** → Iceberg table metadata → `(table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, is_nullable)`

Each virtual provider respects the user's bearer token — users only see metadata for tables they can access (Polaris enforces this via the token).

Results benefit from metadata caching (30s TTL) — repeated `information_schema` queries during a DBeaver session or dbt run are served from cache.

---

## 7. Trino Wire Compatibility

### sqe-trino-compat Crate

Thin HTTP adapter implementing the Trino v1 REST protocol. Enough for DBeaver, Tableau, Superset, and existing dashboards using the Trino JDBC/HTTP driver.

### Endpoints

```
POST   /v1/statement              → submit SQL, returns query ID + first page
GET    /v1/statement/{id}/{token} → fetch next page of results
DELETE /v1/statement/{id}         → cancel query
```

### Implementation

- HTTP server (axum) on coordinator alongside Flight SQL
- Translates Trino wire protocol to internal execution path (same as Flight SQL after SQL received)
- Converts Arrow RecordBatches → Trino JSON column format for response pages
- Maps Trino session properties (catalog, schema) → SQE session
- Basic auth header (username/password) → same Keycloak ROPC flow

### Not Implemented

- Client tags, resource groups, query queuing
- PREPARE/EXECUTE prepared statement protocol
- Session property mutations beyond catalog/schema
- X-Trino-* headers beyond user, catalog, schema, source

---

## 8. Policy Stub

### sqe-policy Crate

```rust
#[async_trait]
trait PolicyEnforcer: Send + Sync {
    async fn evaluate(&self, user: &SessionUser, plan: LogicalPlan) -> Result<LogicalPlan>;
}

struct PassthroughEnforcer;

impl PolicyEnforcer for PassthroughEnforcer {
    async fn evaluate(&self, _user: &SessionUser, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan) // no-op
    }
}
```

Policy DDL statements (GRANT, REVOKE, SHOW GRANTS, SHOW EFFECTIVE POLICY) are parsed but return "policy engine not configured" error. The trait and routing are in place for future OPA/Cedar integration.

---

## 9. Configuration

Single `sqe.toml` for both coordinator and worker:

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "hybrid"                     # "hybrid" = local + distribute, "distributed" = workers only

[worker]
coordinator_url = "http://coordinator:50051"
heartbeat_interval_secs = 5
memory_limit = "8GB"
spill_dir = "/tmp/sqe-spill"

[auth]
keycloak_url = "https://auth.local"
realm = "iceberg"
client_id = "sqe-client"
client_secret = "changeme"
token_refresh_buffer_secs = 60
ssl_verification = false            # WARNING: dev only — MUST be true in production. Disables TLS cert verification for Keycloak.

[catalog]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "iceberg"
metadata_cache_ttl_secs = 30

[storage]
# Fallback only — production uses S3 credential vending from Polaris
s3_endpoint = "http://s3:9000"
s3_region = "us-east-1"
s3_access_key = ""
s3_secret_key = ""
s3_path_style = true

[policy]
engine = "passthrough"              # "passthrough" | "opa" | "cedar" (future)

[metrics]
prometheus_port = 9090              # worker default: 9091
otlp_endpoint = ""                  # empty = disabled
audit_log_path = "/var/log/sqe/audit.json"
```

Environment variable overrides via `SQE_` prefix (e.g., `SQE_AUTH__CLIENT_SECRET`).

---

## 10. Observability

### Prometheus Metrics

Exposed on `/metrics` endpoint (coordinator and workers):

- `sqe_queries_total{status, user, statement_type}`
- `sqe_query_duration_seconds{quantile}`
- `sqe_rows_scanned_total{table}`
- `sqe_bytes_read_total{table}`
- `sqe_active_sessions`
- `sqe_worker_count`
- `sqe_fragments_distributed_total{worker}`
- `sqe_catalog_requests_total{endpoint, status}`
- `sqe_s3_requests_total{operation, status}`
- `sqe_auth_token_refreshes_total{status}`

### OpenTelemetry Traces (optional, via OTLP)

Per-query span tree: `parse → auth → policy → optimize → schedule → execute`. For distributed queries, child spans on workers: `deserialize → execute → stream`. Distributed trace context propagated to workers via Flight metadata (W3C traceparent).

### Audit Log (structured JSON)

One line per query: `{timestamp, user, query_text, tables_accessed, statement_type, duration_ms, rows_returned, status}`

---

## 11. Testing Strategy

### Integration Tests (tests/integration/)

Run against the existing quickstart stack at `data-platform/quickstart/full/`:

1. **Auth flow** — username/password → Flight SQL connect → verify token reaches Polaris
2. **Read path** — SELECT against Iceberg tables, verify data correctness
3. **Write path** — CTAS → verify in Polaris → SELECT back. INSERT, MERGE, DELETE, DROP, RENAME
4. **S3 credential vending** — verify vended creds used, no static keys needed
5. **Distributed** — coordinator + 2 workers → verify fragments distributed, results correct
6. **Trino compat** — Trino JDBC driver → run queries → verify results match Flight SQL
7. **Token refresh** — long-running query survives token refresh mid-execution
8. **Failure** — kill worker mid-query → verify re-assignment or local fallback
9. **Scale** — large table scan distributes across manifests correctly

### Unit Tests

Per-crate for isolated logic: parser, plan splitting, token cache, config parsing, statement classification.

No mocks for catalog/S3 in integration tests — hit real quickstart services.

---

## Key Technology Choices

| Concern | Choice | Rationale |
|---|---|---|
| Language | Rust | Performance, safety, DataFusion ecosystem |
| Query engine | DataFusion | Extensible, Arrow-native, active community |
| Distribution | Custom (Ballista-inspired) | Auth passthrough is first-class, not bolted on |
| Table format | Iceberg v3 via iceberg-rust | Rust-native, REST catalog, DataFusion integration |
| Catalog | Apache Polaris | Existing deployment, credential vending |
| Auth | Keycloak OIDC (ROPC) | Existing IdP, proven in trino-fork |
| Wire protocol | Arrow Flight SQL (primary) + Trino HTTP (compat) | Flight SQL for perf, Trino for migration |
| Storage | S3 via credential vending | No static keys, per-user scoped access |
| HTTP framework | axum | Trino compat layer |
| Serialization | serde + toml | Config parsing |
| Async runtime | tokio | DataFusion/Arrow Flight requirement |

## Trino Fork Equivalence

| Trino Fork Component | SQE Equivalent |
|---|---|
| `KeycloakAuthenticator` + `KeycloakAuthenticatorClient` | `sqe-auth`: Keycloak OIDC client with ROPC + token cache + refresh |
| `CredentialCarryingPrincipal` | `Session` struct carrying user identity + access_token |
| `PasswordAuthenticator` (modified) | Flight SQL auth interceptor → sqe-auth |
| `SessionSecurityModule` + `SessionSecurityProperties` | `sqe-catalog`: per-session catalog with bearer token |
| `TrinoRestCatalog.convert()` token fingerprinting | `sqe-catalog`: token fingerprint in session ID |
| `JwtAuthenticator` token passthrough | Not needed — SQE controls the full auth flow |
| Trino distributed execution | Custom coordinator/worker over Arrow Flight |
