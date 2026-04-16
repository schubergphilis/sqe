# SQE -- Sovereign Query Engine

![Build](badge_url) ![License](badge_url)

A Rust-based distributed SQL query engine for Apache Iceberg tables. Built on [DataFusion](https://datafusion.apache.org/) and [iceberg-rust](https://github.com/apache/iceberg-rust), with pluggable OIDC authentication and bearer token passthrough to [Apache Polaris](https://polaris.apache.org/) REST Catalog.

Designed as a drop-in replacement for Trino in environments where all data lives in Iceberg and fine-grained security (row filters, column masks) is required.

Licensed under the [Apache License 2.0](LICENSE).

## Getting Started

### Prerequisites

- **Rust 1.88+** (install via [rustup](https://rustup.rs/))
- **Docker** and Docker Compose
- **Apache Polaris** (included in the Docker Compose stack)
- S3-compatible storage (MinIO is included in the Docker Compose stack)

### Quick start

```bash
# Start the full stack (Polaris, MinIO, SQE coordinator)
docker compose -f docker-compose.test.yml up --build -d

# Or build and run locally
cargo build --release
SQE_CONFIG=config.toml cargo run --bin sqe-coordinator
```

### First query

Connect with the CLI and run a query against an Iceberg table:

```bash
# Start the interactive SQL client
cargo run --bin sqe-cli -- --host localhost --port 50051 --username admin --protocol flight

# Inside the REPL
SHOW CATALOGS;
SHOW SCHEMAS;
SELECT * FROM warehouse.sales.orders LIMIT 10;
```

## Architecture

```
Client (JDBC / Flight SQL / HTTP)
        |
        v
   +-----------+     OIDC Provider
   |Coordinator|<-- (Keycloak, Auth0,
   |           |     Okta, or any IdP)
   | DataFusion|
   |  + Policy |---> OPA / Cedar (planned)
   +-----+-----+
         | Bearer token passthrough
    +----+----+
    v    v    v
  +---++---++---+   Stateless workers
  | W1 || W2 || W3 |  (distributed mode)
  +-+--++-+--++-+--+
    |     |     |
    v     v     v
   +-----------+
   |  Polaris  |---> S3-compatible storage
   |REST Catalog|
   +-----------+
```

Every query runs as the authenticated user. No service account.

For detailed architecture diagrams, component breakdown, and design decisions, see [docs/datafusion-architecture.md](docs/datafusion-architecture.md).

## Features

- **SQL**: Full ANSI SQL via DataFusion -- window functions (LEAD, LAG, PARTITION BY, etc.), CTEs, subqueries, joins, aggregates, GROUPING SETS. See [docs/features.md](docs/features.md) for a detailed comparison with Trino and Spark.
- **DDL/DML**: CREATE TABLE AS SELECT, INSERT INTO, DELETE FROM, UPDATE, MERGE INTO (CoW), CREATE/DROP VIEW, DROP TABLE, ALTER TABLE RENAME
- **Protocols**: Arrow Flight SQL (primary, gRPC) + Trino HTTP (compatibility layer)
- **Auth**: Pluggable auth chain -- OIDC password, bearer token, API key, mTLS, anonymous, AWS IAM, device code, token exchange
- **Catalog**: Apache Polaris REST Catalog with per-session bearer token passthrough
- **Storage**: S3-compatible storage via Iceberg's FileIO (credential vending or static config)
- **Distributed**: Coordinator to worker architecture with shuffle, distributed sort/join/aggregate, spill-to-disk
- **Observability**: OpenTelemetry (traces, metrics, logs via OTLP/gRPC), Prometheus metrics, JSON audit log
- **Security**: Planned OPA/Cedar policy engine for row-level security and column masking
- **CLI**: Interactive REPL and one-shot mode with Flight SQL and HTTP backends

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `sqe-core` | Shared types, config (TOML), errors |
| `sqe-sql` | SQL parser (sqlparser-rs), statement classifier |
| `sqe-auth` | Pluggable auth chain (OIDC, bearer, API key, mTLS, anonymous, AWS IAM, device code), token cache |
| `sqe-catalog` | Iceberg REST catalog client, DataFusion catalog/schema providers, information_schema |
| `sqe-policy` | PolicyEnforcer trait, passthrough implementation |
| `sqe-planner` | LogicalPlan manipulation, distributed plan splitting |
| `sqe-coordinator` | Flight SQL server, query handler, session manager, Trino HTTP compat |
| `sqe-worker` | Stateless DataFusion executor for distributed mode |
| `sqe-cli` | Interactive SQL client (Flight SQL + HTTP) |
| `sqe-metrics` | Prometheus registry, OpenTelemetry setup, audit logger |
| `sqe-trino-compat` | Trino wire protocol types and HTTP handlers |

## Configuration

SQE uses a TOML config file:

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080

[auth]
issuer_url = "https://idp.example.com/realms/my-realm"
client_id = "sqe"

[catalog]
polaris_url = "https://polaris.example.com/api/catalog"
warehouse = "my_warehouse"

[storage]
s3_endpoint = "https://s3.example.com"
s3_region = "eu-west-1"

[metrics]
prometheus_port = 9090
otlp_endpoint = "http://otel-collector:4317"
```

## Example Queries

```sql
-- Window functions
SELECT customer_id, amount,
  LEAD(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount,
  SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM warehouse.sales.orders;

-- CTE with aggregation
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS total
  FROM warehouse.sales.orders
  GROUP BY 1
)
SELECT month, total, LAG(total) OVER (ORDER BY month) AS prev_month
FROM monthly;

-- CTAS
CREATE TABLE warehouse.analytics.summary AS
SELECT region, COUNT(*) AS cnt, AVG(amount) AS avg_amount
FROM warehouse.sales.orders
GROUP BY region;

-- Views
CREATE VIEW warehouse.analytics.active_customers AS
SELECT customer_id, MAX(order_date) AS last_order
FROM warehouse.sales.orders
GROUP BY customer_id
HAVING MAX(order_date) > CURRENT_DATE - INTERVAL '90' DAY;

-- Metadata
SHOW CATALOGS;
SHOW SCHEMAS;
SHOW TABLES IN sales;
SELECT * FROM warehouse.information_schema.columns WHERE table_name = 'orders';
```

## Roadmap

- [x] Distributed execution (coordinator to worker plan shipping, fragment scheduler, heartbeat, credential refresh)
- [x] Iceberg predicate pushdown (DataFusion 52 optimizer pass)
- [x] Trino HTTP compatibility (pagination, header handling, dual auth, infoUri, system.jdbc.* metadata)
- [x] Worker observability (metrics, OTel trace propagation, memory limits, spill-to-disk)
- [x] Integration and E2E test suite
- [x] Benchmark suite (sqe-bench: TPC-H, TPC-DS, SSB, TPC-C, TPC-E, TPC-BB; `read_parquet()` TVF)
- [x] Flight SQL DoPut (Arrow data ingestion, statement updates, GetTableTypes, GetXdbcTypeInfo)
- [x] Complete data type formatting (all Arrow types to Trino, benchmark comparator, value serialization)
- [x] system.runtime.* virtual tables (queries, nodes, tasks -- query history and cluster topology)
- [x] Distributed query execution wiring (coordinator to worker scan dispatch, fragment tracking, fallback)
- [x] DELETE, UPDATE, MERGE INTO via Copy-on-Write (RisingWave iceberg-rust fork rewrite_files)
- [ ] OPA/Cedar policy engine (row filters, column masks, GRANT/REVOKE SQL)
- [x] OSS security hardening (TLS, rate limiting, query timeouts, session lifecycle, error sanitization, vendor-neutral naming)
- [x] OSS release readiness (Apache 2.0 license, CONTRIBUTING.md, cargo-deny, git-cliff, CI pipelines, retro-tagging, v0.15.0)
- [x] Security and functional audit (AUDIT.md: 1,218 tests, rsa crate removed, all advisory checks clean)
- [x] Production security hardening (43/43 audit findings resolved: token-fingerprint session cache, Flight SQL auth on all endpoints, OIDC error sanitization, OPA role-aware cache, CTAS orphan cleanup, 16 panic-to-error conversions, adaptive sort default)
- [x] Streaming execution Phase A: spill-to-disk, late materialization, file pruning, S3 I/O pipeline, SortMergeJoin fallback
- [x] Streaming execution Phase B: DoExchange shuffle, distributed sort/join/aggregate, multi-endpoint Flight SQL, stage decomposition
- [x] Adaptive sort stripping and S3/auth/write Prometheus metrics
- [x] Observability metrics (spill, shuffle, late-mat, pruning, time-to-first-row)
- [x] Trino SQL compatibility ~95% (see `docs/trino-compatibility.md`): 70+ UDFs, engine-level features (USE, SHOW CREATE TABLE, TRUNCATE, COMMENT ON, SHOW STATS, TRY, format, to_json), Iceberg time travel, 6 metadata TVFs
- [x] Pluggable auth providers (OIDC, bearer token, API key, mTLS, anonymous, AWS IAM, device code, token exchange)
- [x] Iceberg metadata TVFs (`table_snapshots()`, `table_manifests()`, `table_history()`, `table_files()`, `table_partitions()`, `table_refs()` -- snapshot/manifest/partition/ref introspection via SQL)
- [x] Iceberg time travel (`SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01'` -- snapshot resolution + per-session provider registration)
- [ ] Pluggable catalog backends (AWS Glue, Nessie, Hive Metastore, storage-only)
- [x] dbt adapter (dbt-sqe via ADBC Flight SQL -- table, view, incremental, seed)
- [x] ALTER TABLE schema evolution (ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening)
- [x] Trino SQL compatibility matrix (`docs/trino-compatibility.md`)
- [x] Side-by-side benchmark tooling (`sqe-bench compare`)
- [x] Streaming CTAS and INSERT INTO (constant-memory write path, eliminates OOM on SF1+ loads)
- [x] Auto-rewrite `IN (subquery)` for UPDATE/DELETE (DataFusion physical planner workaround, unblocks 5 TPC-E queries)
- [x] Safe Iceberg sort order handling (only partition columns trusted by default, `trust_sort_order` config for opt-in)
- [x] Trino comparison benchmarks (`--compare-trino` flag runs identical queries against SQE + Trino, diffs results)
- [x] Iceberg query caching (table metadata + manifest files + RestCatalog instances) -- SQE beats Trino on TPC-H, SSB, TPC-DS
- [x] Direct Parquet read path for small files (3 MB or less, configurable) -- single S3 GET, bypasses `scan.to_arrow()` redundant requests
- [x] DECIMAL precision fix (`parse_float_as_decimal = true`) -- matches Trino/SQL standard, fixes incorrect query results
- [x] Tuple IN-subquery rewrite (`(col1,col2) IN (SELECT ...)` to OR of ANDs)
- [x] SessionContext caching per token fingerprint (SHA-256, 5-min TTL, 100-entry, atomic via moka `try_get_with`) -- eliminates ~50 ms per-query overhead, invalidated after all DDL
- [ ] Semantic AI layer (RDF/SPARQL, property graph/GQL, vector search, agent interfaces)
- [ ] Helm chart for Kubernetes deployment

## Benchmark Results

**SF0.01, Apr 16 2026 -- SQE on DataFusion 53 vs Trino 465:**

| Suite | SQE | Trino | Speedup | Match |
|---|---|---|---|---|
| TPC-H (22) | 1.3s | 6.9s | **5.3x** | 22/22 |
| SSB (13) | 0.7s | 1.8s | **2.6x** | 13/13 |
| TPC-DS (99) | 11.6s | 22.6s | **1.9x** | 99/99 |
| ClickBench (43) | 0.7s | 1.8s | **2.6x** | 43/43 |
| TPC-C (8 read) | 0.3s | 1.0s | **3.6x** | 7/8 |
| TPC-E (11) | 0.3s | 1.1s | **3.9x** | 7/11 |
| TPC-BB (10) | 0.8s | 1.2s | **1.4x** | 10/10 |
| **Total** | **15.7s** | **36.4s** | **2.3x avg** | **201/206** |

SQE is faster than Trino on every benchmark suite. DataFusion 53 upgrade (from 52) gave a 35% speedup. 5-layer caching + ETag validation + ZSTD compression + LZ4 Flight responses. TPC-DS improved from 93/99 to 99/99 with the DF 53 upgrade.

## Benchmarks

SQE ships with `sqe-bench`, a CLI tool for generating benchmark data, loading it into SQE via the `read_parquet()` TVF, and running query suites to validate correctness and measure performance.

### Supported benchmarks

| Benchmark | Queries | Schema | Focus |
|-----------|---------|--------|-------|
| TPC-H | 22 | 8 tables | Star/snowflake, analytical reads |
| TPC-DS | 99 | 24 tables | Complex SQL, advanced analytics |
| SSB | 13 | 5 tables | Denormalized star, smoke testing |
| TPC-C | 17 | 9 tables | OLTP mix (read + write: DELETE, UPDATE via CoW) |
| TPC-E | 18 | 33 tables | Brokerage OLTP (read + write: UPDATE, DELETE via CoW) |
| TPC-BB | 10 | ~25 tables | SQL-only subset over TPC-DS data + web logs |

### Quick start

```bash
# Generate TPC-H data at scale factor 1 (~1 GB)
cargo run -p sqe-bench -- generate tpch --scale 1 --output ./data

# Load into SQE (creates namespace tpch_sf1 with all 8 tables via CTAS)
cargo run -p sqe-bench -- load tpch --scale 1 --data ./data --host localhost --port 60051 --username root --password ""

# Run all 22 TPC-H queries and report correctness + timing
cargo run -p sqe-bench -- test tpch --scale 1 --host localhost --port 60051 --username root --password ""

# Or use the all-in-one script
./scripts/benchmark-test.sh tpch
```

Results are written to the terminal and saved as a JSON report in `benchmarks/results/`.

---

## Security

SQE has completed a 43-finding production security audit (see `docs/issues.md`). All 43 findings resolved:

| Category | Findings | Status |
|---|---|---|
| Auth and access control | Session cache keyed by token SHA-256, all Flight SQL endpoints authenticated, cancel-query owner verification, AnonymousProvider/ClientCredentials startup warnings | Resolved |
| Information leaking | OIDC error bodies sanitized (7 files), generic errors to clients | Resolved |
| Panic safety | 16 date `.unwrap()` removed, all `[0]` index guards, startup panics converted to `Result` | Resolved |
| Policy | OPA cache key includes user roles (role changes enforced immediately) | Resolved |
| Data integrity | CTAS orphan table cleanup on failure, adaptive sort (never OOM, never silent wrong results) | Resolved |
| Crypto | Token fingerprints use SHA-256 (stable, deterministic), `checksum()` UDF uses SHA-256 | Resolved |
| Supply chain | Third-party iceberg-rust fork pinned by rev, documented in `deny.toml` | Mitigated |

Rate limiting applies to both Flight SQL and Trino HTTP paths. Worker secret is validated at startup in distributed mode. TLS skip is configurable via `tls_skip_verify` (unambiguous) or legacy `ssl_verification`.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust |
| Query Engine | Apache DataFusion 53 |
| Table Format | Apache Iceberg v2 (iceberg-rust 0.9) |
| Catalog | Apache Polaris (Iceberg REST) |
| Auth | Pluggable chain (OIDC, bearer token, API key, mTLS, anonymous, AWS IAM) |
| Wire Protocol | Arrow Flight SQL + Trino HTTP |
| Storage | S3-compatible (AWS S3, Ceph, Garage, R2, etc.) |
| Observability | OpenTelemetry + Prometheus |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to report issues, submit pull requests, and run the test suite.

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.
