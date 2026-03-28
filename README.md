# SQE — Sovereign Query Engine

A Rust-based SQL query engine for Apache Iceberg tables. Built on [DataFusion](https://datafusion.apache.org/) and [iceberg-rust](https://github.com/apache/iceberg-rust), with Keycloak OIDC authentication and bearer token passthrough to [Apache Polaris](https://polaris.apache.org/) REST Catalog.

Designed as a drop-in replacement for Trino in environments where all data lives in Iceberg and fine-grained security (row filters, column masks) is required.

## Architecture

```
Client (JDBC / Flight SQL / HTTP)
        │
        ▼
   ┌──────────┐     Keycloak
   │Coordinator│◄───  OIDC
   │           │     (ROPC)
   │ DataFusion│
   │  + Policy │──► OPA / Cedar (planned)
   └─────┬─────┘
         │ Bearer token passthrough
         ▼
   ┌──────────┐
   │  Polaris  │──► S3 / MinIO
   │REST Catalog│
   └──────────┘
```

Every query runs as the authenticated user. No service account.

## Features

- **SQL**: Full ANSI SQL via DataFusion — window functions (LEAD, LAG, PARTITION BY, etc.), CTEs, subqueries, joins, aggregates, GROUPING SETS. See [docs/features.md](docs/features.md) for a detailed comparison with Trino and Spark.
- **DDL**: CREATE TABLE AS SELECT, INSERT INTO, CREATE/DROP VIEW, DROP TABLE, ALTER TABLE RENAME
- **Protocols**: Arrow Flight SQL (primary, gRPC) + Trino HTTP (compatibility layer)
- **Auth**: OIDC password grant (any OIDC provider) with background token refresh
- **Catalog**: Apache Polaris REST Catalog with per-session bearer token passthrough
- **Storage**: S3-compatible storage via Iceberg's FileIO (credential vending or static config)
- **Observability**: OpenTelemetry (traces, metrics, logs via OTLP/gRPC), Prometheus metrics, JSON audit log
- **Security**: Planned OPA/Cedar policy engine for row-level security and column masking
- **CLI**: Interactive REPL and one-shot mode with Flight SQL and HTTP backends

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `sqe-core` | Shared types, config (TOML), errors |
| `sqe-sql` | SQL parser (sqlparser-rs), statement classifier |
| `sqe-auth` | OIDC password grant (Keycloak, Auth0, Okta, etc.), token cache, background refresh |
| `sqe-catalog` | Iceberg REST catalog client, DataFusion catalog/schema providers, information_schema |
| `sqe-policy` | PolicyEnforcer trait, passthrough implementation |
| `sqe-planner` | LogicalPlan manipulation, distributed plan splitting |
| `sqe-coordinator` | Flight SQL server, query handler, session manager, Trino HTTP compat |
| `sqe-worker` | Stateless DataFusion executor for distributed mode |
| `sqe-cli` | Interactive SQL client (Flight SQL + HTTP) |
| `sqe-metrics` | Prometheus registry, OpenTelemetry setup, audit logger |
| `sqe-trino-compat` | Trino wire protocol types and HTTP handlers |

## Quick Start

### Build

```bash
cargo build --release
```

### Docker

```bash
# Build all images
docker build --target coordinator -t sqe-coordinator .
docker build --target worker -t sqe-worker .
docker build --target cli -t sqe-cli .
```

### Configuration

SQE uses a TOML config file:

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080

[auth]
keycloak_url = "https://keycloak.example.com"
realm = "my-realm"
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

### Run

```bash
# Coordinator
SQE_CONFIG=config.toml cargo run --bin sqe-coordinator

# CLI
cargo run --bin sqe-cli -- --host localhost --port 50051 --username admin --protocol flight
```

### Example Queries

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

- [x] Distributed execution (coordinator → worker plan shipping, fragment scheduler, heartbeat, credential refresh)
- [x] Iceberg predicate pushdown (DataFusion 52 optimizer pass)
- [x] Trino HTTP compatibility (pagination, header handling, dual auth, infoUri, system.jdbc.* metadata)
- [x] Worker observability (metrics, OTel trace propagation, memory limits, spill-to-disk)
- [x] Integration & E2E test suite
- [x] Benchmark suite (sqe-bench: TPC-H, TPC-DS, SSB, TPC-C, TPC-E, TPC-BB; `read_parquet()` TVF)
- [x] Flight SQL DoPut (Arrow data ingestion, statement updates, GetTableTypes, GetXdbcTypeInfo)
- [x] Complete data type formatting (all Arrow types → Trino, benchmark comparator, value serialization)
- [x] system.runtime.* virtual tables (queries, nodes, tasks — query history and cluster topology)
- [ ] MERGE INTO, DELETE (blocked on iceberg-rust Merge-on-Read, ETA Q3 2026)
- [ ] OPA/Cedar policy engine (row filters, column masks, GRANT/REVOKE SQL)
- [x] OSS security hardening (TLS, rate limiting, query timeouts, session lifecycle, error sanitisation, vendor-neutral naming)
- [ ] Pluggable auth providers (bearer token, API key, mTLS, anonymous)
- [ ] Pluggable catalog backends (AWS Glue, Nessie, Hive Metastore, storage-only)
- [ ] Semantic AI layer (RDF/SPARQL, property graph/GQL, vector search, agent interfaces)
- [ ] dbt adapter (dbt-sqe via ADBC Flight SQL)
- [ ] Helm chart for Kubernetes deployment

## Benchmarks

SQE ships with `sqe-bench`, a CLI tool for generating benchmark data, loading it into SQE via the `read_parquet()` TVF, and running query suites to validate correctness and measure performance.

### Supported benchmarks

| Benchmark | Queries | Schema | Focus |
|-----------|---------|--------|-------|
| TPC-H | 22 | 8 tables | Star/snowflake, analytical reads |
| TPC-DS | 99 | 24 tables | Complex SQL, advanced analytics |
| SSB | 13 | 5 tables | Denormalized star, smoke testing |
| TPC-C | 8 | 9 tables | OLTP mix (read queries; write queries require DELETE/MERGE) |
| TPC-E | 11 | 33 tables | Brokerage OLTP (read queries) |
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

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust |
| Query Engine | Apache DataFusion 52 |
| Table Format | Apache Iceberg v2 (iceberg-rust 0.9) |
| Catalog | Apache Polaris (Iceberg REST) |
| Auth | OIDC (Keycloak, Auth0, Okta, or any OIDC provider) |
| Wire Protocol | Arrow Flight SQL + Trino HTTP |
| Storage | S3-compatible (AWS S3, Ceph, Garage, R2, etc.) |
| Observability | OpenTelemetry + Prometheus |
