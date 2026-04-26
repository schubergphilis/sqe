# SQE -- Sovereign Query Engine

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Rust-based distributed SQL query engine for [Apache Iceberg](https://iceberg.apache.org/) tables. Built on [DataFusion](https://datafusion.apache.org/) and [iceberg-rust](https://github.com/apache/iceberg-rust), with pluggable OIDC authentication and bearer token passthrough to [Apache Polaris](https://polaris.apache.org/) REST Catalog.

Every query runs as the authenticated user. No service account.

## Architecture

```
Client (JDBC / Flight SQL / HTTP)
        |
        v
   +-----------+     OIDC Provider
   |Coordinator|<-- (Keycloak, Auth0,
   |           |     Okta, or any IdP)
   | DataFusion|
   |  + Policy |---> OPA / Cedar
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

For detailed Mermaid diagrams (query pipeline, crate dependencies, caching layers, distributed execution), see [docs/architecture.md](docs/architecture.md).

## Features

- **SQL**: Full ANSI SQL via DataFusion 53 -- window functions, CTEs, subqueries, joins, aggregates, GROUPING SETS, ROLLUP
- **DDL/DML**: CREATE TABLE AS SELECT, INSERT INTO, DELETE, UPDATE, MERGE INTO (CoW), CREATE/DROP VIEW, ALTER TABLE
- **Iceberg**: Time travel, metadata TVFs (snapshots, manifests, files, partitions), partition evolution, schema evolution
- **Protocols**: Arrow Flight SQL (primary) + Trino HTTP (compatibility)
- **Auth**: Pluggable chain -- OIDC, bearer token, API key, mTLS, anonymous, AWS IAM, device code, token exchange
- **Distributed**: Coordinator-worker architecture with shuffle, spill-to-disk, adaptive sort
- **Observability**: OpenTelemetry, Prometheus, JSON audit log, `system.runtime.queries` virtual table
- **Performance**: 5-layer caching, star-schema join reorder, dynamic filter pushdown, ZSTD compression
- **Security**: 43/43 audit findings resolved. See [docs/issues.md](docs/issues.md)

## Getting Started

### Prerequisites

- **Rust 1.88+** ([rustup.rs](https://rustup.rs/))
- **Docker** and Docker Compose

### Quick start

```bash
# Start the test stack (Polaris + S3 + SQE)
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
cargo run --release --bin sqe-coordinator -- tests/sqe-test.toml

# Connect with the CLI
cargo run --bin sqe-cli -- --host localhost --port 50051 --username root --protocol flight

# Run a query
sqe> SHOW CATALOGS;
sqe> SELECT * FROM test_warehouse.default.my_table LIMIT 10;
```

For Docker, Kubernetes, TLS, and auth provider setup, see [docs/deployment.md](docs/deployment.md).

## Benchmark Results (SF1 vs Trino 465)

| Suite | SQE | Trino | Speedup | Pass |
|---|---|---|---|---|
| TPC-H (22) | 21.9s | 30.4s | **2.1x** | 22/22 |
| SSB (13) | 6.2s | 4.8s | 0.8x | 13/13 |
| TPC-DS (99) | 50.6s | 31.6s | 1.0x | 99/99 |
| TPC-C (8 read) | 0.5s | 1.6s | **3.4x** | 7/8 |
| TPC-BB (10) | 45.4s | 197.2s | **2.3x** | 10/10 |
| ClickBench (43) | 1.6s | 3.7s | **2.6x** | 43/43 |

**SQE wins 5 of 7 suites at SF1.** 222/222 queries pass across the full suite (TPC-H 22 + TPC-DS 99 + SSB 13 + TPC-C 17 + TPC-E 18 + TPC-BB 10 + ClickBench 43), 154.8s end-to-end. Known limitation: [TPC-DS q72](docs/blog/2026-04-16-our-nemesis-q72.md) (upstream DataFusion CBO gap).

Run your own benchmarks:

```bash
# All-in-one: generate, load, test, compare with Trino
BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpch tpcds ssb clickbench
```

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `sqe-core` | Shared types, config (TOML), errors |
| `sqe-sql` | SQL parser, statement classifier, GRANT/REVOKE |
| `sqe-auth` | Pluggable auth chain (10 providers), token cache |
| `sqe-catalog` | Iceberg REST client, caching, scan execution |
| `sqe-policy` | Policy enforcement (passthrough, OPA) |
| `sqe-planner` | Plan splitting, star-schema reorder, join strategy |
| `sqe-coordinator` | Flight SQL server, query handler, Trino HTTP |
| `sqe-worker` | Stateless DataFusion executor |
| `sqe-cli` | Interactive SQL client |
| `sqe-metrics` | Prometheus, OpenTelemetry, audit logger |
| `sqe-trino-compat` | Trino wire protocol |
| `sqe-bench` | Benchmark suite (7 suites, 222 queries) |

## Configuration

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
```

Full configuration reference: [docs/deployment.md](docs/deployment.md).

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust |
| Query Engine | Apache DataFusion 53 |
| Table Format | Apache Iceberg v2/v3 |
| Catalog | Apache Polaris (Iceberg REST) |
| Wire Protocol | Arrow Flight SQL + Trino HTTP |
| Storage | S3-compatible (AWS, Ceph, MinIO, R2) |
| Observability | OpenTelemetry + Prometheus |
| License | Apache 2.0 |

## Documentation

| Doc | What |
|-----|------|
| [Architecture](docs/architecture.md) | Mermaid diagrams: query pipeline, crate deps, caching, distributed |
| [Deployment](docs/deployment.md) | Docker Compose, Kubernetes, TLS, auth providers, monitoring |
| [Roadmap](docs/roadmap.md) | Full feature checklist (completed, in progress, planned) |
| [Security Audit](docs/issues.md) | 43 findings, all resolved |
| [Trino Compatibility](docs/trino-compatibility.md) | SQL feature matrix vs Trino |
| [Performance Roadmap](docs/specs/performance-roadmap.md) | Optimization history, remaining gaps |

## Blog

| Post | Topic |
|------|-------|
| [Why We Replaced Trino with Rust](docs/blog/2026-03-22-why-we-replaced-trino-with-rust.md) | The decision to build SQE |
| [Benchmark Suite](docs/blog/2026-03-24-benchmark-suite.md) | 7 suites, 222 queries |
| [Trino Compatibility Journey](docs/blog/2026-04-09-trino-compatibility-journey.md) | 63% to 95% SQL coverage |
| [Streaming Writes and Correctness](docs/blog/2026-04-10-streaming-writes-and-correctness.md) | OOM fix, sort order safety |
| [Five Layers of Caching and an 8.8x Speedup](docs/blog/2026-04-12-caching-and-the-8x-speedup.md) | The caching strategy |
| [Security Hardening: 43 Findings](docs/blog/2026-04-13-security-hardening-43-findings.md) | Production audit |
| [DataFusion 53 and the Iceberg Fork](docs/blog/2026-04-14-datafusion-53-and-the-iceberg-fork.md) | DF 53 upgrade, vendoring |
| [Our Nemesis: TPC-DS Q72](docs/blog/2026-04-16-our-nemesis-q72.md) | The one query we can't beat |
| [The Iceberg Matrix and the Quiet Bug Hiding in V3](docs/blog/2026-04-26-the-matrix-and-the-quiet-bug.md) | Integration tests find what unit tests miss |

## Book

SQE's design and development journey is documented in the ebook **"Sovereign by Design: Building a Production Query Engine on DataFusion"** (20 chapters). Source in [docs/ebook/](docs/ebook/). Build with `cd docs/ebook && make`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to report issues, submit pull requests, and run tests.

## License

Apache License 2.0. See [LICENSE](LICENSE).
