# SQE -- Sovereign Query Engine

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Rust-based distributed SQL query engine for [Apache Iceberg](https://iceberg.apache.org/) tables. Built on [DataFusion](https://datafusion.apache.org/) 53.1 and [iceberg-rust](https://github.com/apache/iceberg-rust), with pluggable OIDC authentication and bearer token passthrough to [Apache Polaris](https://polaris.apache.org/) REST Catalog.

Every query runs as the authenticated user. No service account.

**Iceberg coverage: 165/189 (87.3%)** on the public [icebergmatrix.org](https://icebergmatrix.org) scoreboard, fifth overall and the only top-five entry that is not a Spark distribution. See [`docs/iceberg-matrix.md`](docs/iceberg-matrix.md) for the per-cell breakdown and [`docs/iceberg-matrix-compare.md`](docs/iceberg-matrix-compare.md) for the V2/V3 side-by-side against every other engine on the public scoreboard.

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

- **SQL**: Full ANSI SQL via DataFusion 53.1: window functions, CTEs, subqueries, joins, aggregates, GROUPING SETS, ROLLUP. JSON columns, TIME columns, all standard scalar / aggregate / window functions.
- **DDL/DML**: CREATE TABLE AS SELECT, INSERT INTO, DELETE, UPDATE, MERGE INTO. Default Copy-on-Write; opt into Merge-on-Read with `TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')` for position-delete or equality-delete writers (commits via `FastAppendAction` / `RowDeltaAction`).
- **Iceberg**: V2 and V3 end-to-end. Time travel, metadata TVFs (snapshots, manifests, files, partitions, history, refs), partition evolution, schema evolution, hidden partitioning, type promotion, column defaults, nanosecond timestamps, branching and tagging.
- **Catalogs**: Apache Polaris (default), Project Nessie, Unity Catalog OSS, AWS Glue (native SDK), AWS S3 Tables (native SDK), Hive Metastore, JDBC (Postgres/MySQL/SQLite), Hadoop storage-only. All routed through the upstream `iceberg-catalog-loader` factory. Live-tested against real services in `crates/sqe-catalog/tests/backends_integration.rs`. AWS endpoints also reachable through SigV4-signed Iceberg REST.
- **Protocols**: Arrow Flight SQL (primary) + Trino HTTP (compatibility)
- **Auth**: Pluggable chain -- OIDC, bearer token, API key, mTLS, anonymous, AWS IAM, device code, token exchange
- **Distributed**: Coordinator-worker architecture with shuffle, spill-to-disk, adaptive sort
- **Observability**: OpenTelemetry, Prometheus, JSON audit log, `system.runtime.queries` virtual table
- **Performance**: 5-layer caching, star-schema join reorder, dynamic filter pushdown, ZSTD compression
- **Security**: 43/43 audit findings resolved. See [docs/issues.md](docs/issues.md)

## Getting Started

For a five-minute walkthrough that covers all six catalog backends
(REST / HMS / Glue / S3 Tables / JDBC / Hadoop) with sample TOML
configs and verification queries, see [`QUICKSTART.md`](QUICKSTART.md).

### Prerequisites

- **Rust 1.88+** ([rustup.rs](https://rustup.rs/))
- **Docker** and Docker Compose (only for the bundled local stack)

### Quick start (local Polaris stack)

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

For other backends (Glue, S3 Tables, HMS, JDBC, Hadoop) the same
binary works against external infrastructure: see
[`QUICKSTART.md`](QUICKSTART.md) and [`docs/catalogs.md`](docs/catalogs.md).

For Docker, Kubernetes, TLS, and auth provider setup, see [docs/deployment.md](docs/deployment.md).

## Benchmark Results (SF1 vs Trino 465)

| Suite | SQE | Trino | Speedup | Pass |
|---|---|---|---|---|
| TPC-H (22) | 14.5s | 20.0s | **1.4x** | 22/22 |
| SSB (13) | 6.2s | 4.8s | 0.8x | 13/13 |
| TPC-DS (99) | 50.6s | 31.6s | 1.0x | 99/99 |
| TPC-C (8 read) | 0.5s | 1.6s | **3.4x** | 7/8 |
| TPC-BB (10) | 45.4s | 197.2s | **2.3x** | 10/10 |
| ClickBench (43) | 1.6s | 3.7s | **2.6x** | 43/43 |

**SQE wins 5 of 7 suites at SF1.** 222/222 queries pass across the full suite (TPC-H 22 + TPC-DS 99 + SSB 13 + TPC-C 17 + TPC-E 18 + TPC-BB 10 + ClickBench 43). Known limitation: [TPC-DS q72](docs/blog/2026-04-16-our-nemesis-q72.md) (upstream DataFusion CBO gap).

TPC-H specifically benefits from the runtime filter pushdown work (Path B + B-2): SF1 dropped from 21.9s to 14.5s (-21% SQE total, broad per-query gains on lineitem-heavy joins), and SF10 dropped from 163.9s to 143.6s (-12.4%, biggest absolute wins q06 -4.9s, q07 -4.9s, q14 -2.7s). q15 RowDiff (a TPC-H correctness flag from float-based bench data) is also fixed. See [docs/features/runtime-filter-pushdown.md](docs/features/runtime-filter-pushdown.md) for design + the engineering log of five rejected follow-up fixes.

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
| Query Engine | Apache DataFusion 53.1 |
| Table Format | Apache Iceberg v2 / v3 |
| Catalog | Apache Polaris (default), Project Nessie, Unity Catalog OSS, AWS Glue (native), AWS S3 Tables (native), Hive Metastore, JDBC (Postgres/MySQL/SQLite), Hadoop storage-only. Loader-based dispatch via the upstream `iceberg-catalog-loader` factory |
| Wire Protocol | Arrow Flight SQL + Trino HTTP |
| Storage | S3-compatible (AWS, Ceph, R2, rustfs) + local filesystem |
| Observability | OpenTelemetry + Prometheus |
| License | Apache 2.0 |

## Documentation

| Doc | What |
|-----|------|
| [Architecture](docs/architecture.md) | Mermaid diagrams: query pipeline, crate deps, caching, distributed |
| [Deployment](docs/deployment.md) | Docker Compose, Kubernetes, TLS, auth providers, monitoring |
| [Iceberg Matrix](docs/iceberg-matrix.md) | Per-cell SQE coverage on the public scoreboard (165/189, 87.3%) |
| [Iceberg Matrix Comparison](docs/iceberg-matrix-compare.md) | V2/V3 side-by-side against 20 other engines |
| [Trino Compatibility](docs/trino-compatibility.md) | SQL feature matrix vs Trino (~96% coverage) |
| [Catalog Backends](docs/catalogs.md) | Per-backend TOML, credentials, verification queries |
| [Roadmap](docs/roadmap.md) | Full feature checklist (completed, in progress, planned) |
| [Security Audit](docs/issues.md) | 43 findings, all resolved |
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
| [Why a Public Iceberg Matrix Beats Vendor Spec Sheets](docs/blog/2026-04-29-the-iceberg-matrix-as-a-scoreboard.md) | A scoreboard for the lakehouse ecosystem |
| [SQE Talks to Five Catalogs Now: HMS, Nessie, Glue, JDBC, S3 Tables](docs/blog/2026-04-29-five-catalogs-live.md) | The live verification phase + AWS SigV4 |

## Book

SQE's design and development journey is documented in the ebook **"Sovereign by Design: Building a Production Query Engine on DataFusion"** (19 chapters across five parts, ~370 pages). Source in [docs/ebook/](docs/ebook/). Build with `cd docs/ebook && make`. Two of the chapters track the Iceberg matrix journey end to end: chapter 16b ("The Matrix and the Quiet Bug") covers the first honest pass from 99/189 to 129/189; chapter 16c ("Following Through") picks up the punch list and walks the next six months to 164/189.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to report issues, submit pull requests, and run tests.

## License

Apache License 2.0. See [LICENSE](LICENSE).
