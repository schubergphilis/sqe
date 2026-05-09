# SQE: Sovereign Query Engine

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**An Iceberg-first SQL engine. Embedded as one binary on your laptop. Distributed across a cluster. Same SQL, same Iceberg, same identity model.**

SQE is a Rust-based SQL query engine for [Apache Iceberg](https://iceberg.apache.org/) tables, built on [DataFusion 53.1](https://datafusion.apache.org/) and [iceberg-rust](https://github.com/apache/iceberg-rust). Every query runs as the authenticated user. No service account. No shared root.

```bash
cargo install --path crates/sqe-cli
sqe-cli --embedded
sqe> SELECT * FROM 's3://datalake/sales/2026/*.parquet' WHERE region = 'EU';
```

## Why it is cool

**Top-five on the public [Iceberg matrix](https://icebergmatrix.org). 167/189. 88.4%.** Only non-Spark engine in the top five. Per-cell breakdown in [`docs/iceberg-matrix.md`](docs/iceberg-matrix.md); side-by-side against 20 other engines in [`docs/iceberg-matrix-compare.md`](docs/iceberg-matrix-compare.md).

**Wins six of seven benchmark suites against Trino 465 at SF1.** TPC-H, SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench. 222 of 222 queries pass. Tables and method below.

**One binary scales from CLI to cluster.** `sqe-cli --embedded` is a DuckDB-class single-process engine with the same SQL surface as the distributed coordinator. Persistent SQLite-backed Iceberg catalogs at `~/.sqe/warehouse/` survive restarts. Cross-catalog joins across multiple `--catalog NAME=PATH` mounts.

**Multi-catalog and multi-cloud, in one engine.** Apache Polaris, Project Nessie, Unity Catalog OSS, AWS Glue (native SDK), AWS S3 Tables (native SDK), Hive Metastore, JDBC (Postgres, MySQL, SQLite), and Hadoop storage-only. Object stores: S3 (with endpoint override for Ceph, R2, Garage, MinIO), Azure ADLS, GCS, local filesystem, HuggingFace `hf://`.

**Identity flows end to end.** OIDC password grant. The user's bearer token is passed through to Polaris and S3 on every query. Row filters and column masks via OPA or Cedar are enforced at the LogicalPlan layer before the optimizer touches it. No information leakage. PostgreSQL-style RLS semantics.

**Lineage shipped.** Coordinator emits OpenLineage 2-0-2 events with column-level lineage on writes. File and HTTP sinks. Disk-spool fallback for collector outages. Off by default. [`docs/book/src/operations/openlineage.md`](docs/book/src/operations/openlineage.md).

## How SQE differs

|  | SQE | Trino | DuckDB |
|---|---|---|---|
| **Embedded mode** (one binary, no cluster) | yes | no | yes |
| **Distributed mode** (coordinator + workers) | yes | yes | no |
| **Iceberg V2 + V3 read + write** | native | V2 + partial V3 | extension, read-only |
| **Per-query OIDC bearer passthrough** | yes | service account only | n/a (single-tenant) |
| **OPA / Cedar policy at LogicalPlan** | yes | no | no |
| **Multi-catalog in one engine** | 7 backends | one at a time | per-extension |
| **Wire protocols** | Arrow Flight SQL + Trino HTTP | Trino HTTP | extension |
| **Runtime** | Rust binary, no JVM | JVM | C++ binary |
| **Cold start** | sub-second | tens of seconds | sub-second |
| **OpenLineage emitter** | native, column-level | plugin | no |

Two longer comparison docs trace the lineage of these positions:

- vs Trino: [`docs/trino-compatibility.md`](docs/trino-compatibility.md). SQL function and feature parity by category. ~96% coverage.
- vs DuckDB: [`docs/duckdb-comparision.md`](docs/duckdb-comparision.md). What SQE has that DuckDB does not, and vice versa, with the V8 to V12 work that closed the embedded-mode gap.

## Performance receipts (SF1, vs Trino 465)

| Suite | SQE | Trino | Speedup | Pass |
|---|---|---|---|---|
| TPC-H (22) | 19.3s | 26.6s | **2.3x** | 22/22 |
| SSB (13) | 7.6s | 8.3s | **1.1x** | 13/13 |
| TPC-DS (99) | 57.1s | 39.7s | **1.4x slower** | 93/99 |
| TPC-C (8 read) | 0.45s | 3.4s | **9.6x** | 8/8 |
| TPC-E (11) | 10.4s | 138.8s | **7.8x** | 11/11 |
| TPC-BB (10) | 36.9s | 323.6s | **5.5x** | 10/10 |
| ClickBench (43) | 1.7s | 6.3s | **4.6x** | 43/43 |

SQE wins six of seven suites. The TPC-DS regression is concentrated in TPC-DS Q72, where DataFusion's lack of full CBO with NDV statistics costs SQE 13x against Trino. The story of that one query is [docs/blog/2026-04-16-our-nemesis-q72.md](docs/blog/2026-04-16-our-nemesis-q72.md). The May 2026 numbers reflect manifest-derived column statistics flowing into DataFusion's optimizer for the first time, plus Path B+B-2 runtime-filter pushdown ([docs/features/runtime-filter-pushdown.md](docs/features/runtime-filter-pushdown.md)).

Run your own:

```bash
BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpch tpcds ssb clickbench
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
   |  + Policy |---> OPA / Cedar
   +-----+-----+
         | Bearer token passthrough
    +----+----+
    v    v    v
   +---++---++---+   Stateless workers
   | W1|| W2|| W3|  (distributed mode)
   +---++---++---+
    |     |     |
    v     v     v
   +-----------+
   |  Polaris  |---> S3-compatible storage
   |REST Catalog|
   +-----------+
```

Detailed Mermaid diagrams (query pipeline, crate dependencies, caching layers, distributed execution, write path) in [`docs/architecture.md`](docs/architecture.md).

## Get started

Five-minute walkthrough covering all seven catalog backends with sample TOML and verification queries: [`QUICKSTART.md`](QUICKSTART.md).

### Embedded mode (one binary, no cluster)

```bash
cargo install --path crates/sqe-cli
sqe-cli --embedded                  # persistent warehouse at ~/.sqe/warehouse/

sqe> SELECT * FROM '/data/sales.parquet' LIMIT 5;
sqe> SELECT * FROM read_csv('s3://bucket/orders.tsv.gz');
sqe> SELECT * FROM 'hf://datasets/squad/plain_text/train-00000-of-00001.parquet' LIMIT 5;
sqe> SELECT * FROM read_delta('/data/delta/sales', version => '5');
```

Full embedded reference: [`docs/cli-embedded.md`](docs/cli-embedded.md).

### Cluster mode (Polaris + S3 + SQE locally)

```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
cargo run --release --bin sqe-coordinator -- tests/sqe-test.toml

# Connect with the CLI
cargo run --bin sqe-cli -- --host localhost --port 50051 --username root --protocol flight

sqe> SHOW CATALOGS;
sqe> SELECT * FROM test_warehouse.default.my_table LIMIT 10;
```

Same binary against external infrastructure (Glue, S3 Tables, HMS, JDBC, Hadoop): see [`QUICKSTART.md`](QUICKSTART.md) and [`docs/catalogs.md`](docs/catalogs.md).

Docker, Kubernetes, TLS, and auth provider setup: [`docs/deployment.md`](docs/deployment.md).

## Documentation

The reference docs:

| Doc | What |
|---|---|
| [Architecture](docs/architecture.md) | Mermaid diagrams across the engine |
| [Deployment](docs/deployment.md) | Docker Compose, K8s, TLS, auth providers, monitoring |
| [Iceberg Matrix](docs/iceberg-matrix.md) | Per-cell SQE coverage on the public scoreboard |
| [Iceberg Matrix Comparison](docs/iceberg-matrix-compare.md) | V2/V3 side-by-side against 20 engines |
| [Trino Compatibility](docs/trino-compatibility.md) | SQL function and feature matrix vs Trino |
| [DuckDB Comparison](docs/duckdb-comparision.md) | Symmetry between SQE and DuckDB on the embedded side |
| [Embedded CLI Reference](docs/cli-embedded.md) | All flags, dot-commands, TVFs, catalog backends, storage backends |
| [SQL Feature Comparison](docs/features.md) | SQE vs Trino vs Spark SQL vs DuckDB across windows, aggregates, DML, Iceberg, file-format TVFs |
| [Catalog Backends](docs/catalogs.md) | Per-backend TOML, credentials, verification queries |
| [Operations: OpenLineage](docs/book/src/operations/openlineage.md) | Lineage emit, sinks, troubleshooting |
| [Roadmap](docs/roadmap.md) | Full feature checklist |
| [Security Audit](docs/issues.md) | 43 findings, all resolved |

## The book

SQE's design and development journey is documented in **"Sovereign by Design: Building a Production Query Engine on DataFusion"**.

Twenty chapters across five parts. Roughly 370 pages. The story of choosing DataFusion, surviving the Iceberg fork rebase, lifting the matrix from 31% to 88%, building the embedded mode that turned out to be a DuckDB-shaped surprise, and shipping column-level lineage. Source in [`docs/ebook/`](docs/ebook/). Build with `cd docs/ebook && make`.

A few chapters worth reading first:

- [chapter 04 ("You Are the Query")](docs/ebook/chapters/04-you-are-the-query.md) on per-query identity
- [chapter 09 ("What You Cannot See")](docs/ebook/chapters/09-what-you-cant-see.md) on observability
- [chapters 16b and 16c](docs/ebook/chapters/) on the Iceberg matrix journey from 99/189 to 164/189
- [chapter 16d ("The DuckDB Drift")](docs/ebook/chapters/16d-the-duckdb-drift.md) on building embedded mode in two days
- [chapter 16e ("The Lineage Trail")](docs/ebook/chapters/16e-the-lineage-trail.md) on shipping OpenLineage
- [chapter 17 ("What We Would Do Differently")](docs/ebook/chapters/17-what-wed-do-differently.md) on the retro

## Blog

Engineering posts that double as design rationale:

| Post | Topic |
|---|---|
| [Why We Replaced Trino with Rust](docs/blog/2026-03-22-why-we-replaced-trino-with-rust.md) | The decision to build SQE |
| [Five Layers of Caching and an 8.8x Speedup](docs/blog/2026-04-12-caching-and-the-8x-speedup.md) | Caching strategy across the stack |
| [Security Hardening: 43 Findings](docs/blog/2026-04-13-security-hardening-43-findings.md) | Production audit |
| [DataFusion 53 and the Iceberg Fork](docs/blog/2026-04-14-datafusion-53-and-the-iceberg-fork.md) | DF 53 upgrade and vendoring decision |
| [Our Nemesis: TPC-DS Q72](docs/blog/2026-04-16-our-nemesis-q72.md) | The one query we cannot beat |
| [Why a Public Iceberg Matrix Beats Vendor Spec Sheets](docs/blog/2026-04-29-the-iceberg-matrix-as-a-scoreboard.md) | A scoreboard for the lakehouse |
| [SQE Talks to Five Catalogs Now](docs/blog/2026-04-29-five-catalogs-live.md) | The live verification phase plus AWS SigV4 |
| [How We Accidentally Created a DuckDB](docs/blog/2026-05-07-accidentally-duckdb.md) | V8 to V12: file-format TVFs, hf://, Delta, smarter read_csv |
| [Shipping OpenLineage](docs/blog/2026-05-09-shipping-openlineage.md) | Column-level lineage from idea to merged MR |

Full archive in [`docs/blog/`](docs/blog/).

## Crate structure

| Crate | Purpose |
|---|---|
| `sqe-core` | Shared types, config (TOML), errors |
| `sqe-sql` | SQL parser, statement classifier, GRANT/REVOKE |
| `sqe-auth` | Pluggable auth chain (10 providers), token cache |
| `sqe-catalog` | Iceberg REST client, caching, scan execution |
| `sqe-policy` | Policy enforcement (passthrough, OPA) |
| `sqe-planner` | Plan splitting, star-schema reorder, join strategy |
| `sqe-coordinator` | Flight SQL server, query handler, Trino HTTP |
| `sqe-worker` | Stateless DataFusion executor |
| `sqe-cli` | Interactive SQL client (cluster + embedded modes) |
| `sqe-metrics` | Prometheus, OpenTelemetry, audit logger |
| `sqe-lineage` | OpenLineage 2-0-2 emitter; column-level lineage |
| `sqe-trino-compat` | Trino wire protocol |
| `sqe-bench` | Benchmark suite (7 suites, 222 queries) |

## Tech stack

| Component | Technology |
|---|---|
| Language | Rust |
| Query Engine | Apache DataFusion 53.1 |
| Table Format | Apache Iceberg V2 / V3 |
| Catalogs | Polaris, Nessie, Unity Catalog OSS, AWS Glue, AWS S3 Tables, Hive Metastore, JDBC, Hadoop |
| Wire Protocols | Arrow Flight SQL + Trino HTTP |
| Storage | S3, Ceph, R2, ADLS, GCS, local filesystem, HuggingFace `hf://` |
| Observability | OpenTelemetry, Prometheus, OpenLineage 2-0-2 |
| License | Apache 2.0 |

## Contributing

Issues, pull requests, and how to run tests: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
