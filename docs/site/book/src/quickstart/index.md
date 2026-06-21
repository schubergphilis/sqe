# Quickstart Recipes

This is the per-catalog recipe collection: one runnable quickstart per backend and use case. For the single get-running-in-5-minutes walkthrough, see [Getting Started > Quickstart](../getting-started/quickstart.md).

Each quickstart is a self-contained directory you can run end to end. It brings
up everything the use-case needs, runs a few useful queries, and captures the
real output as committed evidence. These pages describe each one at a high
level, what it shows and how it works, and link to the repo for the full
config, compose files, queries, and captured output.

These are the **user-facing source of truth for "how do I run SQE for X."** The
rest of the book explains why SQE is built the way it is; the quickstarts show
how to use it.

## What's possible

### Catalog + authentication (local Docker stack)

| Quickstart | What it shows | Status |
|---|---|---|
| [Polaris + Keycloak (client credentials)](./polaris-keycloak-client-id.md) | Polaris + Keycloak; SQE mints user tokens via the OIDC password grant | validated |
| [Polaris + Keycloak (user token)](./polaris-keycloak-user-token.md) | Same stack; clients bring a pre-minted Keycloak token, SQE validates + passes it through | validated |
| [Project Nessie](./nessie.md) | Nessie as the Iceberg REST catalog (auth-less, anonymous SQE) | validated |
| [Unity Catalog OSS](./unity-oss.md) | Unity Catalog OSS over Iceberg REST (read-only; catalog-browse demo) | validated |

### AWS managed catalogs (CDK bootstrap + teardown)

| Quickstart | What it shows | Status |
|---|---|---|
| [AWS S3 Tables](./aws-s3-tables.md) | AWS S3 Tables (managed Iceberg); CDK bootstrap + teardown; SQE creates the namespace | validated |
| [AWS Glue](./aws-glue.md) | AWS Glue Data Catalog; SQE creates the DB and does a full round-trip | validated |
| [AWS Glue + Lake Formation](./glue-lake-formation.md) | Glue governed by Lake Formation: denied until an explicit LF grant, then succeeds | validated |

### Embedded (single binary, `sqe-cli`)

| Quickstart | What it shows | Status |
|---|---|---|
| [Query local and remote files](./embedded-files.md) | Read files directly with the `read_*` TVFs (no server, no catalog) | validated |
| [Persistent local catalog (SQLite)](./embedded-sqlite-catalog.md) | Local persistent Iceberg catalog backed by SQLite (no server) | validated |
| [Attach multiple catalogs](./attach-catalogs.md) | Attach several persistent catalogs and JOIN across them | validated |
| [Quack (DuckDB wire protocol)](./quack.md) | SQE's DuckDB Quack RPC endpoint, both directions | experimental |

### Operations

| Quickstart | What it shows | Status |
|---|---|---|
| [Observability: metrics + Grafana](./observability.md) | Scrape SQE's Prometheus metrics with VictoriaMetrics + Grafana | validated |

### Benchmarks

| Quickstart | What it shows | Status |
|---|---|---|
| [TPC-H / TPC-DS / SSB](./benchmark.md) | Generate, load, and run the TPC suites against SQE with per-query timings | validated |

## How a quickstart is laid out

Each directory has a `README.md` (the why/how), a standalone `docker-compose.yml`,
the annotated config, a `run.sh` that brings the stack up and captures output, the
demo `queries.sql`, and an `OUTPUT.md` with the real captured result.

Run any of them from a clone of the repo:

```bash
cd quickstart/<name>
cp .env.example .env
./run.sh
```

All quickstarts live in the repo under
[`quickstart/`](https://github.com/schubergphilis/sqe/tree/main/quickstart/).
