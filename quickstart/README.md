---
slug: quickstarts
title: "SQE Quickstarts"
description: "Self-contained, validated quickstarts for running SQE against every catalog and deployment shape: Polaris + Keycloak, AWS S3 Tables / Glue / Lake Formation, Nessie, embedded mode, observability, and benchmarks."
---

# SQE Quickstarts

Each quickstart is a self-contained directory you can run end to end. It brings
up everything the use-case needs, runs a few useful queries, and captures the
real output as committed evidence. The README in each directory explains the
why and the how for a new user, and every config option is annotated.

These are the **user-facing source of truth** for "how do I run SQE for X." The
ebook and `docs/book` stay developer-oriented (why SQE is built the way it is);
the quickstarts answer how to use it, and double as a validation base. Running a
quickstart's `run.sh` from a clean state proves the use-case works.

## How a quickstart is laid out

```
quickstart/
  _shared/                  shared bootstrap assets (DRY), referenced by relative path
    keycloak/realm-iceberg.json
    polaris/bootstrap.sh
    lib.sh
  <name>/
    README.md               frontmatter + why / how / config / run / output / tested / gotchas
    docker-compose.yml       runnable standalone; mounts _shared/ assets
    sqe.toml or CLI flags    the annotated config
    run.sh                   up -> bootstrap -> queries -> capture -> (down)
    queries.sql              the demo queries
    OUTPUT.md                captured real output
    .env.example             defaults (offset ports, placeholder secrets)
```

Run any of them with:

```bash
cd quickstart/<name>
cp .env.example .env
./run.sh
```

## The quickstarts

Grouped into batches by what they share. Status reflects what has been built and
validated from a clean state.

### A. Catalog + authentication (local Docker stack)

| Quickstart | What it shows | Status |
|---|---|---|
| [`polaris-keycloak-client-id`](./polaris-keycloak-client-id/) | Polaris + Keycloak; SQE mints user tokens via the OIDC password grant (client credentials) | **validated 2026-06-06** |
| [`polaris-keycloak-user-token`](./polaris-keycloak-user-token/) | Same stack; clients bring a pre-minted Keycloak token (`--token`), SQE validates + passes it through | **validated 2026-06-06** |
| [`nessie`](./nessie/) | Project Nessie as the Iceberg REST catalog (auth-less, anonymous SQE) | **validated 2026-06-06** |
| [`unity-oss`](./unity-oss/) | Unity Catalog OSS over Iceberg REST (read-only upstream; catalog-browse demo) | **validated 2026-06-06** |

### B. AWS managed catalogs (CDK bootstrap + teardown)

| Quickstart | What it shows | Status |
|---|---|---|
| [`aws-s3-tables`](./aws-s3-tables/) | AWS S3 Tables (managed Iceberg). CDK bootstrap + teardown; SQE creates the namespace | **validated 2026-06-06** |
| [`aws-glue`](./aws-glue/) | AWS Glue Data Catalog (CDK bootstrap + teardown; SQE creates the DB so it works under Lake Formation) | **validated 2026-06-06** |
| `glue-lake-formation` | Glue with Lake Formation fine-grained access. CDK bootstrap + teardown | planned |

### C. Embedded (single binary, `sqe-cli`)

| Quickstart | What it shows | Status |
|---|---|---|
| [`embedded-files`](./embedded-files/) | Read local and remote files directly with the `read_*` TVFs (no server, no catalog) | **validated 2026-06-06** |
| [`embedded-sqlite-catalog`](./embedded-sqlite-catalog/) | Local persistent Iceberg catalog backed by SQLite (no server) | **validated 2026-06-06** |
| `quack-server` | The Quack protocol server | planned |
| `quack-client` | The Quack protocol client | planned |
| [`attach-catalogs`](./attach-catalogs/) | Attach multiple persistent catalogs in embedded mode + cross-catalog JOIN | **validated 2026-06-06** |

### D. Operations

| Quickstart | What it shows | Status |
|---|---|---|
| [`observability`](./observability/) | Scrape SQE's Prometheus metrics with VictoriaMetrics + Grafana (provisioned "SQE Overview" dashboard) | **validated 2026-06-07** |

### E. Benchmarks

| Quickstart | What it shows | Status |
|---|---|---|
| [`benchmark`](./benchmark/) | Generate + load + run TPC-H / TPC-DS / SSB against SQE, with per-query timings (`sqe-bench`) | **validated 2026-06-07** |

## Design

See [`docs/superpowers/specs/2026-06-06-quickstarts-source-of-truth-design.md`](../docs/superpowers/specs/2026-06-06-quickstarts-source-of-truth-design.md)
for the design and the relationship to the older doc-only use-cases work.
