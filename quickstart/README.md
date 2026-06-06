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
| `polaris-keycloak-user-token` | Same stack; clients bring a pre-minted Keycloak token (`--token`), SQE validates + passes it through | planned |
| `nessie` | Project Nessie as the Iceberg REST catalog | planned |
| `rest-catalogs` | Other Iceberg REST catalogs (Unity OSS, generic REST) | planned |

### B. AWS managed catalogs (CDK bootstrap + teardown)

| Quickstart | What it shows | Status |
|---|---|---|
| `s3-tables` | AWS S3 Tables (managed Iceberg). CDK stack to create + destroy the test resources | planned |
| `glue` | AWS Glue Data Catalog. CDK bootstrap + teardown | planned |
| `glue-lake-formation` | Glue with Lake Formation fine-grained access. CDK bootstrap + teardown | planned |

### C. Embedded (single binary, `sqe-cli`)

| Quickstart | What it shows | Status |
|---|---|---|
| `embedded-files` | Read local and remote files directly with the `read_*` TVFs | planned |
| `embedded-sqlite-catalog` | Local persistent Iceberg catalog backed by SQLite | planned |
| `quack-server` | The Quack protocol server | planned |
| `quack-client` | The Quack protocol client | planned |
| `attach-catalogs` | Attaching multiple catalogs / cloud backends in embedded mode | planned |

### D. Operations

| Quickstart | What it shows | Status |
|---|---|---|
| `observability` | Metrics, logs, and traces (VictoriaMetrics + Grafana, or OpenObserve) | planned |

### E. Benchmarks

| Quickstart | What it shows | Status |
|---|---|---|
| `benchmark` | Run TPC-H / TPC-DS / SSB and produce comparable numbers | planned |

## Design

See [`docs/superpowers/specs/2026-06-06-quickstarts-source-of-truth-design.md`](../docs/superpowers/specs/2026-06-06-quickstarts-source-of-truth-design.md)
for the design and the relationship to the older doc-only use-cases work.
