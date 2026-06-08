---
slug: nessie
title: "Project Nessie (Iceberg REST catalog)"
description: "Run SQE against Project Nessie. Nessie speaks the Iceberg REST protocol, so SQE uses the same rest catalog code path it uses for Polaris. Auth-less stack: SQE runs in anonymous dev mode."
---

# Project Nessie (Iceberg REST catalog)

[Project Nessie](https://projectnessie.org/) is a transactional, git-like
catalog for Iceberg tables. It exposes the Iceberg REST protocol — the same
surface as Polaris — so SQE talks to it through the identical `rest` catalog
code path. Swapping Polaris for Nessie is a one-line config change.

This quickstart is about the catalog, not auth. Nessie runs auth-less and SQE
uses its `anonymous` provider, so there is no identity provider to set up. For
the full auth story (real identities, RBAC, token passthrough) see the
[polaris-keycloak quickstarts](./polaris-keycloak-client-id.md).

## How it works

- **Nessie** runs with an in-memory version store and serves the Iceberg REST
  protocol at its `/iceberg` mount point.
- **RustFS** provides S3-compatible warehouse storage. A one-shot `bucket-init`
  container creates the warehouse bucket on startup.
- **SQE** uses the `anonymous` auth provider — every connection is accepted as a
  single anonymous identity. This mode logs a security warning on startup and
  should not be used in production.
- The `polaris_url` in SQE's catalog config simply points at Nessie's `/iceberg`
  endpoint instead of Polaris's `/api/catalog`. SQE issues the same Iceberg REST
  handshake either way.

## What it demonstrates

- SQE connecting to Nessie as an Iceberg REST catalog with no auth configuration.
- Full create/write/read round-trip: `CREATE SCHEMA` → `CREATE TABLE` →
  `INSERT` → `SELECT … GROUP BY`.
- The same `rest` catalog code path works against both Polaris and Nessie — the
  catalog is swappable by config.

**Status:** validated (2026-06-06).

## Run it

Full config, `docker compose`, queries, and captured output are in the repo:

**→ [quickstart/nessie/](https://github.com/schubergphilis/sqe/tree/main/quickstart/nessie/)**

```bash
cd quickstart/nessie
cp .env.example .env
./run.sh
```
