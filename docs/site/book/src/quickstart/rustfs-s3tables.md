---
slug: rustfs-s3tables
title: "RustFS S3 Tables (Iceberg REST catalog inside the object store)"
description: "Run SQE against RustFS S3 Tables, the Iceberg REST catalog built into RustFS. No separate catalog service; SQE signs the catalog calls with the S3 keys (SigV4, service name s3). Auth-less stack: SQE runs in anonymous dev mode."
---

# RustFS S3 Tables (Iceberg REST catalog inside the object store)

[RustFS](https://rustfs.com/) 1.0.0-rc.6 ships S3 Tables: an Iceberg REST
catalog served by the object store itself, at `/iceberg` on the S3 port. A
bucket becomes a table bucket with one management call. From then on RustFS
stores the Iceberg metadata and data as objects and answers the Iceberg REST
protocol for them. There is no Polaris, Nessie or Glue in this stack.

This quickstart is about the catalog, not auth. SQE uses its `anonymous`
provider, so there is no identity provider to set up. For the full auth story
(real identities, RBAC, token passthrough) see the
[polaris-keycloak quickstarts](./polaris-keycloak-client-id.md).

## How it works

- **RustFS** serves the S3 API and the Iceberg REST catalog on one port. A
  one-shot container creates the `warehouse` bucket and a second one enables
  S3 Tables on it with `PUT /iceberg/v1/buckets/warehouse`.
- **The catalog is signed like S3.** RustFS requires AWS SigV4 under the
  service name `s3` on every catalog request, including the payload hash
  header, but does not advertise `rest.sigv4-enabled` in `/v1/config`. The
  `[catalogs.rustfs.auth]` block with `type = "aws"` and `signing_name = "s3"`
  tells SQE to sign client-side with the `[storage]` keys.
- **SQE** uses the `anonymous` auth provider: every connection is accepted as a
  single anonymous identity. This mode logs a security warning on startup and
  should not be used in production.
- RustFS vends no credentials, so `require_vended_credentials = false` and SQE
  reads and writes the data files with the same `[storage]` keys.

## What it demonstrates

- SQE connecting to an Iceberg REST catalog that lives inside the object store,
  with one key pair for metadata and data.
- Client-side SigV4 for a REST catalog that does not announce it.
- Full round-trip: `CREATE SCHEMA`, `CREATE TABLE`, `INSERT`, `SELECT ... GROUP
  BY`, CTAS and `SHOW TABLES`.

**Status:** validated (2026-09-14).

## Run it

Full config, `docker compose`, queries, and captured output are in the repo:

**See: [quickstart/rustfs-s3tables/](https://github.com/schubergphilis/sqe/tree/main/quickstart/rustfs-s3tables/)**

```bash
cd quickstart/rustfs-s3tables
cp .env.example .env
./run.sh
```
