---
slug: unity-oss
title: "Unity Catalog OSS (Iceberg REST, read-only)"
description: "Connect SQE to Unity Catalog OSS over its Iceberg REST adapter and browse the catalog. Unity OSS's Iceberg REST is read-only at this version, so this quickstart enumerates namespaces and tables rather than running DML."
---

# Unity Catalog OSS (Iceberg REST, read-only)

[Unity Catalog OSS](https://github.com/unitycatalog/unitycatalog) exposes an
Iceberg REST adapter, so SQE connects to it through the same `rest` catalog code
path it uses for Polaris and Nessie. This quickstart connects SQE to Unity and
**browses the catalog**: it lists namespaces and tables.

Be clear-eyed about the current limitation: Unity OSS's Iceberg REST adapter is
read-only at this version. Create, drop, and commit are not supported, and the
bundled table is not served as a loadable Iceberg table, so `SELECT` does not
work either. This quickstart exists to show the connection works and to document
the boundary. For full read and write against an Iceberg REST catalog, use the
[Polaris](./polaris-keycloak-client-id.md) or [Nessie](./nessie.md) quickstarts.

## How it works

- **Unity Catalog OSS** runs with its Iceberg REST adapter enabled. It ships a
  bundled demo table (`unity.default.marksheet_uniform`).
- **SQE** connects using the same `polaris_url` config key, pointing at Unity's
  Iceberg REST mount. Auth is the `anonymous` dev-mode provider (Unity OSS runs
  without authentication).
- `run.sh` enumerates schemas and tables, then attempts a `SELECT` and captures
  the failure to document the read-only boundary.

## What it demonstrates

- SQE connecting to Unity Catalog OSS over its Iceberg REST surface.
- Catalog enumeration: `SHOW SCHEMAS` and `SHOW TABLES` return the Unity
  namespace and bundled table.
- The read boundary: a `SELECT` on the bundled table is denied by Unity OSS (not
  a SQE limitation; SQE's REST path is the same one that does full read/write
  against Polaris and Nessie).

**Status:** validated (2026-06-06).

## Run it

Full config, `docker compose`, queries, and captured output are in the repo:

**See: [quickstart/unity-oss/](https://github.com/schubergphilis/sqe/tree/main/quickstart/unity-oss/)**

```bash
cd quickstart/unity-oss
cp .env.example .env
./run.sh
```
