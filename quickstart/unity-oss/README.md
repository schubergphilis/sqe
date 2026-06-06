---
slug: unity-oss
title: "Unity Catalog OSS (Iceberg REST, read-only)"
description: "Connect SQE to Unity Catalog OSS over its Iceberg REST adapter and browse the catalog. Unity OSS's Iceberg REST is read-only at this version, so this quickstart enumerates namespaces and tables rather than running DML."
---

# Unity Catalog OSS (Iceberg REST, read-only)

[Unity Catalog OSS](https://github.com/unitycatalog/unitycatalog) exposes an
Iceberg REST adapter, so SQE connects to it through the same `rest` catalog code
path it uses for Polaris and Nessie. This quickstart points SQE at Unity and
**browses the catalog**: it lists namespaces and tables.

Be clear-eyed about the limitation: **Unity OSS's Iceberg REST adapter is
read-only at this version**. Create / drop / commit are not supported, and the
bundled table is not served as a loadable Iceberg table, so `SELECT` does not
work either ([unitycatalog#3](https://github.com/unitycatalog/unitycatalog/issues/3)).
For full read and write against an Iceberg REST catalog, use the
[Polaris](../polaris-keycloak-client-id/) or [Nessie](../nessie/) quickstarts.
This one exists to show the connection works and to document the boundary.

## What you get

| Service | Image | Role |
|---|---|---|
| `unity` | `unitycatalog/unitycatalog:main-2f2e32d` | Unity Catalog OSS, Iceberg REST adapter at `/api/2.1/unity-catalog/iceberg`. Ships a bundled `unity.default.marksheet_uniform` table. |
| `sqe` | built from this repo | The query engine, anonymous auth (Unity OSS runs auth-less). |

## Prerequisites

- Docker. The Unity OSS image is pinned to a specific `main-*` tag (the project
  does not publish semver tags).

## Run it

```bash
cd quickstart/unity-oss
cp .env.example .env
./run.sh
```

`run.sh` brings up Unity + SQE, browses the catalog, and captures the result to
[`OUTPUT.md`](./OUTPUT.md). It also runs a `SELECT` to show, transparently, that
Unity OSS does not serve the table for reads. Tear down with `./run.sh --down`.

## Configuration

```toml
[catalogs.unity]
polaris_url = "http://unity:8080/api/2.1/unity-catalog/iceberg"
warehouse = "unity"     # the Unity catalog name, used as the REST "warehouse"
```

Auth is the same `anonymous` dev-mode provider as the [Nessie](../nessie/)
quickstart, because Unity OSS runs without authentication. A Databricks-hosted
Unity (with bearer auth on) would instead use SQE's `bearer_token` or a machine
token provider.

## Output

Captured from a clean run (`./run.sh`), committed in [`OUTPUT.md`](./OUTPUT.md):

```
# SHOW SCHEMAS
+-------------+
| schema_name |
+-------------+
| default     |
+-------------+

# SHOW TABLES IN unity.default
+-----------+-------------------+
| namespace | table_name        |
+-----------+-------------------+
| default   | marksheet_uniform |
+-----------+-------------------+

# SELECT * FROM unity.default.marksheet_uniform
Error: table 'unity.default.marksheet_uniform' not found   (Unity OSS read-only)
```

## How it is tested

`run.sh` asserts SQE can connect to Unity and enumerate the catalog
(`SHOW SCHEMAS`, `SHOW TABLES`), and captures the read attempt to document the
limitation. The same `rest` client is exercised by the live test
`unity_catalog::list_via_unity_rest` in the `sqe-catalog` integration suite.
Last validated 2026-06-06.

## Gotchas

- **Read-only**: this is a metadata/browse demo by necessity, not a SQE
  limitation. SQE's REST path is identical to the one that does full read/write
  against Polaris and Nessie.
- **Image tag is pinned** to a `main-*` commit; Unity OSS does not publish
  semver images.
- The Unity OSS image ships `wget`, not `curl`, which is why the healthcheck
  uses `wget`.
