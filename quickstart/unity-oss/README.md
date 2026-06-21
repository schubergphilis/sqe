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
./run.sh             # up -> browse the catalog -> capture output
./run.sh --down      # tear everything down
./run.sh --check     # up -> browse -> assert the namespace + table enumerate
```

`run.sh` brings up Unity + SQE, browses the catalog, and captures the result to
[`OUTPUT.md`](./OUTPUT.md). It also runs a `SELECT` to show, transparently, that
Unity OSS does not serve the table for reads. Tear down with `./run.sh --down`.

## How it works

SQE has one Iceberg REST client. It does not know or care whether the catalog on
the other end is Polaris, Nessie, or Unity. Point `polaris_url` at Unity's
Iceberg REST mount (`/api/2.1/unity-catalog/iceberg`) and SQE issues the same
`GET /v1/config` handshake, then `list-namespaces` and `list-tables` over the
same protocol.

What differs is the catalog's own capability. Unity OSS's Iceberg REST adapter
serves the metadata surface (namespaces, table listings) but not the loadable
table or the commit endpoints, so SQE can browse but a `SELECT` against the
bundled table returns `not found`. This quickstart shows the connection and the
boundary, not a SQE limitation: the identical client does full read and write
against Polaris and Nessie.

## Configuration explained

### The catalog (the whole point)

```toml
[catalogs.unity]
polaris_url = "http://unity:8080/api/2.1/unity-catalog/iceberg"
warehouse = "unity"     # the Unity catalog name, used as the REST "warehouse"
metadata_cache_ttl_secs = 30
```

`polaris_url` is the Iceberg REST base URL, here pointed at Unity's mount rather
than `.../api/catalog` (Polaris) or `.../iceberg` (Nessie). `warehouse` is the
Unity catalog name, sent as the REST `warehouse` parameter in the config
handshake. The TOML key (`unity`) is the SQL catalog name, so identifiers are
`unity.<namespace>.<table>`. `metadata_cache_ttl_secs` caches table metadata for
30 seconds to cut REST round-trips.

### Auth (anonymous, dev only)

```toml
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]
```

Unity OSS runs with auth disabled, so SQE uses the same `anonymous` dev-mode
provider as the Nessie quickstart: every connection is accepted as a single
`anonymous` identity. SQE logs a security warning at startup. A Databricks-hosted
Unity (with bearer auth on) would instead use SQE's `bearer_token` or an
`oidc_password` provider.

### Storage

The `[storage]` block is present but its S3 credentials are placeholders
(`access_key = "unused"`), because this scenario never reads data files: it only
browses metadata. Against a catalog that serves loadable tables, `[storage]`
points SQE's S3 client at the object store holding the Iceberg data.

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

`./run.sh --check` re-runs the browse query (`queries.sql`) against Unity over
Iceberg REST and asserts the invariants in `run.sh`:

- Unity exposes the `default` namespace,
- Unity lists the `marksheet_uniform` table,
- the browse contains no `error`.

The check deliberately does not assert any `SELECT` rows, because Unity OSS's
Iceberg REST is read-only and does not serve the bundled table for reads. The
invariant is exactly the metadata browse the scenario is built to show. The same
`rest` client is also exercised by the live test
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
