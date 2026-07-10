# Unity Catalog OSS (Iceberg REST, read-only)

## Goal

Connect SQE to Unity Catalog OSS over its Iceberg REST adapter and browse the catalog. SQE uses the same `rest` catalog code path it uses for Polaris and Nessie — the connection works the same way.

Be clear-eyed about the current limitation: **Unity OSS's Iceberg REST adapter is read-only at this version**. Create, drop, and commit operations are not supported, and the bundled table is not served as a loadable Iceberg table, so `SELECT` does not work either. This quickstart exists to confirm the connection works and to document where the boundary lies. For full read and write against an Iceberg REST catalog, use the Polaris or Nessie quickstarts.

## Components

| Service | Image | Role |
|---|---|---|
| `unity` | `unitycatalog/unitycatalog:main-2f2e32d` | Unity Catalog OSS. Exposes the Iceberg REST adapter at `/api/2.1/unity-catalog/iceberg`. Ships a bundled `unity.default.marksheet_uniform` table. |
| `sqe` | built from this repo | The query engine, running in anonymous auth mode (Unity OSS runs auth-less). |

## Configuration

### Backend (sqe.toml)

```toml
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]

[catalogs.unity]
polaris_url = "http://unity:8080/api/2.1/unity-catalog/iceberg"
warehouse = "unity"     # the Unity catalog name, used as the REST "warehouse"
```

`type = "anonymous"` is appropriate because Unity OSS runs without authentication. A Databricks-hosted Unity Catalog with bearer auth enabled would instead use SQE's `bearer_token` provider. The Iceberg REST mount path for Unity OSS differs from both Polaris (`/api/catalog`) and Nessie (`/iceberg`): it lives at `/api/2.1/unity-catalog/iceberg`.

### SQL (queries.sql)

```sql
-- 1. List the namespaces Unity exposes (the bundled catalog has `default`).
SHOW SCHEMAS;

-- 2. List the tables in the default namespace.
SHOW TABLES IN unity.default;
```

No DML is included because Unity OSS does not support writes or reads via Iceberg REST at this version.

## The test

`run.sh` brings up Unity and SQE, then runs `queries.sql` as the anonymous user via `sqe-cli` over Flight SQL. It asserts that SQE can connect to Unity and enumerate the catalog (`SHOW SCHEMAS`, `SHOW TABLES`). The script also runs a `SELECT` against the bundled table to capture the read limitation transparently, then writes everything to `OUTPUT.md`. The same `rest` catalog client is exercised by the `list_via_unity_rest` live test in the `sqe-catalog` integration suite (last validated 2026-06-06). Tear down with `./run.sh --down`.

## Output

```
## SQE browsing Unity Catalog OSS over Iceberg REST

sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
+-------------+
| schema_name |
+-------------+
| default     |
+-------------+
+-----------+-------------------+
| namespace | table_name        |
+-----------+-------------------+
| default   | marksheet_uniform |
+-----------+-------------------+

## Read attempt: Unity OSS does not serve the bundled table for SELECT

$ sqe-cli -e "SELECT * FROM unity.default.marksheet_uniform LIMIT 3"
Error: "table 'unity.default.marksheet_uniform' not found"
```
