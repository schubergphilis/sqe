# Project Nessie (Iceberg REST catalog)

## Goal

Connect SQE to Project Nessie, a transactional git-like catalog for Iceberg tables. Nessie exposes the Iceberg REST protocol, the same surface Apache Polaris exposes, so SQE connects through the identical `rest` catalog code path. Swapping Polaris for Nessie is a single config line: point `polaris_url` at Nessie's `/iceberg` endpoint instead.

This quickstart is about the catalog integration, not authentication. Nessie runs auth-less and SQE uses its `anonymous` provider, so there is no identity provider to configure. For the full auth story (real identities, RBAC, token passthrough) see the `polaris-keycloak-client-id` and `polaris-keycloak-user-token` quickstarts.

## Components

| Service | Image | Role |
|---|---|---|
| `rustfs` | `rustfs/rustfs` | S3-compatible object store. The Iceberg warehouse lives here. |
| `bucket-init` | `amazon/aws-cli` | One-shot: creates the `warehouse` bucket, then exits. |
| `nessie` | `ghcr.io/projectnessie/nessie:0.107.5` | Iceberg REST catalog with in-memory version store and S3 storage on RustFS. |
| `sqe` | built from this repo | The query engine, running in anonymous auth mode. |

## Configuration

### Backend (sqe.toml)

```toml
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]

[catalogs.nessie]
polaris_url = "http://nessie:19120/iceberg"   # Nessie's Iceberg REST mount
warehouse = "warehouse"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true
```

`type = "anonymous"` accepts every connection as a single `anonymous` identity. SQE logs a security warning on startup because this disables authentication entirely — use it only to connect to auth-less catalogs without standing up an identity provider; do not use it in production. For `polaris_url`, the key difference from a Polaris setup is the path: Nessie's Iceberg REST surface mounts at `/iceberg`, not `/api/catalog`. SQE issues the same `GET /v1/config?warehouse=...` handshake either way and reads the catalog prefix Nessie returns (`main|warehouse`, the branch + warehouse name).

### SQL (queries.sql)

```sql
CREATE SCHEMA IF NOT EXISTS nessie.demo;

DROP TABLE IF EXISTS nessie.demo.events;
CREATE TABLE nessie.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO nessie.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SHOW SCHEMAS;

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM nessie.demo.events
GROUP BY kind
ORDER BY total DESC;
```

Note that `CREATE SCHEMA` is included in the script: unlike the Polaris quickstarts, Nessie starts empty with no pre-bootstrapped namespace. SQE maps `CREATE SCHEMA` to an Iceberg `create_namespace` call.

## The test

`run.sh` brings the stack up with `docker compose up --wait` and runs `queries.sql` as the anonymous user over Flight SQL. The script asserts the complete create/write/read flow: namespace creation, table creation, insert, and aggregation all succeed against Nessie. Output is captured to `OUTPUT.md`. The same `rest` catalog client is also exercised by the `nessie_namespace_round_trip` live test in the `sqe-catalog` integration suite (last validated 2026-06-06). Tear down with `./run.sh --down`.

## Output

```
sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
+-------------+
| schema_name |
+-------------+
| demo        |
+-------------+
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
```
