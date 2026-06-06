---
slug: nessie
title: "Project Nessie (Iceberg REST catalog)"
description: "Run SQE against Project Nessie. Nessie speaks the Iceberg REST protocol, so SQE uses the same rest catalog code path it uses for Polaris. Auth-less stack: SQE runs in anonymous dev mode."
---

# Project Nessie (Iceberg REST catalog)

[Project Nessie](https://projectnessie.org/) is a transactional, git-like
catalog for Iceberg tables. It exposes the **Iceberg REST protocol**, the same
surface Polaris exposes, so SQE talks to it through the identical `rest` catalog
code path. Swapping Polaris for Nessie is a one-line config change: point
`polaris_url` at Nessie's `/iceberg` endpoint.

This quickstart is about the **catalog**, not auth. Nessie runs auth-less and
SQE uses its `anonymous` provider, so there is no Keycloak and no token to
manage. For the auth story (real identities, RBAC, token passthrough) see the
[polaris-keycloak-client-id](../polaris-keycloak-client-id/) and
[polaris-keycloak-user-token](../polaris-keycloak-user-token/) quickstarts.

## What you get

`docker compose up` starts:

| Service | Image | Role |
|---|---|---|
| `rustfs` | `rustfs/rustfs` | S3-compatible object store. The warehouse lives here. No MinIO. |
| `bucket-init` | `amazon/aws-cli` | One-shot: creates the `warehouse` bucket, then exits. |
| `nessie` | `ghcr.io/projectnessie/nessie:0.107.5` | The Iceberg REST catalog (in-memory version store, S3 storage on RustFS). |
| `sqe` | built from this repo | The query engine, in anonymous auth mode. |

## Prerequisites

- Docker (with Compose v2).

## Run it

```bash
cd quickstart/nessie
cp .env.example .env
./run.sh
```

`run.sh` brings the stack up and runs [`queries.sql`](./queries.sql) as the
anonymous user, capturing the result to [`OUTPUT.md`](./OUTPUT.md). Tear down
with `./run.sh --down`.

By hand once it is up:

```bash
docker compose exec -e SQE_PASSWORD=anonymous sqe \
  sqe-cli --port 50051 --user anonymous -e "SHOW SCHEMAS"
```

Endpoints: Flight SQL `grpc://localhost:60051`, Nessie API
`http://localhost:19120/api/v2/config`.

## Configuration explained

### The catalog (the whole point)

```toml
[catalogs.nessie]
polaris_url = "http://nessie:19120/iceberg"   # Nessie's Iceberg REST mount
warehouse = "warehouse"
```

`polaris_url` is just the Iceberg REST base URL. Against Polaris it is
`.../api/catalog`; against Nessie it is `.../iceberg`. SQE issues the same
`GET /v1/config?warehouse=...` handshake either way and reads the catalog
`prefix` Nessie returns (`main|warehouse`, the branch + warehouse) from the
response. Nothing else in SQE changes.

### Auth (anonymous, dev only)

```toml
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]
```

Every connection is accepted as a single `anonymous` identity. SQE logs a
security warning on startup because this disables authentication entirely. It
is here so you can point SQE at any auth-less Iceberg REST catalog without
standing up an IdP. Do not use it in production.

### Nessie storage

Nessie is configured (in `docker-compose.yml`) with an in-memory version store
and an S3 warehouse on RustFS (`NESSIE_CATALOG_*` env). Nessie hands table
locations back to SQE; SQE reads and writes the actual Iceberg data files with
its own `[storage]` S3 credentials.

## Output

Captured from a clean run (`./run.sh`), committed in [`OUTPUT.md`](./OUTPUT.md).
The anonymous user creates a namespace (SQE maps `CREATE SCHEMA` to an Iceberg
`create_namespace`), creates a table, inserts four rows, and aggregates:

```
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

## How it is tested

`run.sh` asserts the full create/write/read flow against Nessie succeeds and
captures the output. The same `rest` client is exercised by the live test
`nessie::nessie_namespace_round_trip` in the `sqe-catalog` integration suite
(`backends_integration.rs`). Last validated 2026-06-06.

## Gotchas

- **`polaris_url` trailing path**: use Nessie's `/iceberg` mount, not `/api`.
  The `/api/v2/*` endpoints are Nessie's own API; `/iceberg` is the Iceberg REST
  surface SQE speaks.
- **In-memory store**: nothing persists across `./run.sh --down`. Nessie
  supports RocksDB / JDBC version stores for durability.
- **Anonymous mode is dev-only.** To put Nessie behind real auth, configure
  Nessie's OIDC and switch SQE to a `bearer_token` or `oidc_password` provider
  (see the polaris-keycloak quickstarts).
- Offset host ports are in `.env`.
