# Quack: the DuckDB wire protocol

## Goal

Quack is DuckDB's RPC protocol. A DuckDB client can `ATTACH 'quack:host:port'`
and query a remote engine as though it were a local database.

SQE speaks Quack in both directions. As a **server**, a DuckDB client queries
SQE's Iceberg catalogs over the Quack endpoint (`coordinator.quack_port`). As a
**client**, SQE's `quack_query()` table function pulls rows from a remote Quack
endpoint — another SQE instance, or a DuckDB running `quack_serve`. This
quickstart is experimental: Quack is pre-release upstream (targeting DuckDB v2.0)
and the client extension ships from `core_nightly`. The round-trip works today
with duckdb 1.5.3 but the protocol surface is not yet stable.

## Components

| Service | Role |
|---|---|
| `rustfs` | S3-compatible object store (the Iceberg data warehouse) |
| `bucket-init` | One-shot container that creates the `warehouse` bucket in RustFS |
| `nessie` | Iceberg REST catalog (auth-less, in-memory version store) |
| `sqe` | Coordinator with `quack_port = 9494` — the Quack RPC endpoint |
| DuckDB CLI (optional) | Local client for the forward round-trip; not part of the stack |

## Configuration

### Backend (sqe.toml)

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
quack_port = 9494        # enables the DuckDB Quack RPC endpoint
mode = "hybrid"

[auth]
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]

[catalogs.nessie]
polaris_url = "http://nessie:19120/iceberg"
warehouse = "warehouse"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_region = "us-east-1"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true
```

Setting `quack_port` enables the endpoint. The `anonymous` auth provider accepts
any non-empty token — dev mode only. For real auth, swap in the
`polaris-keycloak-*` quickstart's auth section.

## The test

`run.sh` brings the full stack up via `docker compose up -d --wait`, then probes
the Quack endpoint with `GET /` to confirm SQE identifies as a DuckDB Quack
server. If a local `duckdb` 1.5.3+ is on PATH, it goes further: seeds an Iceberg
table in SQE (`nessie.demo.events`), installs the pre-release quack extension
(`INSTALL quack FROM core_nightly`), and has DuckDB run `quack_query()` against
SQE — aggregating the result locally. The server probe always runs; the
round-trip is skipped (with instructions) when DuckDB is not found. Output is
captured to `OUTPUT.md`. Last validated 2026-06-07 with duckdb 1.5.3.

Tear down with `./run.sh --down`.

## Output

```
## The Quack endpoint identifies itself (`GET /`)
$ curl http://localhost:19494/
This is a DuckDB Quack RPC endpoint, served by SQE.

## A DuckDB CLI queries an SQE Iceberg table over Quack
┌──────────┬───────┬────────┐
│   kind   │   n   │ total  │
│ varchar  │ int64 │ double │
├──────────┼───────┼────────┤
│ purchase │     2 │  55.25 │
│ click    │     2 │   2.25 │
└──────────┴───────┴────────┘

## SQE logs on startup (Quack enabled, with its security warnings)
WARNING: the Quack endpoint has NO rate limiting on its auth path -- it is an
un-throttled brute-force / IdP-amplification oracle. Restrict network access to
the Quack port until QUACK-08 lands.
WARNING: the Quack endpoint is PLAINTEXT (no TLS) and binds 0.0.0.0 -- user OIDC
bearer tokens travel in cleartext and can be captured and replayed. Set
[coordinator.tls] cert_file/key_file to enable TLS, or do not expose the Quack
port on untrusted networks.
DuckDB Quack RPC on port 9494 (plaintext)
```
