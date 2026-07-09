---
slug: polaris-keycloak-client-id
title: "Polaris + Keycloak (client credentials)"
description: "Run SQE against Apache Polaris with Keycloak as the identity provider. SQE holds a confidential client and exchanges each user's username + password for a bearer token via the OIDC password grant, then passes that token through to Polaris."
---

# Polaris + Keycloak (client credentials)

Run SQE against an Apache Polaris catalog where **Keycloak** issues the
identities. A user connects to SQE with a username and password; SQE exchanges
those for that user's bearer token using its own confidential client
(`sqe-client` + secret), then passes the token straight through to Polaris.
Polaris decides what the user can see. No service account, no shared
credential: every query runs as the authenticated user.

This is the path you want when an OIDC provider already owns your users and you
want SQE to mint their tokens for them (JDBC tools, the CLI, dbt). If instead
your clients already hold a token and just want SQE to accept it, see the
`polaris-keycloak-user-token` quickstart.

## What you get

`docker compose up` starts these on one network (three long-running services
plus three one-shot setup jobs and the engine):

| Service | Image | Role |
|---|---|---|
| `keycloak` | `quay.io/keycloak/keycloak:26.5.4` | Identity provider. |
| `keycloak-config` | `adorsys/keycloak-config-cli` | One-shot: imports the `iceberg` realm (one confidential client, three users), then exits. |
| `rustfs` | `rustfs/rustfs` | S3-compatible object store. The Iceberg warehouse lives here. No MinIO. |
| `bucket-init` | `amazon/aws-cli` | One-shot: creates the `warehouse` bucket (RustFS does not auto-create), then exits. |
| `polaris` | `apache/polaris:1.6.0` | Iceberg REST catalog, federated to Keycloak (validates the tokens SQE forwards). |
| `polaris-setup` | `curlimages/curl` | One-shot: creates the catalog, the RBAC chain, the OIDC principals, and the `demo` namespace, then exits. |
| `sqe` | built from this repo | The query engine. Flight SQL on 50051, Trino-compat HTTP on 8080. |

Everything addresses everything else by its in-network name (`keycloak:8080`,
`polaris:8181`, `rustfs:9000`), so the token issuer is one consistent hostname
and there is nothing to add to `/etc/hosts`.

## Prerequisites

- Docker (with Compose v2).
- That is all for the demo. The optional integration-test step additionally
  needs a Rust toolchain.

## Run it

```bash
cd quickstart/polaris-keycloak-client-id
cp .env.example .env          # defaults work as-is; edit to change secrets/ports
./run.sh                      # up -> bootstrap -> queries -> capture output
./run.sh --down               # tear everything down
./run.sh --check              # up -> bootstrap -> queries -> assert key invariants
```

`run.sh` brings the stack up, waits for health, runs the bootstrap, executes
`queries.sql` as two different users, and writes the result to
[`OUTPUT.md`](./OUTPUT.md). The first run builds the SQE image, which takes a
few minutes; later runs are fast. Tear everything down with `./run.sh --down`.

To poke at it yourself once it is up:

```bash
# from the host, using the SQE CLI inside the container
docker compose exec -e SQE_PASSWORD=adminuser123 sqe \
  sqe-cli --port 50051 --user adminuser -e "SHOW SCHEMAS"
```

Endpoints (host ports are offset so this stack will not clash with others):

- Flight SQL: `grpc://localhost:60051`
- Trino-compat HTTP: `http://localhost:18080`
- Keycloak: `http://localhost:28080` (admin / admin)
- Polaris: `http://localhost:18181`

## How the auth flows

```
sqe-cli --user adminuser           (username + password)
        |
        v
   SQE coordinator  --- OIDC password grant (sqe-client + secret) --->  Keycloak
        |                                                                  |
        |  <----------------------- adminuser's bearer token --------------+
        v
   Polaris  (validates the token: issuer + signature + audience,
             maps preferred_username -> a principal, applies that
             principal's roles)
        |
        v
   RustFS   (reads/writes the Iceberg data + metadata files)
```

The user never talks to Keycloak directly. SQE does, on their behalf, using the
confidential `sqe-client`. That is the "client credentials" in the name: SQE's
own client identity is what unlocks the password grant.

## Configuration explained

### `sqe.toml` (the engine)

The auth block is the heart of this quickstart:

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"   # must match the realm's sqe-client secret
roles_claim = "realm_access.roles"        # where SQE reads the user's roles
```

- `type = "oidc_password"` selects the Resource Owner Password Credentials
  (ROPC) grant. SQE posts the user's `username` + `password` plus its own
  `client_id` + `client_secret` to `token_url` and gets the user's token back.
- `client_secret` must equal the secret Keycloak created for `sqe-client`
  (set by `SQE_CLIENT_SECRET` in `.env`). A mismatch returns
  `unauthorized_client` on every login.
- `roles_claim` is the JSON path SQE reads roles from. Keycloak puts realm
  roles at `realm_access.roles`. Auth0 / Okta / Entra usually use `groups`;
  point this at whatever your provider emits.

The catalog block names the Polaris warehouse and gives it a SQL name:

```toml
[catalogs.quickstart]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "quickstart"
```

The TOML key (`quickstart`) becomes the SQL catalog, so tables are
`quickstart.<namespace>.<table>`. The `[storage]` block points SQE's own S3
client at RustFS with `s3_path_style = true` and `s3_allow_http = true` (RustFS
speaks plain HTTP path-style). See [`sqe.toml`](./sqe.toml) for every line.

### The Keycloak realm (`_shared/keycloak/realm-iceberg.json`)

One confidential client and three users with three role levels:

| User | Password | Realm roles |
|---|---|---|
| `root` | `root123` (`ROOT_PASSWORD`) | service_admin, catalog_admin, data_writer, table_reader |
| `adminuser` | `adminuser123` | catalog_admin, data_writer, table_reader |
| `testuser` | `testuser123` | table_reader |

`sqe-client` is confidential (`publicClient: false`) with
`directAccessGrantsEnabled: true` so the password grant works. The passwords for
`adminuser` / `testuser` are fixed because the repo's gated integration tests
log in with them.

### Polaris federation (`docker-compose.yml`)

The lines that make Polaris trust Keycloak:

```yaml
polaris.authentication.type: mixed            # accept BOTH internal + Keycloak tokens
quarkus.oidc.auth-server-url: http://keycloak:8080/realms/iceberg
quarkus.oidc.token.issuer:    http://keycloak:8080/realms/iceberg
quarkus.oidc.token.audience:  account
polaris.oidc.principal-mapper.name-claim-path: preferred_username
```

`authentication.type: mixed` lets the bootstrap script use Polaris's own
internal root credential while end users come in via Keycloak. The
`principal-mapper` maps the token's `preferred_username` to a Polaris principal
of the same name. Those principals (and their roles) are created by
`polaris-setup`, which is why a user's catalog access comes from Polaris's own
store, not from the token's role claims.

## Output

Captured from a clean run (`./run.sh`), committed in
[`OUTPUT.md`](./OUTPUT.md):

**adminuser** (catalog_admin + data_writer + table_reader) runs the full
`queries.sql`: list schemas, create a table, insert four rows, aggregate.

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

**testuser** (table_reader only) reads the same table successfully:

```
+----------+---+
| kind     | n |
+----------+---+
| click    | 2 |
| purchase | 2 |
+----------+---+
```

## How it is tested

Two layers, both run from a clean state:

1. **The demo path** (`./run.sh --check`): re-runs the two `sqe-cli` Flight SQL
   invocations and asserts that adminuser sees the `demo` schema and reads the
   purchase total `55.25` with no `error`, and that testuser reads the events
   table (the `purchase`/`click` rows) with no `error`. That confirms both
   users authenticate via their own minted tokens and Polaris applies their
   roles.
2. **The repo's gated integration tests**: the `test_keycloak_*` tests in the
   `sqe-coordinator` integration suite (`integration_test.rs`) (specifically
   `test_keycloak_auth_with_test_users` and `test_keycloak_token_refresh`).
   They authenticate `adminuser` / `testuser` against this exact realm, check
   the roles, and confirm a wrong password is rejected. Run them with:

   ```bash
   ./run.sh --with-tests        # needs a Rust toolchain
   ```

   Last validated 2026-06-06: both tests pass against the stack
   (`2 passed; 0 failed`).

## Gotchas

- **Host ports are offset** (`28080`, `18181`, `19000`, `60051`, `18080`) to
  avoid clashes with other local stacks. Change them in `.env`.
- **`client_secret` mismatch** is the most common failure: the value in
  `sqe.toml` must equal `SQE_CLIENT_SECRET` in `.env`. Symptom:
  `unauthorized_client` or `Authentication failed` on login.
- **RustFS does not auto-create buckets**, so a one-shot `bucket-init` service
  creates the `warehouse` bucket with the AWS CLI before the catalog is used.
- **`KC_HOSTNAME` is fixed** to `http://keycloak:8080` so every token's issuer
  matches what Polaris validates. If you expose Keycloak under a different
  hostname, update the issuer in both Keycloak and the Polaris config together.
- **In-memory persistence**: Keycloak (`start-dev`) and Polaris run in-memory,
  so the realm and catalog are rebuilt on every fresh `up`. That is deliberate
  for a quickstart. For anything durable, back both with Postgres.
- **Iterating on the engine?** `.env` pins `SQE_IMAGE`, so a plain
  `docker compose up` reuses the already-built image. To pick up SQE source
  changes, rebuild explicitly: `docker compose up -d --build sqe`.
