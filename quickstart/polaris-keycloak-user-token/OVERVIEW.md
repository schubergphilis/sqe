# Polaris + Keycloak (user token)

## Goal

The bring-your-own-token path. An upstream application or identity provider has already authenticated the user and holds their bearer token. The client sends that token to SQE with `--token`; SQE validates it (signature, issuer, expiry) against the realm's public JWKS endpoint and passes it through to Polaris. SQE never sees a password and holds no client secret.

Use this quickstart when callers are pre-authenticated: a backend service that already completed the OIDC dance, a CI job with a service-account token, or a gateway that injects the user's JWT. If you want SQE to mint tokens from a username and password instead, use the `polaris-keycloak-client-id` quickstart.

## Components

This quickstart runs the **same Docker stack** as `polaris-keycloak-client-id`. The only difference is the SQE auth provider in `sqe.toml`.

| Service | Image | Role |
|---|---|---|
| `keycloak` | `quay.io/keycloak/keycloak:26.5.4` | Identity provider. Issues tokens; SQE fetches its public JWKS to validate them. |
| `keycloak-config` | `adorsys/keycloak-config-cli` | One-shot: imports the `iceberg` realm (one confidential client + one public client, three users), then exits. |
| `rustfs` | `rustfs/rustfs` | S3-compatible object store. The Iceberg warehouse lives here. |
| `bucket-init` | `amazon/aws-cli` | One-shot: creates the `warehouse` bucket, then exits. |
| `polaris` | `apache/polaris:1.5.0` | Iceberg REST catalog, federated to Keycloak. Validates the tokens SQE forwards. |
| `polaris-setup` | `curlimages/curl` | One-shot: creates the catalog, RBAC roles, OIDC principals, and the `demo` namespace, then exits. |
| `sqe` | built from this repo | The query engine. Flight SQL on 50051, Trino-compat HTTP on 8080. |

## Configuration

### Backend (sqe.toml)

```toml
[[auth.providers]]
type = "bearer_token"
jwks_url = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/certs"
issuer = "http://keycloak:8080/realms/iceberg"   # must equal the token's `iss`
user_claim = "preferred_username"
roles_claim = "realm_access.roles"
allow_unbounded_audience = true   # accept any aud this realm signed
allow_insecure_jwks = true        # JWKS over plain HTTP in-network

[catalogs.quickstart]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "quickstart"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true
```

`type = "bearer_token"` makes SQE a pure validator. It fetches the realm's signing keys (JWKS) once, then verifies every incoming token's signature and `iss` claim — no `client_secret`, no call to the token endpoint. `issuer` must match the token's `iss` exactly, which is why the stack pins `KC_HOSTNAME=http://keycloak:8080`. For production, add an audience mapper in the realm and set `audience = "sqe"` instead of `allow_unbounded_audience`.

### SQL (queries.sql)

```sql
SHOW SCHEMAS;

DROP TABLE IF EXISTS quickstart.demo.events;
CREATE TABLE quickstart.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO quickstart.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM quickstart.demo.events
GROUP BY kind
ORDER BY total DESC;
```

## The test

`run.sh` brings the stack up, then mints tokens from Keycloak's public client (`polaris-frontend-client`, no client secret) to simulate an upstream application. It queries SQE with `--token` and asserts three behaviors: a valid `adminuser` token authorizes the full read/write flow (create table, insert, aggregate); a `testuser` token (table_reader) is allowed to read but is denied a write by Polaris RBAC (403 Forbidden); and a malformed token is rejected by SQE's JWKS validation before any query reaches the catalog. All three results are captured to `OUTPUT.md`. Tear down with `./run.sh --down`.

## Output

```
## adminuser, authenticated by a pre-minted Keycloak token

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

## testuser token (table_reader): read works, write is denied by Polaris RBAC

+------+
| rows |
+------+
| 4    |
+------+

$ sqe-cli --token <testuser-jwt> -e "INSERT ..."   # expect 403
Error: "Failed to commit INSERT transaction: ... 403 Forbidden: Principal 'testuser' ... is not authorized for op ADD_TABLE_SNAPSHOT"

## an invalid token is rejected by SQE before any query runs

$ sqe-cli --token not.a.real.jwt -e "SELECT 1"
Error: "Invalid or expired bearer token"
```
