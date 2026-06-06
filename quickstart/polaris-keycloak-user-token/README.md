---
slug: polaris-keycloak-user-token
title: "Polaris + Keycloak (user token)"
description: "Run SQE against Apache Polaris where clients bring a pre-minted Keycloak bearer token. SQE validates the token against the realm JWKS and passes it through to Polaris. No client secret, no password grant."
---

# Polaris + Keycloak (user token)

The bring-your-own-token path. An upstream application or identity provider has
already authenticated the user and holds their bearer token. The client sends
that token to SQE with `--token`; SQE validates it (signature, issuer, expiry)
against the realm's public keys and passes it through to Polaris. SQE never sees
a password and holds no client secret.

This is the path for pre-authenticated callers: a backend that already did the
OIDC dance, a CI job with a service-account token, a gateway that injects the
user's JWT. If instead you want SQE to mint tokens from a username + password,
use the [`polaris-keycloak-client-id`](../polaris-keycloak-client-id/) quickstart.

## Same stack, one config difference

This quickstart uses the **exact same** Docker stack and shared assets
(`_shared/keycloak/realm-iceberg.json`, `_shared/polaris/bootstrap.sh`) as
[`polaris-keycloak-client-id`](../polaris-keycloak-client-id/). See that README
for the service table, ports, the realm, and the Polaris federation config.

The only difference is the SQE auth provider in [`sqe.toml`](./sqe.toml):

```toml
[[auth.providers]]
type = "bearer_token"
jwks_url = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/certs"
issuer = "http://keycloak:8080/realms/iceberg"   # must equal the token's `iss`
user_claim = "preferred_username"
roles_claim = "realm_access.roles"
allow_unbounded_audience = true                   # accept any aud this realm signed
allow_insecure_jwks = true                        # JWKS over plain http in-network
```

`bearer_token` makes SQE a pure validator: it fetches the realm's signing keys
(JWKS) once, then verifies every incoming token's signature and `iss`. There is
no `client_secret` and no call to Keycloak's token endpoint. `issuer` must match
the token's `iss` claim exactly, which is why the stack fixes
`KC_HOSTNAME=http://keycloak:8080`.

## Run it

```bash
cd quickstart/polaris-keycloak-user-token
cp .env.example .env
./run.sh
```

`run.sh` brings the stack up, then mints a token from Keycloak's **public**
client (`polaris-frontend-client`, no secret) to stand in for an upstream app,
and queries SQE with `--token`. Tear down with `./run.sh --down`.

Mint a token and query by hand:

```bash
TOKEN=$(curl -s -X POST \
  http://localhost:28080/realms/iceberg/protocol/openid-connect/token \
  -d 'grant_type=password&client_id=polaris-frontend-client&username=adminuser&password=adminuser123&scope=openid' \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

docker compose exec sqe sqe-cli --port 50051 --token "$TOKEN" -e "SHOW SCHEMAS"
```

## Output

Captured from a clean run (`./run.sh`), committed in [`OUTPUT.md`](./OUTPUT.md).

**adminuser** (pre-minted token) runs the full `queries.sql`:

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

**testuser** token reads fine but is denied a write by Polaris RBAC:

```
$ sqe-cli --token <testuser-jwt> -e "INSERT ..."
... 403 Forbidden: Principal 'testuser' ... is not authorized for op ADD_TABLE_SNAPSHOT
```

An **invalid token** is rejected by SQE before any query runs:

```
$ sqe-cli --token not.a.real.jwt -e "SELECT 1"
... Invalid or expired bearer token
```

## How it is tested

`run.sh` asserts three behaviors and captures them: a valid token authorizes
the full read/write flow, a reader token is allowed to read but denied a write
(Polaris RBAC), and a malformed token is rejected by SQE's JWKS validation
before reaching the catalog. Last validated 2026-06-06.

## Gotchas

- **`issuer` mismatch** is the usual failure: the token's `iss` claim must equal
  `issuer` in `sqe.toml`. Both are `http://keycloak:8080/realms/iceberg` here
  because `KC_HOSTNAME` is pinned to that name.
- **Token expiry**: Keycloak access tokens are short-lived (minutes). Mint a
  fresh one if you get `Invalid or expired bearer token`.
- **`allow_unbounded_audience = true`** accepts any token this realm signed. For
  production, add an audience mapper to the realm and set `audience = "sqe"`.
- Same offset host ports and in-memory persistence notes as the
  [client-id quickstart](../polaris-keycloak-client-id/#gotchas).
