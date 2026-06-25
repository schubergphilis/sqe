---
slug: polaris-ranger-service-principal
title: "Service principals: per-connection client_credentials"
description: "Connect to SQE with a service principal's own OAuth2 client_id/client_secret instead of a human username/password. SQE runs the client_credentials grant per connection and forwards the token to Polaris; Apache Ranger authorizes the principal at the Polaris boundary."
---

# Service-principal auth: per-connection client_credentials passthrough

This quickstart shows how a client connects to SQE with its **own** OAuth2
`client_id` and `client_secret` (instead of a human username/password), and how
that service principal is authorized by Apache Ranger at the Polaris boundary.

It is the answer to: "can I create a service principal in Keycloak and use its
client_id/client_secret with SQE to connect and forward to Polaris, instead of
the ROPC user/password flow?" Yes. The provider is `client_credentials_passthrough`.

## What happens

```
client (client_id + client_secret as Flight Basic auth)
   -> SQE  runs the OAuth2 client_credentials grant with THOSE credentials
   -> Keycloak mints a token: preferred_username = the SP name, aud = account
   -> SQE forwards the token to Polaris
   -> Polaris maps preferred_username -> principal
   -> Apache Ranger authorizes (USER grants keyed on the SP name)
```

Each distinct client is a distinct service principal. Authorization is
per-connection, not a single server-baked identity. SQE itself holds no
service-principal secret: the connecting client supplies it every time.

## The three service principals

The realm + bootstrap provision three SPs (Keycloak confidential client with
`serviceAccountsEnabled`, a hardcoded `preferred_username` mapper, and an
`aud=account` mapper; a matching Polaris `USER` principal; Ranger user + grant):

| Service principal | Ranger grant | Result |
|---|---|---|
| `sp-admin` | full access (ADMIN) | creates + seeds `sales_wh.sales.orders` |
| `sp-reader` | read on `sales_wh.sales.orders` | `SELECT` allowed |
| `sp-denied` | none | `SELECT` denied |

The proof of per-connection identity: the **same** `SELECT` succeeds for
`sp-reader` and is denied for `sp-denied`. Only the connection credentials differ.

## Run it

```bash
cp .env.example .env
./run.sh            # up -> wait -> test.sh  (Ranger first boot takes 2-4 min)
./run.sh --down     # tear everything down
```

`test.sh` first mints each SP token straight from Keycloak and asserts the token
shape (`preferred_username` + `aud`), then drives SQE: seed as `sp-admin`,
`SELECT` allowed as `sp-reader`, denied as `sp-denied`, write denied for the
read-only SP, and a wrong secret rejected at auth.

### Connecting yourself

Any Flight SQL client uses the `client_id` as the username and the
`client_secret` as the password:

```bash
docker compose exec -e SQE_PASSWORD=sp-reader-secret sqe \
  sqe-cli --port 50051 --user sp-reader -e "SELECT * FROM sales_wh.sales.orders"
```

## Config (`sqe.toml`)

```toml
[[auth.providers]]
type = "client_credentials_passthrough"
token_url = "http://keycloak:8080/realms/iceberg-ranger/protocol/openid-connect/token"
roles_claim = "realm_access.roles"

[policy]
engine = "passthrough"   # authorization is enforced at Polaris + Ranger
```

No `client_id`/`client_secret` in config: those arrive per connection.

## Constraints worth knowing

- **Flight SQL only.** The Trino-compat HTTP Basic-auth path does not route
  through the provider chain (it calls the legacy authenticator directly), so the
  passthrough provider is reachable over Flight SQL, not Trino HTTP Basic auth.
- **Service-principal-only listener.** This provider consumes username/password,
  so it cannot share a listener with `oidc_password` (a human username would be
  tried as a `client_id` and rejected).
- **No SQE-side masking here.** A `client_credentials` SP carries no per-user
  role for SQE to key column masks on, so this quickstart sets
  `policy.engine = "passthrough"` and relies on Polaris + Ranger. Flowing the
  SP's role into SQE's own policy engine is a separate enhancement.
- **Token shape is the make-or-break.** The Keycloak SP client needs the
  hardcoded `preferred_username` mapper (so Polaris maps the right principal) and
  an `aud=account` mapper (Polaris validates `quarkus.oidc.token.audience`). The
  `profile` client scope is excluded from the SP clients so its built-in username
  mapper does not collide with the hardcoded one. `test.sh` verifies all of this.

## Files

- `keycloak/realm-ranger.json` -- realm with the three SP confidential clients
- `polaris/bootstrap-data.sh` -- catalogs, namespaces, Polaris principals (incl. the SPs)
- `ranger/bootstrap-ranger.sh` -- Ranger users + USER grants for the SPs
- `sqe.toml` -- the `client_credentials_passthrough` provider
- `test.sh` -- token-shape check + the per-connection allow/deny proof
