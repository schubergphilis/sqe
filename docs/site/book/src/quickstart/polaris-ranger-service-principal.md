# Service principals: per-connection client_credentials

## Goal

Connect to SQE with a service principal's own OAuth2 `client_id` and `client_secret` instead of a human username/password. SQE runs the OAuth2 client_credentials grant per connection, with the credentials that connection supplies, and forwards the minted token to Polaris; Apache Ranger authorizes the principal at the Polaris boundary. Each distinct client is a distinct service principal: authorization is per-connection, not a single server-baked identity, and SQE itself holds no service-principal secret.

Use this quickstart when machines connect (pipelines, dashboards, services) rather than humans. For the human username/password flow see `polaris-keycloak-client-id`; for the `GRANT`/`REVOKE`-to-Ranger story on the same stack see `polaris-ranger-keycloak`.

## What happens

```
client (client_id + client_secret as Flight Basic auth)
   -> SQE  runs the OAuth2 client_credentials grant with THOSE credentials
   -> Keycloak mints a token: preferred_username = the SP name, aud = account
   -> SQE forwards the token to Polaris
   -> Polaris maps preferred_username -> principal
   -> Apache Ranger authorizes (USER grants keyed on the SP name)
```

## Components

| Service | Image | Role |
|---|---|---|
| `keycloak` | `quay.io/keycloak/keycloak` | Identity provider. Mints the service-principal tokens. |
| `keycloak-config` | `adorsys/keycloak-config-cli` | One-shot: imports the realm with the three SP confidential clients, then exits. |
| `rustfs` | `rustfs/rustfs` | S3-compatible object store. The Iceberg warehouse lives here. |
| `bucket-init` | `amazon/aws-cli` | One-shot: creates the warehouse bucket, then exits. |
| `ranger-db` | `postgres` | Ranger Admin's database. |
| `ranger-admin` | `apache/ranger` | Policy store. Holds the USER grants keyed on the SP names. |
| `ranger-setup` | `curlimages/curl` | One-shot: creates the Ranger users + grants for the SPs, then exits. |
| `polaris` | `apache/polaris` | Iceberg REST catalog. Maps `preferred_username` to a principal; enforces via its Ranger authorizer. |
| `polaris-setup` | `curlimages/curl` | One-shot: creates catalogs, namespaces, and the Polaris `USER` principals, then exits. |
| `sqe` | built from this repo | The query engine. Flight SQL on 50051. |

## Configuration

```toml
[[auth.providers]]
type = "client_credentials_passthrough"
token_url = "http://keycloak:8080/realms/iceberg-ranger/protocol/openid-connect/token"
roles_claim = "realm_access.roles"

[policy]
engine = "passthrough"   # authorization is enforced at Polaris + Ranger
```

There is no `client_id`/`client_secret` in the config: those arrive per connection. Any Flight SQL client uses the `client_id` as the username and the `client_secret` as the password.

## The three service principals

The realm and bootstrap provision three SPs, each a Keycloak confidential client with `serviceAccountsEnabled`, a hardcoded `preferred_username` mapper, and an `aud=account` mapper; a matching Polaris `USER` principal; and a Ranger user + grant:

| Service principal | Ranger grant | Result |
|---|---|---|
| `sp-admin` | full access (ADMIN) | creates + seeds `sales_wh.sales.orders` |
| `sp-reader` | read on `sales_wh.sales.orders` | `SELECT` allowed |
| `sp-denied` | none | `SELECT` denied |

The proof of per-connection identity: the **same** `SELECT` succeeds for `sp-reader` and is denied for `sp-denied`. Only the connection credentials differ.

## The test

`run.sh` brings the stack up (Ranger's first boot takes 2-4 minutes), then runs `test.sh`. The test first mints each SP token straight from Keycloak and asserts the token shape (`preferred_username` + `aud`), then drives SQE: seed as `sp-admin`, `SELECT` allowed as `sp-reader`, `SELECT` denied as `sp-denied`, a write denied for the read-only SP, and a wrong secret rejected at auth. Tear down with `./run.sh --down`.

## Constraints worth knowing

- **Flight SQL only.** The Trino-compat HTTP Basic-auth path does not route through the provider chain, so the passthrough provider is reachable over Flight SQL, not Trino HTTP Basic auth.
- **Service-principal-only listener.** This provider consumes username/password, so it cannot share a listener with `oidc_password`: a human username would be tried as a `client_id` and rejected.
- **No SQE-side masking here.** A client_credentials SP carries no per-user role for SQE to key column masks on, so this quickstart sets `policy.engine = "passthrough"` and relies on Polaris + Ranger.
- **Token shape is the make-or-break.** The SP client needs the hardcoded `preferred_username` mapper (so Polaris maps the right principal) and an `aud=account` mapper (Polaris validates the token audience). The `profile` client scope is excluded from the SP clients so its built-in username mapper does not collide with the hardcoded one. `test.sh` verifies all of this.
