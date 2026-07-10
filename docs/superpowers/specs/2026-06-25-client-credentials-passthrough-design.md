# Design: per-connection client_credentials passthrough + service-principal quickstart

Date: 2026-06-25
Status: approved (brainstorming), pending implementation plan
Branches: `feat/auth-client-credentials-passthrough` (Part 1), `feat/quickstart-service-principal` (Part 2)

## Goal

Let an end-user client authenticate to SQE by presenting its own OAuth2
`client_id` and `client_secret` on the connection (Trino-style), instead of a
human username/password (ROPC). SQE runs the `client_credentials` grant per
connection with the client-supplied credentials, then forwards the resulting
bearer token to Polaris. Each distinct client is a distinct service principal,
so authorization is per-connection, not a single server-baked identity.

This mirrors the data-platform's service-principal model: every consumer gets
its own Keycloak confidential client, its own Polaris `SERVICE` principal, and
its own Ranger grant. The difference here is that the credential travels on the
SQE connection and SQE does the token exchange, rather than the data-platform
backend minting and handing out the credential.

## Non-goals

- SQE-side column masking / row filtering keyed on the service principal. The
  new provider does surface roles in the `Identity`, so this becomes possible,
  but the quickstart relies on Ranger at the Polaris boundary and ships with
  `policy.engine = "passthrough"`.
- Trino HTTP wire protocol support in v1. v1 targets Flight SQL (the primary
  protocol). Trino-compat is a noted follow-up pending a check of whether its
  Basic-auth path feeds the same `FlightCredentials`.
- Reusing the data-platform backend to provision the principals. Provisioning
  is a standalone script in the SQE quickstart (decided during brainstorming).

## Why the existing providers do not solve this

Reviewed in `crates/sqe-auth`:

- `client_credentials` (config variant `ClientCredentials`, factory.rs:73-101)
  wraps the legacy `Authenticator` -> `OAuthClient`. It uses one server-config
  `client_id`/`client_secret` and ignores the per-connection handshake. It also
  hardcodes `scope=PRINCIPAL_ROLE:ALL` (oauth.rs:77), a Polaris-ism that is
  wrong against a Keycloak token endpoint.
- `OidcM2mProvider` (oidc_m2m.rs) is the cleaner machine-to-machine provider,
  but it too uses config-held credentials and ignores the handshake
  (`authenticate(_credentials)` at oidc_m2m.rs:215). It is also not wired to any
  config variant.
- `oidc_password` (ROPC) consumes the handshake username/password but runs
  `grant_type=password` with SQE's own confidential client. Wrong grant.

The handshake already carries per-connection credentials in
`FlightCredentials { username, password, bearer_token, client_cert_cn }`
(provider.rs:20-26). No existing provider treats `username`/`password` as an
OAuth `client_id`/`client_secret`.

## Part 1 - new auth provider `client_credentials_passthrough`

New file `crates/sqe-auth/src/oidc_client_credentials.rs` implementing
`AuthProvider`.

### Credential extraction

- `username` becomes `client_id`, `password` becomes `client_secret`.
- If either is absent, return `AuthError::NotMyCredentials` so the chain
  continues to the next provider.

### Token exchange

- `POST {token_url}` form-encoded:
  `grant_type=client_credentials`, `client_id=<username>`,
  `client_secret=<password>`, and `scope=<scope>` only when a scope is
  configured. No default `PRINCIPAL_ROLE:ALL`.
- HTTP 200: parse `access_token`, `expires_in`.
- HTTP 400/401: `AuthError::AuthFailed` (definitive, stop the chain).
- Network / parse failure: `AuthError::Internal`.

### Identity construction

- `user_id`: `preferred_username` claim from the decoded token if present,
  otherwise the `client_id`. This keeps SQE's identity aligned with the Polaris
  principal name and the Ranger user.
- `roles`: extracted from `realm_access.roles` via a configurable
  `roles_claim` (default `realm_access.roles`), so both Polaris/Ranger and any
  future SQE-side policy see the principal's roles.
- `subject`: `sub` claim (configurable `subject_claim`).
- `catalog_token`: the access token (forwarded to Polaris and S3).
- `expires_at`: now + `expires_in`.

JWT claim extraction reuses the same approach already used by
`oidc_password.rs` (decode payload, walk dot-separated claim path). No signature
verification here: the token is forwarded to Polaris which validates it.

### Token cache and refresh

The `client_credentials` grant does not issue a refresh token, and the
credentials only exist on the handshake, so `refresh_catalog_token(&Identity)`
cannot re-run the grant from `Identity` alone (it has no secret).

Decision: keep a small in-memory cache keyed by `client_id` holding
`{ client_secret, access_token, expires_at }`, with single-flight refresh
(mirrors the structure of `OidcM2mProvider`, but multi-tenant via a map).
`authenticate` populates it; `refresh_catalog_token` looks up by
`identity.user_id`-derived `client_id` and re-runs the grant when within the
refresh skew.

Security tradeoff, documented in the module: live service-principal secrets are
held in memory for the lifetime of active connections. They are wrapped in
`SecretString`, never logged, never persisted, and evicted on expiry. This is
the same trust level as the access tokens already held in `Session`.

### Config variant

New `AuthProviderConfig::ClientCredentialsPassthrough`:

```toml
[[auth.providers]]
type           = "client_credentials_passthrough"
token_url      = "http://keycloak:8080/realms/<realm>/protocol/openid-connect/token"
roles_claim    = "realm_access.roles"   # default
subject_claim  = "sub"                    # default
# scope        = "..."                    # optional, omitted by default
accept_invalid_certs = true               # dev only
```

No `client_id` / `client_secret` in config: that is the whole point. Wired in
`factory.rs` to construct the new provider. Hand-written `Debug` in config.rs
already covers secret-bearing variants; this variant holds no secret so it
falls through to the name-only summary.

### Deployment constraint

This provider and `oidc_password` both consume `username`/`password`, so they
cannot share one listener: a human username would be tried as a `client_id` and
rejected. The provider is for service-principal-only deployments. This is
documented in the module doc and the config doc. The quickstart is SP-only.

### Tests (Part 1)

- Unit: missing username or password -> `NotMyCredentials`.
- Unit: 401 from a mock token endpoint -> `AuthFailed`.
- Unit: 200 with a crafted JWT payload -> `Identity` with expected
  `user_id` (preferred_username), `roles`, `subject`, `expires_at`.
- Unit: no `scope` param sent when scope unset; `scope` sent when set.
- Unit: cache hit within skew avoids a second HTTP call; refresh after skew
  re-runs the grant.
- Config: TOML round-trips the new variant; `Debug` does not print a secret
  (there is none, but assert the variant summary).

## Part 2 - quickstart `polaris-ranger-service-principal`

Built on `Part 1`. Copies the plumbing of `quickstart/polaris-ranger-keycloak`,
drops Spark and the parity harness, and demonstrates per-connection identity by
provisioning two service principals.

### Layout

```
quickstart/polaris-ranger-service-principal/
  docker-compose.yml          # from ranger-keycloak; DROP spark + data-seed; ADD sp-setup
  sqe.toml                    # client_credentials_passthrough provider; policy passthrough
  .env / .env.example
  run.sh / test.sh
  keycloak/realm-sp.json      # base realm + polaris-frontend-client + realm roles (sqe_reader)
  polaris/bootstrap-data.sh   # catalog sales_wh, namespace sales, tables orders + restricted
  ranger/                     # servicedef-polaris.json, install.properties, init_postgres.sh, bootstrap-ranger.sh
  bootstrap-service-principal.sh  # the standalone 3-plane provisioner
  README.md / OVERVIEW.md
```

### Realm

New isolated realm `iceberg-sp` (decided: isolated, not shared with
`iceberg-ranger`). The realm JSON imports the base realm, the
`polaris-frontend-client` Polaris needs, and the realm role `sqe_reader`. The
two service-principal clients are created by the bootstrap script, not baked in
the realm JSON, to mirror the data-platform's programmatic creation.

### The standalone provisioner

`bootstrap-service-principal.sh` runs as a `sp-setup` one-shot container (curl
image), `depends_on` polaris-setup and ranger-setup completed, and blocks the
`sqe` service. Idempotent (check-then-create), uses `_shared/lib.sh` helpers.
For each service principal it provisions all three planes:

- Plane 1 - Keycloak (admin REST): confidential client with
  `serviceAccountsEnabled=true`, all interactive flows disabled, secret set to
  a fixed dev value from `.env`. Under the passthrough provider SQE does not
  hold the secret in config; the connecting client (here, the test harness)
  supplies it. The fixed value in `.env` exists only so the test harness can
  present a known secret per SP. Mappers:
  hardcoded `preferred_username = <sp-name>`, an `aud` mapper for `account`, and
  a realm-role mapper so `realm_access.roles` carries the assigned roles.
  Assign the service-account user the realm role(s).
- Plane 2 - Polaris (management REST):
  `POST /principals { name: "<sp-name>", type: "USER" }`. Use `USER` to match
  the known-working `polaris-ranger-keycloak` bootstrap (which creates `USER`
  principals and federates them through the OIDC principal-mapper + Ranger). The
  data-platform uses `SERVICE`; do not carry that choice on faith unless `SERVICE`
  is confirmed to federate identically through Polaris + Ranger.
- Plane 3 - Ranger (REST): create user `<sp-name>`, role, and policy.

Two principals:

- `sp-reader`: realm role `sqe_reader`; Ranger policy grants `table-data-read`
  (plus list/traverse baseline) on `sales_wh -> sales -> orders`.
- `sp-denied`: provisioned identically but with no data-read grant.

### SQE config (sqe.toml)

```toml
[[auth.providers]]
type        = "client_credentials_passthrough"
token_url   = "http://keycloak:8080/realms/iceberg-sp/protocol/openid-connect/token"
roles_claim = "realm_access.roles"
accept_invalid_certs = true

[policy]
engine = "passthrough"   # authorization is enforced at Polaris + Ranger
```

Catalog and storage blocks copied verbatim from the ranger-keycloak quickstart.
Polaris OIDC + Ranger env (issuer `iceberg-sp`,
`principal-mapper.name-claim-path=preferred_username`,
`authorization.type=ranger`) copied with the realm name swapped. No
`[access_control]` GRANT/REVOKE block: SQE under a passthrough provider runs as
whatever SP connected and cannot issue per-user grants; the Ranger bootstrap
pre-grants instead.

### Validation (test.sh)

The proof of per-connection identity: same SQL, different connection
credentials, different outcome.

- Mint a token directly from Keycloak for each SP via `client_credentials` and
  assert it issues (covers the realm + client wiring).
- Connect to SQE Flight SQL with `sp-reader` client_id/secret:
  `SELECT ... FROM sales_wh.sales.orders` -> allowed.
- Connect with `sp-denied` client_id/secret: same query -> denied by Ranger.
- Uses `_shared/lib.sh` assert helpers and `check_summary`. `run.sh` is
  `compose up --wait` then `test.sh`.

### After-work updates

Per the project's after-completing-work checklist: README roadmap, nextsteps.md,
and a short quickstart README/OVERVIEW explaining the SP flow and the
single-protocol / SP-only-listener caveats.

## Follow-on (2026-06-25b): service principals over Trino + dbt

Extends the provider to the other two transports.

- Trino-compat HTTP Basic auth previously called the legacy `Authenticator`
  directly, bypassing `[[auth.providers]]`. Now both coordinator binaries route
  Basic auth through the auth chain. CRITICAL: the deployed binary is
  `sqe-server` (`src/bin/sqe_server.rs`, the Dockerfile ENTRYPOINT), not
  `sqe-coordinator` (`src/main.rs`) -- both have their own `AuthenticatorAdapter`
  and both were updated. A shared `identity_to_session` helper lives in
  `crates/sqe-coordinator/src/auth_session.rs`. Backward compatible (empty
  providers -> chain wraps the legacy Authenticator).
- The quickstart `sqe.toml` adds a `bearer_token` provider next to the
  passthrough provider (different credential fields, so they coexist on one
  listener). `test.sh` proves allow/deny over Trino HTTP Basic auth and a
  client-fetched bearer token. 10/10 on a live stack.
- dbt-sqe (`adapters/dbt-sqe`) gains `method`/`client_id`/`client_secret`/`token`
  profile fields. OAuth creds travel as Flight Basic auth (server runs the
  grant); `token` sets the ADBC bearer header. Mapping isolated in a dbt-free
  `auth.py` with 7 unit tests.
- Not done (already exists, not demoed): the interactive Trino OAuth2 external
  browser flow (`oauth2.rs`, `[auth.external]`).

## Shipped (2026-06-25) and where it deviates from the plan

Both parts are implemented and validated end to end (test.sh 7/7 against a live
Keycloak + Polaris 1.5 + Ranger 2.8 stack). Deviations from the plan above, all
chosen for reliability:

- Keycloak SP clients are defined declaratively in the realm JSON
  (`keycloak/realm-ranger.json`, imported by keycloak-config-cli), not created by
  a standalone admin-REST script. The quickstart's SPs are fixed and known, so
  the declarative path is the idiomatic, lower-risk choice and still self-contained
  (no data-platform dependency). The token shape was verified by booting Keycloak
  alone and decoding a minted token before building the rest.
- Provisioning is split across the two existing one-shot bootstraps rather than a
  single new container: Polaris SP principals go in `polaris/bootstrap-data.sh`
  (it already holds the Polaris admin token and creates principals); Ranger SP
  users + grants go in `ranger/bootstrap-ranger.sh` (it already holds Ranger
  access and runs at the right time). Same 3-plane outcome, reusing proven timing.
- `preferred_username` collision avoided by excluding the `profile` client scope
  from the SP clients (its built-in username mapper would otherwise compete with
  the hardcoded `preferred_username` mapper). Verified: token carries
  `preferred_username = <sp-name>`, `aud = account`.
- Realm reused (`iceberg-ranger`) rather than a fresh `iceberg-sp`, to keep the
  proven Polaris issuer/OIDC config unchanged (the advisor flagged issuer
  mismatch as a trap when renaming realms).
- Trino-compat confirmed OUT: the Trino HTTP Basic-auth path calls the legacy
  authenticator directly and bypasses the provider chain, so the passthrough
  provider is Flight-SQL-only. Documented as a hard boundary, not a choice.

## Sequencing

1. Part 1 on `feat/auth-client-credentials-passthrough`: provider + config +
   factory + tests. Merge and validate.
2. Part 2 on `feat/quickstart-service-principal` (depends on Part 1): quickstart
   files + provisioner + test harness.

Two PRs, matching the themed-branch habit (one logical change per PR).

## Validate the token chain BEFORE building the stack

Unit tests prove the provider's logic but nothing about whether Keycloak ->
Polaris -> Ranger accepts the forwarded token. The ranger-keycloak stack takes
2-4 min per boot, so do not build all of Part 2 and then discover the token is
wrong. First, against a minimal Keycloak + Polaris:

1. Create one service-account client, mint a `client_credentials` token by hand
   (curl), base64-decode the payload.
2. Confirm the four token-shape assumptions below.
3. Feed the token straight to a Polaris management/catalog call and confirm it
   is accepted (not 401) and maps to the expected principal.

Only then build the compose, bootstrap, and test harness outward from a token
shape that is already known to work.

## Open risks (in priority order)

1. `aud` is the most likely failure. Polaris in `polaris-ranger-keycloak`
   validates `quarkus.oidc.token.audience: account`. A Keycloak
   `client_credentials` token does NOT get `aud: account` for free (the service
   account has no client role on `account`, so the default audience-resolve
   mapper adds nothing). Add an explicit audience mapper whose value matches
   exactly what Polaris validates in THIS compose (`account`). The data-platform
   uses `aud: sqe`; do not copy that value while keeping Polaris's `account`
   check or every call 401s.
2. `preferred_username` must be the SP name, not `service-account-<clientId>`,
   or Polaris maps to the wrong principal and the Ranger policy silently never
   matches (looks like a policy bug). The hardcoded mapper handles this; confirm
   it lands in the minted token.
3. Issuer: realm `iceberg-sp` must match Polaris's `quarkus.oidc.auth-server-url`
   and `token.issuer`. Easy to miss when swapping the realm name.
4. Principal type: use `USER` (matches the working quickstart). See Plane 2.
5. Auth-chain purity: SQE must be configured with ONLY the passthrough provider
   (no `oidc_password` in the chain), and the test client must pass the SP's
   client_id/secret in the username/password fields. A stray ROPC provider or a
   human username yields a green run that never touches the new code.
6. Trino-compat: confirm whether the Trino HTTP Basic-auth path produces the
   same `FlightCredentials`. If not, note as a follow-up. v1 targets Flight SQL.
