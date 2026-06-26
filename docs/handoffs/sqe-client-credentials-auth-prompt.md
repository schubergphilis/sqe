# Handoff: per-connection client_credentials passthrough auth + service-principal quickstart

You are picking up a designed-but-unimplemented feature. Read the design first,
then implement Part 1, validate, then implement Part 2.

## Read first

- Design doc (authoritative): `docs/superpowers/specs/2026-06-25-client-credentials-passthrough-design.md`
- Existing auth crate: `crates/sqe-auth/src/` (especially `provider.rs`,
  `oidc_password.rs`, `oidc_m2m.rs`, `oauth.rs`, `factory.rs`, `authenticator.rs`)
- Config enum: `crates/sqe-core/src/config.rs` (`AuthProviderConfig`, ~line 794;
  hand-written `Debug` ~line 947)
- Reference quickstart to copy: `quickstart/polaris-ranger-keycloak/`
- Shared helpers: `quickstart/_shared/lib.sh`, `quickstart/_shared/polaris/bootstrap.sh`

## The one-line goal

An end-user client presents its own OAuth2 `client_id` and `client_secret` on
the SQE connection (username = client_id, password = client_secret). SQE runs
the `client_credentials` grant per connection with those credentials and
forwards the resulting bearer token to Polaris. Each client is a distinct
service principal; authorization is per-connection via Ranger at the Polaris
boundary. This is NOT the existing server-baked `client_credentials` provider.

## Why existing providers do not fit (do not reuse them)

- `client_credentials` (config `ClientCredentials`) uses server-config creds,
  ignores the handshake, and hardcodes `scope=PRINCIPAL_ROLE:ALL`
  (`oauth.rs:77`). Wrong.
- `OidcM2mProvider` also uses config creds, ignores the handshake
  (`oidc_m2m.rs:215`). Wrong, but its caching/refresh structure is a good model.
- `oidc_password` consumes the handshake but runs `grant_type=password`. Wrong.

The handshake carries per-connection creds in
`FlightCredentials { username, password, bearer_token, client_cert_cn }`
(`provider.rs:20-26`). No provider treats username/password as client_id/secret.

## Part 1 - branch `feat/auth-client-credentials-passthrough`

New provider `crates/sqe-auth/src/oidc_client_credentials.rs` implementing
`AuthProvider`:

1. Extract `username` -> client_id, `password` -> client_secret. If either is
   missing, return `AuthError::NotMyCredentials`.
2. `POST {token_url}` form body `grant_type=client_credentials`,
   `client_id`, `client_secret`, and `scope` only if configured. No default
   scope. 200 -> parse token; 400/401 -> `AuthFailed`; network/parse ->
   `Internal`.
3. Build `Identity`: `user_id` = `preferred_username` claim or client_id;
   `roles` from `realm_access.roles` (configurable `roles_claim`); `subject` =
   `sub`; `catalog_token` = access token; `expires_at` from `expires_in`.
   Reuse the claim-walk approach from `oidc_password.rs` (no signature
   verification; Polaris validates the forwarded token).
4. In-memory cache keyed by client_id holding `{ client_secret, access_token,
   expires_at }`, single-flight refresh (model on `OidcM2mProvider`).
   `refresh_catalog_token` looks the entry up by client_id and re-runs the grant
   within the refresh skew. Secrets wrapped in `SecretString`, never logged or
   persisted, evicted on expiry.

Config + wiring:

- Add `AuthProviderConfig::ClientCredentialsPassthrough { token_url,
  roles_claim (default realm_access.roles), subject_claim (default sub),
  scope: Option<String>, accept_invalid_certs }` in `config.rs`. No client_id /
  client_secret fields. TOML tag: `client_credentials_passthrough`.
- Wire it in `factory.rs` to construct the new provider.
- Confirm the hand-written config `Debug` handles the new (secret-free) variant.

Constraint to document in the module and config docs: this provider and
`oidc_password` cannot share a listener (both consume username/password). This
provider is for service-principal-only deployments.

Tests (`crates/sqe-auth`): missing username/password -> NotMyCredentials; 401 ->
AuthFailed; 200 with crafted JWT -> expected Identity fields; scope omitted when
unset / sent when set; cache hit avoids second HTTP call; refresh after skew
re-runs grant. Config round-trip + Debug-no-secret test in `config.rs`.

Verify: `cargo build --all`, `cargo test --all`,
`cargo clippy --all-targets --all-features -- -D warnings`.

## Part 2 - branch `feat/quickstart-service-principal` (depends on Part 1)

New quickstart `quickstart/polaris-ranger-service-principal/`. Copy
`polaris-ranger-keycloak/`, DROP `spark/`, the `data-seed` service, and
`parity-test.sh`. Then:

- Realm: new isolated realm `iceberg-sp`. `keycloak/realm-sp.json` imports the
  base realm + `polaris-frontend-client` + realm role `sqe_reader`. The two SP
  clients are created by the bootstrap script, NOT baked in the realm JSON.
- New `bootstrap-service-principal.sh` (standalone, idempotent, uses
  `_shared/lib.sh`), run as a `sp-setup` one-shot container that depends on
  polaris-setup + ranger-setup completed and blocks `sqe`. For each SP it does
  three planes:
  - Keycloak admin REST: confidential client, `serviceAccountsEnabled=true`, all
    interactive flows off, secret = fixed dev value from `.env` (only so the
    test harness can present a known secret; SQE does not hold it). Mappers:
    hardcoded `preferred_username = <sp-name>`, `aud` mapper for `account`,
    realm-role mapper. Assign service-account user the realm role.
  - Polaris management REST: `POST /principals {name, type: "SERVICE"}`.
  - Ranger REST: create user, role, policy.
  - SPs: `sp-reader` (role `sqe_reader`, Ranger grant `table-data-read` +
    baseline list/traverse on `sales_wh -> sales -> orders`); `sp-denied` (no
    data-read grant).
- `sqe.toml`: `[[auth.providers]] type = "client_credentials_passthrough"`,
  `token_url` pointing at the `iceberg-sp` realm, `roles_claim`,
  `accept_invalid_certs = true`. `[policy] engine = "passthrough"`. No
  `[access_control]` block. Copy catalog/storage/Polaris-OIDC+Ranger env from
  the source quickstart, swapping the realm to `iceberg-sp`.
- `polaris/bootstrap-data.sh`: catalog `sales_wh`, namespace `sales`, tables
  `orders` and `restricted` (seed a couple of rows).
- `test.sh`: mint a token per SP directly from Keycloak and assert it issues;
  connect to SQE Flight SQL as `sp-reader` -> `SELECT FROM sales_wh.sales.orders`
  allowed; connect as `sp-denied` -> same query denied. Use `lib.sh` asserts +
  `check_summary`. `run.sh` = `compose up --wait` then `test.sh`.

Verify: `cd quickstart/polaris-ranger-service-principal && ./run.sh` passes
(Ranger first-boot takes 2-4 min). Confirm the SP token carries `account` in
`aud`.

## After-work (both parts)

Update `README.md` roadmap, `nextsteps.md`, and add a quickstart README/OVERVIEW
explaining the SP flow plus the SP-only-listener and Flight-SQL-only caveats.

## Open items to resolve during implementation

- Trino-compat: check whether the Trino HTTP Basic-auth path produces the same
  `FlightCredentials`. If yes, the provider works there too; if not, note as a
  follow-up. v1 targets Flight SQL.
- Confirm Keycloak issues the client_credentials token cleanly (the new provider
  sends no scope, so the `PRINCIPAL_ROLE:ALL` problem does not apply here).

## Workflow reminders

Never push to main. One logical change per PR (Part 1 and Part 2 are separate
PRs). Branch -> commit -> push -> open MR.
