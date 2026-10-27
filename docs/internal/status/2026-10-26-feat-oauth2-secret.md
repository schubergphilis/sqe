# feat/oauth2-secret

**Date:** 2026-10-26

## What changed

Found during the Databricks Unity Catalog testing (playground3): an
`ATTACH ... (TYPE iceberg_rest, SECRET <bearer>)` mount stops working when
the Databricks token expires (one hour), and the only fix was to replace
the secret by hand.

- `sqe-core` `Secret::OAuth2 { client_id, client_secret, token_url, scope }`
  (serde tag `oauth2`). Debug redacts the client secret; Drop zeroizes it.
- `sqe-sql`: `CREATE SECRET n (TYPE oauth2, CLIENT_SECRET '...' [, CLIENT_ID,
  TOKEN_URL, SCOPE])`. `CLIENT_SECRET` is required.
- `sqe-catalog` `build_iceberg_rest`: the secret maps onto the vendored
  iceberg-rust REST props `credential` (`id:secret` or the bare secret),
  `oauth2-server-uri` and `scope`, and drops any `token`. The vendored
  client runs the client-credentials grant, caches the token and, on a
  401, drops it and retries once with a fresh exchange.
- Docs: `operations/catalogs.md` (secret kinds table, oauth2 example, the
  service-principal trust note) and `getting-started/cli.md`.

## Gate

- `cargo test -p sqe-catalog --features rest --test it mount_rest`: 9 passed,
  including `build_iceberg_rest_with_oauth2_secret` (form body carries
  `grant_type=client_credentials`, `client_id`, `client_secret`, `scope`;
  config and namespaces see the exchanged bearer) and
  `oauth2_secret_renews_an_expired_token` (401 on the first token, the
  second exchange succeeds).
- `cargo test -p sqe-core --lib secret`: 49 passed. `cargo test -p sqe-sql
  --lib attach`: 14 passed.
- `cargo check-server`, `cargo check -p sqe-cli`, clippy on the three
  crates: clean.

## Live test (playground3, embedded, 2026-10-26)

Service principal `sqe-probe-oauth2` created for the test (OAuth secret,
7-day lifetime), TOKEN_URL `https://<workspace>/oidc/v1/token`, SCOPE
`all-apis`. Deleted afterwards (workspace and account).

- `WAREHOUSE 'main'` (account users hold USE CATALOG): ATTACH works,
  `information_schema.schemata` lists `ucmain.default`.
- `WAREHOUSE 'pg3-bronze'`: USE CATALOG and EXTERNAL USE SCHEMA need MANAGE
  on the catalog, which the tester lacks. The SP was added to the
  account-level `Databricks_Admins` group (catalog-wide ALL PRIVILEGES +
  EXTERNAL USE SCHEMA) for the duration of the read only, then removed.
  - Group membership took ~1.5 min to reach UC (`/v1/config` 403 -> 200).
  - Through the oauth2 mount: 56 tables listed in `sqe_probe`,
    `count(*)` on `dbx_basic` = 4, rows read with vended S3 credentials.
  - The group is account-managed: the workspace SCIM API refuses member
    changes; the account API (`databricks account groups patch`) works.
  - Deleting a SP in the workspace leaves the account SP; delete both.

## Open

- Token renewal after an hour is covered by the wiremock test only.
- `SHOW SCHEMAS IN <catalog>` is unsupported in embedded mode (added to the
  parity issue).
- Security note: an oauth2 secret is a service credential. Every session
  reading through the mount uses the principal's grants, the same trust
  level as an admin-created bearer secret.

**NEXT:** COMMENT as a table property and the LOCATION decision on managed
CREATE TABLE.
