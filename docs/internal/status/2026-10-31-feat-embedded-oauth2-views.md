**STATUS 2026-10-31: embedded Iceberg views on OAuth2 attaches (`feat/embedded-oauth2-views`).**

Removes the OAuth2 refusal `feat/embedded-view-gaps` (!1595) left.

- Vendored iceberg-rust, `SQE PATCH (embedded OAuth2 views)`:
  `HttpClient::bearer_token` (authenticates a never-sent probe request and
  reads the `Bearer` header back; the test-only `token()` now wraps it) and
  `HttpClient::bearer_token_rejected` (feeds a probe request carrying the
  token to `AuthSession::on_unauthorized`). Exposed as
  `RestCatalog::bearer_token` / `bearer_token_rejected`. The OAuth2 session
  refreshes only through `on_unauthorized`, never on its own, so SQE's raw
  path needs the explicit 401 hand-back.
- `SessionCatalog` (sqe-catalog `rest_catalog.rs`): `oauth2_client()` is
  set when there is no bearer, no SigV4 and a `credential` prop.
  `raw_bearer()` returns the session bearer or the exchanged token;
  `execute_raw()` sends with it and, on a 401 in OAuth2 mode, reports the
  token, re-reads one and retries once (`RequestBuilder::try_clone`).
  `rest_request` and `authorize_table_access` use it; the ETag spawn
  resolves the token before spawning (best effort, no retry).
- Fingerprint: `oauth2_identity` (credential + `oauth2-server-uri` +
  `scope` + `audience` + `resource`, every prop the exchange sends) replaces the empty bearer in `rest_catalog_token_fingerprint`,
  so `REST_CATALOG_CACHE`, table-cache keys and the user breaker no longer
  collide between OAuth2 credentials on one URL and warehouse.
- Embedded (`sqe-cli/src/embedded.rs`): `rest_catalogs` holds
  `Arc<SessionCatalog>` (no more `Option`); the OAuth2 error is gone.

Verified: wiremock test (token exchange, 401 on the first token, refresh,
retry; asserts two exchanges) and a fingerprint unit test; sqe-catalog lib
700 pass. Live against the test-stack Polaris with
`CREATE SECRET (TYPE oauth2, CLIENT_ID 'root', ...)`: CREATE VIEW, read,
SHOW CREATE VIEW, SHOW TABLES (type VIEW), DROP VIEW.

**NEXT:** nothing queued on embedded views. Open: the coordinator never
passes a `credential`, so this path is embedded-only; if a coordinator
catalog ever uses OAuth2 client credentials, the same `execute_raw` covers
it.
