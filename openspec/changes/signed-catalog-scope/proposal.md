# Signed catalog scope for one client's sessions (#908)

## Why

An orchestrating service can bind a workspace to some catalogs, each read-write or read-only (like Unity Catalog workspace bindings), and it enforces those bindings for the statements it runs itself. Jobs it launches (dbt-sqe, Python clients) connect to SQE directly with the person's token, and the job's own code controls that connection. Today such a session sees every catalog the token can see in Polaris and can switch to any of them with `USE` or `X-Trino-Catalog`. A plain header cannot narrow it: the job's code could drop it.

## What Changes

- For tokens of one configured OAuth client (`azp`), SQE requires a **catalog scope**: a compact JWS (ES256) signed by a configured issuer, keys from its https JWKS URL, with claims `sub`, `jti`, `catalogs` (name -> `read_write` | `read_only`) and `exp`. It is sent as header `x-sqe-catalog-scope` (HTTP) or Flight SQL call metadata of the same name.
- SQE checks the signature, `sub` and `jti` equal to the bearer's, and expiry. A missing or invalid scope refuses the session with a reason and an audit record.
- A scoped session sees only the listed catalogs: any other is "catalog not found", `SHOW CATALOGS` lists only them, `USE` and `X-Trino-Catalog` can pick only them, `ATTACH CATALOG` is refused.
- Every write (DML, DDL, `CALL`, `COPY`, CTAS, write branches, transactions) into a `read_only` catalog is refused at plan time.
- Grants still decide access; the scope only narrows it. Tokens of other clients are unchanged; the feature is off when unconfigured.

## Capabilities

### New Capabilities

- `catalog-scope`: verifying a signed catalog scope and narrowing a session to it.

### Modified Capabilities

None.

## Impact

- `sqe-auth`: the JWKS fetch/verify code in `bearer_token.rs` is extracted into a reusable verifier (https-only, algorithm pinned per key) used for both bearer tokens and scopes; `azp` and `jti` are read from the verified bearer claims.
- `sqe-core`: `Session` gains `catalog_scope: Option<Arc<CatalogScope>>`; config `[session.catalog_scope]` (`client_id`, `issuer`, `jwks_url`).
- `sqe-coordinator`: session build and the session cache key (`session_cache_key`, `session_context.rs:1306`); catalog registration (static, discovered warehouses, Hive roots, `ATTACH`); `SHOW CATALOGS`; `USE`; a plan-time write guard; re-check per Flight SQL request and on `Session::rotate_credentials`.
- `sqe-trino-compat` and the Flight SQL service: read the header / metadata.
- Audit: refusal records carry the catalog names, never the token or scope.
