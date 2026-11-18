# Signed catalog scope for one client's sessions (#908)

## Why

An orchestrating service can bind a workspace to some catalogs, each read-write or read-only, much like Unity Catalog workspace bindings. It enforces those bindings for the statements it runs itself.

Jobs it launches (dbt-sqe, Python clients) connect to SQE directly with the person's token, and the job's own code controls that connection. Today such a session sees every catalog the token can see in Polaris, and it can switch to any of them with `USE` or `X-Trino-Catalog`. A plain header cannot narrow it, because the job's code could drop it.

The person stays the identity end to end. The job's token is the user's token, issued to the orchestrator's runs client (`sub` = the person, `azp` = the runs client), and SQE passes that same bearer to Polaris and S3. There is no service account. The scope narrows what the user can reach in this session; it never widens it.

## What Changes

- For tokens of one configured OAuth client (`azp`), SQE requires a **catalog scope**: a compact JWS (ES256) signed by a configured issuer, with its keys at an https JWKS URL. Its claims are:
  - `sub`
  - `jti`
  - `catalogs` (name -> `read_write` | `read_only`)
  - `exp`

  It is sent in an HTTP header or as Flight SQL call metadata. The name is configurable and defaults to `x-sqe-catalog-scope`.
- SQE checks the signature, that `sub` and `jti` equal the bearer's, and the expiry. A missing or invalid scope refuses the session, with a reason and an audit record.
- A scoped session sees only the listed catalogs:
  - Any other catalog is "catalog not found".
  - `SHOW CATALOGS` lists only them.
  - `USE` and `X-Trino-Catalog` can pick only them.
  - `ATTACH` / `DETACH`, `CREATE` / `DROP SECRET` and `GRANT` / `REVOKE` / policy DDL are refused.
- Every write into a `read_only` catalog is refused before it commits: DML, DDL, `CALL`, `COPY`, CTAS, write branches and transactions.
- Grants still decide access, and the scope only narrows it. Polaris sees the same user, through the same bearer, as without a scope. Tokens of other clients are unchanged, and the feature is off when it is not configured.

## Capabilities

### New Capabilities

- `catalog-scope`: verifying a signed catalog scope and narrowing a session to it.

### Modified Capabilities

None.

## Impact

- **`sqe-auth`**
  - `Identity` gains `token_binding` (`azp`, `jti`, `sub`), read from the token the caller presented. Classifying a token needs no new verifier.
  - The scope is verified by a second `BearerTokenProvider` instance configured for the scope issuer (D2).
- **`sqe-core`**
  - `Session` gains `token_binding` and `catalog_scope`, with `check_catalog_read` / `check_catalog_write`.
  - New config section `[session.catalog_scope]`: `client_id`, `issuer`, `jwks_url`, `header_name`.
- **`sqe-coordinator`**
  - The front doors (Trino HTTP, Flight SQL).
  - Session context build and both cache keys.
  - Catalog registration (static, discovered, Hive, attached) and lazy discovery.
  - `SHOW CATALOGS`, `USE`, and the statement gate.
- **`sqe-catalog`**: a read-only and in-scope backstop on `SessionCatalog`, plus sibling-warehouse resolution for views.
- **`sqe-write`**: a write check where INSERT/CTAS/UPDATE/DELETE/MERGE and DDL resolve their target catalog.
- **Audit**: refusal records carry the catalog names, never the token or the scope.
