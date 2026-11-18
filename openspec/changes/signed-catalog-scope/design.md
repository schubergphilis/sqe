# Design: signed catalog scope

## Context

`Session` (`crates/sqe-core/src/session.rs`) carries the user, the token and the session catalog, but no `azp`, `jti` or scope. The per-token `SessionContext` cache keys on token, roles, groups and session catalog/schema.

Catalogs are resolved in several places:

- static config;
- discovered Polaris warehouses (`session_context.rs`), plus lazy discovery in `query_handler`;
- Hive roots;
- runtime `ATTACH`;
- `SHOW CATALOGS`;
- `USE`;
- maintenance `CALL`;
- sibling warehouses that views reference (`schema_provider.rs`).

There is no single choke point for the whole statement. There is one for Polaris, though: every Polaris catalog a session uses is a `SessionCatalog` built by `build_session_catalog` (or `SessionCatalog::for_session` / `for_sibling_warehouse`).

## Decisions

### D1. Hide at registration, guard where the write target resolves, backstop in `SessionCatalog`

1. **Registration.** Every loop that registers catalogs into a session's `SessionContext` skips the out-of-scope ones before it builds them: configured, discovered, attached and Hive catalogs.
   - Lazy discovery and the session-catalog builders check scope too.
   - An unregistered catalog is "catalog not found" to DataFusion, so reads cannot reach it.
   - `datafusion` (SQE's scratch memory catalog) and SQE's virtual `system` schemas stay visible.
   - A Polaris warehouse named `system` is visible only when it is in scope.
2. **Write target.** INSERT, CTAS, UPDATE, DELETE and MERGE resolve their target catalog in `write_handler::resolve_session_catalog`, DDL in `catalog_ops`, `CALL` in `maintenance`, and the Hive paths and Flight ingest each in their own place.
   - Each of these calls `Session::check_catalog_write(catalog)`.
   - An out-of-scope catalog is "catalog not found". A `read_only` one is "catalog `<b>` is read-only in this session".
3. **Backstop.** A `SessionCatalog` built for a scoped session carries the scope's access for its warehouse.
   - Its mutating operations (create/drop/rename namespace and table, commits, views, schema updates, the write probe) refuse in a `read_only` warehouse.
   - Building one for an out-of-scope warehouse fails.
   - A write path that misses step 2 still cannot commit into a read-only catalog.
4. **Statements a scoped session may not run at all.** These are refused in the statement dispatch, in both the buffered and the streaming path:
   - `ATTACH` / `DETACH`;
   - `CREATE` / `DROP SECRET`;
   - DCL (`GRANT`, `REVOKE`, policy DDL).

The plan-walker from the first draft is dropped: with registration filtered and the `SessionCatalog` backstop in place, a scan cannot name a catalog that is not registered.

### D2. Verify the scope with a second `BearerTokenProvider`

The scope is a JWT like any bearer, so the existing provider verifies it. A second instance is configured for the scope issuer:

- `issuer` = the configured issuer, `jwks_url` = the configured https URL;
- `allow_unbounded_audience = true`, because the scope has no audience;
- `exp` is required and leeway is 30 s;
- after decoding, `alg` must be ES256.

This reuses the JWKS cache, the refetch rate limit and the per-key algorithm pin without moving code. The scope verifier lives in a new `sqe-auth` module; `bearer_token.rs` gains only a `pub(crate)` entry to `validate_jwt`.

### D3. Where the scope is checked

- **Every front door, per request.** Trino HTTP reads the header on each request. Flight SQL reads the call metadata on each RPC, whether the session came from a session id or a bearer.
- **Per-request clone.** The verified scope goes on a per-request clone of the stored session (`Session::with_catalog_scope`). The stored session never holds a scope, so a request without one cannot inherit one.
- **Fail closed at convergence.** `create_session_context` and `build_session_catalog` refuse a session whose `token_binding.client_id` is the scoped client and that carries no scope, or a scope whose `jti`/`sub` do not match or that has expired. MCP, Quack, the web UI and ingest therefore cannot reach the scoped client's catalogs without a scope, even though they have no scope header.
- **Rotation.** When a Flight session's bearer rotates (`rotate_credentials`), the binding changes with the new token. The next request needs a scope for the new `jti`. One scope per token, not per job.
- **Cache keys.** `session_cache_key` and `QueryCache::session_scope` append a digest of the sorted `(catalog, mode)` list only when a scope is present. Unscoped keys stay byte-identical, so existing caches are unaffected.

### D4. Only one client is scoped

- A bearer whose `azp` equals `client_id` must present a scope.
- No other bearer is affected. A scope sent with another client's bearer is ignored and logged at debug.

This keeps the feature off for every existing caller.

### D5. Catalog names are compared after the identifier fold

Scope catalog names are folded like SQL identifiers (ASCII lowercase) before comparison, so `"A"` in the scope and `a` in SQL agree with what DataFusion resolves. If a name appears twice after folding, the narrower mode wins.

### D6. End-to-end identity is unchanged

The scope is a second, signed statement about the same bearer. It does not replace the bearer.

- The session's access token is the bearer the caller presented, or the user's exchanged token when token exchange is configured.
- Polaris and S3 receive it exactly as without a scope, so Polaris authorizes the person and audits the person.
- `token_binding` is read from the token the caller presented (the verified JWT, or the exchange's subject token), never from an exchanged catalog token. The scope's `sub` and `jti` must match that token.

## Risks

- **Session build cost.** The JWKS is cached with the same refresh policy as the bearer path, and verification is one ES256 check per request.
- **A write path that skips D1.2.** The `SessionCatalog` backstop (D1.3) catches every Polaris write. Hive writes rely on D1.2, so the Hive write entry points are listed in the tests.
- **A new front door that never reads the scope.** D3's convergence check fails it closed for the scoped client.
