# Design: signed catalog scope

## Context

`Session` (`crates/sqe-core/src/session.rs:39`) carries the user, token and session catalog, but no `azp`, `jti` or scope. The per-token `SessionContext` cache keys on token, roles, groups and session catalog/schema. Catalogs are resolved in several places: static config, discovered Polaris warehouses (`session_context.rs:560,595`), Hive roots, runtime `ATTACH` (`runtime_catalog.rs`), `SHOW CATALOGS` (`query_handler/show.rs`), `USE`, and maintenance `CALL` (`maintenance.rs:3368`). There is no single choke point.

## Decisions

### D1. Two layers: hide at registration, guarantee at plan time

Filtering at each resolution site makes other catalogs invisible ("catalog not found", `SHOW CATALOGS`), but one missed site would leak. So the real guarantee is a plan-time walker, run after policy rewriting and before execution, over every `TableScan` and every write target (DML, DDL, `CALL`, `COPY`, CTAS, branch writes, transaction commits):

- a scan whose catalog is not in scope -> refused as "catalog not found";
- a write whose catalog is not `read_write` -> refused "catalog `<b>` is read-only in this session".

Statements that bypass the logical plan (`CALL`, DDL handled in `query_handler`, Flight SQL ingest) call the same `CatalogScope::check_write(catalog)` before their commit. A test lists every write entry point and asserts it calls the check (a match over the statement enum without a wildcard arm, per the dispatch rule).

### D2. Verifier extracted from `bearer_token.rs`

`JwksVerifier { issuer, jwks_url, http, keys cache }` with `verify(&str) -> Claims`, https-only and algorithm pinned per key, moved out of `BearerTokenProvider` as a pure move, then used twice. The scope verifier accepts only ES256 and requires `exp`; clock skew 30 s like the bearer path.

### D3. Where the scope is checked

- HTTP/Trino: per request, since each request carries the bearer.
- Flight SQL: at handshake, and again on every request, because the bearer can rotate (`Session::rotate_credentials`). A rotated bearer with a new `jti` needs a scope for that `jti`; the old scope is dropped and the session refused until one arrives. One scope per token, not per job.
- The verified scope is stored on `Session` as `Arc<CatalogScope>`; the session cache key includes a hash of the sorted `(catalog, mode)` list, so two scopes for the same token never share a `SessionContext`.

### D4. Only one client is scoped

A bearer whose `azp` equals `client_id` must present a scope; any other bearer must not be affected, and a scope sent with another client's bearer is ignored (logged at debug). This keeps the feature off for every existing caller.

### D5. Catalog names are compared after the identifier fold

Scope catalog names are folded like SQL identifiers (ASCII lowercase) before comparison, so `"A"` in the scope and `a` in SQL agree with what DataFusion resolves.

## Risks

- Session build cost: the JWKS is cached with the same refresh policy as the bearer path; verification is one ES256 check per request.
- A write path that skips D1's check. Mitigation: the exhaustive-match test and the plan walker.
