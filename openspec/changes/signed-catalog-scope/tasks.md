## 1. Prerequisites

- [x] 1.1 `Identity.token_binding` (`azp`, `jti`, `sub`) from the presented token; carried into `Session` by `into_session`
- [x] 1.2 Scope verifier: a `BearerTokenProvider` for the scope issuer (D2); bearer tests unchanged
- [x] 1.3 Config `[session.catalog_scope]` (`client_id`, `issuer`, `jwks_url`, `header_name`) + `sqe.toml.example`; off when unset

## 2. Scope verification

- [x] 2.1 `CatalogScope` type + ES256 verification of `sub`, `jti`, `exp`, `catalogs`; unit tests for each refusal
- [x] 2.2 Read the scope header on HTTP/Trino and Flight SQL metadata per request; refuse with reason + audit
- [x] 2.3 Fail closed at `create_session_context` / `build_session_catalog` for the scoped client without a valid scope (D3)
- [x] 2.4 Both cache keys include the scope digest, only when a scope is present

## 3. Narrowing

- [x] 3.1 Filter configured, discovered, Hive and attached catalogs at registration; lazy discovery; view sibling warehouses
- [x] 3.2 `SHOW CATALOGS`, `USE`, `X-Trino-Catalog`
- [x] 3.3 Refuse `ATTACH`/`DETACH`, `CREATE`/`DROP SECRET` and DCL in scoped sessions (buffered and streaming dispatch)
- [x] 3.4 `check_catalog_write` where writes resolve their target (DML, CTAS, DDL, CALL, Hive, ingest)
- [x] 3.5 `SessionCatalog` backstop: read-only warehouses refuse every mutation

## 4. Tests and docs

- [ ] 4.1 Integration: acceptance 1-3 with a test issuer (local JWKS over https in the fixture)
- [x] 4.2 Book: security page documents the scope, the claims and the header
