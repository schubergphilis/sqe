## 1. Prerequisites

- [ ] 1.1 Read `azp` and `jti` from the verified bearer claims into `Session`
- [ ] 1.2 Extract `JwksVerifier` from `bearer_token.rs` as a pure move (D2); bearer tests unchanged
- [ ] 1.3 Config `[session.catalog_scope]` + `sqe.toml.example`; off when unset

## 2. Scope verification

- [ ] 2.1 `CatalogScope` type + ES256 verification of `sub`, `jti`, `exp`, `catalogs`; unit tests for each refusal
- [ ] 2.2 Read `x-sqe-catalog-scope` on HTTP/Trino and Flight SQL metadata; refuse with reason + audit
- [ ] 2.3 Flight SQL: re-check per request and on `rotate_credentials` (D3)
- [ ] 2.4 Session cache key includes the scope hash

## 3. Narrowing

- [ ] 3.1 Filter static, discovered, Hive and runtime catalogs at registration; refuse `ATTACH`
- [ ] 3.2 `SHOW CATALOGS`, `USE`, `X-Trino-Catalog`
- [ ] 3.3 Plan-time walker: scans and write targets (D1)
- [ ] 3.4 `check_write` in non-plan write paths (CALL, DDL, COPY, ingest, branches, transactions) + exhaustive-match test

## 4. Tests and docs

- [ ] 4.1 Integration: acceptance 1-3 with a test issuer (local JWKS over https in the fixture)
- [ ] 4.2 Book: security page documents the scope, the claims and the header
