**STATUS 2026-11-18: signed catalog scope for one OAuth client's sessions (`feat/signed-catalog-scope`, #908, openspec signed-catalog-scope).**

`[session.catalog_scope]` makes sessions of one client (`azp`) present an ES256 JWS bound to the bearer's `sub` + `jti`. The door check runs on the Trino HTTP submit path and on Flight SQL metadata, with an audited refusal. The fail-closed check (`Session::effective_scope`) runs in core at context build and on every catalog read/write check, so MCP and Quack sessions of that client are refused too. Narrowing: `build_session_catalog` refuses out-of-scope catalogs as "not found"; configured, discovered, Hive and attached catalogs are filtered at registration; `SHOW CATALOGS` / `USE` / `SET CATALOG` are limited; `classify_user_sql` refuses ATTACH / DETACH / secrets / DCL / tag DDL; `SessionCatalog::ensure_writable` refuses mutations in `read_only` catalogs and Hive DDL checks write access per root. Both cache keys gain `:scope=<digest>` only when a scope is present, so unscoped keys are byte-identical.

Limits: no integration test yet (task 4.1, needs a test issuer with an https JWKS in the fixture). The `check_statement` filter has a wildcard arm by design (it is a filter, not a dispatch).

**NEXT:** integration test for acceptance 1-3 against a local issuer; then the orchestrator side mints the scope.
