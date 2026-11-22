**STATUS 2026-11-22: catalog scope reads the producer's version 1 contract (`feat/catalog-scope-iss-typ-908`, #908, OpenSpec `signed-catalog-scope` task 2.5).**

- The producer's contract adds `iss`, `iat` and the header `typ` to the claims #908 was filed with (`sub`, `jti`, `catalogs`, `exp`). It proposes that a verifier SHOULD check `iss` and refuse an unknown `typ`.
- `iss`: already required and compared with `issuer` through `BearerTokenProvider` (`required_spec_claims`). Unchanged; a test for a missing `iss` was added.
- `iat`: accepted and not checked (claims are read as a JSON value).
- `typ`: new `[session.catalog_scope] token_type` (default `sqe-catalog-scope+jwt`, neutral like `header_name`). The verifier reads a scope whose `typ` is `token_type`, `JWT` or absent, and refuses any other value. The comparison ignores case and an `application/` prefix. The refusal reason leaves out the presented `typ`, because that value is unauthenticated input. A deployment for the orchestrator sets `token_type = "portal-catalog-scope+jwt"` next to its `header_name`.
- Tests: `cargo test -p sqe-core --lib catalog_scope` (12) and `cargo test -p sqe-auth --lib catalog_scope` (13) pass.
- Not run: `make test-integration` (the #908 acceptance suite with a local https JWKS), or a live scope from the orchestrator (not built yet).

**NEXT:** set `token_type` and `header_name` in the orchestrator's SQE deployment when its scope flag is turned on, then run the #908 acceptance suite with the orchestrator's real issuer.
