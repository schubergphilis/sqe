# Design

## D1. Where the mode lives

The mode is a `mode` key on `[session.catalog_scope]`, next to the issuer, JWKS and client it governs. It is never a session setting or a header, so neither a run's code nor a client can change it. An absent section means off, because there is no issuer to verify against and a fail-closed default would refuse every session of an existing deployment.

## D2. Mode table

| runs client (`azp = client_id`) | binding | union | off |
|---|---|---|---|
| no scope | refused, audited | refused, audited | allowed, unscoped |
| bad, expired or other-`jti` scope | refused | refused | header ignored |
| JWKS unreachable | refused | refused | not fetched |
| valid scope | bound catalogs only, read-only enforced | verified and audited; every catalog the person can see | header ignored |

Other clients are unchanged in every mode.

`union` and `off` differ on a runs-client session without a scope. That row is what the per-mode tests assert.

## D3. How `union` avoids narrowing

`catalog_scope_gate::attach` verifies the scope and audits it. In `union` it then returns the session without attaching the scope. `sqe_core::catalog_scope::set_scoped_client` is set only in `binding`, so the fail-closed check at context build (MCP, Quack, every door) does not refuse an unscoped runs-client session in `union` or `off`. In `union`, the requirement to present a scope is enforced at the doors that can carry one (Flight SQL, HTTP). In `binding`, it is also enforced at context build for doors that cannot.

## D4. Refusal reasons

`ScopeReason { NotInWorkspace, ReadOnly }` travels as the source of the coded error, the same way `RefusalReason` does for admission (#902). `audit_emitter::outcome_of` puts its wire name in `error_type`. The client message for `NotInWorkspace` stays "Catalog 'x' not found". Where DataFusion wraps the error and drops the source, the audit falls back to the Trino error type, as today. The integration test asserts the reason on the paths the acceptance names: SELECT, INSERT, CREATE TABLE, CALL and USE.
