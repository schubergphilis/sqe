# Catalog scope mode: binding, union or off

## Why

The signed catalog scope (`signed-catalog-scope`) narrows a runs-client session to the catalogs its workspace binds, with read-only bindings enforced. A deployment has to be able to choose how far that goes. Examples are a k3s quickstart that has no workspace bindings yet, or an operator who wants the scope's proof of origin without the narrowing. Today the only choice is to configure the section or not.

The orchestrator's own engine guard (the Spark half) names its refusals `catalog_not_in_workspace` and `catalog_read_only`. SQE's refusals carry no such reason, so the two engines cannot be audited side by side.

## What Changes

- `[session.catalog_scope] mode = "binding" | "union" | "off"`. It is read at startup only and defaults to `binding` when the section is present.
  - `binding`: today's behaviour. A scope is required for the runs client. Only its catalogs are visible, read-only bindings refuse writes, and a failed verification or an unreachable JWKS refuses the session.
  - `union`: the scope is still required for the runs client and still verified and audited, but it does not narrow. The session sees every catalog the person can see.
  - `off`: no workspace check. The header is ignored, and the runs client is treated like any other client.
- Without the section, the feature stays off as before. `binding` or `union` without `client_id`, `issuer` and `jwks_url` is a startup error. Startup logs the active mode.
- A scoped session's refusal of an out-of-scope catalog, or of a write into a read-only one, carries a reason: `catalog_not_in_workspace` or `catalog_read_only`. The reason is the audit record's `error_type`. The client still gets "catalog not found" for a hidden catalog, so a hidden catalog stays indistinguishable from a missing one. The read-only message names its reason.

## Capabilities

### Modified Capabilities

- `catalog-scope`: a startup mode, and named refusal reasons.

## Impact

- `sqe-core`: `CatalogScopeConfig.mode`, the scoped client only in `binding`, and a typed scope refusal reason.
- `sqe-coordinator`: the gate installs per mode (`union` verifies without attaching); audit `error_type` from the reason; startup log.
- Docs: configuration reference and the security page.
