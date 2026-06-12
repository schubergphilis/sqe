# Per-caller namespace visibility in metadata listings

## Why

SQE leaks the **names** of namespaces a caller holds no grants in. Verified live on the demo stack (2026-06-12, user `team-a-dev`, who has zero grants in `team_a_data.limited`):

```text
SHOW SCHEMAS FROM team_a_data            → ['limited', 'public']   ← leak
SHOW TABLES  FROM team_a_data.limited    → []                      ← correctly hidden
SELECT * FROM team_a_data.limited.audit  → TABLE_NOT_FOUND         ← correctly denied
```

Everything *inside* the namespace is already protected per caller: Polaris+OPA filter table listings and deny loads on the user's own bearer (SQE's REST backend forwards it — `crates/sqe-catalog/src/rest_catalog.rs`). But the namespace list itself is taken verbatim from Polaris's `listNamespaces`, which returns **all** names once catalog-level listing is allowed. `SqeCatalogProvider` caches that list at construction (`crates/sqe-catalog/src/catalog_provider.rs:84`) and `schema_names()` serves it to `SHOW SCHEMAS`, `information_schema.schemata`, and Flight SQL `GetDbSchemas` unfiltered.

A namespace name alone can be sensitive (`limited`, `pre_acquisition_audit`, `customer_x_poc`). The data platform's own browse APIs and MCP tools already hide ungranted namespaces (OPA `namespace_visible` rule, hide-on-403); the engine path is the only metadata surface left that leaks names. The platform's visibility test suite (`data-platform/scripts/visibility/test_trino_visibility.py`) currently prints a `[WARN]` for exactly this; this change turns that warning into a pass.

## What Changes

Filter the namespace list per caller when the session's catalog provider is built, using probes that Polaris+OPA already authorize per user:

- **Probe.** For each namespace returned by `listNamespaces`, issue a concurrent `get_namespace` (→ Polaris `LOAD_NAMESPACE_METADATA`) through the session catalog — it carries the caller's bearer (`SessionCatalog`, `rest_catalog.rs:1572`). Polaris's OPA bridge answers per caller: 403 → the caller has no reachable grant there → **drop the name**. Any other failure (timeout, 5xx) → **keep the name** (fail-open; contents stay protected by the per-op checks, same posture as the platform's browse filtering).
- **Where.** `SqeCatalogProvider` construction (`catalog_provider.rs`), so every consumer of `schema_names()` — `SHOW SCHEMAS`, `information_schema.schemata`, Flight SQL `GetDbSchemas`/`GetTables` — sees one consistent, filtered list. `information_schema` is always appended and never filtered.
- **Config.** `namespace_visibility_filter = true` (default ON) under the catalog config, OFF restores today's behavior. Only meaningful for the REST/Polaris backend; single-identity backends (Glue/HMS/JDBC/Hadoop) skip the filter — there is no per-user identity to filter by (see the existing single-identity warning, `rest_catalog.rs:381`).
- **Concurrency bound.** Probes run concurrently with a small cap (e.g. 8) so catalogs with many namespaces don't serialize N round-trips; the cost is paid once per provider build (per session), not per query.

### Coordination with the platform (OPA policy semantics)

The probe makes Polaris's `LOAD_NAMESPACE_METADATA` decision the visibility rule. The data platform's rego seed (data-platform repo, `quickstart/assets/policies/`) maps that op as of 2026-06-10 and allows it when a grant covers the namespace or the catalog. One semantic gap to verify during implementation: the platform's `namespace_visible` rule also shows a namespace when the caller holds only a **table-level** grant inside it. If `LOAD_NAMESPACE_METADATA` does not fire for table-only grantees, the rego needs a small extension (in the data-platform repo) so both surfaces agree — track it as a coordination task, not an SQE code change.

## Impact

- **Affected specs:** `sql-metadata-visibility` (new requirement set under this change).
- **Affected code:** `crates/sqe-catalog/src/catalog_provider.rs` (filter at `cached_namespaces` build), `crates/sqe-catalog/src/rest_catalog.rs` (probe helper if one doesn't fall out naturally), config plumbing in `sqe-coordinator` session setup. `info_schema.rs` should need no change if it derives schemata from the provider — verify.
- **Backward compatibility:** behavior change is the point — ungranted names disappear from metadata listings for non-privileged callers. Privileged callers (catalog-wide grants) see everything, unchanged. The config flag restores old behavior.
- **Performance:** + up to N concurrent `GetNamespace` calls per session-catalog build for REST backends; zero per-query cost; zero cost for single-identity backends and when the flag is off.

## Non-goals

- Table-level metadata filtering — already correct (Polaris filters per caller).
- A direct SQE→OPA integration — Polaris stays the single security plane the engine talks to.
- Filtering for single-identity (non-REST) backends — nothing per-user to enforce.
- Changes to `sqe-policy` GRANT vocabulary — this is catalog-side visibility, not engine-side authorization.

## Verification

The data-platform repo ships a protocol-level test suite that pins the contract end to end:

```bash
# from data-platform repo — Trino wire (JDBC-equivalent), as a non-privileged user
uv run scripts/visibility/test_trino_visibility.py --insecure --password '...'
```

Acceptance: its `[WARN] namespace name 'limited' visible in SHOW SCHEMAS` disappears, all PASS lines stay green, and a privileged user (`team-a-admin`) still sees the full list.
