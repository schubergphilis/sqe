# Polaris dynamic catalog discovery (lazy probe)

## Goal

Let SQE resolve Polaris catalogs **dynamically** so any warehouse the caller is
authorized for is queryable as `"<warehouse>".<namespace>.<table>` without a
static `[catalogs.<name>]` TOML entry or an SQE restart. The motivating case:
the Terraform provider creates warehouses at runtime (random-suffixed,
per-tenant), and today only statically-declared catalogs are queryable.

## Motivation

Today every queryable catalog must be declared in `sqe.toml` as
`[catalogs.<name>]` (pinned to one `warehouse`) and the coordinator restarted.
A warehouse created as e.g. `main_warehouse_9d679d` is writable in Polaris but
returns:

```
unknown catalog 'main_warehouse_9d679d' in 3-part identifier; configured
catalogs are ["datafusion","main_warehouse","system"].
```

This breaks IaC / multi-tenant / per-user-demo provisioning.

## Approach: lazy probe on reference (chosen)

When the coordinator pre-flight check hits an **unknown** 3-part catalog
qualifier and discovery is enabled, it attempts to resolve that one warehouse
against Polaris using the caller's bearer, building the **same**
`SqeCatalogProvider` static catalogs use. Polaris itself rejects warehouses the
caller is not authorized for (or that do not exist); on any failure the
existing "unknown catalog" error fires unchanged.

Rejected alternatives:

- **Eager list-all at session start** (Polaris management `GET /catalogs`):
  needs management-scoped permissions the query bearer usually lacks, a
  separate client, and registers every visible warehouse per session. More
  surface, more cost, more auth scope. Lazy probe needs none of it.
- **Auto-fire the existing runtime `ATTACH`** (`runtime_catalog.rs` /
  `mount.rs`): that path builds a `WritableIcebergCatalog`, **not** the
  policy-aware `SqeCatalogProvider`. Discovered catalogs would silently lack
  policy enforcement, dynamic-filter pushdown, and late materialization — a
  behavior split from static catalogs. Rejected.

## Architecture

### 1. Config (`sqe-core/src/config.rs`)

Add a discovery mode under `[catalogs]`:

```toml
[catalogs]
discovery = "static"        # default; today's behavior
# discovery = "polaris-auto"  # lazy probe on unknown 3-part reference
```

A `CatalogDiscovery` enum (`Static` | `PolarisAuto`), `#[serde(default)]` to
`Static` for back-compat. The quickstart config sets `polaris-auto`.

### 2. Resolution hook (`sqe-coordinator/src/query_handler.rs`)

The existing pre-flight (around the `extract_catalog_qualifiers` /
`known_catalog_names` check) is in an async context. New behavior when
`discovery == PolarisAuto`:

```
for qualifier not in known_catalog_names():
    if discovery == PolarisAuto:
        cfg = template_catalog_config(warehouse = qualifier)
        match resolve_catalog(cfg, storage, session_bearer).await:
            Ok(provider) => ctx.register_catalog(qualifier, provider); accept
            Err(_)       => leave unresolved
if any qualifier still unresolved: return existing "unknown catalog" error
```

`resolve_catalog` mirrors the static path:
`SessionCatalog::for_session_with(cfg, storage, table_cache, bearer)` ->
`SqeCatalogProvider::try_new_with_policy(session_catalog, storage, warehouse,
policy_store, session_user)`. So a discovered catalog is byte-for-byte the same
provider type as a static one.

**Template config:** the default catalog's connection (the catalog named by
`query.default_catalog`, else the legacy `[catalog]` block) supplies
`catalog_url`, `auth`, and `backend = Rest`; only `warehouse` is overridden to
the referenced name. Global `[storage]` and `metadata_cache_ttl_secs` are
inherited. (No separate `discovery_template` config in v1 — YAGNI.)

### 3. Security scoping (per-user session, not process-global)

The discovered provider is registered into the **per-user session context**,
never a process-global registry. It is built with *that user's* bearer, so
Polaris vends *their* S3 creds and enforces *their* authz. Registering it
process-globally (as the `ATTACH` registry does) would leak catalog existence
across users and reuse the first resolver's vended creds for everyone — a
security hole. Per-session keeps it correct:

- A warehouse the caller is not authorized for fails the probe and surfaces the
  **identical** "unknown catalog" error as a nonexistent one — no information
  leak (PostgreSQL-RLS-style invisibility, consistent with SQE's policy model).
- Each user only ever sees / queries warehouses their token authorizes.

### 4. Caching and drop-out

The discovered provider lives in the cached session context (per user+token,
~5 min idle TTL — the existing session cache). New sessions re-resolve. A
dropped or renamed warehouse stops resolving on the next session refresh,
comfortably "within TTL". No new cache layer is introduced; table metadata
continues to honor `metadata_cache_ttl_secs` exactly as today. A second
reference to the same warehouse within a session reuses the registered provider
(no re-probe).

### 5. Backends

Discovery is Polaris/REST-specific. The probe always builds a `Rest`-backend
`CatalogConfig`. Non-REST backends (Glue, S3Tables, HMS) are unaffected and
still require static declaration or `ATTACH`.

## Data flow

```
Client: SELECT ... FROM "main_warehouse_9d679d".analytics.orders
  -> coordinator pre-flight: "main_warehouse_9d679d" not in known catalogs
  -> discovery=polaris-auto: build Rest CatalogConfig (template + warehouse=...)
  -> SessionCatalog::for_session_with(cfg, storage, table_cache, user_bearer)
       (Polaris REST: GET /v1/config, GET /v1/namespaces  -- with user bearer)
       Polaris 403/404 -> Err -> "unknown catalog" error (no leak)
  -> SqeCatalogProvider::try_new_with_policy(...)  -> ctx.register_catalog(name)
  -> qualifier now known -> plan + execute as usual
```

## Error handling

- Probe failure (auth denied, not found, network) -> the qualifier stays
  unresolved -> the existing `SqeError::Catalog("unknown catalog ...")` is
  returned. Unauthorized and nonexistent are indistinguishable to the client.
- `discovery = static` (default): the pre-flight is unchanged — no probe, same
  error as today.

## Testing strategy

Integration tests against the quickstart Polaris + S3 stack:

1. **Lazy-resolve hit:** create a Polaris warehouse `foo` (no `sqe.toml` entry,
   no restart); `SELECT count(*) FROM foo.ns.tbl` resolves and returns rows.
2. **Miss:** reference a nonexistent warehouse -> "unknown catalog" error.
3. **Authz denial:** a warehouse the token is not authorized for -> identical
   "unknown catalog" error (no leak), and not queryable.
4. **Static mode unchanged:** `discovery = static` behaves exactly as today
   (unknown name errors without a probe).
5. **In-session reuse:** a second reference in the same session does not
   re-probe Polaris (registered provider reused).
6. **Drop-out:** after a warehouse is dropped, a fresh session no longer
   resolves it (within session TTL).

## Files

| File | Change |
|---|---|
| `crates/sqe-core/src/config.rs` | `CatalogDiscovery` enum + `[catalogs] discovery` field (default `Static`) |
| `crates/sqe-coordinator/src/query_handler.rs` | pre-flight: lazy resolve unknown qualifiers when `PolarisAuto`; `template_catalog_config` helper |
| `crates/sqe-coordinator/src/session_context.rs` | (if needed) share the static catalog-build helper so the discovery path and session path stay identical |
| `quickstart/sqe/assets/sqe-config/sqe.toml` | set `discovery = "polaris-auto"` |
| `crates/sqe-coordinator/tests/...` | discovery integration tests |

## Success criteria

1. With `discovery = polaris-auto`, creating Polaris warehouse `foo` makes
   `SELECT ... FROM foo.ns.tbl` resolve with no `sqe.toml` change and no
   restart.
2. Unauthorized / nonexistent warehouses are not queryable and leak nothing.
3. `static` mode behaves exactly as today.
4. Discovered catalogs enforce the same policy / authz / scan behavior as
   static ones (same `SqeCatalogProvider`).

## Out of scope (v1)

- `SHOW CATALOGS` listing undiscovered warehouses (lazy = on-demand; an
  optional management-API list for `SHOW CATALOGS` only is a possible
  follow-up).
- Dynamic discovery for non-REST backends (Glue/S3Tables/HMS).
- A dedicated `[catalogs] discovery_template` block (default-catalog template
  suffices).
