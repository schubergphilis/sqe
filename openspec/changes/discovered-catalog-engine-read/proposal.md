## Why

The engine credential identity (`engine-credential-identity`) lets a person with metadata-only access in Polaris read through SQE: SQE loads the table as the person, checks `select` in the policy engine, and fetches the storage keys as the engine. It applies only to catalogs declared in `sqe.toml`. A warehouse reached through `[query] catalog_discovery = "polaris-auto"` reads with the person's own delegated keys (the discovery template drops `credentials`, task 2.1 of that change), so a metadata-only person cannot read it.

A deployment that adds Polaris warehouses at run time therefore has to add a `[catalogs.<name>]` block with `credentials` for each new warehouse and restart SQE before people can read it. This change gives discovered warehouses the engine identity without a per-warehouse entry or a restart, while keeping the guarantees that made the identity safe: default-deny `select`, keys only after the person's own load and `select`, reads only, and no engine reach outside the configured Polaris.

## What changes

- **Opt-in, default off.** A new setting `[query] discovery_engine_read = "off" | "template"` (default `"off"`). Off is today's behaviour: the template drops `credentials` and a discovered warehouse reads with the person's delegated keys.
- With `"template"`, a discovered warehouse that passes the **membership proof** (below) gets the template catalog's engine identity. The engine credentials, the catalog URL and the token endpoint come only from the template; nothing the caller names reaches them.
- **Membership proof.** A discovered warehouse gets the identity only when (1) its name matches `[query] discovery_engine_warehouses` (a required list of exact names or `*` globs), (2) it is not a configured catalog key, warehouse alias or Hive catalog, (3) the person's own `GET /config?warehouse=` at the template URL succeeds, and (4) the engine's `GET /config?warehouse=` at the same URL succeeds and returns the same catalog `prefix`. Any failure leaves the warehouse on the person's delegated keys.
- **Default deny without a registry.** With the opt-in on, the policy store denies `select` by default on every catalog name except the configured catalogs without `credentials`. The rule exists from startup, so no discovered name can be read with engine keys before its deny is in force, and no run-time registration can be missed.
- **Writes are unchanged.** Discovered write targets (`discover_session_catalog`, maintenance procedures) keep the person's delegation.
- **Engine token scope.** Discovered warehouses share the template's engine token (one token per endpoint, client id and scope). This covers a deployment whose IdP issues that token with OAuth `scope = "profile"`. Polaris then takes the engine's principal roles from the token claims, and the engine reads data only in the warehouses where those roles hold read-data.
- **Startup validation** refuses the opt-in unless the template has a `client_credentials` engine identity, `require_vended_credentials`, `policy.engine = "ranger"` with `catalog_scoped = true`, and a non-empty `discovery_engine_warehouses`.
- **Audit.** One `auth` event per proof outcome (proved, or refused with the failed step), and the existing key-fetch event marks discovered catalogs.

## Out of scope

- A per-warehouse engine client or per-warehouse OAuth scope, for example a `PRINCIPAL_ROLE:<warehouse>_reader` pattern. This change uses one engine token for all discovered warehouses. Narrowing that token is possible future work.
- Write keys for discovered warehouses. Writes stay delegated, as for configured catalogs.
- Discovery against more than one Polaris. Discovery has one template, and the proof binds every discovered warehouse to it.
- Hive catalogs and file TVFs.

## Impact

- `sqe-core` config: two `[query]` keys and validation.
- `sqe-coordinator`: `discovery_template`, `discover_catalog_provider`, the proof, audit.
- `sqe-policy`: `RangerStore` engine-catalog membership becomes a rule. The crate is at its size ceiling and the budget is not raised (design D9).
- Docs: the catalog discovery and vended-credentials pages.
