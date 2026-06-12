# Design — per-caller namespace visibility

## Decision: probe Polaris, don't talk to OPA

Three ways to filter namespace names per caller were considered:

1. **Probe-based via existing REST calls (chosen).** For each listed
   namespace, call `get_namespace` with the session's bearer; drop on
   403. Polaris+OPA already make this exact decision per caller — SQE
   adds no new trust relationship, no new endpoint config, no new
   dependency. The mechanism is identical to how table listings are
   already per-caller: forward the bearer, let the security plane
   decide.
2. **Direct SQE→OPA queries.** Matches the platform's browse-API
   implementation (`namespace_visible` rule) but gives the engine a
   second security backend to configure, reach, and keep consistent
   with Polaris. Rejected: Polaris is the engine's single security
   plane by design.
3. **Polaris-side response filtering.** The cleanest end state (every
   client of `listNamespaces` benefits), but it's an upstream Polaris
   change with its own OPA-integration design questions. Out of our
   hands on any useful timeline; the probe approach is forward
   compatible with it (probes simply stop finding anything to drop).

## Mechanics

- Filter once, where `cached_namespaces` is built
  (`SqeCatalogProvider` construction). Every metadata surface —
  `SHOW SCHEMAS`, `information_schema.schemata`, Flight SQL
  `GetDbSchemas` — reads that one list, so one filter point covers all
  of them and they can never disagree.
- Probes run via the session catalog (bearer-bound), concurrently with
  a cap of 8 in-flight. A 30-namespace catalog costs ~4 round-trip
  waves once per session-catalog build. No per-query cost.
- **403/Forbidden → drop. Everything else → keep.** Fail-open mirrors
  the platform's browse filtering: a Polaris hiccup must not blank a
  user's catalog tree; the namespace *contents* remain protected by
  the per-operation checks regardless of what the list shows.
- `information_schema` is appended after filtering, never probed.
- Single-identity backends (Glue/HMS/JDBC/Hadoop) bypass the filter
  entirely — the process identity sees what it sees; there is no
  caller to scope to (consistent with the existing single-identity
  warning in `rest_catalog.rs`).

## Why not cache across sessions

`cached_namespaces` is already per-session-catalog; the probe results
inherit that lifetime. A cross-session `(user, warehouse)` TTL cache
would save the probes on reconnect but adds invalidation questions
(grant changes must take effect on next session at the latest). Start
without it; add only if session-build latency shows up in practice.

## Failure modes considered

- **Polaris slow:** probes share the catalog client's existing timeout;
  worst case the session build is delayed by one timeout wave, after
  which fail-open keeps all names.
- **Token expired mid-build:** probes 401 → fail-open keeps names
  (the subsequent real queries will surface the auth problem loudly).
- **All probes denied:** user with zero grants sees an empty schema
  list (plus `information_schema`) — exactly what the platform's
  browse API shows that user today.
