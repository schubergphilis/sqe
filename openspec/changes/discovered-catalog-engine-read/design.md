## Context

How things work today (main at `d3ca277a1`):

- **Choosing the template.** `discovery_template` (`sqe-coordinator/src/session_context.rs`) clones the template catalog. That is the default catalog if it is REST, otherwise the first REST catalog.
  - The clone is kept only when the template's `auth` is per-user (finding 752). An engine-held `auth` would otherwise travel to any warehouse a caller can spell.
  - The clone clears `tvf_prefixes` and `credentials` (#940). That second clear is the interim fail-closed rule this change relaxes.
- **Read path.** `discover_catalog_provider` sets `warehouse` on the clone and builds the catalog with the person's bearer.
  - If Polaris refuses or does not know the warehouse, the result is `None`, which surfaces as the usual "unknown catalog" error. An unauthorized warehouse looks the same as a missing one.
  - Its callers are the session context build, `query_handler.rs` (lazy registration), `query_handler/catalog.rs` (`SHOW STATS`, `load_table`) and `query_handler/time_travel.rs`.
- **Write path.** `discover_session_catalog` is the write-path twin, used by `maintenance.rs` and the write targets.
- **Where the identity attaches.** `build_session_catalog` attaches the engine identity when `cat_cfg.credentials` is `Some`, through `engine_identity_wiring::engine_identity_for`. That function fails closed when no policy store is installed.
- **Default deny.** `RangerStore` default-denies `select` for the names in `engine_catalogs`, a `HashSet<String>` built once at startup from `SqeConfig::engine_read_catalog_names` (config keys plus warehouse aliases). The deny applies only under `catalog_scoped = true`, because the first segment of a namespace key must be the catalog.
- **Engine token.** It comes from `sqe_auth::per_catalog::resolve_bearer`, cached per `(token_endpoint, client_id, scope)`, and the scope is sent verbatim when set.
- **Session catalog scope (#908).** In binding mode, a session is refused any catalog outside its signed scope before it builds that catalog.

## Goals and non-goals

- Goal: discovered warehouses get the engine identity with no per-warehouse config and no restart.
- Goal: every guarantee of `engine-credential-identity` holds for a discovered warehouse: the person's own load first, then `select` with default deny, then keys; reads only; and the markers.
- Goal: with the opt-in off, behaviour is byte-for-byte today's.
- Non-goal: a different engine principal or OAuth scope per warehouse.

## Decisions

**D0. Interim stays the default.** The opt-in is `[query] discovery_engine_read`: `"off"` (default) or `"template"`. With `"off"`, `discovery_template` clears `credentials` exactly as today, and the existing test `discovery_template_drops_engine_credentials` keeps passing unchanged. Any failure in the steps below falls back to the `"off"` behaviour for that warehouse (the person's delegated keys). That is fail-closed: the person reads only what Polaris already lets them read with their own keys.

**D1. Where the credentials come from.** With `"template"`, `discovery_template` keeps the template's `credentials`, and only the template's. The URL, token endpoint, client id, secret and scope are all the template's. The caller supplies only the warehouse name, and that name goes into `GET /config?warehouse=` and the catalog prefix, never into an endpoint or a credential. The finding 752 filter (per-user `auth` only) stays as it is: the person's bearer still loads, and the engine identity only fetches keys.

**D2. Membership proof.** Before `build_session_catalog` attaches the identity to a discovered warehouse `W`, all of the following must hold, in this order:

1. **Allowlist.** `W` matches `[query] discovery_engine_warehouses`, a list of exact names or `*` globs. Matching is case-sensitive on the name Polaris resolves. The list is required with the opt-in on. One pattern such as `ws-*` covers every future warehouse of a deployment, so no per-warehouse edit is needed.
2. **Configuration wins.** `W` is not a configured catalog key, a warehouse alias or a Hive catalog. Today the session already resolves configured names before discovery (`configured_session_catalog`); the proof re-checks this so the identity can never attach under a configured name.
3. **The person resolves W.** The person's discovery call succeeds at the template URL. This is the existing `build_catalog_provider` call; a 403 or 404 ends here as "unknown catalog".
4. **The engine resolves the same catalog.** The engine's token calls `GET /config?warehouse=W` at the template URL and gets 200, with the same `overrides.prefix` as the person's response. A 403 from Polaris means the engine principal has no role in `W`; a different prefix means the two calls did not land on the same catalog. Either one ends the proof.

The verdict is cached per `(W, template URL)` for the lifetime of the session catalog. A session context rebuild or a catalog cache refresh (!683) runs the proof again. A refused proof is not cached across sessions, so a warehouse that gains an engine grant works at the next session build without a restart.

What the proof does not need to show: *per-table* reach. That stays with the existing per-table rules. The person's own undelegated `loadTable` must succeed, the `ReadGate` must allow `select`, and Polaris `/credentials` then scopes the keys to that one table and to the engine principal's privilege in `W`.

So the engine's reach in a discovered warehouse is the intersection of four things:
- Polaris grants to the engine principal in `W`;
- the person's metadata access to the table;
- Ranger `select` for the person;
- the allowlist.

**D3. Same Polaris.** "Belongs to the configured Polaris" means it is reachable at the template's `catalog_url` and resolvable by both the person and the engine there, with the same prefix (D2 steps 3 and 4). No other URL can be reached: discovery has no way to name one. A warehouse in that Polaris that the engine principal holds no role in fails step 4. A warehouse the operator does not want engine-read stays outside the allowlist, or its engine grant is withheld in Polaris. Both are explicit operator controls.

**D4. Default deny by rule, not by registration.** `RangerStore.engine_catalogs` changes from a set to an enum:

- `Listed(HashSet<String>)`: today's behaviour, used when the opt-in is off.
- `AllExcept(HashSet<String>)`: used with the opt-in on. It is built at startup from the configured catalogs *without* `credentials` (keys and warehouse aliases). Every other first segment counts as an engine catalog and needs an access grant for `select`.

A runtime registry (adding `W` when its proof passes) was rejected:

- It needs a lock on the policy hot path.
- A missed or reordered registration would attach engine keys before the deny exists.
- A name that is denied once is never removed.

The rule holds from startup, so the invariant "the deny is in force before any identity is attached" holds by construction, and the caller's spelling of `W` cannot slip past it.

The cost: with the opt-in on, a discovered warehouse that falls back to the person's keys is also default-deny in Ranger. That is stricter than today and acceptable. An operator who turns on engine reads for discovered warehouses writes Ranger `select` policies for them anyway (the vended-credentials page already says to add `select` policies first). The startup log line names the mode.

The read gate asks under the same SQL name the rewriter's `TableScope` uses: the discovered catalog's registered name, which is the caller's identifier after ASCII lowercase folding (#337). `AllExcept` makes the spelling irrelevant to the deny itself. The policy key and the rewriter use the same name, so `select` and masks resolve the same policy.

**D5. Engine token and `scope`.** Every discovered warehouse uses the template's engine token, one cached token per `(endpoint, client_id, scope)`. The scope is sent verbatim.

Two deployment shapes:

- **`scope = "profile"`.** An IdP issues the engine token with this OAuth scope and Polaris maps principal roles from the token's claims. The token then activates every role the IdP grants the engine client. This is the shape in use, and it is untested today.
- **`scope = "PRINCIPAL_ROLE:ALL"`.** Polaris's own token endpoint activates all of the principal's roles. Same reach.

In both cases the engine's reach across discovered warehouses is the union of its principal roles' catalog grants, and D2 step 4 is how SQE observes it per warehouse. Nothing in this change parses or rewrites the scope.

The tests pin the behaviour:
- a configured `scope` is sent unchanged;
- the cache is shared across warehouses, so two discovered warehouses cause one token request;
- a token without `PRINCIPAL_ROLE` in its scope still passes the proof when Polaris answers 200.

**D6. Call sites.**

- **Gain the identity:** `discover_catalog_provider` and its read callers (session context build, lazy registration in `query_handler.rs`, `query_handler/catalog.rs`, `time_travel.rs`).
- **Never gain it:** `discover_session_catalog`, which serves the write targets and `maintenance.rs`. It keeps clearing `credentials` in both modes, so writes stay delegated, as D1 of the parent change requires.
- **Follows the session catalog:** the `TableReloader`. Worker scan credentials come from the loaded table, which carries `sqe.key-source = engine`. `select_scan_credentials` must treat a discovered engine-keyed table exactly like a configured one (task 2.6 verifies this).

**D7. Startup validation.** With `discovery_engine_read = "template"`, `SqeConfig::validate` refuses the config unless all of the following hold:

- `catalog_discovery = "polaris-auto"`;
- the template catalog has `credentials` of type `client_credentials` and `require_vended_credentials = true`;
- `policy.engine = "ranger"` with `catalog_scoped = true`;
- `discovery_engine_warehouses` is non-empty.

These are the same refusals as `validate_engine_credentials`, applied to the template. `production_mode` adds nothing new.

**D8. Audit.**

- Each proof outcome writes one `AuditKind::Auth` event. The actor is the person and `app_name` is the engine client id. The detail holds `warehouse`, `discovered = true` and either `proved` or the failed step (`allowlist`, `configured_name`, `person_config`, `engine_config`, `prefix_mismatch`).
- The existing key-fetch event gains `discovered = true` for discovered catalogs.
- No token, key or prefix value is logged.

**D9. Size budget.** `sqe-policy` is at its ceiling and the ceiling is not raised.

- The `RangerStore` change is a type swap inside `ranger_store/engine_catalogs.rs`, plus one constructor. Its net source change should be near zero.
- If `cargo xtask budgets --check` reports `sqe-policy` over its ceiling, the `engine_catalogs` unit tests move to `crates/sqe-policy/tests/`, which the budget does not count, in the same MR.
- `sqe-coordinator` and `sqe-core` take the new code. Their headroom is checked before the build starts.

## Risks

- **An engine principal over-granted in Polaris.** With the opt-in on, every allowlisted warehouse where the engine holds read-data is engine-readable for any person whom Ranger allows `select`. Mitigations: Ranger default deny (D4), the allowlist (D2.1), and the docs stating that the engine principal should hold read-data only, granted per warehouse.
- **Proof latency.** One extra `GET /config` per discovered warehouse per session build. It is cached per session catalog, and the token is cached.
- **The in-memory policy store has no default deny.** The integration tests prove the key path, not the deny. The deny is proven by the Ranger unit tests and one case in the live Ranger suite.

## Resolved

- "scope = profile": the engine token's OAuth scope as an IdP issues it in a deployment where Polaris maps roles from claims (D5). It is not a new SQE setting. If the owner meant a per-warehouse scope instead, that is the out-of-scope item in the proposal and needs its own decision.
