## 1. Config and validation

- [x] 1.1 `[query] discovery_engine_read = "off" | "template"` (default `"off"`) and `[query] discovery_engine_warehouses: Vec<String>` (exact names and `*` globs); config docs and env table (D0, D2)
  - No env overlay exists for `catalog_discovery`, so none was added; documented on the catalogs page.
- [x] 1.2 Startup validation for `"template"`: polaris-auto, template `client_credentials` + `require_vended_credentials`, Ranger + `catalog_scoped`, non-empty allowlist (D7)

## 2. Coordinator

- [x] 2.1 `discovery_template` keeps the template's `credentials` only with `"template"`; `discover_session_catalog` clears them in both modes (D0, D1, D6)
  - Done differently: `discovery_template` still clears them in both modes; `discover_catalog_provider` sets them from `engine_credentials_for` only after a proved verdict. `discover_session_catalog` never calls it.
- [x] 2.2 Membership proof: allowlist, configured-name check, person `/config` (existing call), engine `/config` with prefix compare; verdict cached per session catalog (D2, D3)
  - Done differently: the person's `/config` is an explicit call before the build, so the proof finishes before anything is built. Reserved names (`system`, `datafusion`) refuse at the configured-name step.
- [x] 2.3 Attach the identity only after a proved verdict; any failed step falls back to the delegated client (D0, D2)
  - A failed proof step falls back. A key fetch refused after a passed proof does not: on Polaris the engine passes `/config` without a role, so the read fails with `ENGINE_CREDENTIALS_FAILED` (D2, measured in 4.5).
- [x] 2.4 Read gate and policy key use the registered SQL name, the same one as the rewriter's `TableScope` (D4)
- [x] 2.5 Audit: proof outcome event; `discovered = true` on key-fetch events; no secret values (D8)
  - The proof event has no resource: the audit object types are table and view. It names the warehouse in its text.
  - Unit tests cover the proof event's content (proved and refused) and the key-fetch marker read back from an audit file. Proof-event delivery through an installed audit log is not tested: the sink is process-wide, so it is left to the IT in 4.5.
- [x] 2.6 Check that `select_scan_credentials` and the `TableReloader` treat a discovered engine-keyed table the same as a configured one (D6)
  - By reading: `select_scan_credentials` looks only at the table's vended config; the reloader reloads through the same session catalog and its engine identity.

## 3. Policy store

- [x] 3.1 `RangerStore` engine catalogs as `Listed` / `AllExcept`; with the opt-in on, built from the configured catalogs without `credentials`; startup log names the mode (D4)
  - `AllExcept` also exempts `datafusion`, `information_schema` and the reserved `system` schemas. Hive roots attached at runtime are not exempt.
- [ ] 3.2 `cargo xtask budgets --check` stays green for `sqe-policy`, `sqe-coordinator` and `sqe-core` with no ceiling raise; if `sqe-policy` is over, move the `engine_catalogs` tests to `crates/sqe-policy/tests/` (D9)
  - Not as designed: these crates were already over their ceilings before this change; raised by owner decision instead.

## 4. Tests

- [x] 4.1 Unit (`sqe-core`): each refusal in 1.2; default is `"off"`; allowlist glob matching
- [x] 4.2 Unit (`sqe-coordinator`): `discovery_template_drops_engine_credentials` unchanged for `"off"`; with `"template"` the template keeps `credentials`; `discover_session_catalog` drops them in both modes; proof comparator (same prefix, different prefix, missing prefix)
  - The template never keeps `credentials` (see 2.1), so `discovery_template_drops_engine_credentials` covers both modes.
- [x] 4.3 Unit (`sqe-policy`): `AllExcept` denies an unlisted name and a differently spelled name; a configured catalog without `credentials` is unchanged; `Listed` behaves as before; the deny needs `catalog_scoped`
  - Also one test per reserved `system` schema, `datafusion`, `information_schema`, and config vs attached Hive roots.
- [x] 4.4 wiremock: engine `/config` 403, wrong prefix, or outside the allowlist -> person's load carries the delegation header and no `/credentials` call; proof passes -> no delegation header, `/credentials` with the engine token; configured `scope = "profile"` sent verbatim in the token form; two discovered warehouses -> one token request
  - Proof level in `discovery_engine::tests::proof`: off, allowlist miss, configured or reserved name, prefix mismatch, missing prefix, person 403, engine 403, pass, scope in the token form, one token for two warehouses.
  - Load level in `tests/it/discovery_engine_load.rs` (plain tests, `cargo test -p sqe-coordinator --test it discovery_engine_load`): a table load through `discover_catalog_provider` on a proved warehouse returns the engine's keys, sends no delegation header and one `/credentials` call with the engine bearer, and the token form's scope is exactly `profile`; engine 403, a different engine prefix and a warehouse outside the allowlist each load with the person's keys, `vended-credentials` on the load and no `/credentials` call; two proved warehouses share one token request; a warehouse outside the session's catalog scope makes no call at all.
- [x] 4.5 IT (`tests/it/catalog_discovery_test.rs`, test stack, `InMemoryPolicyStore`):
  - a metadata-only person reads a discovered warehouse through the engine;
  - a warehouse created in Polaris during the test is readable with no restart;
  - an allowlisted warehouse where the engine has no role: Polaris answers the engine's `/config`, so the proof passes, the key fetch gets 403 and the read fails with `ENGINE_CREDENTIALS_FAILED` and no rows (no fallback to the person's keys);
  - a write to a discovered warehouse uses the person's delegation;
  - binding-mode catalog scope (#908) refuses a discovered warehouse outside the signed scope before any proof call.
- [ ] 4.6 Live Ranger suite (`make test-access-control`): a discovered warehouse with no `select` policy reads zero rows with no `/credentials` call; adding the policy makes it readable at the next policy refresh
- [ ] 4.7 State in the MR which suites ran locally; CI runs none of the integration suites

## 5. Docs

- [x] 5.1 Catalog discovery page: the opt-in, the allowlist, the proof, and the fallback
- [x] 5.2 Vended-credentials page: grant the engine principal read-data per warehouse, and write Ranger `select` policies before turning the opt-in on; OAuth scope shapes (D5)
  - Both live in operations/catalogs.md, under "Engine reads for discovered warehouses".
