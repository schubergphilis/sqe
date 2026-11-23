## 1. Config and validation

- [ ] 1.1 `[query] discovery_engine_read = "off" | "template"` (default `"off"`) and `[query] discovery_engine_warehouses: Vec<String>` (exact names and `*` globs); config docs and env table (D0, D2)
- [ ] 1.2 Startup validation for `"template"`: polaris-auto, template `client_credentials` + `require_vended_credentials`, Ranger + `catalog_scoped`, non-empty allowlist (D7)

## 2. Coordinator

- [ ] 2.1 `discovery_template` keeps the template's `credentials` only with `"template"`; `discover_session_catalog` clears them in both modes (D0, D1, D6)
- [ ] 2.2 Membership proof: allowlist, configured-name check, person `/config` (existing call), engine `/config` with prefix compare; verdict cached per session catalog (D2, D3)
- [ ] 2.3 Attach the identity only after a proved verdict; any failed step falls back to the delegated client (D0, D2)
- [ ] 2.4 Read gate and policy key use the registered SQL name, the same one as the rewriter's `TableScope` (D4)
- [ ] 2.5 Audit: proof outcome event; `discovered = true` on key-fetch events; no secret values (D8)
- [ ] 2.6 Check that `select_scan_credentials` and the `TableReloader` treat a discovered engine-keyed table the same as a configured one (D6)

## 3. Policy store

- [ ] 3.1 `RangerStore` engine catalogs as `Listed` / `AllExcept`; with the opt-in on, built from the configured catalogs without `credentials`; startup log names the mode (D4)
- [ ] 3.2 `cargo xtask budgets --check` stays green for `sqe-policy`, `sqe-coordinator` and `sqe-core` with no ceiling raise; if `sqe-policy` is over, move the `engine_catalogs` tests to `crates/sqe-policy/tests/` (D9)

## 4. Tests

- [ ] 4.1 Unit (`sqe-core`): each refusal in 1.2; default is `"off"`; allowlist glob matching
- [ ] 4.2 Unit (`sqe-coordinator`): `discovery_template_drops_engine_credentials` unchanged for `"off"`; with `"template"` the template keeps `credentials`; `discover_session_catalog` drops them in both modes; proof comparator (same prefix, different prefix, missing prefix)
- [ ] 4.3 Unit (`sqe-policy`): `AllExcept` denies an unlisted name and a differently spelled name; a configured catalog without `credentials` is unchanged; `Listed` behaves as before; the deny needs `catalog_scoped`
- [ ] 4.4 wiremock: engine `/config` 403, wrong prefix, or outside the allowlist -> person's load carries the delegation header and no `/credentials` call; proof passes -> no delegation header, `/credentials` with the engine token; configured `scope = "profile"` sent verbatim in the token form; two discovered warehouses -> one token request
- [ ] 4.5 IT (`tests/it/catalog_discovery_test.rs`, test stack, `InMemoryPolicyStore`):
  - a metadata-only person reads a discovered warehouse through the engine;
  - a warehouse created in Polaris during the test is readable with no restart;
  - an allowlisted warehouse where the engine has no role falls back to the person's keys and fails on storage;
  - a write to a discovered warehouse uses the person's delegation;
  - binding-mode catalog scope (#908) refuses a discovered warehouse outside the signed scope before any proof call.
- [ ] 4.6 Live Ranger suite (`make test-access-control`): a discovered warehouse with no `select` policy reads zero rows with no `/credentials` call; adding the policy makes it readable at the next policy refresh
- [ ] 4.7 State in the MR which suites ran locally; CI runs none of the integration suites

## 5. Docs

- [ ] 5.1 Catalog discovery page: the opt-in, the allowlist, the proof, and the fallback
- [ ] 5.2 Vended-credentials page: grant the engine principal read-data per warehouse, and write Ranger `select` policies before turning the opt-in on; OAuth scope shapes (D5)
