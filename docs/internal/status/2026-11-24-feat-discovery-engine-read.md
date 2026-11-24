# feat/discovery-engine-read

**NEXT:** run the catalog-discovery IT cases (openspec task 4.5) and the live
Ranger suite (4.6) on the test stack before the opt-in is turned on anywhere.

Implements `openspec/changes/discovered-catalog-engine-read/`.

## Done

- `[query] discovery_engine_read = "off" | "template"` and
  `discovery_engine_warehouses`, with startup validation.
- Proof in `sqe-coordinator/src/discovery_engine.rs`: allowlist, configured or
  reserved name, person `/v1/config`, engine `/v1/config`, same prefix. The
  person's call is made explicitly before the build rather than reusing the
  build's own call, so credentials are set only after a proved verdict.
- `RangerStore` engine catalogs are `Listed` or `AllExcept`; `AllExcept`
  exempts configured catalogs without credentials, config Hive roots,
  `datafusion`, `information_schema` and the reserved `system` schemas.
  A Hive root attached at runtime is not exempt.
- Audit: one proof event per proof, `discovered` on key-fetch events. The
  proof event has no resource (the audit object types are table and view).
- Engine token cache shared, so two warehouses fetch one token.
- Docs: operations/catalogs.md, "Engine reads for discovered warehouses".

## Tests run

- `cargo test -p sqe-core --lib discovery_engine`
- `cargo test -p sqe-policy --lib engine_catalog` (11)
- `cargo test -p sqe-coordinator --lib discovery` (wiremock proof tests: off,
  allowlist miss, configured or reserved name, prefix mismatch, missing prefix,
  person 403, engine 403, pass, one token for two warehouses)

## Not run

- IT 4.5 (test stack) and live Ranger suite 4.6.
- An end-to-end load test that checks the delegation header and the
  `/credentials` call on a discovered table. The load path is the same one the
  configured-catalog engine identity tests in `sqe-catalog` cover.
