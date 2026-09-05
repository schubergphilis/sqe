# 802: Environment overrides are a hand-maintained allowlist, and code reads env vars around it

- Scope: sqe-core (`config.rs`), sqe-coordinator, sqe-worker, sqe-catalog, deploy/
- Severity: medium
- Category: operability
- Effort: M
- Status: open
- Issue: #634

## Problem

`SqeConfig::apply_env_overrides` (`crates/sqe-core/src/config.rs:5396-6103`)
is a 700-line function that maps environment variables to fields one line at
a time:

```rust
env_override_str("SQE_COORDINATOR__MODE", &mut self.coordinator.mode);
env_override_bool("SQE_COORDINATOR__DEBUG", &mut self.coordinator.debug);
```

It contains 129 `env_override_*` calls. The config model has about 338
public fields across 39 `*Config` structs in the same file, so roughly two
thirds of the documented `SQE_<SECTION>__<FIELD>` convention
(`config.rs:5337-5338`) does not work. The comment at `config.rs:5453-5457`
records the consequence: a mistyped `SQE_COORDINATOR__MEMORY_LIMIT` "silently
keeps the file value" and OOM-killed a 31 GB box, and the fix was to add two
more hand-written un-namespaced names (`SQE_MEMORY_LIMIT`, `SQE_MEMORY_POOL`).

Because the allowlist is incomplete, code has grown its own env reads that
bypass the config model and its validation:

- `crates/sqe-coordinator/src/credential_refresh.rs:31,39` read
  `SQE_COORDINATOR__CREDENTIAL_PUSH_CONNECT_TIMEOUT_SECS` and
  `..._REQUEST_TIMEOUT_SECS` directly, using the config naming convention
  without going through config.
- `crates/sqe-worker/src/executor.rs:191` reads `SQE_WORKER_ROW_GROUP_PRUNING`
  (single underscore, no section).
- `crates/sqe-catalog/src/scan_memory.rs:52` reads `SQE_CATALOG__SCAN_FETCH_AHEAD`
  via `TRACKING_ENV`.
- `crates/sqe-coordinator/src/mode.rs:58` reads `SQE_MODE`.
- `crates/sqe-worker/src/compaction.rs:639-663` reads seven `SQE_IT_*`
  variables in non-test code, three with `.expect(...)`.

The deployment side reflects the same split. `deploy/docker-compose.sqe.yml:22-26`
configures auth with the legacy flat names (`SQE_AUTH__KEYCLOAK_URL`,
`SQE_AUTH__REALM`, `SQE_AUTH__CLIENT_SECRET`) while the Helm chart only
passes secrets (`SQE_STORAGE__S3_*`, `SQE_WORKER__WORKER_SECRET`,
`SQE_AUTH__CLIENT_SECRET`) and relies on a mounted TOML for everything else.

## Why it matters

- Operators cannot know which env vars work without reading a 700-line
  function; the failure mode is silent (the file value wins).
- Values read outside `SqeConfig` skip `validate()`, skip `production_mode`
  checks and skip the redaction logic in `SecretString`, so a future secret
  read this way would be logged.
- Two knobs for the same thing (`SQE_COORDINATOR__PRODUCTION_MODE` and
  `SQE_PRODUCTION_MODE`, `config.rs:5408-5411`) is the start of a third
  naming scheme.

## Proposed change

1. Replace the hand-written function with a generic layered loader: parse
   the TOML to `toml::Value`, walk `SQE_<SECTION>__<FIELD>[__<SUBFIELD>]` env
   vars into the same tree (typed by the existing serde schema), then
   deserialize once. `figment` or `config-rs` both do this; a 60-line
   in-house walker is also fine given the flat two-level convention.
2. Keep the short operator aliases (`SQE_MEMORY_LIMIT`, `SQE_PRODUCTION_MODE`,
   `SQE_MODE`) as an explicit, documented alias table of at most ten entries.
3. Move the bypass reads into config fields: `coordinator.credential_push_connect_timeout_secs`,
   `worker.row_group_pruning`, `catalog.scan_fetch_ahead`. Delete the
   `SQE_IT_*` reads from `sqe-worker/src/compaction.rs` (they belong in the
   test that uses them).
4. Emit one `info!` line per applied override, redacting fields typed
   `SecretString`, so a startup log shows exactly which env vars took effect.
5. Add `sqe-server --print-config` that dumps the merged, redacted config.

## Acceptance criteria

- `grep -c env_override_ crates/sqe-core/src/config.rs` is below 15.
- `grep -rn 'env::var("SQE_' crates --include='*.rs' | grep -v /tests/ | grep -v config.rs`
  returns only the alias table.
- Test: set `SQE_QUERY__MAX_CONCURRENT_PER_USER=3` with a TOML that says 10;
  the loaded config reads 3. Set `SQE_QUERY__MAX_CONCURRENT_PER_USR=3`; startup
  fails with "unknown override".
- `deploy/docker-compose.sqe.yml` uses `[[auth.providers]]` names or is
  deleted (see 809).

## Related

- 805 (config model monolith).
- 809 (deployment artifact drift).
- June 2026 codebase analysis, High item 13 (dual auth config) still open:
  `config.rs:1872-1908` still has both `keycloak_url`/`realm` and `providers`.
- Overlaps 100 and 102 (sqe-core view); this file adds the bypass reads in other crates and the deploy-side naming split.
