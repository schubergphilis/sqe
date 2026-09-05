# 102: The `SQE_*` env override surface is a 460-line hand-written list; unknown TOML keys are accepted silently

- Scope: sqe-core (config)
- Severity: medium
- Category: maintainability
- Effort: M (days)
- Status: open
- Issue: #545

## Problem

`SqeConfig::apply_env_overrides` (`crates/sqe-core/src/config.rs:5396-5857`)
is one 460-line function that names every overridable field by hand, one call
per field:

```rust
env_override_u16("SQE_COORDINATOR__FLIGHT_SQL_PORT", &mut self.coordinator.flight_sql_port);
env_override_u16("SQE_COORDINATOR__TRINO_HTTP_PORT", &mut self.coordinator.trino_http_port);
env_override_str("SQE_COORDINATOR__MODE", &mut self.coordinator.mode);
```

Counts verified on the current file:

| Item | Count |
|---|---|
| `pub` fields across the config structs | 354 |
| distinct `"SQE_..."` env keys wired | 116 |
| `env_override_*` call sites | 129 |
| typed helper functions doing the same job | 8 (`config.rs:6103-6209`) |

So about two thirds of the configuration cannot be set from the environment,
and which two thirds is only discoverable by reading the function. Several
knobs have bespoke handling outside the helpers: `SQE_CATALOG__SCAN_FETCH_AHEAD`
(`config.rs:5619-5623`), the per-provider secret loop (`config.rs:5544-5572`),
the `SQE_MEMORY_POOL` short name (`config.rs:5461`), and the legacy
`SQE_CATALOG__POLARIS_URL` then `SQE_CATALOG__CATALOG_URL` ordering
(`config.rs:5589-5590`). The documented convention `SQE_<SECTION>__<FIELD>`
(`config.rs:5335`) is therefore not something the code guarantees.

On the TOML side, no config struct carries `#[serde(deny_unknown_fields)]`
(`grep -n deny_unknown_fields crates/sqe-core/src/config.rs` finds only a test
comment at `config.rs:7788`). A misspelled key such as `[coordinator]
prodution_mode = true` deserializes cleanly and is dropped.

## Why it matters

Every new field costs a second edit in `apply_env_overrides`, and the ones
that are forgotten become "you cannot set that in Helm" support tickets. The
Helm chart and the docs have to be kept in sync with a list that lives only in
this function. Reviewers cannot tell from a struct definition whether a field
is env-settable. The eight typed helpers are where the fail-open behaviour in
issue 100 lives, and there are eight copies of it to fix.

Silent acceptance of unknown TOML keys has the same failure mode as issue 100
from the other direction: the operator wrote the setting, the server ignored
it, nothing failed.

## Proposed change

1. Replace the hand list with a layered loader. Either the `config` or `figment`
   crate (TOML file, then `SQE_` prefixed env with `__` separator, then
   explicit aliases for the handful of legacy names) or a small derive-free
   walker that uses the serde field names. The mapping rule becomes a property
   of the struct, not a list.
2. Keep an explicit alias table for the non-conforming names that must keep
   working (`SQE_PRODUCTION_MODE`, `SQE_MEMORY_POOL`, `SQE_CATALOG__POLARIS_URL`,
   `SQE_AUTH__PROVIDERS__<N>__CLIENT_SECRET`) and test each alias.
3. Add `#[serde(deny_unknown_fields)]` to every struct SQE owns outright. For
   `CatalogConfig` keep the `polaris_url` alias (the existing test at
   `config.rs:7791` already guards it).
4. Emit a startup INFO line listing every env override that took effect
   (key only for secrets), so the effective configuration is auditable.

## Acceptance criteria

- `apply_env_overrides` is gone or under 50 lines of alias handling.
- A test that sets `SQE_QUERY__SORT_MODE`, `SQE_STORAGE__S3_REGION` and one
  field that today has no override (pick any from the 238 uncovered) and
  asserts all three land.
- A test that a TOML with an unknown key under `[coordinator]` fails to load
  and names the key.
- `docs/site/book` configuration page regenerated from the struct (or at least
  re-checked) so it lists the same keys the loader accepts.

## Related

- `100-typed-env-overrides-fail-open.md`
- `101-stringly-typed-mode-knobs-validated-at-use-site.md`
- `103-config-monolith-and-sqe-core-scope.md`
- Overlaps `802-hand-maintained-env-override-allowlist.md` (cross-cutting view); fix once.
