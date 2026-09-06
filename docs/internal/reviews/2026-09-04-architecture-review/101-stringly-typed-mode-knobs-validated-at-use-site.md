# 101: Mode knobs are `String` fields parsed at the use site with silent fallbacks; `validate()` never sees them

- Scope: sqe-core (config), sqe-coordinator consumers
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/config-startup-code-structure`, 2026-09-05)
- Issue: #544

## Resolution (2026-09-05)

Fixed on `fix/config-startup-code-structure`. `sort_mode`, `query_profile`,
`memory_pool`, `flight_compression` / `spill_compression` / `shuffle_compression`,
and `coordinator.mode` are enums deserialized with `snake_case`. Unknown values
fail at load. Silent `defaulting to` fallbacks are gone.


## Problem

`SqeConfig` stores enumerated settings as free-form strings and converts them
lazily, deep inside the coordinator, with an "unknown value means default"
rule. `validate()` (`crates/sqe-core/src/config.rs:4644-5152`) does not check
any of them; a grep of that range for `sort_mode`, `query_profile`,
`memory_pool`, `spill_compression`, `flight_compression`,
`shuffle_compression`, `coordinator.mode`, `worker.spill.backend`,
`audit.format`, `audit.export.target`, `openlineage.auth_mode` and
`maintenance.compaction.strategy` finds no validation for any of them.

Fields, all `pub ...: String`:

- `query.query_profile` `config.rs:221`, `query.sort_mode` `config.rs:260`
- `coordinator.mode` `config.rs:577`, `coordinator.memory_pool` `config.rs:626`
- `coordinator.spill_compression` `:636`, `flight_compression` `:640`, `shuffle_compression` `:644`
- `worker.spill.backend` `:1205`, `audit.format` `:3422`, `audit.export.target` `:3488`
- `openlineage.auth_mode` `:3613`, `maintenance.compaction.strategy` `:4165`

The parsers fall back silently:

`config.rs:76-95`:

```rust
impl SortMode {
    /// Parse from config string. Returns `Adaptive` for unknown values.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            ...
            _ => {
                tracing::warn!(sort_mode = s, "Unknown sort_mode, defaulting to partition_only");
                Self::PartitionOnly
            }
```

The doc comment says Adaptive, the code returns PartitionOnly, and the field's
own doc at `config.rs:257` says the default is `"adaptive"`. Three statements
about the same fallback, two of them wrong. `ProfileMode::parse`
(`config.rs:112-127`) and `CatalogDiscovery::parse` (`config.rs:143-148`)
follow the same pattern, and the memory-pool string is matched again in
`crates/sqe-coordinator/src/runtime.rs:64` ("Unknown coordinator.memory_pool,
defaulting to greedy").

By contrast, the newer enums (`McpAuthMode`, `MaintenanceMode`,
`CatalogDiscovery` when used via serde, `AccessControlBackend` with `FromStr`)
are typed and fail at deserialization. The codebase has both conventions.

## Why it matters

A typo such as `sort_mode = "adaptve"` or `memory_pool = "Fair"` passes
`validate()`, the server starts, and the operator runs with a different memory
or sort policy than they configured. The WARN is emitted once per session
context build, buried in the query log. For `sort_mode` the wrong fallback
strips ORDER BY on non-partition keys, which is a visible result difference
that will be filed as an engine bug. For `memory_pool` it changes OOM
behaviour under concurrency.

Every new consumer of these strings has to re-implement the parse and pick its
own fallback, which is how the doc/code disagreement above arose.

## Proposed change

1. Convert the listed fields to enums with `#[derive(Deserialize)]` and
   `#[serde(rename_all = "snake_case")]` (accept the existing aliases with
   `#[serde(alias = "partition-only")]`). `SortMode`, `ProfileMode` and
   `FlightCompression` already exist as enums; make them the field types.
2. For env overrides, route through `FromStr` so a bad value is a startup error
   (see `100-typed-env-overrides-fail-open.md`).
3. Delete the `parse(&str) -> Self` fallbacks, or keep them only behind a
   `TryFrom<&str>` that returns `Err`.
4. Add `#[serde(deny_unknown_fields)]` where the struct is fully owned by SQE so
   misspelled keys are caught too (see 102 for the caveat on `CatalogConfig`).

## Acceptance criteria

- `toml::from_str` of a config with `sort_mode = "adaptve"` or
  `memory_pool = "Fair"` returns an error naming the key and the accepted values.
- `grep -rn 'defaulting to' crates/sqe-core/src/config.rs crates/sqe-coordinator/src/runtime.rs`
  returns nothing.
- Existing configs under `quickstart/` and `helm/` still load (run the config
  load tests in `crates/sqe-core/src/config.rs` and `cargo test -p sqe-coordinator config`).
- The `SortMode` doc comment, field doc and default function agree.

## Related

- `100-typed-env-overrides-fail-open.md`
- `102-hand-maintained-env-override-surface.md`
- Codebase analysis 2026-06-26 item 16 "Config struct bloat" (still holds).
