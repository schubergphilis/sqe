# 100: Malformed `SQE_*` environment overrides are ignored with a warning, so security knobs silently keep their file value

- Scope: sqe-core (config)
- Severity: high
- Category: security
- Effort: S (hours)
- Status: open
- Issue: #543

## Problem

Every typed environment override in `SqeConfig::apply_env_overrides` parses the
variable and, on failure, logs a WARN and keeps the previous value. Startup then
continues as if the variable had not been set.

`crates/sqe-core/src/config.rs:6189-6197`:

```rust
fn env_override_bool(key: &str, target: &mut bool) {
    if let Ok(val) = std::env::var(key) {
        match val.to_lowercase().as_str() {
            "true" | "1" | "yes" => *target = true,
            "false" | "0" | "no" => *target = false,
            _ => tracing::warn!("{key}={val:?} is not a valid bool, ignoring"),
        }
    }
}
```

The same shape repeats for `env_override_u8/u16/u32/u64/f64/usize` at
`config.rs:6139-6209` and for `env_override_parse` at `config.rs:6120-6131`
("keeping previous value"). Note that `val` is not trimmed, so `"true "` (a
trailing space from a Helm template or a `.env` file) is rejected.

Among the knobs wired through these helpers (`config.rs:5396-5860`):

- `SQE_PRODUCTION_MODE` and `SQE_COORDINATOR__PRODUCTION_MODE` (`config.rs:5410-5414`), which gate `validate_production`.
- `SQE_COORDINATOR__ALLOW_UNAUTHENTICATED_WORKERS` (`config.rs:5419`).
- `SQE_CATALOG__REQUIRE_VENDED_CREDENTIALS` (`config.rs:5598`).
- `SQE_AUTH__SKIP_TLS_VERIFY`-style booleans and every port, timeout and memory-limit integer.

`SqeConfig::load` (`config.rs:5294-5302`) has no way to report these failures:
`apply_env_overrides` returns `()`. `validate()` runs later on the merged
struct and cannot see that an override was requested and dropped.

## Why it matters

A deployment that sets `SQE_PRODUCTION_MODE=yes ` or `SQE_PRODUCTION_MODE=True`
with a stray character boots in non-production mode. `validate_production`
never runs, so rate limiting, worker authentication and identity-collapse
checks that the operator believes are enforced are not. The only signal is one
WARN line in the startup log, which nobody reads when the process comes up
healthy. The same applies to memory limits: a malformed
`SQE_COORDINATOR__MEMORY_LIMIT` leaves the TOML default in place and the OOM
shows up hours later with no obvious cause.

Configuration that was explicitly requested and could not be applied must be a
startup failure, not a log line. This is the same fail-closed rule the code
already applies to TOML parse errors two lines earlier in `load`.

## Proposed change

1. Change the `env_override_*` helpers in `crates/sqe-core/src/config.rs` to
   return `Result<(), String>` (or push into an `errors: &mut Vec<String>`),
   trim the value before parsing, and have `apply_env_overrides` return
   `crate::error::Result<()>`.
2. Propagate from `SqeConfig::load`: a malformed override becomes
   `SqeError::Config("SQE_PRODUCTION_MODE=\"yes \" is not a valid bool")` and
   the process exits, exactly like a TOML syntax error.
3. Collapse the seven near-identical typed helpers into one generic
   `env_override_parse<T: FromStr>` plus a bool-specific one that accepts the
   documented spellings after `trim()`.
4. Keep `env_override_str` as is (any string is valid) but document that empty
   string is a valid override.

Consider the same treatment for `SortMode::parse`, `ProfileMode::parse` and the
other "unknown value falls back" enums; that is tracked separately in
`101-stringly-typed-mode-knobs-validated-at-use-site.md`.

## Acceptance criteria

- `cargo test -p sqe-core` includes tests that `SqeConfig::load` returns `Err`
  for `SQE_PRODUCTION_MODE=maybe`, `SQE_COORDINATOR__FLIGHT_SQL_PORT=abc`,
  `SQE_COORDINATOR__MEMORY_LIMIT=8GB ` (trailing space is accepted after trim,
  but `8 GBs` is rejected) and `SQE_CATALOG__DEFAULT_TABLE_FORMAT_VERSION=300`.
- A test that `SQE_PRODUCTION_MODE=" true "` sets `production_mode = true`.
- `grep -n 'ignoring\|keeping previous value' crates/sqe-core/src/config.rs`
  returns no hits in the env override helpers.
- Startup with a malformed override exits non-zero and names the variable.

## Related

- `102-hand-maintained-env-override-surface.md` (the surface these helpers
  implement).
- `101-stringly-typed-mode-knobs-validated-at-use-site.md`.
- Production readiness review 2026-07-25, "stricter startup validation".
