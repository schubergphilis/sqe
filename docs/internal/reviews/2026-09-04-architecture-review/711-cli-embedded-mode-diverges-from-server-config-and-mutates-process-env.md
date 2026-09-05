# 711: sqe-cli embedded mode re-derives engine configuration by hand and mutates process environment after the runtime starts

- Scope: sqe-cli (main.rs, embedded.rs)
- Severity: low
- Category: maintainability
- Effort: S (hours)
- Status: open
- Issue: #631

## Problem

Embedded mode builds its `SessionContext` by copying the coordinator's tuning flag by flag, `crates/sqe-cli/src/embedded.rs:265-284`:

```rust
// Same DataFusion tuning the cluster coordinator applies. See
// the comments in `sqe-coordinator/src/session_context.rs` for
// the rationale on each flag.
.set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
.set_str("datafusion.sql_parser.dialect", "DuckDB")
.set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 64 * 1024 * 1024)
...
```

The coordinator's list lives in a different crate and nothing ties the two together; a flag added to the coordinator (for example the Parquet view-type setting, or the `#363` list-files cache override which the CLI does carry at `:301-303`) is only in the CLI if someone remembers.

Configuration is also read from the environment in places the coordinator reads from `sqe.toml`:

- The masking key: `std::env::var("SQE_POLICY__MASK_KEY")`, `embedded.rs:316-319`. Without it the CLI silently registers plain SHA-256 for `sha256_udf`, so `--embedded` produces different masked values than the server for the same policy.
- AWS region: `std::env::set_var("AWS_REGION", r)`, `crates/sqe-cli/src/main.rs:268-270`, executed inside `#[tokio::main]` after the multi-threaded runtime is up. `set_var` is documented as unsound in multi-threaded programs on POSIX and is `unsafe` in Rust 2024; this is exactly the pattern the edition change targets.

Auth parity: `--token` is accepted only for Flight (`main.rs:333-343`), while the HTTP path takes only username and password, although the Trino server accepts bearer tokens (`server.rs:1080-1087`). The password prompt path uses `.unwrap()` on stdin and `.expect()` on `rpassword` (`main.rs:352-359`), which panics with a backtrace when stdin is not a TTY (CI, piped input) instead of printing a usage error.

## Why it matters

The embedded CLI is the quickstart and the tool used to reproduce bugs locally. When its session differs from the server's, "works in sqe-cli, fails in the coordinator" (or the reverse) becomes a recurring triage cost. The `set_var` after runtime start is a latent crash or misread that is very hard to attribute when it happens.

## Proposed change

1. Move the coordinator's `SessionConfig` construction into a shared function in `sqe-core` (or a small `sqe-session` module) that both `sqe-coordinator/src/session_context.rs` and `sqe-cli/src/embedded.rs` call, parameterized only by what legitimately differs (memory pool, object store registry, catalog list). Same for UDF registration.
2. Let embedded mode load the same `[policy]` and `[storage]` sections from an optional `sqe.toml` (`--config`), with env vars as override, so the mask key and S3 settings follow the same precedence as the server.
3. Pass the region into the Glue/S3 Tables builders explicitly instead of `set_var`; if the SDK needs env, set it before the runtime is created (build the runtime manually as `sqe_server.rs` already does).
4. Accept `--token` for the HTTP protocol (send `Authorization: Bearer`). Replace the `unwrap`/`expect` on credential prompts with an error that says stdin is not a terminal.

## Acceptance criteria

- `grep -n 'set_str\|set_bool\|set_usize' crates/sqe-cli/src/embedded.rs` returns nothing; the config comes from a shared constructor also used by the coordinator.
- `grep -n 'std::env::set_var' crates/sqe-cli/src` returns nothing.
- `sqe-cli --protocol http --token X -e 'SELECT 1'` works against a coordinator with bearer auth enabled.
- `echo | sqe-cli -e 'SELECT 1'` (no TTY, no credentials) exits non-zero with a one-line message and no panic.

## Related

- 700 (embedded mode is one of the three divergent SQL entry points).
- 100-range findings on `sqe-core` config.
- Memory: Glue sidecar bridge notes the server needs `SQE_STORAGE__*` not `AWS_*`; the CLI resolves both (`embedded.rs:157-215`), a third precedence scheme.
