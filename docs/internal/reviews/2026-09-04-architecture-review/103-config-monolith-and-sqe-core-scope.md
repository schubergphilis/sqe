# 103: `config.rs` is a 9,900-line file and `sqe-core` carries runtime and authorization logic

- Scope: sqe-core
- Severity: medium
- Category: architecture
- Effort: M (days)
- Status: partial (branch `fix/config-startup-code-structure`, 2026-09-05)
- Issue: #546

## Resolution (2026-09-05)

Partial on `fix/config-startup-code-structure`. `config.rs` is now
`crates/sqe-core/src/config/` (`mod.rs` + `env_overlay.rs`) and the tokio
runtime builder lives in `sqe-core::runtime`. Full split of 39 structs and
moving auth/runtime out of sqe-core is follow-up (overlaps 805).


## Problem

`crates/sqe-core/src/config.rs` is 9,904 lines, of which about 6,200 are
non-test code (the test module begins at `config.rs:6211`). The 2026-06-26
review measured the same file at 6,030 lines and recommended splitting it; it
has grown 64 percent since. It holds 40-plus structs and enums, roughly 130
`fn default_*` one-liners (`config.rs:4305-4547`), the validation
(`config.rs:4644-5292`), the env layer (`config.rs:5353-5857`), and three
pieces of behaviour that are not configuration:

- `TvfPolicy` and `TvfCaller` with `check_path`, `check_object_store`,
  `check_endpoint`, URL canonicalisation and `{user}` placeholder substitution
  (`config.rs:2499-2963`). This is the authorization gate for file TVFs
  (issue history: `read_parquet` reaching `/etc/shadow` and IMDS). It is
  called from `sqe-coordinator/src/session_context.rs:745,822` and
  `runtime_catalog.rs`.
- `SecurityConfig::resolve_client_ip` with forwarded-for parsing and loopback
  detection (`config.rs:3835-3928`).
- `WorkerMemoryConfig::resolve_bytes` and `ResolvedWorkerMemory`
  (`config.rs:1421-1492`), the memory budget arithmetic.

`sqe-core` itself is described in `CLAUDE.md` as "Shared types, config,
errors" but its `Cargo.toml` pulls `tokio`, `tokio-util`, `futures`, `sha2`,
`hex`, `zeroize`, `toml`, `serde_json`, `url`, `uuid` and `chrono`, and it
contains a 1,188-line `process_memory.rs` with a background
`tokio::spawn` watcher (`process_memory.rs:458-473`), a 707-line
`secret.rs` file store, and `supervised_task.rs`. Every one of the ten
downstream crates, including `sqe-sql` which only needs `SqeError`, compiles
all of it.

## Why it matters

The file is past the point where a reviewer can hold the invariants. Issues
100, 101 and 102 are all symptoms: they are three separate ways the same
function family fails open, and they went unnoticed because the function is
buried in 6,000 lines of struct definitions. `git blame` and MR review on this
file are slow, and merge conflicts on `config.rs` are a recurring cost for
any two branches that each add a knob.

Authorization logic in a config crate is the wrong layer: `TvfPolicy` has no
tests near the code that enforces it, and its callers have to construct a
`TvfCaller` from `sqe_core::config`, which couples session identity to the
configuration module.

## Proposed change

1. Turn `config.rs` into `config/` with one file per section (`coordinator.rs`,
   `worker.rs`, `auth.rs`, `catalog.rs`, `storage.rs`, `policy.rs`, `audit.rs`,
   `maintenance.rs`, `mcp.rs`), plus `validate.rs`, `env.rs`, `defaults.rs`.
   Pure move, no behaviour change, so it can land in one MR with
   `cargo test -p sqe-core` as the gate.
2. Move `TvfPolicy`/`TvfCaller` enforcement out of config. The struct with the
   serde fields stays; the `check_*` methods move to a `sqe-core::tvf_policy`
   module or, better, to `sqe-catalog` next to the TVF providers that call it.
3. Split `sqe-core` into a leaf `sqe-core` (types, `SqeError`, `SecretString`,
   `Session`, config structs; no tokio) and `sqe-runtime` (process memory,
   supervised tasks, secret file store). `sqe-sql` then depends on a crate with
   no async runtime.

## Acceptance criteria

- No file under `crates/sqe-core/src/config/` exceeds 1,500 lines.
- `cargo tree -p sqe-sql -e normal | grep tokio` is empty.
- `TvfPolicy::check_path` has unit tests in the module that owns it, covering
  the IMDS host and the absolute-path cases from the incident history.
- `cargo test --all` unchanged.

## Related

- Codebase analysis 2026-06-26, recommendation 2 "Split config.rs" (still
  open; reported here as a new file).
- `100`, `101`, `102` in this directory.
- `docs/internal/reviews/2026-07-25-production-readiness-review.md` names
  `config.rs` among the oversized modules.
