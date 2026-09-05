# 808: Correctness depends on thread stack size, and the three runtimes pick three different sizes

- Scope: sqe-coordinator (`main.rs`, `bin/sqe_server.rs`), sqe-worker (`main.rs`), Makefile, scripts
- Severity: medium
- Category: reliability
- Effort: M
- Status: open

## Problem

The write path and the policy rewriter overflow a default thread stack, and
the repo compensates with per-entry-point stack settings that do not agree:

| Context | Stack | Where |
|---|---|---|
| `sqe-server` runtime threads | 8 MiB | `crates/sqe-coordinator/src/bin/sqe_server.rs:762-766` |
| `sqe-coordinator` runtime threads | 8 MiB | `crates/sqe-coordinator/src/main.rs:110-114` |
| `sqe-worker` runtime threads | 2 MiB (tokio default) | `crates/sqe-worker/src/main.rs:7` `#[tokio::main]` |
| `make test-integration` | 32 MiB | `Makefile:242` `IT_RUST_MIN_STACK ?= 33554432` |
| `scripts/integration-test.sh` default | 8 MiB | `scripts/integration-test.sh:77` |
| access-control / spark e2e | 32 MiB | `scripts/access-control-test.sh:137`, `scripts/spark-access-control-test.sh:162` |
| CI Aikido test job | 32 MiB | `.gitlab-ci.yml:59` |

The Makefile comment says why: write e2e suites "SIGABRT below 32 MiB"
(`Makefile:238-241`). `crates/sqe-coordinator/tests/it/insert_overwrite_e2e.rs:11-14`
says the same. `scripts/integration-test.sh:45-48` records that the 2 MiB
default "overflows in debug builds on the CTAS + policy-rewriter path".

The only guards are release-only tests (`prod_stack_4k..32k`, skipped in the
default debug run per `scripts/integration-test.sh:58-60`) and the manual
stack constants. Nothing asserts that the code paths fit the 8 MiB production
stack, and the standalone worker does not even get the 8 MiB.

## Why it matters

- A stack overflow in Rust is a process abort, not an error: one deep
  `IN (...)` list or one MERGE with a wide policy rewrite takes down the
  coordinator and every in-flight query on it.
- The debug-vs-release frame-size difference means the tests cannot tell
  you whether production is safe; a path that needs 20 MiB in debug might
  need 6 MiB or 9 MiB in release and no test distinguishes those.
- Workers run under compose with a quarter of the stack that Helm workers
  get (see 801), so a query is reproducible in one environment and a crash
  in the other.
- Deep async state machines that need tens of megabytes are a design
  signal: recursive plan visitors (`plan.apply` closures in
  `query_handler.rs:6875`, `audit_resources.rs:47`, `dim_build_swap.rs:75,87`)
  and the recursive policy rewrite are being run on the request thread.

## Proposed change

1. One runtime builder in sqe-core (`runtime::build_server_runtime(cfg)`) used
   by all three binaries, with `thread_stack_size` from
   `coordinator.runtime_stack_mib` (default 8) so the value is visible in
   config and equal everywhere.
2. Run the recursive phases on a dedicated large-stack thread instead of
   the runtime thread: wrap the policy rewrite and the write-path plan
   construction in `stacker::maybe_grow` or run them under
   `spawn_blocking` on a `std::thread::Builder::stack_size(64 MiB)` pool.
   This bounds the requirement to those phases and lets the runtime threads
   return to the tokio default.
3. Add a release-profile test that runs the deepest known shapes (the
   existing `prod_stack_*` guards plus one CTAS with a 5-level nested MERGE
   policy) with `RUST_MIN_STACK` unset and the runtime builder at the
   production value; make it part of the required job, not `--ignored`.
4. Drop `RUST_MIN_STACK` from Makefile, scripts and CI once the tests pass
   without it.

## Acceptance criteria

- `grep -rn RUST_MIN_STACK Makefile scripts .gitlab-ci.yml crates` returns 0 hits.
- `sqe-worker` and `sqe-server --mode worker` build their runtime from the same function.
- The release-profile deep-shape test passes with the default test-thread stack.
- A debug-build `make test-integration` completes without setting a stack override.

## Related

- 801 (entry-point drift).
- 807 (test strategy).
- Memory: Write e2e RUST_MIN_STACK; sqlite-gated coordinator test.
