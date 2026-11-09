# 2026-11-09 fix/test-stack-cargo-env

Open point from !1699: `cargo test -p sqe-coordinator --lib` aborted with a
stack overflow unless `RUST_MIN_STACK=33554432` was exported.

## What changed

- `.cargo/config.toml`: `[env] RUST_MIN_STACK = "33554432"`. `make test`
  and CI (`AIKIDO_RUST_MIN_STACK`) already ran unit tests with 32 MiB; a
  bare `cargo test` now does the same. An exported `RUST_MIN_STACK` still
  wins (`force` is off).
- Cause: `flight_sql::tests::do_get_ddl_kind_yields_schema_then_completes`
  (and `prepared_execution_reuses_stored_plan_instead_of_replanning`) drive
  the full GetFlightInfo/DoGet path in a debug build. A `#[tokio::test]`
  body runs on the 2 MiB test thread, where `on_planning_stack` falls back
  to the caller's stack (current-thread runtime, or `block_on` on the test
  thread). Reproduced with the float/decimal rule disabled, so not a
  regression of !1699.
- Production: unaffected. `build_server_runtime` sets an explicit thread
  stack (8 MiB default) and planning runs on the 64 MiB planning thread;
  cargo's `[env]` only reaches processes cargo itself starts.

## Tests

- `cargo test -p sqe-coordinator --lib` with no `RUST_MIN_STACK` in the
  shell -> 805 passed, 1 ignored (aborted before).

**NEXT:** typed read of shredded variants (#870).
