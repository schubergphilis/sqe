# 611: Worker tests cover scan memory accounting and auth gates; nothing exercises registration, credential refresh, error propagation, or cancellation end to end

- Scope: sqe-worker (tests/, src/flight_service.rs tests), sqe-coordinator (tests/it)
- Severity: medium
- Category: testing
- Effort: M (days)
- Status: open
- Issue: #618

## Problem

The worker crate has three integration test files
(`crates/sqe-worker/tests/`): `hive_partition_materialize_test.rs`,
`slow_consumer.rs`, `zero_pruning_memory.rs`. All three drive
`execute_scan_streaming` or the Flight budget wrappers in-process. The unit
tests in `flight_service.rs:1527-2097` are thorough on one axis only: secret
and signature acceptance for `do_get`, `refresh_credentials` and
`compact_file_group`, plus three frame-budget tests. What is not tested
anywhere in the workspace:

- **Heartbeat and registration** against a real coordinator handler: the only
  heartbeat tests point at a closed port and assert "does not panic"
  (`crates/sqe-worker/src/heartbeat.rs:100-139`). Registry behaviour is tested
  with direct method calls, never through the Flight action.
- **Credential refresh end to end**: the coordinator's refresh tracker and the
  worker's `CredentialStore` are each unit-tested, but no test pushes a
  `refresh_credentials` action while a scan is mid-stream and asserts the next
  file open uses the new key. The `no_active_scan` response path is untested
  against a real fragment lifecycle.
- **Error propagation and retry**: no test sends a worker error of a specific
  kind and asserts the coordinator's registry state afterwards (see issue 602
  for why this matters). `TerminateOnErrorStream` has unit tests
  (`distributed_scan.rs:2320,2352`) but the retry loop at `:715-830` has none
  with a failing worker.
- **Client cancellation**: `AccountedEncodeStream::drop` and the credential
  cleanup guard are asserted only by reading the code. No test drops a
  `do_get` response mid-stream and checks that the scan budget returns to zero
  and the credential map entry is gone.
- **Compaction endpoint**: tests stop at "accepts correct secret and signature
  then fails downstream" (`flight_service.rs`, `compact_file_group_accepts_correct_secret_and_signature_then_fails_downstream`).
  The rewrite itself is covered only by a manual, env-gated test
  (`compaction.rs`, `SQE_IT_METADATA_LOCATION must be set`).
- **do_exchange**: no test at all outside the `ShuffleManager` unit test,
  which is consistent with issue 600 (nothing calls it).
- **Two-worker behaviour** lives only in `make test-distributed`, which is
  local-only (CLAUDE.md: integration tests have no CI equivalent since #387).

The June 2026 codebase analysis item 14 asked for "integration tests for
`sqe-worker`: fragment execution, shuffle data exchange, heartbeat, and
credential forwarding". Only fragment execution has landed.

## Why it matters

The worker boundary is where the security model (per-user credentials) and
the reliability model (retry, fallback) meet. Both have changed repeatedly
(issues #76, #206, #219, #220, #233, #407, #432 all touched this file) with
no harness that would catch a regression in the protocol, only in memory
accounting.

## Proposed change

Add `crates/sqe-worker/tests/protocol/` using the in-process pattern already
present in `tests/common/mod.rs`:

1. `heartbeat_roundtrip.rs`: start a minimal `FlightServiceServer` with the
   coordinator's `do_action_fallback` and a `WorkerRegistry`, run
   `start_heartbeat_task` against it, assert the worker appears healthy within
   two intervals, then stop the task and assert it ages out.
2. `credential_refresh_mid_scan.rs`: object store stub that records the
   credentials used per `get`; three-file ticket; publish new credentials after
   file one; assert files two and three used the new key.
3. `error_classification.rs`: worker stub returning `PermissionDenied`,
   `ResourceExhausted`, `Unavailable`; assert registry health and retry count
   per kind (lands with issue 602).
4. `cancel_mid_stream.rs`: consume two frames of a `do_get` response, drop it,
   assert `ByteBudget::used_bytes() == 0` and `CredentialStore` is empty.
5. Promote the manual compaction test to run against the local Polaris +
   RustFS stack under `make test-integration` with a `FILTER`.

## Acceptance criteria

- `cargo test -p sqe-worker --test protocol` runs in under 60 s with no
  external services.
- Each of the five scenarios above exists and passes.
- `docs/internal/reviews/2026-06-26-codebase-analysis.md` item 14 can be marked
  done.

## Related

- Issues 602, 608, 609 each list a test these files would host.
- Memory: "Local integration gate", "Which CI jobs to trust".
