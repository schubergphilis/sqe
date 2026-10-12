**STATUS 2026-09-07: distributed worker reliability and test infrastructure batch (#658) implemented on `fix/distributed-worker-reliability-p1`; 74 of 109 combined findings closed.**

Branch `fix/distributed-worker-reliability-p1` (work package #658) closes
109, 313, 402, 513, 602, 609, 611, 705 and 706, and lands the Trino half
of 707 and item 5 of 807: `ScanTask` versions are derived by a builder
and validated on both ends (`decode_for_worker` refuses under-declared
or unknown fields); `try_distribute` is a decision plus a fallible action
with `query.distribution_fallback = error | local` (default `error`),
every exit records `scheduler_decisions{decision,reason}`, and local
decisions are visible as `CoordinatorLocalScanExec`; the fragment retry
drives the stream, so a worker lost before its first batch is re-dispatched
and one lost mid-stream fails the query while still penalising the worker;
worker failures carry a typed `WorkerFault` (`x-sqe-fault` metadata plus
gRPC code) classified where errors are still typed, and only
resource-exhausted, internal and transport faults mark a worker unhealthy
(user faults such as an expired token fail fast and leave the registry
alone); the cleanup sentinel is gone; the heartbeat is a `WorkerHello`
(instance id, build, scan-task version, capacity, failures since last)
answered by a `CoordinatorHello`, version skew is rejected with
`FailedPrecondition`, a restarted instance clears its credential-refresh
state, capacity is exported as `sqe_worker_reported_*`, and re-admission
waits for a clean interval or a quiet period; `crates/sqe-worker/tests/protocol`
covers heartbeat round-trip, credential refresh mid-scan, fault
classification over real tonic, and cancel-mid-stream in under a second;
the maintenance sqlite rig is shared as `test_support::sqlite_rig` and
16 committing write tests run under `--features test-sqlite` (CI runs them
via `--all-features`); sqe-catalog gained wiremock tests for every
`load_table` branch, the statistics path over a real fake manifest list,
information_schema row and call counts, a `Catalog` trait mock for the
non-REST handle and a two-worker concurrency test; one `sqe-trino-protocol`
crate holds the wire types plus the shared Trino HTTP and Flight clients
used by sqe-cli and sqe-bench (no server-crate dependency, faithful
decimal and integer widths, unbounded pagination, foreign `nextUri`
rejected); sqe-bench declares its modules once and its verdict and CTAS
logic are integration-tested; the bench sets `X-Trino-Schema` instead of
rewriting SQL, with a tokenizer-based qualifier only behind
`--qualify-tables`, and three TPC-DS/TPC-E queries got their canonical
aliases back.
Partial: 707 on Flight still qualifies because the coordinator's Flight
path ignores `USE` (a real engine gap: `Session::default_schema` is never
set over Flight); `sqe-bench compare` was not run (no stack); axum still
reaches the binaries through tonic's `server` feature and workspace-hack;
411 (`make test-distributed` failure cases) and 807 (ignore vocabulary,
`xtask test-groups`, CI group job) are deferred to a follow-up MR; the
worker compaction live test skips until a coordinator-side FILTER case
creates its table; EXPLAIN still omits `CoordinatorLocalScanExec` because
`explain.rs` does not run `try_distribute`. New finding from the rig: CoW
MERGE reports `rows_affected` as rows written (the rewritten target), not
rows the clauses touched; the tests pin the current behaviour.
**NEXT:** follow-up MR for 411 + 807, then the leftovers: 806 (#638),
205/212 (#653), 610 (#657), 627 (#627), the #659 partials, and the Flight
`USE`/default-schema gap. Build-time lever A2 (sccache to S3) before the
next multi-agent batch.
