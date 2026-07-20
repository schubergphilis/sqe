# Benchmark harness unification — design

Date: 2026-07-20
Status: SP1 approved for implementation; SP2-SP4 are context.

## Motivation

The benchmark test infrastructure has grown into ~13 `benchmark-*.sh` scripts
plus a `sqe-bench` binary with overlapping logic. The largest scripts
(`benchmark-test.sh` 940 lines, `benchmark-matrix.sh` 810) and Rust files
(`test.rs` 715, `comparison.rs` 547) are hard to reason about, the unified
`benchmark.sh` only covers the read+compare path, and the `run` verb runs
each SQE query twice when comparing. This blocks a clean sf0.1 -> sf10 scaling
campaign.

Goal (full unification): `benchmark.sh` is the single infra entry point;
`sqe-bench` owns all benchmark logic; `benchmark-test.sh` and dead scripts are
retired; CI runs the unified harness.

This is too large for one plan. It decomposes into four sub-projects, each
with its own spec -> plan -> implementation cycle.

## Decomposition

- **SP1 — Run/compare core hardening + Rust split** (this spec, approved).
  Pure quality/robustness of the read+compare core. No behavior migration.
- **SP2 — Write-path verbs (`provision` + `reset`).** Move generate+load into
  `sqe-bench provision`; `reset` restores write-suite tables to baseline using
  the `rollback_to_snapshot` procedure landed in !659.
- **SP3 — Script retirement + CI cutover.** Fold `benchmark-test.sh`
  capabilities (bloom A/B, attach, external warehouse) into `benchmark.sh`
  flags; move CI onto it; delete `benchmark-test.sh` + dead scripts
  (`matrix`/`split`/`mor-vs-cow`); update docs.
- **SP4 — Scale readiness.** Scale-aware knobs (Trino memory, timeouts,
  memory limits), stale-coordinator/port guards, then the sf0.1 validation
  and sf10 runs.

Order: SP1 -> SP2 -> SP3 -> SP4. SP1 is the foundation the rest build on.

## SP1 design

### Problem recap

In `run.rs`, per suite when `--compare-trino`:
1. `test::run_and_report` executes every SQE query, classifies vs expected,
   writes the report JSON.
2. `comparison::run_comparison` re-loads the query files, re-executes every
   SQE query **and** Trino, writes the compare JSON.

SQE queries execute twice. `run.rs` bails when any result is `Fail|Error`, and
`Error` folds in timeouts, so a vacuous or timed-out query exit-1s the run
(observed: clickbench 2 vacuous + a 60s timeout failed an otherwise-clean run).

### 1. Single-pass compare

One per-suite driver executes each query against SQE exactly once, and against
Trino alongside it when comparing. Both artifacts (report JSON, compare JSON)
derive from the single sweep.

```
run_suite(sqe, trino: Option<..>, suite, scale, opts) -> SuiteOutcome
  queries = query::load_query_files(suite)            // once
  for q in queries:
    sql   = query::prefix_tables(q.sql, ns, suite)
    sqe_r = execute::run_query(sqe, &sql, timeout(q))  // the only SQE execution
    let cmp = match &trino {
      Some(t) => Some(compare::classify(&sqe_r, &execute::run_query(t, &sql, timeout(q)), canonical)),
      None => None,
    };
    records.push(Record { status: status::classify(&sqe_r, expected), cmp });
  report::write_json_report(suite, scale, &records)
  if trino.is_some(): compare::write_report(suite, scale, &records)
  SuiteOutcome { records }
```

`execute::run_query(client, sql, timeout) -> QueryRun { rows, duration, outcome }`
is the single place a query runs (SQE or Trino), carrying the existing
`tokio::select!` timeout. DML skipping (compare only) stays in the compare
classification path, not the SQE path.

### 2. Status taxonomy + exit policy

Replace the current `TestStatus { Pass, Fail, Diff, Skip, Error }` (timeouts
folded into `Error`, no vacuous marker) with:

```
enum QueryOutcome {
    Pass,             // rows match expected/canonical
    WrongRows(String),// real correctness failure vs expected/canonical
    Error(String),    // query errored (plan/exec/transport after retry)
    Timeout(u64),     // exceeded the per-query timeout
    Vacuous,          // 0 rows where that is acceptable (informational)
    Diff(String),     // canonical-acceptable divergence (e.g. q28 regexp)
    Skip(String),     // requires-unmet / DML-in-compare
}
```

Exit policy (approved: **real failures only**): the run exits non-zero iff any
query is `Error` or `WrongRows`. `Timeout`, `Vacuous`, `Diff`, `Skip` are
counted and surfaced prominently in the summary but do not fail the run.
Rationale: at sf10 a timeout is a tuning signal, not a correctness bug; SP4 can
add a stricter opt-in later if needed.

The summary line reports each bucket count so timeouts/vacuous are never
silently hidden.

### 3. Timeout (minor)

The per-query timeout already resolves as `BENCH_QUERY_TIMEOUT_SECS` env >
`-- timeout: Ns` file header > 300s default. SP1 keeps this, applies it in the
single `execute::run_query` (so it is applied once, not per pass), and surfaces
`BENCH_QUERY_TIMEOUT_SECS` as a documented `benchmark.sh` env knob.

### 4. Module split

`sqe-bench/src/test.rs` (715) splits into:
- `query.rs` — `QueryFile`, `load_query_files`, `parse_query_file`,
  `prefix_tables` (the single owner; `comparison` stops reaching into `test`).
- `execute.rs` — `run_query` primitive + timeout resolution.
- `status.rs` — `QueryOutcome` + classify-vs-expected + expected-rows loader.
- `suite.rs` (or slimmed `test.rs`) — the per-suite driver above.

`sqe-bench/src/comparison.rs` (547) splits into `compare/`:
- `compare/mod.rs` — the compare classification entry + `run_suite` compare arm.
- `compare/classify.rs` — `CompareStatus`, match/row-diff/vacuous/dialect-diff.
- `compare/report.rs` — `ComparisonReport` + JSON writer.

No file over ~300 lines; each module has one purpose and is unit-testable in
isolation.

### 5. Error handling

- Transport-shaped SQE errors keep the existing single retry on a fresh
  connection (moved into `execute::run_query`).
- Trino submit/poll errors classify as `Error` on the compare side (as today);
  a Trino crash mid-suite does not abort the SQE sweep.
- Timeout returns `QueryOutcome::Timeout(secs)`, distinct from `Error`.

### 6. Testing

- Unit: `status::classify` for each `QueryOutcome` variant (synthetic
  rows/expected); `compare::classify` for match / row-diff / vacuous /
  dialect-diff / one-side-error.
- Unit: exit-policy predicate (only Error/WrongRows -> fail).
- Gated (`run_attach_gated` style, `BENCH_STACK_UP=1`): a single-pass assertion
  that a compared suite executes each SQE query once (query-count or
  instrumentation), and that report JSON + compare JSON are both emitted from
  one sweep.
- Existing `run_attach_gated` stays green; artifact shapes (BENCH_SUMMARY line,
  JSON schema) are unchanged for backward compatibility with committed results
  and `render-benchmark-charts.py`.

### Non-goals (SP1)

- No write-path verbs (SP2). No script retirement / CI cutover (SP3). No
  scale-knob or sf0.1/sf10 runs (SP4). No change to the JSON artifact schema
  or the `BENCH_SUMMARY` contract.

## Success criteria

- A compared suite executes each SQE query exactly once (verified).
- A run with only vacuous/timeout/diff outcomes exits zero; a run with an
  Error or WrongRows exits non-zero.
- `test.rs` and `comparison.rs` are replaced by focused modules, none over
  ~300 lines, with `prefix_tables`/query-loading owned once.
- `BENCH_SUMMARY` line and report/compare JSON schemas unchanged.
- clippy `-D warnings` clean; unit tests + `run_attach_gated` green.

## Rollback

SP1 is internal refactor of `sqe-bench`; revert the branch to restore the
prior double-pass behavior. Artifact schemas are unchanged, so committed
benchmark results and chart tooling are unaffected either way.
