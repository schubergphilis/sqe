# Benchmark Harness SP1: Run/Compare Core Hardening + Rust Split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `sqe-bench` read+compare core execute each query once, classify outcomes honestly (real-failure vs timeout vs vacuous), and split the two oversized files into focused modules — without changing any artifact schema.

**Architecture:** Introduce a single `execute::run_query` primitive and a `suite::run_suite` driver that runs each SQE query once and, when comparing, Trino alongside it, emitting both the report JSON and the compare JSON from one sweep. Extract `query`, `execute`, `status` modules from `test.rs` and a `compare/` module tree from `comparison.rs`. The legacy `run_benchmark_test`/`run_comparison`/`run_and_report` entry points become thin wrappers so the `test`/`compare`/`run` verbs keep working.

**Tech Stack:** Rust, tokio, arrow, serde, DataFusion-backed `BenchClient` trait (`client::BenchClient`).

## Global Constraints

- The `BENCH_SUMMARY:<bench>:<pass>:<fail>:<diff>:<skip>:<error>:<total>:<total_ms>` stdout line is UNCHANGED (8 colon-separated fields, same buckets). Shell parsers depend on it.
- The report JSON (`BenchmarkReport`/`Summary`/`QueryReportEntry`, status strings `pass|fail|diff|skip|error`) and compare JSON (`ComparisonReport`/`ComparisonSummary`/`QueryComparison`/`CompareStatusReport`) schemas are UNCHANGED.
- Exit policy: a run exits non-zero IFF at least one query outcome is `Error` or `WrongRows`. `Timeout`, `Vacuous`, `Diff`, `Skip` never fail the run.
- Per-query timeout resolution is unchanged: `BENCH_QUERY_TIMEOUT_SECS` env > positive `-- timeout: Ns` file header > 300s default.
- Each SQE query executes exactly once per suite run, even with `--compare-trino`.
- No file over ~300 lines when practical; each module has one responsibility.
- clippy `--all-targets -- -D warnings` clean.
- Branch: `refactor/bench-harness-unification` (already created).

---

## Target file structure

- `crates/sqe-bench/src/query.rs` (new) — `QueryFile`, `load_query_files`, `parse_query_file`, `normalize_query_id`, `prefix_tables`. Sole owner of query loading + table qualification.
- `crates/sqe-bench/src/status.rs` (new) — `QueryOutcome`, `classify_vs_expected`, `load_expected`, `compare_results`, `CompareStatus` (SQE-vs-expected), `legacy_bucket`, `is_real_failure`.
- `crates/sqe-bench/src/execute.rs` (new) — `QueryRun`, `run_query`.
- `crates/sqe-bench/src/suite.rs` (new) — `SuiteOutcome`, `run_suite` (single-pass driver).
- `crates/sqe-bench/src/test.rs` (shrinks) — re-exports + thin `run_benchmark_test`/`run_and_report` wrappers over `suite`.
- `crates/sqe-bench/src/compare/mod.rs`, `compare/classify.rs`, `compare/report.rs` (new tree, replaces `comparison.rs`) — compare driver arm, `CompareStatusReport` classification, `ComparisonReport` JSON writer.
- `crates/sqe-bench/src/report.rs` (modified) — map `QueryOutcome` → legacy 5 buckets.
- `crates/sqe-bench/src/run.rs` (modified) — call `suite::run_suite` once/suite; exit via `status::is_real_failure`.
- `crates/sqe-bench/src/lib.rs` (modified) — module declarations.

Task order below is dependency-safe: extract leaf modules first (no behavior change, tests keep passing), then introduce the taxonomy + primitive, then the single-pass driver, then rewire callers, then delete the old file.

---

### Task 1: Extract `query` module (mechanical move, no behavior change)

**Files:**
- Create: `crates/sqe-bench/src/query.rs`
- Modify: `crates/sqe-bench/src/test.rs`, `crates/sqe-bench/src/lib.rs`, `crates/sqe-bench/src/comparison.rs`

**Interfaces:**
- Produces: `query::QueryFile { id: String, requires: Vec<String>, timeout_secs: u64, sql: String }`; `query::load_query_files(benchmark: &str) -> anyhow::Result<Vec<QueryFile>>`; `query::parse_query_file(content: &str) -> (String, Vec<String>, u64, String)`; `query::normalize_query_id(id: &str) -> String`; `query::prefix_tables(sql: &str, namespace: &str, benchmark: &str) -> String`.

- [ ] **Step 1: Move the items verbatim.** Cut `QueryFile` (struct), `load_query_files`, `parse_query_file`, `normalize_query_id`, and `prefix_tables` (plus its private helpers, if any, and the `prefix_tables` unit tests currently in `test.rs`) out of `test.rs` into a new `query.rs`. Keep signatures byte-for-byte. Make all five `pub`. Preserve their existing doc comments and unit tests (move the `#[cfg(test)]` cases that reference only these functions).

- [ ] **Step 2: Declare the module.** In `lib.rs`, add `pub mod query;` (alphabetical with the others).

- [ ] **Step 3: Update references.** In `test.rs`, replace uses with `crate::query::{...}` (or `use crate::query;`). In `comparison.rs`, change `crate::test::prefix_tables` → `crate::query::prefix_tables`. Grep to confirm no other `crate::test::prefix_tables` / `::load_query_files` callers remain: `rg 'test::(prefix_tables|load_query_files|parse_query_file|normalize_query_id|QueryFile)' crates/sqe-bench`.

- [ ] **Step 4: Build + test.**

Run: `cargo test -p sqe-bench --lib query 2>&1 | tail -20`
Expected: the moved `prefix_tables` tests PASS; crate compiles.

- [ ] **Step 5: Commit.**

```bash
git add crates/sqe-bench/src/query.rs crates/sqe-bench/src/test.rs crates/sqe-bench/src/comparison.rs crates/sqe-bench/src/lib.rs
git commit -m "refactor(bench): extract query module (load/parse/prefix_tables) from test.rs"
```

---

### Task 2: Extract `status` module + introduce `QueryOutcome` taxonomy

**Files:**
- Create: `crates/sqe-bench/src/status.rs`
- Modify: `crates/sqe-bench/src/test.rs`, `crates/sqe-bench/src/lib.rs`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces:
  - `status::CompareStatus` — the SQE-vs-expected enum currently returned by `compare_results` (`Pass | Diff(String) | Fail(String)`), moved verbatim.
  - `status::compare_results(batches: &[RecordBatch], expected: &Expected, tol: f64) -> anyhow::Result<CompareStatus>` and `status::load_expected(benchmark: &str, scale: f64, id: &str) -> anyhow::Result<Option<Expected>>` — moved verbatim from `test.rs` (keep their existing `Expected` type; move it too).
  - `status::QueryOutcome { Pass, WrongRows(String), Error(String), Timeout(u64), Vacuous, Diff(String), Skip(String) }` (`#[derive(Debug, Clone, PartialEq)]`).
  - `status::classify_vs_expected(benchmark: &str, scale: f64, id: &str, rows: usize, batches: &[RecordBatch]) -> QueryOutcome` — wraps `load_expected`+`compare_results`: `Ok(Some(exp))` → map `CompareStatus::{Pass→Pass, Diff→Diff, Fail→WrongRows}`; `Ok(None)` → `Pass`; `Err(e)` → `Error(...)`.
  - `status::is_real_failure(o: &QueryOutcome) -> bool` — `matches!(o, QueryOutcome::Error(_) | QueryOutcome::WrongRows(_))`.
  - `status::legacy_bucket(o: &QueryOutcome) -> LegacyBucket` where `enum LegacyBucket { Pass, Fail, Diff, Skip, Error }`; mapping: `Pass|Vacuous → Pass`, `WrongRows → Fail`, `Diff → Diff`, `Skip → Skip`, `Error|Timeout → Error`.

- [ ] **Step 1: Move expected-comparison code.** Move `CompareStatus`, `compare_results`, `load_expected`, and their `Expected` type + private helpers + their unit tests from `test.rs` into `status.rs`, verbatim, all `pub`. In `lib.rs` add `pub mod status;`.

- [ ] **Step 2: Write the failing test for the taxonomy + predicates.** Append to `status.rs`:

```rust
#[cfg(test)]
mod outcome_tests {
    use super::*;

    #[test]
    fn is_real_failure_only_error_and_wrongrows() {
        assert!(is_real_failure(&QueryOutcome::Error("x".into())));
        assert!(is_real_failure(&QueryOutcome::WrongRows("x".into())));
        for o in [
            QueryOutcome::Pass,
            QueryOutcome::Timeout(60),
            QueryOutcome::Vacuous,
            QueryOutcome::Diff("x".into()),
            QueryOutcome::Skip("x".into()),
        ] {
            assert!(!is_real_failure(&o), "{o:?} must not be a real failure");
        }
    }

    #[test]
    fn legacy_bucket_maps_new_variants_onto_five() {
        assert_eq!(legacy_bucket(&QueryOutcome::Pass), LegacyBucket::Pass);
        assert_eq!(legacy_bucket(&QueryOutcome::Vacuous), LegacyBucket::Pass);
        assert_eq!(legacy_bucket(&QueryOutcome::WrongRows("x".into())), LegacyBucket::Fail);
        assert_eq!(legacy_bucket(&QueryOutcome::Diff("x".into())), LegacyBucket::Diff);
        assert_eq!(legacy_bucket(&QueryOutcome::Skip("x".into())), LegacyBucket::Skip);
        assert_eq!(legacy_bucket(&QueryOutcome::Error("x".into())), LegacyBucket::Error);
        assert_eq!(legacy_bucket(&QueryOutcome::Timeout(60)), LegacyBucket::Error);
    }
}
```

- [ ] **Step 2b: Run it to see it fail.** Run: `cargo test -p sqe-bench --lib outcome_tests 2>&1 | tail -15`. Expected: FAIL — `QueryOutcome`, `LegacyBucket`, `is_real_failure`, `legacy_bucket` not defined.

- [ ] **Step 3: Implement the taxonomy.** Add to `status.rs`:

```rust
/// Per-query outcome, richer than the legacy 5 report buckets so the human
/// summary and the run's exit code can treat timeouts and vacuous results
/// distinctly. Mapped back to the legacy buckets by `legacy_bucket` for the
/// stable `BENCH_SUMMARY` line and JSON schema.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutcome {
    Pass,
    WrongRows(String),
    Error(String),
    Timeout(u64),
    Vacuous,
    Diff(String),
    Skip(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyBucket { Pass, Fail, Diff, Skip, Error }

/// The run fails only on genuine correctness/execution failures. Timeouts and
/// vacuous results are surfaced but do not fail the run (see SP1 design).
pub fn is_real_failure(o: &QueryOutcome) -> bool {
    matches!(o, QueryOutcome::Error(_) | QueryOutcome::WrongRows(_))
}

/// Collapse a `QueryOutcome` onto the five legacy report buckets so the
/// `BENCH_SUMMARY` line and JSON `summary` stay byte-compatible. Timeout folds
/// into Error exactly as the pre-refactor code did (it built
/// `TestStatus::Error("Timed out ...")`); Vacuous folds into Pass.
pub fn legacy_bucket(o: &QueryOutcome) -> LegacyBucket {
    match o {
        QueryOutcome::Pass | QueryOutcome::Vacuous => LegacyBucket::Pass,
        QueryOutcome::WrongRows(_) => LegacyBucket::Fail,
        QueryOutcome::Diff(_) => LegacyBucket::Diff,
        QueryOutcome::Skip(_) => LegacyBucket::Skip,
        QueryOutcome::Error(_) | QueryOutcome::Timeout(_) => LegacyBucket::Error,
    }
}

/// Classify an executed SQE result against the expected-rows manifest.
/// Mirrors the pre-refactor logic in `run_benchmark_test`.
pub fn classify_vs_expected(
    benchmark: &str,
    scale: f64,
    id: &str,
    batches: &[arrow_array::RecordBatch],
) -> QueryOutcome {
    match load_expected(benchmark, scale, id) {
        Ok(Some(expected)) => match compare_results(batches, &expected, 1e-4) {
            Ok(CompareStatus::Pass) => QueryOutcome::Pass,
            Ok(CompareStatus::Diff(m)) => QueryOutcome::Diff(m),
            Ok(CompareStatus::Fail(m)) => QueryOutcome::WrongRows(m),
            Err(e) => QueryOutcome::Error(format!("compare error: {e}")),
        },
        Ok(None) => QueryOutcome::Pass,
        Err(e) => QueryOutcome::Error(format!("failed to load expected: {e}")),
    }
}
```

(Adjust the `RecordBatch` import path to match the crate's existing usage in `test.rs`.)

- [ ] **Step 4: Run to verify pass.** Run: `cargo test -p sqe-bench --lib status 2>&1 | tail -15`. Expected: PASS (moved `compare_results` tests + new `outcome_tests`).

- [ ] **Step 5: Commit.**

```bash
git add crates/sqe-bench/src/status.rs crates/sqe-bench/src/test.rs crates/sqe-bench/src/lib.rs
git commit -m "refactor(bench): extract status module + QueryOutcome taxonomy, exit predicate, legacy mapping"
```

---

### Task 3: `execute::run_query` primitive (single execution + timeout + retry)

**Files:**
- Create: `crates/sqe-bench/src/execute.rs`
- Modify: `crates/sqe-bench/src/lib.rs`

**Interfaces:**
- Consumes: `crate::client::BenchClient`, `crate::status::QueryOutcome`, `crate::compare::classify::is_transport_error` (moved in Task 5 — until then, inline a local copy; Task 5 dedups).
- Produces:
  - `execute::QueryRun { rows: usize, duration: std::time::Duration, result: Result<Vec<arrow_array::RecordBatch>, String>, timed_out_after: Option<u64> }`.
  - `execute::run_query(client: &dyn BenchClient, id: &str, sql: &str, timeout_secs: u64) -> QueryRun` — runs the query once with the existing `tokio::select!` timeout; on a transport-shaped error, retries once on a fresh connection (as `comparison.rs` does today); never panics.
  - `execute::resolve_timeout(query_timeout_secs: u64) -> u64` — `BENCH_QUERY_TIMEOUT_SECS` env > `query_timeout_secs` if >0 > 300.

- [ ] **Step 1: Write the failing test** (uses a stub client):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::BenchClient;
    use arrow_array::RecordBatch;

    struct Stub { delay_ms: u64, err: Option<String> }
    #[async_trait::async_trait]
    impl BenchClient for Stub {
        async fn execute(&self, _sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
            match &self.err { Some(e) => anyhow::bail!("{e}"), None => Ok(vec![]) }
        }
        async fn execute_update(&self, _sql: &str) -> anyhow::Result<()> { Ok(()) }
        fn protocol_name(&self) -> &str { "stub" }
    }

    #[tokio::test]
    async fn timeout_yields_timed_out_after() {
        let run = run_query(&Stub { delay_ms: 10_000, err: None }, "q", "SELECT 1", 1).await;
        assert_eq!(run.timed_out_after, Some(1));
        assert!(run.result.is_err());
    }

    #[test]
    fn resolve_timeout_precedence() {
        // header value used when env unset and header > 0
        assert_eq!(resolve_timeout(60), 60);
        // zero header -> default
        assert_eq!(resolve_timeout(0), 300);
    }
}
```

- [ ] **Step 2: Run to see it fail.** Run: `cargo test -p sqe-bench --lib execute 2>&1 | tail -15`. Expected: FAIL — `run_query`/`resolve_timeout`/`QueryRun` not defined.

- [ ] **Step 3: Implement.** Port the timeout `tokio::select!` block and the transport retry out of `run_benchmark_test`/`run_comparison` into `run_query`. `QueryRun.rows` = sum of `batch.num_rows()` on success, else 0. `resolve_timeout` copies the env>header>300 logic from Task-2 source. Add `pub mod execute;` to `lib.rs`.

- [ ] **Step 4: Run to verify pass.** Run: `cargo test -p sqe-bench --lib execute 2>&1 | tail -15`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add crates/sqe-bench/src/execute.rs crates/sqe-bench/src/lib.rs
git commit -m "refactor(bench): execute::run_query primitive (one execution, timeout, transport retry)"
```

---

### Task 4: Update `report.rs` to consume `QueryOutcome`

**Files:**
- Modify: `crates/sqe-bench/src/report.rs`

**Interfaces:**
- Consumes: `status::{QueryOutcome, LegacyBucket, legacy_bucket}`.
- Produces: `report::QueryResult { id: String, outcome: QueryOutcome, duration: std::time::Duration, rows: usize }` (rename of the old `test::QueryResult`; `status` field becomes `outcome: QueryOutcome`). `print_summary`/`write_json_report`/`count_results` signatures otherwise unchanged.

- [ ] **Step 1: Update the type + mapping.** Move `QueryResult` into `report.rs` (or a small `types.rs`), replacing `status: TestStatus` with `outcome: QueryOutcome`. Rewrite `count_results`, the `print_summary` match, and the `write_json_report` status-string match to go through `legacy_bucket`: `LegacyBucket::Pass→"pass"`, `Fail→"fail"`, `Diff→"diff"`, `Skip→"skip"`, `Error→"error"`. The `message` for JSON: `WrongRows(m)|Diff(m)|Skip(m)|Error(m)→Some(m)`, `Timeout(n)→Some(format!("Timed out after {n}s"))`, `Pass|Vacuous→None`.

- [ ] **Step 2: Add a human-summary line for the new buckets.** After the existing `Results:` line in `print_summary`, add a second line counting `timeout` and `vacuous` outcomes directly from `results` (not via `legacy_bucket`), e.g. `Extra: {timeout} timeout, {vacuous} vacuous (non-failing)`. Do NOT alter the `BENCH_SUMMARY:` line.

- [ ] **Step 3: Update `report.rs` unit tests.** Change `make_results()` to build `QueryOutcome` values; keep the `count_results_correct` and `write_json_report_creates_file` assertions (they check the legacy buckets/strings, which must still hold). Add one case asserting a `Timeout(60)` result serializes with `status == "error"` and a `Vacuous` result with `status == "pass"`.

- [ ] **Step 4: Run tests.** Run: `cargo test -p sqe-bench --lib report 2>&1 | tail -20`. Expected: PASS; `BENCH_SUMMARY` field order/count unchanged.

- [ ] **Step 5: Commit.**

```bash
git add crates/sqe-bench/src/report.rs
git commit -m "refactor(bench): report.rs maps QueryOutcome to stable legacy buckets + extra timeout/vacuous line"
```

---

### Task 5: Split `comparison.rs` into `compare/` (mechanical, no behavior change)

**Files:**
- Create: `crates/sqe-bench/src/compare/mod.rs`, `crates/sqe-bench/src/compare/classify.rs`, `crates/sqe-bench/src/compare/report.rs`
- Delete: `crates/sqe-bench/src/comparison.rs`
- Modify: `crates/sqe-bench/src/lib.rs`, `crates/sqe-bench/src/run.rs`, `crates/sqe-bench/src/main.rs` (any `comparison::` references)

**Interfaces:**
- Produces: `compare::run_comparison(...)` (same signature as today's `comparison::run_comparison`), `compare::classify::classify_status(...)` and `compare::classify::is_transport_error(&str) -> bool`, `compare::report::*` (re-export of `ComparisonReport` etc. from `crate::report`).

- [ ] **Step 1: Create the module tree.** Move `classify_status`, `is_transport_error`, `load_expected_rows`, and the `CompareStatus`-classification helpers into `compare/classify.rs`. Move `run_comparison` + the record/summary assembly into `compare/mod.rs`. Keep the `ComparisonReport`/`ComparisonSummary` JSON writer path in `compare/report.rs` (thin wrappers over the types that already live in `crate::report`). Move `comparison.rs`'s `#[cfg(test)]` tests next to the functions they cover.

- [ ] **Step 2: Rewire.** `lib.rs`: replace `pub mod comparison;` with `pub mod compare;`. Update `run.rs` and `main.rs`: `comparison::run_comparison` → `compare::run_comparison`. `rg 'comparison::' crates/sqe-bench` must return nothing after. `git rm crates/sqe-bench/src/comparison.rs`.

- [ ] **Step 3: Point `execute.rs` at the shared `is_transport_error`.** Replace the inlined copy from Task 3 with `crate::compare::classify::is_transport_error`.

- [ ] **Step 4: Build + test.** Run: `cargo test -p sqe-bench --lib compare 2>&1 | tail -20`. Expected: PASS; crate compiles; behavior identical.

- [ ] **Step 5: Commit.**

```bash
git add -A crates/sqe-bench/src/
git commit -m "refactor(bench): split comparison.rs into compare/{mod,classify,report}"
```

---

### Task 6: `suite::run_suite` single-pass driver

**Files:**
- Create: `crates/sqe-bench/src/suite.rs`
- Modify: `crates/sqe-bench/src/lib.rs`

**Interfaces:**
- Consumes: `query`, `execute::{run_query, resolve_timeout}`, `status::{QueryOutcome, classify_vs_expected}`, `report::QueryResult`, `compare::classify::classify_status`, `report::{ComparisonReport, QueryComparison, ...}`.
- Produces:
  - `suite::SuiteOutcome { results: Vec<report::QueryResult>, comparison: Option<report::ComparisonReport> }`.
  - `suite::run_suite(sqe: &dyn BenchClient, trino: Option<&dyn BenchClient>, benchmark: &str, scale: f64, query_filter: Option<&str>, catalog: Option<&str>, namespace_override: Option<&str>, sqe_endpoint: &str, trino_endpoint: Option<&str>) -> anyhow::Result<SuiteOutcome>`.

- [ ] **Step 1: Write a gated single-pass test.** Create `crates/sqe-bench/tests/single_pass_gated.rs` guarded by `BENCH_STACK_UP=1` (mirror `run_attach_gated.rs`'s skip pattern). It runs `run_suite` for `tpch` with a `--query q01` filter against the live coordinator + a Trino stub-or-real, and asserts the SQE client was invoked exactly once for the query (use a counting wrapper `BenchClient` that increments an `AtomicUsize` in `execute`). Assert `outcome.comparison.is_some()` and `outcome.results.len() == 1`.

- [ ] **Step 2: Run to see it fail/skip.** Run: `cargo test -p sqe-bench --test single_pass_gated 2>&1 | tail -15`. Expected: compiles; SKIPS cleanly without `BENCH_STACK_UP=1` (prints a skip notice, exits 0) — matching `run_attach_gated`.

- [ ] **Step 3: Implement `run_suite`.** Build the namespace exactly as `run_benchmark_test` does (tpcbb→tpcds; `catalog` prefix). Load queries once via `query::load_query_files`. For each query: honor `query_filter` (via `query::normalize_query_id`); `requires` non-empty → push `QueryOutcome::Skip`; else `sql = query::prefix_tables(...)`, `timeout = execute::resolve_timeout(query.timeout_secs)`, `sqe = execute::run_query(sqe, &query.id, &sql, timeout)`. Map `sqe` → `QueryOutcome`: `timed_out_after=Some(n)` → `Timeout(n)`; `result=Err(e)` → `Error(e)`; `result=Ok(batches)` → `classify_vs_expected(benchmark, scale, &query.id, &batches)`, but if `sqe.rows==0` and that classification is `Pass` with no expected file, use `Vacuous`. Push `report::QueryResult`. When `trino` is `Some`, skip DML (reuse the DML detection from `compare/mod.rs`), run `execute::run_query(trino, ...)` once, and build a `QueryComparison` via `compare::classify::classify_status(...)`. Assemble `Option<ComparisonReport>` only when `trino.is_some()`. Add `pub mod suite;`.

- [ ] **Step 4: Run gated test (if stack available).** Run: `BENCH_STACK_UP=1 RUST_MIN_STACK=67108864 cargo test -p sqe-bench --test single_pass_gated 2>&1 | tail -15`. Expected: PASS (SQE invoked once; comparison present). If no stack in this environment, confirm the SKIP path and note it.

- [ ] **Step 5: Commit.**

```bash
git add crates/sqe-bench/src/suite.rs crates/sqe-bench/src/lib.rs crates/sqe-bench/tests/single_pass_gated.rs
git commit -m "feat(bench): suite::run_suite single-pass driver (SQE once + optional Trino), gated test"
```

---

### Task 7: Rewire `test.rs` wrappers + `run.rs` onto `run_suite`

**Files:**
- Modify: `crates/sqe-bench/src/test.rs`, `crates/sqe-bench/src/run.rs`

**Interfaces:**
- Consumes: `suite::run_suite`, `status::is_real_failure`, `report::{print_summary, write_json_report}`.
- Produces: `test::run_benchmark_test(...) -> anyhow::Result<Vec<report::QueryResult>>` and `test::run_and_report(...)` unchanged in signature (thin wrappers).

- [ ] **Step 1: Reimplement the wrappers.** `test::run_benchmark_test` becomes: `let out = suite::run_suite(client, None, benchmark, scale, query_filter, catalog, namespace_override, "", None).await?; Ok(out.results)`. `test::run_and_report` calls it, then `report::print_summary` + `report::write_json_report` (unchanged). Delete the now-dead per-query loop body from `test.rs` (it moved into `suite`).

- [ ] **Step 2: Rewire `run.rs` to a single call per suite.** Replace the `run_and_report` + separate `run_comparison` block with one `suite::run_suite(bench_client.as_ref(), trino_client.as_deref(), suite, args.scale, args.query.as_deref(), Some("golden"), None, &endpoint, args.trino_endpoint.as_deref()).await?` call. Print the summary + write the report JSON from `out.results` (via `report::*`). If `out.comparison` is `Some`, write the compare JSON + print the existing `compare {suite}: ...` line. Set `any_failure |= out.results.iter().any(|r| status::is_real_failure(&r.outcome))`.

- [ ] **Step 3: Build the Trino client once (not per query).** In `run.rs`, when `args.compare_trino`, construct the `TrinoBenchClient` (with catalog + user, as landed in !657) once before the suite loop and pass `Some(&client)` into `run_suite`. Keep the `--compare-trino needs BENCH_TRINO_ENDPOINT` guard.

- [ ] **Step 4: Verify exit policy + no double-run.** Run: `cargo build -p sqe-bench 2>&1 | tail -5` (clean). Run the existing gated read test: `BENCH_STACK_UP=1 RUST_MIN_STACK=67108864 cargo test -p sqe-bench --test run_attach_gated 2>&1 | tail -15`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add crates/sqe-bench/src/test.rs crates/sqe-bench/src/run.rs
git commit -m "refactor(bench): run verb + test wrappers use single-pass run_suite; exit on real failures only"
```

---

### Task 8: End-to-end validation + clippy + docs knob

**Files:**
- Modify: `scripts/benchmark.sh` (doc comment only), `crates/sqe-bench/src/*` (clippy fixes if any)

- [ ] **Step 1: clippy.** Run: `cargo clippy -p sqe-bench --all-targets -- -D warnings 2>&1 | tail -20`. Expected: clean. Fix any warnings inline.

- [ ] **Step 2: Full unit suite.** Run: `cargo test -p sqe-bench --lib 2>&1 | tail -15`. Expected: all PASS.

- [ ] **Step 3: Live single-pass compare smoke (if stack up).** With the golden stack running: `BENCH_PROFILE=local BENCH_SCALE=1 BENCH_COMPARE=1 BENCH_QUERY=q01 scripts/benchmark.sh tpch`. Expected: q01 matches on both engines; `[bench] Running q01` appears exactly once (single pass); exit 0.

- [ ] **Step 4: Exit-policy smoke.** Run a suite known to contain a vacuous/canonically-empty query at SF1 (clickbench) without compare: `BENCH_PROFILE=local BENCH_SCALE=1 scripts/benchmark.sh clickbench`. Expected: exit 0 despite vacuous/timeout outcomes (no `Error`/`WrongRows`); the `Extra: N timeout, N vacuous` line appears; `BENCH_SUMMARY:` line still has 8 fields.

- [ ] **Step 5: Document the timeout knob.** In `scripts/benchmark.sh`'s header `# Env:` block, add `#   BENCH_QUERY_TIMEOUT_SECS  per-query timeout override (default: per-file header or 300s)`.

- [ ] **Step 6: Commit.**

```bash
git add scripts/benchmark.sh crates/sqe-bench/src
git commit -m "chore(bench): clippy clean + document BENCH_QUERY_TIMEOUT_SECS knob (SP1 complete)"
```

---

## Self-Review

- **Spec coverage:** single-pass (Tasks 6-7), taxonomy + exit policy (Task 2, Task 7 step 2/4), timeout applied once + documented (Tasks 3, 8), module split query/execute/status/suite/compare (Tasks 1-6), schema stability (Task 4 + Global Constraints), testing (unit in Tasks 2-4, gated single-pass in Task 6). Covered.
- **Placeholder scan:** relocation tasks name exact items and say "verbatim"; new code shown in full; no TBD/TODO.
- **Type consistency:** `QueryOutcome`/`LegacyBucket`/`is_real_failure`/`legacy_bucket` defined in Task 2 and consumed by report (4), suite (6), run (7); `QueryResult.outcome` rename introduced in Task 4 and consumed in 6-7; `run_suite` signature defined in Task 6 matches its call in Task 7.
- **Contract note (resolved):** the richer taxonomy maps back to the 5 legacy buckets via `legacy_bucket` (Timeout→error bucket for the `BENCH_SUMMARY` line, matching pre-refactor behavior, but non-failing per `is_real_failure`). This intentional line-vs-exit asymmetry is documented in Task 2's `legacy_bucket` doc comment and the design spec.
