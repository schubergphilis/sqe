# Fix DataFusion 54 named TVF arguments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Restore `read_parquet`/`read_csv`/`read_json`/`read_delta` named arguments (`access_key => '...'`, `delimiter => ';'`, ...) under DataFusion 54, which broke them and blocks ALL benchmark data loads.

**Architecture:** DataFusion 54's default table-function planner (`datafusion-sql-54/src/relation/mod.rs:162-170`) only accepts `FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))` and then resolves that expr against an EMPTY schema. So under DF54: `name => value` (Named) fails with "Unsupported function argument type"; `name = value` (Unnamed binary expr) fails with "No field named name" (column validated against empty schema). Probe-confirmed both. The escape: rewrite named args into positional `'key=value'` STRING LITERALS (no Named, no column ref) in SQE's SQL layer, and teach `parse_file_tvf_args` to read them. Probe-confirmed a positional `'access_key=x'` literal reaches `parse_file_tvf_args`. This matches SQE's "wrap sqlparser, post-parse transform" strategy (precedent: `sqe-sql/src/trino_compat.rs` `VisitMut` rewrites of `TableFactor::Table`).

**Tech Stack:** Rust, DataFusion 54, sqlparser 0.62. Mirrors `crates/sqe-sql/src/trino_compat.rs` (VisitMut over `TableFactor`) and `crates/sqe-catalog/src/file_tvf_common.rs` (`parse_file_tvf_args`).

**Probe evidence (against the live DF54 SQE, all confirmed):**
- `read_parquet('p')` -> reaches TVF (normal path-disabled error). [baseline]
- `read_parquet('p', access_key => 'x')` -> `Unsupported function argument type: access_key => 'x'`. [DF54 rejects Named]
- `read_parquet('p', access_key = 'x')` -> `Schema error: No field named access_key`. [DF54 validates column vs empty schema]
- `read_parquet('p', 'access_key=x')` -> reaches TVF: `unexpected argument expression Utf8("access_key=x")`. [positional literal reaches parse_file_tvf_args -> the rewrite target works]

**Branch:** `fix/df54-named-tvf-args` off `feat/ranger-mask-vocabulary` (current HEAD), so the benchmark validates the full current code + this fix. The fix is independent of the policy work (clean diff in `sqe-sql` + `sqe-catalog`); the MR can target `main` or stack as decided at MR time.

**Gates before MR:** `cargo build --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all`. The previously-failing `sqe-cli::embedded::read_csv_tvf_custom_delimiter` MUST now PASS (it uses `delimiter => ';'`). Remaining pre-existing env-flaky failures: `sqe-auth oidc_m2m`, `sqe-coordinator channel_pool`.

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `crates/sqe-catalog/src/file_tvf_common.rs` | `parse_file_tvf_args`: accept positional `Literal("key=value")` args | Modify |
| `crates/sqe-sql/src/tvf_named_args.rs` | `rewrite_named_tvf_args(sql) -> String`: VisitMut Named/`=`-binary -> `'key=value'` literal for file TVFs | Create |
| `crates/sqe-sql/src/lib.rs` | `pub mod tvf_named_args; pub use ...` | Modify |
| `crates/sqe-coordinator/src/query_handler.rs` (+ write path) | apply the rewrite at the SQL-normalization chokepoint that the CTAS load also passes through | Modify |
| `crates/sqe-cli/...` (read_csv_tvf test) | confirm it passes (no change expected) | Verify |

---

## Task 1: Trace the CTAS load SQL path (decide the wiring chokepoint)

NO code change. This task de-risks coverage (per the failing case: the benchmark fails at a CTAS load whose inner `read_parquet(...)` must hit the rewrite).

- [ ] **Step 1:** Read `crates/sqe-coordinator/src/query_handler.rs` around the `execute_stream` entry (search `rewrite_trino_compat` at ~1728/1896) and how a `CREATE TABLE AS SELECT` is dispatched to `write_handler`. Read `crates/sqe-coordinator/src/write_handler.rs:600-630` (the `ctx.sql(&select_sql)` CTAS load) and trace where `select_sql` comes from: is it extracted from the ALREADY-rewritten AST/SQL, or re-derived from the original user SQL?
- [ ] **Step 2:** Read `crates/sqe-sql/src/pipeline_types.rs` `pre_parse_pipeline` and how `query_handler.rs:660-661` uses it. Determine whether a rewrite placed in `pre_parse_pipeline` (or right after `rewrite_trino_compat`) reaches the inner `read_parquet` of a CTAS by the time `write_handler` runs `ctx.sql(&select_sql)`.
- [ ] **Step 3:** Write findings as a comment block at the top of the new `tvf_named_args.rs` (created in Task 3) stating the exact chokepoint to wire (Task 4). DECISION RULE: prefer the single earliest point every statement passes through (likely alongside `rewrite_trino_compat`, or inside `pre_parse_pipeline`) such that the rewritten SQL is what reaches DataFusion for BOTH a plain `SELECT ... read_parquet(...)` AND a `CTAS ... AS SELECT ... read_parquet(...)`. If no single string-level chokepoint covers the CTAS sub-select (because `write_handler` re-extracts from original SQL), the chokepoint is `write_handler`'s `select_sql` construction too: wire it in BOTH places. Record which.

No commit (findings feed Task 3/4).

---

## Task 2: `parse_file_tvf_args` accepts positional `'key=value'` literals

**Files:** Modify `crates/sqe-catalog/src/file_tvf_common.rs`

- [ ] **Step 1 (TDD): tests first.** In the `#[cfg(test)]` module of `file_tvf_common.rs` (or add one), add tests that call `parse_file_tvf_args("read_parquet", &exprs, extra)` with `exprs` built from `datafusion_expr` literals:
```rust
    use datafusion_expr::{lit, Expr};
    fn lit_str(s: &str) -> Expr { lit(s) }

    #[test]
    fn positional_key_value_literal_parses_s3_creds() {
        let exprs = vec![
            lit_str("s3://bucket/data/*.parquet"),
            lit_str("access_key=AKIA123"),
            lit_str("secret_key=sekret"),
            lit_str("region=eu-west-1"),
        ];
        let args = parse_file_tvf_args("read_parquet", &exprs, |_, _| false).unwrap();
        assert_eq!(args.path, "s3://bucket/data/*.parquet");
        assert_eq!(args.access_key.as_deref(), Some("AKIA123"));
        assert_eq!(args.secret_key.as_deref(), Some("sekret"));
        assert_eq!(args.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn positional_value_may_contain_equals() {
        // base64 secrets contain '='; split on the FIRST '=' only.
        let exprs = vec![lit_str("s3://b/x.parquet"), lit_str("secret_key=ab==cd==")];
        let args = parse_file_tvf_args("read_parquet", &exprs, |_, _| false).unwrap();
        assert_eq!(args.secret_key.as_deref(), Some("ab==cd=="));
    }

    #[test]
    fn positional_unknown_key_goes_to_extra_callback() {
        // a non-S3 key (e.g. delimiter for read_csv) is offered to the `extra` closure.
        let mut seen = None;
        let exprs = vec![lit_str("/x.csv"), lit_str("delimiter=;")];
        let _ = parse_file_tvf_args("read_csv", &exprs, |k, v| {
            if k == "delimiter" { seen = Some(v.to_string()); true } else { false }
        });
        assert_eq!(seen.as_deref(), Some(";"));
    }
```
Run them first to confirm they FAIL (positional literal currently hits the `other` -> error branch).

- [ ] **Step 2: Implement.** In the `for expr in exprs.iter().skip(1)` loop, ADD a match arm for string literals BEFORE the existing `other => plan_err!` fallback. Read the existing `Expr::BinaryExpr` arm to reuse the exact key dispatch (`match name { "access_key" => ..., _ => extra(name, value) }`). The new arm:
```rust
            Expr::Literal(sv, _) => {
                let kv = scalar_to_str(sv).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "{fn_name}: positional argument must be a non-null 'key=value' string"
                    ))
                })?;
                let (name, value) = kv.split_once('=').ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "{fn_name}: positional argument '{kv}' must be 'key=value'"
                    ))
                })?;
                // dispatch `name`/`value` exactly as the BinaryExpr arm does
                // (factor the shared key->field assignment into a local closure
                // or helper so both arms call it).
            }
```
Factor the `match name { "access_key" => out.access_key = Some(value.into()), ... , _ => { if !extra(name, value) { return plan_err!("{fn_name}: unknown argument '{name}'") } } }` block out of the BinaryExpr arm into a shared helper used by both arms (DRY). Keep the BinaryExpr arm working (back-compat / programmatic callers).

- [ ] **Step 3:** Run `cargo test -p sqe-catalog file_tvf 2>&1 | tail -20` (new + existing pass) and `cargo clippy -p sqe-catalog --all-targets -- -D warnings 2>&1 | tail -5`.
- [ ] **Step 4: Commit.**
```bash
git add crates/sqe-catalog/src/file_tvf_common.rs
git commit -m "feat(catalog): parse_file_tvf_args accepts positional 'key=value' literals (DF54)"
```

---

## Task 3: `rewrite_named_tvf_args` AST rewrite in sqe-sql

**Files:** Create `crates/sqe-sql/src/tvf_named_args.rs`; modify `crates/sqe-sql/src/lib.rs`

- [ ] **Step 1: Study the precedent.** Read `crates/sqe-sql/src/trino_compat.rs` fully — it defines a `VisitorMut` over the statement AST, matches `TableFactor::Table { name, args, .. }`, and builds `FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(...)))`. Mirror its imports (`sqlparser::ast::{...}`), its `rewrite_*(sql: &str) -> String` shape (parse with the same parser sqe-sql uses, run `visit`, re-serialize via `stmt.to_string()`), and its handling of the parse-failure fallback (return the original SQL unchanged if parsing fails, so non-standard SQL still flows).

- [ ] **Step 2 (TDD): tests first.** Create `tvf_named_args.rs` with `pub fn rewrite_named_tvf_args(sql: &str) -> String { todo!() }` and tests:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rewrites_named_arrow_arg_to_positional_literal() {
        let out = rewrite_named_tvf_args(
            "SELECT * FROM read_parquet('s3://b/x.parquet', access_key => 'AKIA', region => 'eu')");
        // named args become positional 'key=value' string literals
        assert!(out.contains("'access_key=AKIA'"), "got: {out}");
        assert!(out.contains("'region=eu'"), "got: {out}");
        assert!(!out.contains("=>"), "no named args should remain: {out}");
        // the path arg is untouched
        assert!(out.contains("'s3://b/x.parquet'"));
    }
    #[test]
    fn rewrites_eq_binary_arg_to_positional_literal() {
        // `name = value` parses as an Unnamed binary expr; also normalize it.
        let out = rewrite_named_tvf_args(
            "SELECT * FROM read_csv('/x.csv', delimiter = ';')");
        assert!(out.contains("'delimiter=;'"), "got: {out}");
    }
    #[test]
    fn leaves_non_tvf_functions_untouched() {
        let sql = "SELECT count(*) FROM t WHERE foo(a => 1) > 0";
        // foo is not a file TVF; do not rewrite it.
        assert_eq!(rewrite_named_tvf_args(sql), sql_normalized(sql));
        // (If exact string match is brittle due to re-serialization, assert the
        // output still contains the foo(...) call unchanged in shape, or that
        // round-tripping an unrelated query is a no-op up to formatting.)
    }
    #[test]
    fn covers_ctas_inner_select() {
        let out = rewrite_named_tvf_args(
            "CREATE TABLE t AS SELECT * FROM read_parquet('s3://b/*.parquet', access_key => 'k')");
        assert!(out.contains("'access_key=k'"), "CTAS inner TVF must be rewritten: {out}");
    }
    #[test]
    fn unparseable_sql_returned_unchanged() {
        let weird = "THIS IS NOT SQL ;;;";
        assert_eq!(rewrite_named_tvf_args(weird), weird);
    }
}
```
Add a `sql_normalized` helper in the test if needed (parse+to_string the input) for the no-op comparison.

- [ ] **Step 3: Implement.** A `VisitorMut` whose `pre_visit_table_factor` (or `post_visit_table_factor` / the relevant sqlparser 0.62 visit hook — mirror trino_compat's hook) matches `TableFactor::Table { name, args: Some(TableFunctionArgs { args, .. }), .. }`. If the function name (lowercased, last ident) is in the file-TVF set `{"read_parquet","read_csv","read_json","read_delta"}` (CHECK whether `iceberg_metadata_tvf`/`read_iceberg_metadata` also take named args via grep of its `parse`; include it if so), transform each arg:
  - `FunctionArg::Named { name, arg: FunctionArgExpr::Expr(value_expr), .. }` -> build a string `"{name}={literal_value}"` and replace with `FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value("{name}={value}" as single-quoted string)))`. Extract the literal value from `value_expr` (it is an `Expr::Value(SingleQuotedString)` or number); render it as its raw string (no surrounding quotes) so the combined literal is `'name=value'`.
  - `FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::BinaryExpr { left: Identifier(name), op: Eq, right: Value(v) }))` -> same `'name=value'` positional literal.
  - Leave the first positional path arg and any other unnamed non-binary args unchanged.
Use the same `Value`/`Expr::Value` construction sqlparser 0.62 uses (check trino_compat.rs:379 for the exact `Expr::Value(Value::SingleQuotedString(...).into())` form for THIS sqlparser version). Re-serialize the whole statement with `to_string()`. On parse failure, return the input unchanged.

CAUTION (value fidelity): only handle value exprs that are scalar literals (`Expr::Value`) or numbers. If a named arg's value is something else (a column ref, expr), leave that single arg as-is (do not crash) and let downstream error — but for the known file-TVF args the values are always string/number literals.

- [ ] **Step 4:** Register in `crates/sqe-sql/src/lib.rs`: `pub mod tvf_named_args;` and `pub use tvf_named_args::rewrite_named_tvf_args;`. Run `cargo test -p sqe-sql tvf_named_args 2>&1 | tail -20` (all pass) + clippy.
- [ ] **Step 5: Commit.**
```bash
git add crates/sqe-sql/src/tvf_named_args.rs crates/sqe-sql/src/lib.rs
git commit -m "feat(sql): rewrite named TVF args to positional 'key=value' literals (DF54)"
```

---

## Task 4: Wire the rewrite into the query path (coverage)

**Files:** Modify `crates/sqe-coordinator/src/query_handler.rs` (and `write_handler.rs` if Task 1 found the CTAS sub-select needs it)

- [ ] **Step 1: Apply at the chokepoint(s) from Task 1.** Wherever `sqe_sql::rewrite_trino_compat(&sql)` is applied (query_handler.rs ~1728/1896), apply `sqe_sql::rewrite_named_tvf_args` to the SQL too (compose them: rewrite_trino_compat first or after — pick the order that round-trips cleanly; both are parse->rewrite->serialize so order is independent, but run named-arg rewrite on the output of the other). If Task 1 found `write_handler`'s `ctx.sql(&select_sql)` does NOT receive the rewritten SQL, also apply `rewrite_named_tvf_args` to `select_sql` (and any other `ctx.sql(&...)` site that can carry a user `read_parquet(...)`, e.g. the CTAS/MERGE/COPY load paths the benchmark uses).

- [ ] **Step 2: Build both binaries.** `cargo build -p sqe-coordinator --bins 2>&1 | tail -8` (clean). `cargo clippy -p sqe-coordinator --all-targets -- -D warnings 2>&1 | tail -8`.
- [ ] **Step 3: Commit.**
```bash
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/src/write_handler.rs
git commit -m "feat(coordinator): apply named-TVF-arg rewrite on the query + load paths"
```

---

## Task 5: Confirm the read_csv unit test + gates

**Files:** Verify `crates/sqe-cli/...` `read_csv_tvf_custom_delimiter`

- [ ] **Step 1:** Run the previously-failing test: `cargo test -p sqe-cli read_csv_tvf_custom_delimiter 2>&1 | tail -15`. It uses `delimiter => ';'`. If the test path goes through the rewrite, it now PASSES. If it does NOT (the embedded CLI may have its own SQL entry that bypasses the coordinator rewrite), determine the embedded CLI's SQL chokepoint and apply `rewrite_named_tvf_args` there too (grep `sqe-cli` for where it builds/executes SQL). Add a note; this is part of "the rewrite must cover every user-SQL entry point."
- [ ] **Step 2: Full gates.** `cargo build --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all 2>&1 | tail -25`. Confirm `read_csv_tvf_custom_delimiter` now passes and the ONLY remaining failures are the env-flaky `sqe-auth oidc_m2m` / `sqe-coordinator channel_pool` (network tests untouched here).
- [ ] **Step 3: Commit** any cli wiring change.
```bash
git add crates/sqe-cli/
git commit -m "fix(cli): route embedded SQL through named-TVF-arg rewrite"
```

---

## Task 6: Live benchmark validation (controller)

The stack is up; the controller does this after a rebuild (not a subagent).
- [ ] Rebuild the bench SQE and run `BENCH_SCALE=0.1 PROFILE=debug ./scripts/benchmark-test.sh` (all suites). Confirm data now LOADS (no "Unsupported function argument type") and queries run. Capture pass/fail per suite.
- [ ] Triage any genuine query failures (now reachable): report each with the query + error. Real query bugs are separate from this load fix.
- [ ] Per CLAUDE.md, commit the benchmark JSON result(s) under `benchmarks/results/` if a clean run is produced.

---

## Out of scope
- A custom DataFusion `RelationPlanner` (the universal alternative); not needed since the string-rewrite + positional-literal path is probe-confirmed and matches SQE's idiom. Revisit only if a SQL entry point proves un-coverable by the rewrite.
- Any query-engine perf work; this is a correctness/compat fix for data loading.
