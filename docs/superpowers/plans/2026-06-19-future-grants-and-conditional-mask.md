# FUTURE grants + conditional-mask verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GRANT/REVOKE ... ON FUTURE TABLES IN SCHEMA <x>` support (translated to a Ranger schema-wide table wildcard) and verify + document that a CUSTOM column mask can reference sibling columns of the same row.

**Architecture:** Part 1 extends the existing GRANT-to-Ranger translation (`extract_grant_statement`) with the already-parsed `GrantObjects::FutureTablesInSchema` variant, emitting `table = "*"` which the existing Ranger write path (`build_resource_map`) already turns into a wildcard table resource. Part 2 is verification-only: SQE's `parse_sql_predicate` already collects every bare identifier in a mask expression into the stub schema, so a CUSTOM mask referencing siblings already resolves against the real scan schema via `normalize_col`; we add tests proving it and document the capability plus its bare-name limitation.

**Tech Stack:** Rust, sqlparser 0.62 (GenericDialect, `GrantObjects::FutureTablesInSchema` already parsed), DataFusion 54 (`logical_expr::builder::table_scan`, `normalize_col`), `InMemoryPolicyStore` for the end-to-end rewriter test.

**Branch:** `feat/future-grants-and-conditional-mask` (already created off `origin/main`).

**Key semantic decision (approved):** Ranger's `table:*` wildcard matches existing AND future tables; Ranger cannot express Snowflake's future-only semantics. So in SQE `ON FUTURE TABLES IN SCHEMA x` becomes the same schema-wide wildcard as a hypothetical `ON ALL TABLES`: it covers every table in the schema, present and future. This is documented, not faked.

**Process constraints (from CLAUDE.md):**
- NEVER `git add -A`. Stage only the explicit files each task names (untracked benchmark JSONs and pre-existing dirty PDFs must stay unstaged).
- Run `cargo clippy --all-targets --all-features -- -D warnings` and `cargo test -p <crate>` before each commit.
- Docs must pass the forbidden-character gate: `grep -rn '—' <file>` returns zero hits in prose; no endash, no Unicode arrows; use `->` only in code blocks.

---

### Task 1: Translate `FUTURE TABLES IN SCHEMA` to a Ranger table wildcard

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs:3322` (add match arm in `extract_grant_statement`)
- Test: `crates/sqe-coordinator/src/query_handler.rs` (test module near line 4943, alongside `extract_grant_statement_basic_table`)

- [ ] **Step 1: Write the failing tests**

Add these two tests next to the existing `extract_grant_statement_*` tests (around line 4985):

```rust
    #[test]
    fn extract_grant_statement_future_tables_in_schema() {
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // FUTURE TABLES maps to a schema-wide table wildcard ("*"): it covers
        // existing and future tables, since Ranger cannot express future-only.
        let sql = "GRANT SELECT ON FUTURE TABLES IN SCHEMA my_catalog.sales TO ROLE analyst";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "SELECT");
        assert_eq!(stmt.catalog.as_deref(), Some("my_catalog"));
        assert_eq!(stmt.namespace.as_deref(), Some("sales"));
        assert_eq!(stmt.table.as_deref(), Some("*"));
        assert!(matches!(stmt.grantee, Grantee::Role(ref n) if n == "analyst"));
    }

    #[test]
    fn extract_grant_statement_future_tables_single_part_schema() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // One-part schema name: namespace only, catalog absent.
        let sql = "GRANT SELECT ON FUTURE TABLES IN SCHEMA sales TO alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.catalog, None);
        assert_eq!(stmt.namespace.as_deref(), Some("sales"));
        assert_eq!(stmt.table.as_deref(), Some("*"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-coordinator extract_grant_statement_future -- --nocapture`
Expected: FAIL. The current `_ => (None, None, None)` fallback arm matches `FutureTablesInSchema`, so `namespace` and `table` will be `None`, not `Some("sales")` / `Some("*")`.

- [ ] **Step 3: Add the translation match arm**

In `extract_grant_statement`, immediately after the `AllTablesInSchema` arm (it ends at `crates/sqe-coordinator/src/query_handler.rs:3332`), insert:

```rust
            Some(sqlparser::ast::GrantObjects::FutureTablesInSchema { schemas })
                if !schemas.is_empty() =>
            {
                // Ranger has no "future-only" resource; a table wildcard ("*")
                // covers existing and future tables in the schema. We document
                // this as the SQE meaning of ON FUTURE TABLES.
                let name = &schemas[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), Some("*".to_string())),
                    2 => (
                        Some(parts[0].clone()),
                        Some(parts[1].clone()),
                        Some("*".to_string()),
                    ),
                    _ => (None, Some(name.to_string()), Some("*".to_string())),
                }
            }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sqe-coordinator extract_grant_statement -- --nocapture`
Expected: PASS (both new tests and all existing `extract_grant_statement_*` tests).

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`
Expected: no warnings.

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat(grants): translate GRANT ON FUTURE TABLES IN SCHEMA to Ranger table wildcard"
```

---

### Task 2: Verify the Ranger resource map for a FUTURE grant is a table wildcard

**Files:**
- Test: `crates/sqe-policy/src/grants/ranger.rs` (test module; `build_resource_map` is `pub` and `ResourceLevel` is in scope)

This is a verification test: `build_resource_map` already emits `{"table": "*"}` when `table = Some("*")` at `ResourceLevel::Table`. Lock that behavior so a future refactor cannot silently drop the wildcard.

- [ ] **Step 1: Confirm the test module and `ResourceLevel` import**

Run: `grep -n 'mod tests\|use super\|ResourceLevel\|fn build_resource_map' crates/sqe-policy/src/grants/ranger.rs | head`
Expected: a `#[cfg(test)] mod tests` block and a `ResourceLevel` enum reachable from it. If `ResourceLevel` is defined in a sibling module, add the matching `use` inside the test (mirror how existing tests in this file import it).

- [ ] **Step 2: Write the test**

Add to the `tests` module in `crates/sqe-policy/src/grants/ranger.rs`:

```rust
    #[test]
    fn build_resource_map_future_tables_emits_table_wildcard() {
        // A FUTURE grant arrives as table = Some("*"). At table level the
        // resource map must carry an explicit "table": "*" so Ranger applies
        // the policy to every (existing and future) table in the namespace.
        let m = build_resource_map(
            "",                 // no realm root
            "sales_wh",
            Some("sales"),
            Some("*"),
            ResourceLevel::Table,
        );
        assert_eq!(m.get("catalog").map(String::as_str), Some("sales_wh"));
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
        assert_eq!(m.get("table").map(String::as_str), Some("*"));
    }
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p sqe-policy build_resource_map_future_tables -- --nocapture`
Expected: PASS.

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p sqe-policy --all-targets -- -D warnings`
Expected: no warnings.

```bash
git add crates/sqe-policy/src/grants/ranger.rs
git commit -m "test(grants): lock Ranger table-wildcard resource map for FUTURE grants"
```

---

### Task 3: Prove a CUSTOM mask expression can reference sibling columns (parse level)

**Files:**
- Test: `crates/sqe-policy/src/policy_expr.rs` (test module starting near line 137)

`parse_sql_predicate` collects every bare identifier via `IdentCollector` into the stub schema (`policy_expr.rs:60-85`). A CASE referencing a sibling column must therefore parse into an `Expr` that references both columns.

- [ ] **Step 1: Confirm test helpers in the module**

Run: `sed -n '120,160p' crates/sqe-policy/src/policy_expr.rs`
Expected: a `#[cfg(test)] mod tests` with a way to build a `SessionIdentity` (e.g. a `default_identity()` helper or `SessionIdentity { .. }` literal). Reuse whatever the neighboring tests use to construct the identity.

- [ ] **Step 2: Write the test**

Add to the `tests` module in `crates/sqe-policy/src/policy_expr.rs`. Use the same identity-construction the neighboring tests use; the snippet below assumes a `SessionIdentity` literal (adjust to a local helper if one exists):

```rust
    #[test]
    fn custom_mask_can_reference_sibling_column() {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::logical_expr::Expr;

        let identity = SessionIdentity {
            username: "bob".to_string(),
            roles: vec![],
            database: "db".to_string(),
            schema: "sales".to_string(),
        };

        // Mask `salary` based on the sibling `department` column.
        let expr = parse_sql_predicate(
            "CASE WHEN department = 'HR' THEN salary ELSE '0' END",
            &identity,
        )
        .expect("sibling-referencing CASE mask must parse");

        // Both column names must appear as referenced columns in the parsed expr.
        let mut seen: Vec<String> = Vec::new();
        expr.apply(|e| {
            if let Expr::Column(c) = e {
                seen.push(c.name.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert!(
            seen.iter().any(|n| n == "department"),
            "sibling column `department` must be in scope, saw {seen:?}"
        );
        assert!(
            seen.iter().any(|n| n == "salary"),
            "masked column `salary` must be referenced, saw {seen:?}"
        );
    }
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p sqe-policy custom_mask_can_reference_sibling_column -- --nocapture`
Expected: PASS. If `SessionIdentity` fields differ, fix the literal to match the actual struct (check `crates/sqe-policy/src/session_udf.rs`).

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p sqe-policy --all-targets -- -D warnings`
Expected: no warnings.

```bash
git add crates/sqe-policy/src/policy_expr.rs
git commit -m "test(policy): prove CUSTOM mask expr can reference sibling columns (parse level)"
```

---

### Task 4: Prove a sibling-referencing CUSTOM mask resolves end-to-end through the rewriter

**Files:**
- Test: `crates/sqe-policy/src/plan_rewriter.rs` (test module; uses `InMemoryPolicyStore`, `table_scan`, `parse_sql_predicate`)

This is the strongest proof: build a real `TableScan` with `department` + `salary`, register a `MaskType::Custom` mask on `salary` that references `department`, run the full `evaluate`, and assert the rewritten plan builds. A successful build means DataFusion's `normalize_col` resolved the sibling reference against the real scan schema. If siblings were out of scope, `builder.project()` would error.

- [ ] **Step 1: Confirm imports available in the test module**

Run: `sed -n '520,566p' crates/sqe-policy/src/plan_rewriter.rs`
Expected: the test module's existing `use` lines. You will additionally need (add inside the test fn or module as needed):
- `use crate::policy_store::InMemoryPolicyStore;`
- `use crate::{ResolvedPolicy, MaskType, PolicyEnforcer};`
- `use crate::policy_expr::parse_sql_predicate;` (already imported at module top, line 27 — reuse)
- `use crate::session_udf::SessionIdentity;`
- `use datafusion::logical_expr::builder::table_scan;`
- `use datafusion::arrow::datatypes::{DataType, Field, Schema};`
- `use datafusion::common::TableReference;`
- `use sqe_core::SessionUser;`
- `use std::sync::Arc;`

- [ ] **Step 2: Write the test**

Add to the `tests` module in `crates/sqe-policy/src/plan_rewriter.rs`:

```rust
    #[tokio::test]
    async fn custom_mask_referencing_sibling_resolves_end_to_end() {
        use crate::policy_store::InMemoryPolicyStore;
        use crate::session_udf::SessionIdentity;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::TableReference;
        use datafusion::logical_expr::builder::table_scan;
        use sqe_core::SessionUser;

        // Real scan schema with the masked column and a sibling it references.
        let schema = Schema::new(vec![
            Field::new("salary", DataType::Utf8, true),
            Field::new("department", DataType::Utf8, true),
        ]);
        // namespace "hr", table "employees" so resolve_policy_key matches the
        // InMemoryPolicyStore key "hr.employees".
        let scan = table_scan(
            Some(TableReference::partial("hr", "employees")),
            &schema,
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        // CUSTOM mask on `salary` that reads the sibling `department`.
        let identity = SessionIdentity {
            username: "bob".to_string(),
            roles: vec![],
            database: "db".to_string(),
            schema: "hr".to_string(),
        };
        let mask_expr = parse_sql_predicate(
            "CASE WHEN department = 'HR' THEN salary ELSE '0' END",
            &identity,
        )
        .unwrap();

        let mut policy = ResolvedPolicy::default();
        policy
            .column_masks
            .insert("salary".to_string(), MaskType::Custom(mask_expr));

        let store = InMemoryPolicyStore::new();
        store.add_table_policy("hr", "employees", policy).await;

        let rewriter = PolicyPlanRewriter::new(Arc::new(store));
        let user = SessionUser {
            username: "bob".to_string(),
            roles: vec![],
            ..Default::default()
        };

        // The rewrite must succeed: a failure here means the sibling column did
        // not resolve against the scan schema during builder.project().
        let rewritten = rewriter
            .evaluate(&user, scan)
            .await
            .expect("rewrite with sibling-referencing CUSTOM mask must succeed");

        // The masked projection references both columns; confirm the plan still
        // exposes `salary` and the rendered plan mentions `department`.
        let rendered = format!("{}", rewritten.display_indent());
        assert!(
            rendered.contains("department"),
            "rewritten plan must reference the sibling column, got:\n{rendered}"
        );
    }
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p sqe-policy custom_mask_referencing_sibling_resolves_end_to_end -- --nocapture`
Expected: PASS.

Likely fixups (do whichever the compiler/runtime demands, do not change the assertion intent):
- If `SessionUser` has no `Default`, construct it with all required fields explicitly (check `crates/sqe-core/src/session.rs:62`).
- If `display_indent()` is not the right method, use `format!("{rewritten:?}")` or `rewritten.display_indent_schema()` to get a string containing column names.
- If `table_scan`'s signature differs, check `datafusion::logical_expr::builder::table_scan` (verified present in datafusion-expr 54 at `logical_plan/builder.rs:2061`).

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p sqe-policy --all-targets -- -D warnings`
Expected: no warnings.

```bash
git add crates/sqe-policy/src/plan_rewriter.rs
git commit -m "test(policy): prove sibling-referencing CUSTOM mask resolves end-to-end"
```

---

### Task 5: Document FUTURE grants and conditional masking

**Files:**
- Modify: `docs/ranger-access-control.md` (add a FUTURE grants subsection to the GRANT section)
- Modify: `docs/ranger-fine-grained-enforcement.md` (add a "Masking on the value of another column" subsection)
- Modify: `nextsteps.md` (mark these two items done)
- Modify: `README.md` (roadmap checklist, if a governance line exists)

- [ ] **Step 1: Document FUTURE grants in `docs/ranger-access-control.md`**

Find the GRANT section (search for `GRANT` and `ALL TABLES` / object-grant examples). Add a subsection. Use `->` only inside code blocks; no emdash/endash/arrows in prose:

```markdown
### Future tables in a schema

`GRANT SELECT ON FUTURE TABLES IN SCHEMA sales_wh.sales TO ROLE analyst` grants
the privilege across every table in the namespace. SQE translates it to a Ranger
policy with a table wildcard (`table = "*"`). New tables created later in
`sales` are covered automatically, with no follow-up grant.

One difference from Snowflake: Snowflake's FUTURE grant applies only to objects
created after the grant. Ranger has no future-only resource, so SQE's wildcard
also covers tables that already exist in the schema. The grant means "every
table in this schema, present and future." Use a table-specific grant when you
need to scope to a single existing table.
```

- [ ] **Step 2: Document conditional masking in `docs/ranger-fine-grained-enforcement.md`**

Find the CUSTOM mask section (search for `CUSTOM` / `valueExpr` / `{col}`). Add:

```markdown
### Masking on the value of another column

A CUSTOM mask is an arbitrary SQL expression, and it can reference other columns
of the same row, not only the column being masked. The Ranger `valueExpr` uses
`{col}` for the masked column; any other bare column name resolves against the
table's scan schema.

Example: mask `salary` only for rows outside the HR department.

```sql
-- Ranger CUSTOM mask valueExpr on column `salary`:
CASE WHEN department = 'HR' THEN {col} ELSE '0' END
```

Limitation: only bare column names resolve. A qualified reference such as
`t.department` fails to parse, and SQE fails closed by restricting the column
(it is dropped from the result, not returned raw). Reference siblings by their
bare name.
```

- [ ] **Step 3: Update `nextsteps.md` and `README.md`**

In `nextsteps.md`, mark FUTURE grants and conditional masking as done (match the file's existing checklist style). In `README.md`, tick the corresponding roadmap line if one exists; otherwise leave it.

- [ ] **Step 4: Run the forbidden-character gate**

Run:
```bash
grep -rnP '[\x{2014}\x{2013}\x{2192}\x{2190}\x{25B6}]' docs/ranger-access-control.md docs/ranger-fine-grained-enforcement.md
```
Expected: zero hits. Also eyeball for forbidden words (delve, leverage, utilize, facilitate, comprehensive, robust, "it's worth noting", etc.). Fix any.

- [ ] **Step 5: Commit**

```bash
git add docs/ranger-access-control.md docs/ranger-fine-grained-enforcement.md nextsteps.md README.md
git commit -m "docs(ranger): document FUTURE grants and conditional (sibling-column) masking"
```

---

## Self-Review

**Spec coverage:**
- FUTURE grant parse + translate -> Task 1. Ranger wildcard resource -> Task 2. Conditional-mask parse-level proof -> Task 3. Conditional-mask end-to-end proof -> Task 4. Documentation (both features) -> Task 5. Covered.

**Placeholder scan:** No TBD/TODO. Every code step has complete code. Fixup notes in Task 4 Step 3 are explicit, bounded, and do not change assertion intent.

**Type consistency:** `extract_grant_statement` returns the `(catalog, namespace, table)` tuple used everywhere (verified `query_handler.rs:3298-3334`). `build_resource_map(realm, catalog, namespace, table, level)` signature matches `ranger.rs:118`. `MaskType::Custom(Expr)`, `ResolvedPolicy.column_masks`, `InMemoryPolicyStore::add_table_policy(namespace, table, policy)`, `PolicyPlanRewriter::new(Arc<dyn PolicyStore>)`, and `PolicyEnforcer::evaluate(&self, &SessionUser, LogicalPlan)` all verified against source. `SessionIdentity` fields (`username`, `roles`, `database`, `schema`) match the summary; Task 3/4 steps instruct verifying against `session_udf.rs` if they differ.

**Out of scope (not in this plan):** `ALTER TABLE SET TAGS` DDL, Iceberg-to-Ranger tag sync, role activation. These are separate branches.
