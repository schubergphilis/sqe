# ALTER TABLE SET TAGS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ALTER TABLE ... SET TAGS / UNSET TAGS` (SQE-native) and `ALTER TABLE ... MODIFY|ALTER COLUMN <col> SET TAG / UNSET TAG` (Snowflake-compatible) DDL that authors column->tag-label associations in the Iceberg `sqe.column-tags` table property, replacing the hand-written-JSON `SET TBLPROPERTIES` footgun.

**Architecture:** A hand-rolled pre-parser in `sqe-sql` (mirroring `try_parse_ref_ddl`) lowers all four syntaxes to one internal `SetTagsStatement { table, ops }`. The classifier intercepts it before sqlparser. A coordinator handler loads the table, reads the current `sqe.column-tags` map, applies merge semantics via a pure `apply_tag_ops` function, and commits one `TableUpdate::SetProperties`, reusing the exact commit + cache-invalidation path already used by `SET TBLPROPERTIES`.

**Tech Stack:** Rust. sqlparser 0.62 (no native TAG-column variant, hence pre-parse). iceberg-rust `TableUpdate::SetProperties`. serde_json for the `{"col":["tag",...]}` shape.

**Branch:** `feat/alter-table-set-tags` (already created off `origin/main`).

**Decisions (approved by user):**
- Both syntaxes supported, lowered to one internal node.
- Per-column **merge** semantics: `SET` only touches named columns (union tags, dedup); `UNSET TAGS (col)` removes all tags on a column; Snowflake `UNSET TAG name` removes that one label.
- Snowflake `SET TAG name = 'value'`: the **value is ignored**, the tag name becomes the label.

**Semantic guardrails:**
- `CREATE TAG` / `DROP TAG` (Iceberg snapshot refs, `ddl.rs`) are unrelated and parsed first; the new forms use `SET TAGS`/`UNSET TAGS`/`SET TAG`-after-`MODIFY|ALTER COLUMN`, which never collide.
- The parser returns `Ok(None)` for any `ALTER TABLE` it does not recognize (e.g. `SET TBLPROPERTIES`, `ADD COLUMN`, `ALTER COLUMN ... TYPE`), so existing paths are untouched. It returns `Err` only when a clearly-tag statement is malformed.

**Process constraints (CLAUDE.md):**
- NEVER `git add -A`; stage only the files each task names.
- `cargo clippy -p <crate> --all-targets -- -D warnings` and `cargo test -p <crate>` green before each commit.
- Docs pass the forbidden-character gate (no emdash/endash/Unicode arrows; `->` only in code blocks).

---

### Task 1: Internal AST + pre-parser in `sqe-sql`

**Files:**
- Create: `crates/sqe-sql/src/tags.rs`
- Modify: `crates/sqe-sql/src/lib.rs` (add `pub mod tags;`)

**Grammar (case-insensitive keywords; identifiers may be dotted/quoted):**
```
ALTER TABLE <table> SET TAGS ( <col> = ( <tag> [, <tag>]* ) [, <col> = (...)]* )
ALTER TABLE <table> UNSET TAGS ( <col> [, <col>]* )
ALTER TABLE <table> (MODIFY|ALTER) COLUMN <col> SET TAG <name> [= <val>] [, <name> [= <val>]]*
ALTER TABLE <table> (MODIFY|ALTER) COLUMN <col> UNSET TAG <name> [, <name>]*
```
`<tag>`/`<name>` may be a quoted string `'PII'` or a bare identifier `PII`. `<val>` is accepted and discarded.

- [ ] **Step 1: Write the AST + the full failing test suite**

Create `crates/sqe-sql/src/tags.rs` with the AST and a `#[cfg(test)] mod tests` containing the cases below. The TESTS ARE THE CONTRACT for this parser; implement the parser (Step 3) to make them pass.

```rust
//! Column-tag authoring DDL: `ALTER TABLE ... SET TAGS / UNSET TAGS` (SQE-native)
//! and the Snowflake-compatible `MODIFY|ALTER COLUMN <col> SET TAG / UNSET TAG`
//! forms. These author column->tag-label associations stored in the
//! `sqe.column-tags` table property. They are DISTINCT from Iceberg snapshot
//! tags (`CREATE TAG` / `DROP TAG`, see `ddl.rs`).

use sqe_core::{Result, SqeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagAction {
    Set,
    /// Remove the listed tags; an empty tag list removes ALL tags on the column.
    Unset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTagOp {
    pub column: String,
    pub tags: Vec<String>,
    pub action: TagAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetTagsStatement {
    pub table: String,
    pub ops: Vec<ColumnTagOp>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> SetTagsStatement {
        try_parse_set_tags(sql)
            .expect("parse must not error")
            .expect("must recognize as SET TAGS")
    }

    #[test]
    fn native_set_single_column_multi_tag() {
        let s = parse("ALTER TABLE sales.orders SET TAGS (email = ('PII','GDPR'))");
        assert_eq!(s.table, "sales.orders");
        assert_eq!(s.ops.len(), 1);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
        assert_eq!(s.ops[0].action, TagAction::Set);
    }

    #[test]
    fn native_set_multi_column() {
        let s = parse("ALTER TABLE t SET TAGS (email = ('PII'), salary = ('PII','CONF'))");
        assert_eq!(s.ops.len(), 2);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII"]);
        assert_eq!(s.ops[1].column, "salary");
        assert_eq!(s.ops[1].tags, vec!["PII", "CONF"]);
    }

    #[test]
    fn native_set_bare_identifier_tags() {
        // Tags may be bare identifiers, not just quoted strings.
        let s = parse("ALTER TABLE t SET TAGS (email = (PII, GDPR))");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
    }

    #[test]
    fn native_unset_tags_removes_all() {
        let s = parse("ALTER TABLE t UNSET TAGS (email, salary)");
        assert_eq!(s.ops.len(), 2);
        assert_eq!(s.ops[0].column, "email");
        assert!(s.ops[0].tags.is_empty());
        assert_eq!(s.ops[0].action, TagAction::Unset);
        assert_eq!(s.ops[1].column, "salary");
        assert_eq!(s.ops[1].action, TagAction::Unset);
    }

    #[test]
    fn snowflake_modify_column_set_tag_value_ignored() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email SET TAG PII = 'true'");
        assert_eq!(s.ops.len(), 1);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII"]);
        assert_eq!(s.ops[0].action, TagAction::Set);
    }

    #[test]
    fn snowflake_modify_column_multi_tag() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email SET TAG PII = 'true', GDPR = 'x'");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
    }

    #[test]
    fn snowflake_alter_column_synonym() {
        // ALTER COLUMN is a synonym for MODIFY COLUMN.
        let s = parse("ALTER TABLE t ALTER COLUMN email SET TAG PII");
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII"]);
    }

    #[test]
    fn snowflake_unset_tag_named() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email UNSET TAG GDPR");
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["GDPR"]);
        assert_eq!(s.ops[0].action, TagAction::Unset);
    }

    #[test]
    fn quoted_table_and_column() {
        let s = parse(r#"ALTER TABLE "sales"."orders" SET TAGS ("email" = ('PII'))"#);
        assert_eq!(s.table, "sales.orders");
        assert_eq!(s.ops[0].column, "email");
    }

    #[test]
    fn trailing_semicolon_ok() {
        let s = parse("ALTER TABLE t SET TAGS (c = ('X'));");
        assert_eq!(s.ops[0].tags, vec!["X"]);
    }

    #[test]
    fn not_set_tags_returns_none() {
        // SET TBLPROPERTIES, ADD COLUMN, ALTER COLUMN TYPE, CREATE/DROP TAG must
        // all fall through (Ok(None)).
        for sql in [
            "ALTER TABLE t SET TBLPROPERTIES ('write.format.default' = 'parquet')",
            "ALTER TABLE t ADD COLUMN x INT",
            "ALTER TABLE t ALTER COLUMN x TYPE BIGINT",
            "ALTER TABLE t CREATE TAG v1",
            "ALTER TABLE t DROP TAG v1",
            "SELECT 1",
        ] {
            assert!(
                try_parse_set_tags(sql).unwrap().is_none(),
                "must not claim: {sql}"
            );
        }
    }

    #[test]
    fn malformed_set_tags_errors() {
        // Recognizably SET TAGS but broken -> Err, not None (clear diagnostic).
        assert!(try_parse_set_tags("ALTER TABLE t SET TAGS (email = )").is_err());
        assert!(try_parse_set_tags("ALTER TABLE t SET TAGS email = ('PII')").is_err());
    }
}
```

- [ ] **Step 2: Add the module and run tests to verify they FAIL to compile/pass**

Add to `crates/sqe-sql/src/lib.rs` (near the other `pub mod` lines, e.g. by `pub mod ddl;`):
```rust
pub mod tags;
```
Run: `cargo test -p sqe-sql tags:: -- --nocapture`
Expected: FAIL (— `try_parse_set_tags` not defined). This confirms the test harness compiles against the AST.

- [ ] **Step 3: Implement `try_parse_set_tags` and its helpers**

Add to `crates/sqe-sql/src/tags.rs` (above the test module). This is a complete reference implementation; adjust internals as needed to make every Step 1 test pass, but keep the public signature `pub fn try_parse_set_tags(sql: &str) -> Result<Option<SetTagsStatement>>`.

```rust
/// Try to parse a column-tag DDL. Returns `Ok(None)` if `sql` is not one of the
/// SET TAGS / UNSET TAGS / MODIFY|ALTER COLUMN SET TAG forms, so the caller falls
/// through to sqlparser. Returns `Err` when the input is recognizably a tag
/// statement but malformed.
pub fn try_parse_set_tags(sql: &str) -> Result<Option<SetTagsStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }
    let after_at = trimmed["ALTER TABLE ".len()..].trim_start();
    let (table, rest) = split_identifier(after_at)?;
    let rest = rest.trim_start();
    let rest_upper = rest.to_uppercase();

    if rest_upper.starts_with("SET TAGS") {
        let body = rest["SET TAGS".len()..].trim_start();
        let ops = parse_native_set_list(body)?;
        return Ok(Some(SetTagsStatement { table, ops }));
    }
    if rest_upper.starts_with("UNSET TAGS") {
        let body = rest["UNSET TAGS".len()..].trim_start();
        let ops = parse_native_unset_list(body)?;
        return Ok(Some(SetTagsStatement { table, ops }));
    }

    let after_col = if rest_upper.starts_with("MODIFY COLUMN ") {
        Some(&rest["MODIFY COLUMN ".len()..])
    } else if rest_upper.starts_with("ALTER COLUMN ") {
        Some(&rest["ALTER COLUMN ".len()..])
    } else {
        None
    };
    if let Some(after) = after_col {
        let (column, rest2) = split_identifier(after)?;
        let rest2 = rest2.trim_start();
        let rest2_upper = rest2.to_uppercase();
        // SET TAG (singular) but not SET TAGS (plural, native form).
        if rest2_upper.starts_with("SET TAG") && !rest2_upper.starts_with("SET TAGS") {
            let body = rest2["SET TAG".len()..].trim_start();
            let tags = parse_snowflake_assignments(body)?;
            return Ok(Some(SetTagsStatement {
                table,
                ops: vec![ColumnTagOp { column, tags, action: TagAction::Set }],
            }));
        }
        if rest2_upper.starts_with("UNSET TAG") && !rest2_upper.starts_with("UNSET TAGS") {
            let body = rest2["UNSET TAG".len()..].trim_start();
            let tags = parse_snowflake_names(body)?;
            return Ok(Some(SetTagsStatement {
                table,
                ops: vec![ColumnTagOp { column, tags, action: TagAction::Unset }],
            }));
        }
        // MODIFY/ALTER COLUMN but not a tag op: not ours.
        return Ok(None);
    }
    Ok(None)
}

/// Read a leading (possibly dotted/quoted) identifier; return (cleaned, rest).
/// `"a"."b"` and `a.b` both yield `a.b`. Quotes are stripped, dots preserved.
fn split_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let mut out = String::new();
    let mut in_quote = false;
    let mut end = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '"' => { in_quote = !in_quote; end = i + c.len_utf8(); }
            _ if in_quote => { out.push(c); end = i + c.len_utf8(); }
            _ if c.is_alphanumeric() || c == '_' || c == '.' => {
                out.push(c);
                end = i + c.len_utf8();
            }
            _ => break,
        }
    }
    if out.is_empty() {
        return Err(SqeError::Execution("SET TAGS: expected an identifier".into()));
    }
    Ok((out, &s[end..]))
}

/// Strip a balanced outer `( ... )` and return the inner slice.
fn strip_parens(s: &str) -> Result<&str> {
    let s = s.trim();
    let inner = s
        .strip_prefix('(')
        .and_then(|x| x.strip_suffix(')'))
        .ok_or_else(|| SqeError::Execution("SET TAGS: expected parentheses".into()))?;
    Ok(inner)
}

/// Split on top-level `,` (ignoring commas inside parentheses or single quotes).
fn split_top_level(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut cur = String::new();
    for c in s.chars() {
        match c {
            '\'' => { in_str = !in_str; cur.push(c); }
            '(' if !in_str => { depth += 1; cur.push(c); }
            ')' if !in_str => { depth -= 1; cur.push(c); }
            ',' if !in_str && depth == 0 => { parts.push(cur.trim().to_string()); cur.clear(); }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

/// Strip surrounding single quotes or double quotes from a tag/name token.
fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// `( col = ( 'tag', ... ), col2 = (...) )`
fn parse_native_set_list(body: &str) -> Result<Vec<ColumnTagOp>> {
    let inner = strip_parens(body)?;
    let mut ops = Vec::new();
    for item in split_top_level(inner) {
        let eq = item.find('=').ok_or_else(|| {
            SqeError::Execution(format!("SET TAGS: expected `col = (...)`, got `{item}`"))
        })?;
        let (col, _) = split_identifier(item[..eq].trim())?;
        let tags_part = item[eq + 1..].trim();
        let tags_inner = strip_parens(tags_part)?;
        let tags: Vec<String> = split_top_level(tags_inner)
            .into_iter()
            .map(|t| unquote(&t))
            .filter(|t| !t.is_empty())
            .collect();
        if tags.is_empty() {
            return Err(SqeError::Execution(format!(
                "SET TAGS: column `{col}` has no tags"
            )));
        }
        ops.push(ColumnTagOp { column: col, tags, action: TagAction::Set });
    }
    if ops.is_empty() {
        return Err(SqeError::Execution("SET TAGS: empty tag list".into()));
    }
    Ok(ops)
}

/// `( col, col2, ... )` -> Unset with empty tags (remove all on each column).
fn parse_native_unset_list(body: &str) -> Result<Vec<ColumnTagOp>> {
    let inner = strip_parens(body)?;
    let mut ops = Vec::new();
    for item in split_top_level(inner) {
        let (col, _) = split_identifier(item.trim())?;
        ops.push(ColumnTagOp { column: col, tags: vec![], action: TagAction::Unset });
    }
    if ops.is_empty() {
        return Err(SqeError::Execution("UNSET TAGS: empty column list".into()));
    }
    Ok(ops)
}

/// Snowflake `name [= 'val'] [, name [= 'val']]*` -> tag names (values discarded).
fn parse_snowflake_assignments(body: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for item in split_top_level(body) {
        let name_part = match item.find('=') {
            Some(eq) => item[..eq].trim(),
            None => item.trim(),
        };
        let (name, _) = split_identifier(&unquote(name_part))?;
        names.push(name);
    }
    if names.is_empty() {
        return Err(SqeError::Execution("SET TAG: expected a tag name".into()));
    }
    Ok(names)
}

/// Snowflake `name [, name]*` -> tag names.
fn parse_snowflake_names(body: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for item in split_top_level(body) {
        let (name, _) = split_identifier(&unquote(item.trim()))?;
        names.push(name);
    }
    if names.is_empty() {
        return Err(SqeError::Execution("UNSET TAG: expected a tag name".into()));
    }
    Ok(names)
}
```

- [ ] **Step 4: Run the suite until green**

Run: `cargo test -p sqe-sql tags:: -- --nocapture`
Expected: all tests PASS. Iterate on helper internals only (do not change test assertions).

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p sqe-sql --all-targets -- -D warnings`
```bash
git add crates/sqe-sql/src/tags.rs crates/sqe-sql/src/lib.rs
git commit -m "feat(sql): parse ALTER TABLE SET TAGS / UNSET TAGS (+ Snowflake column-tag forms)"
```

---

### Task 2: Classify and route the new statement

**Files:**
- Modify: `crates/sqe-sql/src/classifier.rs` (import, `StatementKind::SetTags` variant, `name()` arm, any exhaustive matches, pre-scan hook)

- [ ] **Step 1: Write failing routing tests**

Add to the `#[cfg(test)] mod tests` in `crates/sqe-sql/src/classifier.rs` (near `test_alter_table_set_tblproperties_is_alter_table_props`, ~line 1312):

```rust
    #[test]
    fn set_tags_classifies_as_set_tags() {
        let result = parse_and_classify("ALTER TABLE t SET TAGS (email = ('PII'))");
        assert!(
            matches!(result, Ok(StatementKind::SetTags(_))),
            "expected SetTags, got: {result:?}"
        );
    }

    #[test]
    fn modify_column_set_tag_classifies_as_set_tags() {
        let result = parse_and_classify("ALTER TABLE t MODIFY COLUMN email SET TAG PII = 'true'");
        assert!(matches!(result, Ok(StatementKind::SetTags(_))));
    }

    #[test]
    fn set_tblproperties_still_classifies_as_alter_table_props() {
        // Guard: the new pre-scan must not steal SET TBLPROPERTIES.
        let result = parse_and_classify(
            "ALTER TABLE t SET TBLPROPERTIES ('write.format.default' = 'parquet')",
        );
        assert!(matches!(result, Ok(StatementKind::AlterTableProps(_))));
    }

    #[test]
    fn create_tag_still_classifies_as_refddl() {
        // Guard: Iceberg snapshot CREATE TAG must not be stolen.
        let result = parse_and_classify("ALTER TABLE t CREATE TAG v1");
        assert!(matches!(result, Ok(StatementKind::RefDdl(_))));
    }
```

- [ ] **Step 2: Run, verify FAIL**

Run: `cargo test -p sqe-sql set_tags -- --nocapture` and `... create_tag_still`
Expected: FAIL (`StatementKind::SetTags` not defined).

- [ ] **Step 3: Add the variant and wiring**

In `crates/sqe-sql/src/classifier.rs`:

1. Extend the import at line 10-ish:
```rust
use crate::tags::{try_parse_set_tags, SetTagsStatement};
```

2. Add the enum variant (in `pub enum StatementKind`, near `RefDdl`/`PartitionEvolution`, ~line 95-103):
```rust
    /// `ALTER TABLE ... SET TAGS / UNSET TAGS` and the Snowflake-compatible
    /// `MODIFY|ALTER COLUMN ... SET TAG / UNSET TAG` column-tag authoring DDL.
    SetTags(Box<SetTagsStatement>),
```

3. Add the `name()` arm (near line 160):
```rust
            StatementKind::SetTags(_) => "settags",
```

4. Add the pre-scan hook INSIDE the existing `if upper.starts_with("ALTER TABLE ")` block (after the `try_parse_partition_evolution` block, ~line 321). Order matters: RefDdl (`CREATE/DROP TAG`) and partition evolution are tried first; this is safe because their keyword sequences differ.
```rust
        // ALTER TABLE ... SET TAGS / UNSET TAGS / MODIFY|ALTER COLUMN ... SET TAG.
        // Column-tag authoring; distinct from Iceberg snapshot CREATE/DROP TAG above.
        if let Some(set_tags) = try_parse_set_tags(trimmed)? {
            return Ok(StatementKind::SetTags(Box::new(set_tags)));
        }
```

5. If the compiler flags any other exhaustive `match` on `StatementKind` (e.g. the `is_*` helpers around line 173-201, or a `requires_*` method), add a `SetTags` arm consistent with how `RefDdl`/`AlterTableProps` are treated there (it is a DDL write that mutates a table; mirror `AlterTableProps`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sqe-sql -- --nocapture`
Expected: PASS (new routing tests + all existing). Pay attention that `set_tblproperties_still...` and `create_tag_still...` pass (no regression).

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p sqe-sql --all-targets -- -D warnings`
```bash
git add crates/sqe-sql/src/classifier.rs
git commit -m "feat(sql): classify and route ALTER TABLE SET TAGS as StatementKind::SetTags"
```

---

### Task 3: Pure merge function `apply_tag_ops`

**Files:**
- Modify: `crates/sqe-coordinator/src/tag_source_impl.rs` (add `apply_tag_ops`, expose `parse_column_tags` + the property key to the crate)

- [ ] **Step 1: Write failing tests**

Add to the `#[cfg(test)] mod tests` in `crates/sqe-coordinator/src/tag_source_impl.rs`:

```rust
    use sqe_sql::tags::{ColumnTagOp, TagAction};

    fn map(pairs: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(c, ts)| (c.to_string(), ts.iter().map(|t| t.to_string()).collect()))
            .collect()
    }

    #[test]
    fn apply_tag_ops_set_merges_and_dedups() {
        let cur = map(&[("email", &["PII"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["PII".into(), "GDPR".into()],
            action: TagAction::Set,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(got.get("email").unwrap(), &vec!["PII".to_string(), "GDPR".to_string()]);
    }

    #[test]
    fn apply_tag_ops_set_leaves_other_columns_untouched() {
        let cur = map(&[("email", &["PII"]), ("region", &["GEO"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["GDPR".into()],
            action: TagAction::Set,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(got.get("region").unwrap(), &vec!["GEO".to_string()]);
    }

    #[test]
    fn apply_tag_ops_unset_named_removes_one() {
        let cur = map(&[("email", &["PII", "GDPR"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["GDPR".into()],
            action: TagAction::Unset,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(got.get("email").unwrap(), &vec!["PII".to_string()]);
    }

    #[test]
    fn apply_tag_ops_unset_all_drops_column() {
        let cur = map(&[("email", &["PII", "GDPR"]), ("region", &["GEO"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec![],
            action: TagAction::Unset,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert!(!got.contains_key("email"));
        assert!(got.contains_key("region"));
    }

    #[test]
    fn apply_tag_ops_unset_last_tag_drops_column() {
        let cur = map(&[("email", &["PII"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["PII".into()],
            action: TagAction::Unset,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert!(!got.contains_key("email"));
    }
```

- [ ] **Step 2: Run, verify FAIL**

Run: `cargo test -p sqe-coordinator apply_tag_ops -- --nocapture`
Expected: FAIL (`apply_tag_ops` not defined).

- [ ] **Step 3: Implement `apply_tag_ops` and widen visibility**

In `crates/sqe-coordinator/src/tag_source_impl.rs`:
- Change `const PROP_KEY: &str = "sqe.column-tags";` to `pub(crate) const PROP_KEY: &str = "sqe.column-tags";`
- Change `fn parse_column_tags(...)` to `pub(crate) fn parse_column_tags(...)` (Task 4 reuses it).
- Add:
```rust
use sqe_sql::tags::{ColumnTagOp, TagAction};

/// Apply column-tag operations to the current tag map (merge semantics).
/// `Set` unions tags (dedup, stable order). `Unset` removes the listed tags, or
/// ALL tags on the column when the list is empty. A column whose tag list becomes
/// empty is dropped from the map.
pub(crate) fn apply_tag_ops(
    current: &std::collections::HashMap<String, Vec<String>>,
    ops: &[ColumnTagOp],
) -> std::collections::HashMap<String, Vec<String>> {
    let mut map = current.clone();
    for op in ops {
        match op.action {
            TagAction::Set => {
                let entry = map.entry(op.column.clone()).or_default();
                for t in &op.tags {
                    if !entry.contains(t) {
                        entry.push(t.clone());
                    }
                }
                if entry.is_empty() {
                    map.remove(&op.column);
                }
            }
            TagAction::Unset => {
                if op.tags.is_empty() {
                    map.remove(&op.column);
                } else if let Some(entry) = map.get_mut(&op.column) {
                    entry.retain(|t| !op.tags.contains(t));
                    if entry.is_empty() {
                        map.remove(&op.column);
                    }
                }
            }
        }
    }
    map
}
```
(If the test module does not already `use super::*;`, the test's `apply_tag_ops` reference resolves via that import; add it if missing.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sqe-coordinator apply_tag_ops -- --nocapture`
Expected: all PASS.

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`
```bash
git add crates/sqe-coordinator/src/tag_source_impl.rs
git commit -m "feat(coordinator): apply_tag_ops merge logic for column-tag DDL"
```

---

### Task 4: Coordinator handler + dispatch

**Files:**
- Modify: `crates/sqe-coordinator/src/catalog_ops.rs` (add `set_column_tags`)
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (dispatch `StatementKind::SetTags`)

- [ ] **Step 1: Add the handler `set_column_tags`**

In `crates/sqe-coordinator/src/catalog_ops.rs`, add a method on the same impl block as `apply_ref_ddl` (which shows the load_table + commit pattern at ~line 769). Use the existing helpers `parse_object_name`, `parse_table_ref`, `session_catalog_for`, `catalog_qualifier`, and `crate::tag_source_impl::{apply_tag_ops, parse_column_tags, PROP_KEY}`:

```rust
    /// Author column tags (`ALTER TABLE ... SET TAGS / UNSET TAGS` and the
    /// Snowflake column forms). Reads the current `sqe.column-tags` property,
    /// applies merge semantics, and commits the new map as a single
    /// `TableUpdate::SetProperties`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn set_column_tags(
        &self,
        session: &Session,
        stmt: &sqe_sql::tags::SetTagsStatement,
    ) -> sqe_core::Result<()> {
        let object_name = parse_object_name(&stmt.table)?;
        let table_ident = parse_table_ref(&object_name)?;

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(&object_name).as_deref())
            .await?;

        let table = session_catalog.load_table(&table_ident).await?;
        let current = crate::tag_source_impl::parse_column_tags(table.metadata().properties());

        let new_map = crate::tag_source_impl::apply_tag_ops(&current, &stmt.ops);
        let json = serde_json::to_string(&new_map).map_err(|e| {
            SqeError::Execution(format!("failed to serialize column tags: {e}"))
        })?;

        let mut updates = HashMap::new();
        updates.insert(crate::tag_source_impl::PROP_KEY.to_string(), json);

        info!(
            username = %session.user.username,
            table = %table_ident,
            num_cols = new_map.len(),
            "Authoring column tags"
        );

        session_catalog
            .commit_schema_update(&table_ident, vec![TableUpdate::SetProperties { updates }], vec![])
            .await?;
        session_catalog.invalidate_table(&table_ident).await;
        Ok(())
    }
```

Notes for the implementer:
- Confirm `parse_object_name` accepts `&str` (it is used as `parse_object_name(table_ref)` in `apply_ref_ddl`, where `table_ref` is `&str`). If it needs `&String`, pass `&stmt.table`.
- Confirm `table.metadata().properties()` returns `&HashMap<String, String>` (iceberg-rust `TableMetadata::properties`). `parse_column_tags` takes `&HashMap<String, String>` per its definition; match the borrow.
- `HashMap`, `TableUpdate`, `SqeError`, `info`, `instrument`, `Session` are already imported in this file (used by `set_table_properties`/`apply_ref_ddl`); add `use serde_json;` only if not present.

- [ ] **Step 2: Dispatch the statement**

In `crates/sqe-coordinator/src/query_handler.rs`, add an arm next to `StatementKind::AlterTableProps` (~line 865), reusing the same cache invalidation:

```rust
                StatementKind::SetTags(stmt) => {
                    self.catalog_ops.set_column_tags(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    // Tag->column associations changed; flush cached tag policies so
                    // the next query re-resolves masks against the new tags.
                    self.invalidate_policy_cache();
                    Ok(vec![])
                }
```
If `StatementKind` is matched in more than one place in this file (e.g. a routing/whitelist match for write-vs-read), add a `SetTags` arm wherever the compiler demands, mirroring `AlterTableProps`.

- [ ] **Step 3: Build the crate**

Run: `cargo build -p sqe-coordinator`
Expected: compiles. Fix any borrow/import mismatches per the Step 1 notes.

- [ ] **Step 4: Run the coordinator test suite**

Run: `cargo test -p sqe-coordinator`
Expected: all pass except the known pre-existing `channel_pool::second_get_to_unreachable_does_not_reuse_failed_connect` sandbox-network flake (unrelated). Note it in the report; do not chase it.

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`
```bash
git add crates/sqe-coordinator/src/catalog_ops.rs crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat(coordinator): execute ALTER TABLE SET TAGS via SetProperties commit"
```

---

### Task 5: Documentation + blogs

**Files:**
- Modify: `docs/ranger-fine-grained-enforcement.md` (replace the raw-JSON tag-authoring guidance with `SET TAGS`)
- Modify: `docs/ranger-tag-storage-decision.md` (note the DDL surface)
- Modify: `docs/blog/2026-06-19-snowflake-governance-on-open-iceberg.md` (show `SET TAGS` where it currently implies hand-written JSON; this file may not be on `main` yet but exists on this branch lineage — if absent, skip and say so)
- Modify: `nextsteps.md`, `README.md` roadmap (mark done if a line exists)

- [ ] **Step 1: Update the tag-authoring docs**

In `docs/ranger-fine-grained-enforcement.md`, find where tags are authored (search `sqe.column-tags` / `SET TBLPROPERTIES`). Present `SET TAGS` as the primary way; keep the raw property as the underlying storage. Use `->` only in code blocks; no emdash/endash/arrows in prose:

````markdown
### Authoring column tags

Attach tags to columns with `SET TAGS`. SQE stores the association in the
`sqe.column-tags` table property; the DDL writes that property for you.

```sql
ALTER TABLE sales.orders SET TAGS (email = ('PII', 'GDPR'), salary = ('PII'));

-- remove all tags on a column:
ALTER TABLE sales.orders UNSET TAGS (salary);
```

Snowflake's column-tag syntax works too. The tag name becomes the label; SQE has
no tag values, so the assigned value is ignored.

```sql
ALTER TABLE sales.orders MODIFY COLUMN email SET TAG PII = 'true';
ALTER TABLE sales.orders MODIFY COLUMN email UNSET TAG GDPR;
```

`SET TAGS` merges: it changes only the columns you name and leaves the rest of
the table's tags in place. The mask that a tag triggers still lives in the Ranger
tagPolicy; `SET TAGS` only authors which columns carry which label.
````
Add one caveat line where appropriate: the association lives in the Iceberg property that SQE reads. Until the separate Iceberg-to-Ranger tag sync lands, other engines (Spark/Kyuubi) do not see these column tags.

- [ ] **Step 2: Update the storage-decision doc + the blog**

In `docs/ranger-tag-storage-decision.md`, add a short note that the user-facing surface is now `ALTER TABLE SET TAGS` (not raw `SET TBLPROPERTIES`). In `docs/blog/2026-06-19-snowflake-governance-on-open-iceberg.md`, if it shows tag authoring via raw JSON, replace that snippet with the `SET TAGS` form (keeping voice). If the blog file is not present on this branch, skip it and report that.

- [ ] **Step 3: nextsteps.md / README.md**

Mark `ALTER TABLE SET TAGS` done in `nextsteps.md` (file's existing style). Tick a README roadmap line if one exists; else leave README.

- [ ] **Step 4: Forbidden-character gate**

Run:
```bash
grep -rnP '[\x{2014}\x{2013}\x{2192}\x{2190}\x{25B6}]' docs/ranger-fine-grained-enforcement.md docs/ranger-tag-storage-decision.md docs/blog/2026-06-19-snowflake-governance-on-open-iceberg.md
```
Expected: zero hits. Scan for forbidden words too. Fix any.

- [ ] **Step 5: Commit**

```bash
git add docs/ranger-fine-grained-enforcement.md docs/ranger-tag-storage-decision.md nextsteps.md
# add the blog + README only if you modified them:
# git add docs/blog/2026-06-19-snowflake-governance-on-open-iceberg.md README.md
git commit -m "docs(ranger): document ALTER TABLE SET TAGS column-tag authoring DDL"
```

---

## Self-Review

**Spec coverage:** AST + parser (both native + Snowflake forms) -> Task 1. Classify + route -> Task 2. Merge semantics -> Task 3. Execute (load, merge, commit, invalidate) + dispatch -> Task 4. Docs + blogs -> Task 5. All decisions (both syntaxes, merge, value-ignored) covered.

**Placeholder scan:** No TBD/TODO. Task 1 provides a complete reference parser; the test suite is the contract. Task 4 has explicit borrow/import verification notes rather than guesses.

**Type consistency:** `SetTagsStatement { table: String, ops: Vec<ColumnTagOp> }`, `ColumnTagOp { column, tags, action }`, `TagAction::{Set,Unset}` used identically across sqe-sql (define), classifier (`StatementKind::SetTags(Box<SetTagsStatement>)`), tag_source_impl (`apply_tag_ops(&HashMap, &[ColumnTagOp])`), catalog_ops (`set_column_tags(&self, &Session, &SetTagsStatement)`), and query_handler (dispatch). `PROP_KEY` + `parse_column_tags` widened to `pub(crate)` in Task 3 and consumed in Task 4. Commit path (`commit_schema_update` + `invalidate_table`) matches `set_table_properties`/`apply_ref_ddl` (verified `catalog_ops.rs:741`, `:796`, `:754`).

**Guardrails verified:** `CREATE/DROP TAG` (RefDdl) and `SET TBLPROPERTIES` (AlterTableProps) are tried/handled on disjoint keywords; Task 2 includes explicit no-regression tests for both.

**Out of scope:** `SHOW TAGS` read-back, Iceberg-to-Ranger tag sync, role activation, FUTURE-grant work (separate, already shipped).
