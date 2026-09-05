# 704: Trino-path information_schema and EXPLAIN reshaping is triggered by SQL substring and applied to arbitrary user result sets

- Scope: sqe-trino-compat (info_schema_compat.rs, explain_compat.rs, server.rs)
- Severity: medium
- Category: reliability
- Effort: S (hours)
- Status: open
- Issue: #624

## Problem

The decision to rewrite the SQL and to post-process the result is a substring test on the raw statement, `crates/sqe-trino-compat/src/info_schema_compat.rs:28-33`:

```rust
pub fn is_metadata_query(sql: &str) -> bool {
    let s = sql.trim_start().to_ascii_lowercase();
    s.contains("information_schema")
        || s.starts_with("show columns")
        || s.starts_with("describe ")
        || s.starts_with("desc ")
}
```

When it fires, `build_paginated_result` (`server.rs:1006-1013`) runs `apply_info_schema_compat` on every batch, which:

- filters out rows whose `table_catalog` or `catalog_name` column does not equal the session catalog (`info_schema_compat.rs:237-260`, `scope_catalog`), and
- rewrites any column named `data_type` through `arrow_display_to_trino_type` (`:216-235`).

Both look up columns by name on whatever batch the query produced. A user table with a `table_catalog` column (audit and lineage tables commonly have one) queried as `SELECT * FROM audit.events WHERE note LIKE '%information_schema%'` has rows silently dropped because they do not match the `X-Trino-Catalog` header. A user column named `data_type` is rewritten.

The pre-execution rewrite has the same flaw. `qualify_information_schema` (`:46-84`) scans the raw text and inserts `"catalog".` before any `information_schema.` not preceded by `.` or an identifier char. The unit test `leaves_string_literal_unchanged` (`:462`) only covers a literal that is not followed by a dot. `SELECT 'information_schema.tables'` is rewritten inside the literal to `'"cat".information_schema.tables'`. Comments are not skipped either.

`explain_compat::is_explain` (`explain_compat.rs:18-20`) and `classify_update_type` (`server.rs:543-565`) use the same leading-keyword approach; the latter reports `CREATE VIEW`, `CREATE SCHEMA` and `CREATE ROLE` all as `updateType: "CREATE TABLE"` with `updateCount: 0`.

## Why it matters

Silent row loss on a user query is a correctness failure with no log line. It is triggered by data content (a literal or comment mentioning information_schema) rather than by what the query does. The catalog-scoping step is the same one that hides the `system` and `datafusion` catalogs from BI tools, so it cannot simply be removed; it needs to run only when the plan actually reads `information_schema`.

## Proposed change

1. Decide "is metadata query" from the parsed statement, not the text. `sqe-sql` already produces the AST for classification; expose `fn reads_information_schema(&Statement) -> bool` (walk `TableFactor::Table` object names) and `fn statement_kind(&Statement) -> UpdateType` for `classify_update_type`.
2. Do the catalog qualification as an AST pass on `ObjectName` inside `rewrite_trino_compat` (it already has the `TrinoCompatVisitor` with `post_visit_table_factor`, `sqe-sql/src/trino_compat.rs:420`), so literals and comments are never touched, and pass the session catalog in through the front door from issue 700.
3. Apply `scope_catalog` and `translate_data_type` only when (1) is true, and only to columns the built-in `information_schema` provider actually emits; add an explicit allow-list of (`table`, `column`) pairs.
4. Return `updateType` from the classified statement (`CREATE VIEW`, `CREATE SCHEMA`, etc.).

## Acceptance criteria

- Unit test: a user table with `table_catalog` and `data_type` columns, queried with `information_schema` in a string literal and in a comment, returns all rows and the original `data_type` values over the Trino path.
- Unit test: `SELECT 'information_schema.tables'` is returned verbatim by the rewrite.
- Unit test: `CREATE VIEW v AS SELECT 1` reports `updateType: "CREATE VIEW"`.
- `is_metadata_query`, `is_explain`, and `classify_update_type` take a `&Statement`, not `&str`.

## Related

- 700 (single front door; this becomes an AST pass there).
- Memory: identifier case folding #337 was another pre-parse text rewrite.
