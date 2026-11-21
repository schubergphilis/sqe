**STATUS 2026-11-21: Databricks column tag syntax (`feat/databricks-column-tag-syntax`).**

`crates/sqe-sql/src/tags.rs` now parses the Unity Catalog column forms
`ALTER TABLE t ALTER COLUMN c SET TAGS ('k' = 'v' | 'k', ...)` and
`... UNSET TAGS ('k', ...)`. They classify as `StatementKind::SetTags` and go
through the existing `require_tag_authority` gate and `sqe.column-tags`
writer, so no coordinator change was needed. The parser keeps the values in
`parse_databricks_tag_list`, but `ColumnTagOp` still carries keys only, so the
values are dropped at that point.

The table-level form `ALTER TABLE t SET TAGS ('k' = 'v')` used to fall through
to the native `SET TAGS (col = (...))` list parser. It now returns an error
that points to the column form. Tags on tables, views, schemas and catalogs
need their own storage first.

Tests: `cargo test -p sqe-sql --lib` (679 passed), including new parser and
classifier cases. No compose stack needed.

**NEXT:** keep tag values in `sqe.column-tags` (compatible with the field-id
keying planned in #920), then the `information_schema` tag views.
