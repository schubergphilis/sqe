**STATUS 2026-11-13: MERGE resolves bare columns in a NOT MATCHED clause to the source (`fix/merge-bare-source-columns-893`, #893).**

Found while proving dbt snapshots for #891 against a local coordinator: the second `dbt snapshot` run failed on dbt-core's default `snapshot_merge_sql`, which writes `when not matched and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert' then insert (cols) values (cols)`. The MERGE rewrite joins target and source and rendered the VALUES as written, so a bare name both sides carry was ambiguous.

Changed:

- `crates/sqe-write/src/merge_sql.rs`: `qualify_bare_source_columns` rewrites a bare identifier that names a source column to `<source scratch table>.<col>`. Applied in `resolve_insert_expr` (both the copy-on-write SELECT and the merge-on-read insert stream call it) and to the predicate of NOT MATCHED / NOT MATCHED BY TARGET clauses in `classify_merge_clauses`. MATCHED and NOT MATCHED BY SOURCE clauses are unchanged: there a bare name shared by both sides stays ambiguous, as in PostgreSQL.

Tests: unit `merge_sql::tests::not_matched_bare_columns_resolve_to_the_source` (bare predicate, bare VALUES inside a function call); integration `merge_rows_affected_e2e::merge_not_matched_bare_columns_are_source_columns` (dbt's alias names and quoting, copy-on-write and merge-on-read, rows and `rows_affected` asserted).

Gate: `make test-integration FILTER=merge_` 9 passed (exit 0); `cargo test -p sqe-write --lib merge_sql` 32 passed; `cargo clippy -p sqe-write --all-targets -- -D warnings` clean; `cargo xtask budgets --check` ok. The failing case was reproduced first against a coordinator built from main (`VALUES (id, name)` -> "Ambiguous reference to unqualified field id"; `VALUES (s.id, s.name)` worked).

**NEXT:** dbt snapshots need this engine build; dbt-sqe #891 documents snapshot support against it.
