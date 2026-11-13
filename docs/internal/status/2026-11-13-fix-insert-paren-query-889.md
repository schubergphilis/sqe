**STATUS 2026-11-13: parenthesised INSERT source queries parse; dbt-sqe append sends the model bare (`fix/insert-paren-query-889`, #889).**

A downstream dbt test project kept one append model CTE-free because dbt-sqe's `append` strategy emits `INSERT INTO t (<model>)`, and sqlparser 0.62 reads `(WITH` as the start of a column list.

Changed:

- `crates/sqe-sql/src/insert_paren_query.rs`: `rewrite_insert_paren_query` drops the parentheses around an INSERT source that starts with `WITH`, `SELECT`, `VALUES` or `(`, when the matching `)` ends the statement. Parse-gated: SQL that parses, or still fails after the rewrite, is unchanged. Wired into the front door's fallback chain and `pre_parse_pipeline`.
- `trino_compat::apply_anonymous_column_aliases` also names the unnamed columns of an INSERT source (`_colN`). The source is matched by position, so names change nothing, but `INSERT INTO t (a, b) SELECT 4, 4` failed with "Projections require unique expression names". Found by the probe for this issue.
- dbt-sqe `incremental.sql`, `append`: `INSERT INTO {{ this }}` followed by the model, no parentheses. A model ending in a `--` comment would also have swallowed the closing parenthesis.

Tests: unit `insert_paren_query::tests` (2), `trino_compat::tests::insert_source_columns_get_names_so_equal_literals_plan`; integration `insert_value_integrity_e2e::insert_takes_a_parenthesised_source_query_889` (WITH, VALUES, `((SELECT`, dbt's quoted multi-line text, equal literals with and without a column list; rows asserted).

Gate: `make test-integration FILTER=889` 1 passed, `FILTER=insert` 25 passed; `cargo test -p sqe-sql` 644 + 94 passed; `cargo test -p sqe-coordinator --lib` 828 passed; dbt-sqe `pytest tests` 28 passed. Live: dbt-core 1.12 against a local coordinator, an append model with a CTE and a trailing `--` comment, three runs, one row per run.

**NEXT:** the dbt test project can put the CTE back in its append model once it runs a dbt-sqe and SQE build that carry this commit; either half alone fixes it.
