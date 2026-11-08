**STATUS 2026-11-07: lateral column aliases resolve (`feat/lateral-column-alias-878`, #878).**

`SELECT a + 1 AS b, b * 2 AS c FROM t` failed with "Schema error: No field named b": DataFusion
resolves select-list names against the `FROM` relation only. The dbx-migrator inlined the alias
and dropped the parentheses (`a + 1 * 2`).

- `sqe_sql::inline_lateral_alias` (`lateral_alias.rs`): replaces bare references to one name in
  every select list that aliases it in an earlier item, with the item's expression wrapped in
  parentheses. Subqueries and lambdas that bind the name keep their scope; `t.b` is never touched.
- `file_tvf_prepare::plan_with_lateral_aliases`: `plan_sql`, `plan_sql_without_policy`,
  `plan_parsed` and `create_logical_plan` retry on `SchemaError::FieldNotFound` for an unqualified
  name, one name per round (at most 16, for chains). Driven by the planner's error, so a `FROM`
  column always wins over an alias (Databricks' rule). The last planning error is returned when no
  alias explains it.
- Docs: `dml.md` "Lateral column aliases".

Tests: `sqe-sql` `lateral_alias::tests` (7: parentheses, chains, order, qualified, subquery and
lambda scope, CTE + union arms, case folding); `sqe-catalog`
`file_tvf_prepare::tests::lateral_column_alias_resolves_after_the_from_columns` (issue query,
column-wins, chain, aggregate, unknown name keeps its error); integration
`sql_compat_test::test_sql_compat_08_lateral_alias` (`tests/sql/08_lateral_alias.sql`).
Gate: `make test-integration FILTER=test_sql_compat_08_lateral` -> 1 passed. Unit: sqe-sql 624,
sqe-catalog 791 passed; clippy clean on both.

Left open: the error does not say which `SELECT` failed, so the name is inlined in every `SELECT`
that defines it as an earlier alias. A `SELECT` where the name is both a `FROM` column and an
earlier alias, in a statement where another `SELECT` lacks the column, reads the alias there
(documented in `dml.md`). The rewrite parses with DuckDB's dialect; after !1680 (#851) merges,
switch it to `ExecutionDialect` so a pipe query can use a lateral alias too.

**NEXT:** #875, then #758, #815, #812, #848, #870; refactors #637, #636, #589 (#576 after those).
