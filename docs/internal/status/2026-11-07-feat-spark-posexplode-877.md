**STATUS 2026-11-07: Spark `posexplode` / `explode` in the SELECT list (`feat/spark-posexplode-877`, #877).**

`crates/sqe-sql/src/trino_compat.rs` `rewrite_spark_explode` runs from `post_visit_query`, so it
reaches every SELECT (subqueries, CTEs, set-operation branches) on every protocol through the SQL
front door. One `posexplode(a)` item becomes two:
`unnest(range(0, CAST(array_length(a) AS BIGINT))) AS pos, unnest(a) AS col`. DataFusion zips
select-list unnests, so position `i` pairs with element `i`. `explode(a)` becomes `unnest(a) AS col`.

Tests:
- `sqe-sql` unit tests `*explode*` (5): rewrite shape, aliases, subquery/UNION, untouched cases.
- `sqe-catalog` `tests/it/trino_row_compat.rs` `*explode*` (4): run against DataFusion. They lock the
  column names and types (`pos:Int64,col:Int64`), the 0 origin, empty/NULL arrays giving no rows,
  and a NULL element being kept.

Left open:
- `explode` of a MAP (Spark returns `key`, `value`) and `explode_outer` / `posexplode_outer`.
- `LATERAL VIEW posexplode(...)` (Spark's FROM-clause form) is not rewritten.
- The argument is evaluated twice for `posexplode`.

**NEXT:** tell the dbx-migrator to pass `posexplode` through instead of blocking it.
