**STATUS 2026-10-27: bare `FROM t` expands to `SELECT * FROM t` (`fix/from-first-select-star`).**

Reported against `FROM pg3_gold__marts.revenue_by_month`: the query ran and returned "0 rows". sqlparser parses a bare `FROM t` as `SelectFlavor::FromFirstNoSelect` with an empty projection; DataFusion planned zero columns.

- `sqe_sql::from_first::expand_from_first_select`: a `VisitorMut` that sets the flavor to `Standard` and the projection to `*` at every query depth. Runs in `SqlFrontDoor::normalize` before `_colN` aliasing, so Flight, Trino, embedded and MCP all get it. Policy rewriting runs later on the LogicalPlan, so masks and row filters apply to the expanded `*`.
- Docs: SQL reference, limitations and cheat-sheet no longer list FROM-first as unparseable; the CLI page shows it.

Gates:
- `cargo test -p sqe-sql --lib` -> 590 passed (4 new in `from_first::tests`).
- Embedded: `FROM t`, `FROM t ORDER BY .. LIMIT`, `WITH c AS (FROM t) ...`, `FROM t SELECT v` all return rows.

Not done: `FROM t WHERE ...` is a sqlparser parse error (its FROM-first path stops after the table list). Needs an upstream sqlparser change or a source-level fallback.

**NEXT:** decide whether `FROM t WHERE/GROUP BY` is worth an upstream sqlparser patch.
