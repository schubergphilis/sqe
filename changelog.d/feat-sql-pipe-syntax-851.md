### Added

- SQL pipe syntax: `FROM t |> WHERE ... |> AGGREGATE ... GROUP BY ... |> ORDER BY ...` (#851)

    The engine parses with `sqe_sql::ExecutionDialect`, DuckDB plus the `|>` operator, at every
    planner entry point and in the front door's tokenization check. DuckDB, the dialect the
    session names, read `|>` as `|` then `>`, so the front door refused every pipe query.
    DataFusion plans `WHERE`, `SELECT`, `EXTEND`, `AGGREGATE`, `ORDER BY`, `LIMIT`, `AS`,
    `JOIN` and `UNION` / `INTERSECT` / `EXCEPT` steps; `SET`, `DROP`, `RENAME`, `TABLESAMPLE`,
    `CALL`, `PIVOT` and `UNPIVOT` fail with "pipe operator is not supported yet".
