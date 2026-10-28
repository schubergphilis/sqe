### Fixed

- A bare `FROM t` returns every column instead of "0 rows"

    sqlparser reads DuckDB's `FROM t` as a SELECT with an empty projection,
    and DataFusion planned zero output columns, so every client printed
    "0 rows". The SQL front door now rewrites it to `SELECT * FROM t`, in
    CTEs, subqueries, set-operation branches and `INSERT ... FROM t` too,
    on every protocol. `FROM t SELECT a` already worked. A `WHERE` after a
    bare `FROM t` still does not parse.
