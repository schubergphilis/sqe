### Fixed

- `INSERT INTO t (WITH ... SELECT ...)` and other parenthesised source queries parse (#889)

    sqlparser only took `(SELECT` after the table name, so `(WITH`, `(VALUES` and
    `((SELECT` were read as a column list and failed with "Expected: ), found: x".
    A parse-gated rewrite drops the one pair of parentheses; a real column list is
    never touched. dbt-sqe's `append` incremental strategy no longer wraps the model
    in parentheses, so every model that starts with a CTE appends. `INSERT ...
    SELECT 4, 4` no longer fails with "Projections require unique expression
    names": unnamed INSERT source columns get `_colN` names as SELECT columns do.
