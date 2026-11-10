### Fixed

- `EXPLAIN FULL` plans its query with the same rewrites the query gets on its own

    The inner statement of `EXPLAIN FULL` skipped the front door's AST
    rewrites, so a query that ran on its own could fail to explain: Trino's
    `FETCH FIRST n ROWS ONLY` did not plan, an exponent literal such as
    `25e-1` stayed a decimal (and a comparison with a NaN double failed),
    and identifier folding and `information_schema` qualification did not
    apply.
