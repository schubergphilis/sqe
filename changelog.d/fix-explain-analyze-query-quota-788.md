### Fixed

- `EXPLAIN ANALYZE` and `EXPLAIN FULL` run under the query's memory cap and join fallback (#788)

    Both statements run the query to collect operator metrics, but they ran
    it on the session's shared pool, outside the per-query pool every SELECT
    gets. Any user could exceed `query.max_query_memory` that way, sorts had
    no per-partition share, and a join that the query completes through the
    sort-merge retry (#784) was not retried. The metrics therefore described
    a different execution from the query's: on the engine bench, a full
    `ORDER BY` of 150M rows at 3 GB completes as a query but failed as
    `EXPLAIN ANALYZE` once its merges took the whole pool. Both statements
    now run with the query's pool, sort share and join fallback, report the
    retry's plan when the fallback replans, and a memory refusal returns the
    `ResourceExhausted` error code.
