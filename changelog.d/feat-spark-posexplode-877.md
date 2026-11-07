### Added

- Spark `explode` and `posexplode` in the SELECT list (#877)

    `posexplode(a)` returns `pos` (BIGINT, starting at 0) and `col`; `explode(a)`
    returns `col`. Both are rewritten to DataFusion `unnest`, so an empty or NULL
    array gives no rows, as in Spark. `posexplode(a) AS (p, v)` and `explode(a) AS v`
    keep the user's names. Arrays only: `explode` of a MAP and the `_outer`
    variants are not supported.
