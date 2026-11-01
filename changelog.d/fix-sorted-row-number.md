### Changed

- `ROW_NUMBER() OVER (PARTITION BY ...)` over sorted input runs in one vectorised pass

    A window whose every expression is `row_number()` over the same partition keys, with input already sorted on those keys, now counts rows per key run instead of keeping per-partition state. On 150M rows with 15M keys the window's CPU drops from about 39 s to under 0.5 s, the query's total CPU from 80 s to 50 s, and its wall time from 11.4-11.8 s to 7.6-8.9 s. Set `SQE_SORTED_ROW_NUMBER=off` to use DataFusion's window exec.
