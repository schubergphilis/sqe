### Changed

- "First row per partition" (`ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1`) runs as a hash aggregate instead of a window over a full sort

    The window sorted the whole input by `p, o` to keep one row per
    partition. SQE now plans it as a group-by on `p` that keeps the row
    sorting first by `o`, with partial and final stages, memory accounting
    and spilling. "Latest row per customer" over 150M rows and 15M customers
    goes from ~8.9 s to ~5.6 s at a 3 GiB memory limit, with the same rows.
    `rn = 1`, `rn <= 1` and `rn < 2` are rewritten; any other bound, OR,
    RANK, a missing `PARTITION BY`, or a string or list column keeps the
    window plan. Among rows that tie on every `ORDER BY` key, one is kept,
    as arbitrary as the window's choice.
