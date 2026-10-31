### Fixed

- `ORDER BY` a column the query does not select returned unordered rows

    `SELECT id + 0 AS r FROM t ORDER BY id` came back as interleaved sorted
    runs when the scan read the table as more than one partition (a table
    with several data files). The projection hides the sort column, so the
    partitions were concatenated instead of merged in order. They are now
    merged in order beneath the projection; `ORDER BY ... LIMIT` keeps its
    global cap.
