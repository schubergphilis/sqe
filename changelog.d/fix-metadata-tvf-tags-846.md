### Fixed

- Iceberg metadata table functions (`table_snapshots`, `table_files`, ...) answer under a tag-aware policy store again (#846)

    The policy rewriter looked up column tags for the scan under the function
    name (`table_snapshots()`), which the metadata cache never holds, so every
    call was refused with "tag state unknown", even for an admin. A metadata
    table function's output columns carry no column tags. The target table's
    tags are still applied inside the function: `table_files` and
    `table_partitions` withhold the statistics of masked or restricted
    columns, and withhold all of them when the tags are unreadable.
