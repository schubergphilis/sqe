### Added

- Time travel in `sqe-cli --embedded`

    `SELECT ... FROM iceberg.s.t FOR VERSION AS OF <snapshot id>`, `FOR
    VERSION AS OF 'branch-or-tag'`, `FOR TIMESTAMP AS OF` and `FOR
    SYSTEM_TIME AS OF` now read the table at that snapshot, on the warehouse
    and on attached Iceberg catalogs. Before, the clause failed to parse
    ("Expected: one of UPDATE or SHARE"). The snapshot is resolved by the
    same code as the coordinator, so both answer the same snapshot and fail
    with the same message. A table outside an Iceberg catalog errors with
    `time travel reads Iceberg tables only`.
