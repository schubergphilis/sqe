### Added

- Shredded variant fields prune row groups (#870)

    A comparison of `variant_get(v, '$.path', '<type>')` or
    `try_variant_get` with a literal (`=`, `<`, `<=`, `>`, `>=`, literal on
    either side) now skips the row groups whose shredded `typed_value`
    statistics exclude it, on the coordinator's direct read and on the
    iceberg-rust reader (new vendor hook `RowGroupPruner`). Supported types:
    int, bigint, string, date, boolean, on a top-level field or a nested
    object path. A row group is kept whenever a row could hold the value
    outside `typed_value`: a residual `value`, a mismatched type, or a field
    that is not shredded. The new `row_groups_pruned_variant` scan metric
    shows up in `EXPLAIN ANALYZE`. Limits: data files are not pruned (variant
    manifest bounds are still skipped), and a worker scan reads every row
    group.

### Fixed

- Data-file writes honour `write.parquet.row-group-size-bytes` (#870)

    SQE's write path built its Parquet properties without the table's row
    group size, so every file got the parquet-rs row-count cap whatever the
    table said. A positive value now caps each row group at that many bytes;
    tables without the property write as before.
