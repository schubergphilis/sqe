### Added

- `delete_ratio_threshold`: compaction rewrites a data file once its deleted rows reach a share of its rows

    `CALL system.rewrite_data_files(..., delete_ratio_threshold => 0.3)`,
    `[maintenance.compaction] delete_ratio_threshold` and the table property
    `sqe.maintenance.compaction.delete-ratio-threshold` mark a data file as
    delete-heavy when its deleted rows are at least that share of its rows.
    On a format-version 3 table each data file has at most one deletion
    vector, so `delete_file_threshold` (a count of delete files) never passes
    1 there; the share counts the deletion vector's cardinality, and V2
    position delete files written for one data file. Equality deletes are not
    counted. A file meeting either threshold is rewritten, and `table_health`
    reports both in `delete_heavy_files`. Off by default; the value must be
    greater than 0 and at most 1.
