### Changed

- `table_files` / `$files` lists delete files and deletion vectors, with Iceberg's `content` column

    It listed data files only, so a merge-on-read table's position deletes,
    deletion vectors and equality deletes showed only as counts in the
    `$snapshots` summary. It now lists every live file of the current
    snapshot, as Spark's `files` and Trino's `$files` do, with new columns
    `content` (0 data, 1 position deletes, 2 equality deletes),
    `referenced_data_file`, `content_offset`, `content_size_in_bytes`,
    `equality_ids` and `spec_id`. A V3 deletion vector is `content = 1` in
    Puffin. The new columns are appended after the old ten. A query that
    counts or sums data files adds `WHERE content = 0`.
