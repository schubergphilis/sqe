### Added

- The embedded `sqe-cli` passes a variant to the `json*` functions as its
  JSON text, as the coordinator does, so `json_get_int(doc, ...)` works on a
  V3 `JSON` column there too (#849)

### Changed

- `delete_file_threshold` above 1 no longer fires on SQE's own deletes on
  format-version 3 tables (#849)

    On V3 position deletes are deletion vectors: each data file has at most
    one, replaced on every `DELETE`. `rewrite_data_files(delete_file_threshold
    => N)` and the scheduler's `delete_file_threshold` count delete files, so
    for N > 1 they only see equality deletes. Use `rewrite_all => true` or a
    threshold of 1 on a delete-heavy V3 table.

### Fixed

- The Spark interop stack (`docker-compose.spark.yml`) starts and stays
  healthy: Ivy could not write its cache under the image's
  `HOME=/nonexistent`, Spark could not reach the storage endpoint the catalog
  returns (`localhost:19000`, which overrides the client's `s3.endpoint`), and
  concurrent `spark-sql` runs corrupted the shared local Derby metastore
