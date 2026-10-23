### Added

- Integration tests for file table functions under a staging-only prefix

    `read_csv` and `read_json` read a workspace's staging path with the `[storage]` key under `allow_inline_credentials = false`, and the same file outside `allowed_object_store_prefixes` is an error that names the setting, never 0 rows (`read_parquet` was already covered). A Ranger test pins how row filters reach file table functions: by the scan name (`read_csv()`), not the path, so a filter on `read_csv()` applies to every CSV file the user reads and not to the same rows read with `read_json`.
