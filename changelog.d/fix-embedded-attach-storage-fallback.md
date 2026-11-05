### Fixed

- Embedded `ATTACH ... (TYPE iceberg_rest)` reads its data files from the store the CLI is configured for

    An ATTACH with no `S3_*` option now takes the endpoint, region, keys,
    session token and path style from `--s3-*`, `SQE_STORAGE__S3_*` or
    `AWS_*`. Before, its FileIO fell back to the ambient AWS chain and failed
    with "region is missing" against RustFS or MinIO unless the ATTACH spelled
    every setting out. An ATTACH that names any `S3_*` option still uses only
    its own options, and vended credentials still replace the keys. The
    server is unchanged: a user's ATTACH never borrows the coordinator's
    keys. New ATTACH option `S3_SESSION_TOKEN` for temporary keys. The book
    now documents the `S3_*` options.
