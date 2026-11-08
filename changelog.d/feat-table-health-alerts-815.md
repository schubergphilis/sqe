### Added

- Table-health gauges and alert rules for delete-file and manifest build-up (#815)

    The maintenance scheduler now also sets `sqe_table_data_files` and
    `sqe_table_manifests` per opted-in table. `deploy/observability/alerts.yml` gains
    `SqeTableDeleteFileAmplification` (more than one delete file per two data files, at
    least 50 delete files) and `SqeTableManifestCountHigh` (more than 500 manifests), with
    promtool unit tests (`make alerts-test`). The Helm chart installs the same rules as a
    `PrometheusRule` when `prometheusRule.enabled` is true.

### Fixed

- `CALL system.rewrite_manifests` merges manifests and reports the counts (#815)

    The procedure committed a new snapshot with the same manifests: without a
    cluster function the iceberg-rust action rewrote nothing. It now merges the data
    manifests of each partition spec, up to `commit.manifest.target-size-bytes`.
    `input_count` and `output_count` are the manifests before and after; `input_count`
    was always 0, and `output_count` counted the snapshot summary's properties.
