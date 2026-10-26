### Fixed

- SQE reads a `timestamptz` column that Databricks or Spark wrote

    A Parquet file without an Arrow schema decodes a UTC-adjusted timestamp
    as `Timestamp(us, "UTC")`, where SQE plans the column as `"+00:00"`. The
    scan refused the batch with `column types must match schema types`. Both
    labels hold UTC instants, so the scan now relabels the column.

- A copy-on-write DELETE, UPDATE, MERGE, INSERT OVERWRITE or truncate commits an `overwrite` snapshot, not a `replace`

    `replace` means the data did not change (compaction). Incremental readers,
    such as Spark streaming and Databricks, skip those snapshots, so they
    missed the rows SQE deleted or changed. Compaction still commits
    `replace`.

- ATTACH of an Iceberg REST catalog no longer fails on one table that does not load

    A Unity Catalog namespace also lists its Delta tables, and loading one
    answers 400 `not an Iceberg compatible table`. The attached catalog now
    skips such a table with a warning; the other tables stay readable. The
    vendored `datafusion-iceberg` carries this as an SQE patch.
