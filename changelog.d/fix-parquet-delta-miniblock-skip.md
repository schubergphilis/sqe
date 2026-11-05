### Fixed

- Reads of Iceberg tables written by Snowflake (and Databricks Photon) no longer fail with "cannot skip miniblock of size 512" when a delete applies

    parquet 59.3.1 could not skip rows inside a `DELTA_BINARY_PACKED`
    miniblock wider than 32 values. Snowflake writes 512-value miniblocks and
    Photon 256-value ones, and skipping is what a row selection does: applying
    a position delete or deletion vector, or a page-index predicate. A
    merge-on-read table from either engine therefore failed on any read that
    crossed a deleted row. arrow and parquet move to 59.3.2, which carries the
    upstream fix (apache/arrow-rs#11021, backported in #11754). A hand-encoded
    512-value page in `sqe-catalog` guards the dependency against a downgrade.
