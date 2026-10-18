### Added

- Iceberg V3 `unknown` columns

    `CREATE TABLE t (id INT, u UNKNOWN)` (or Spark's spelling, `VOID`)
    declares an all-NULL column of the V3 `unknown` type, and
    `ALTER TABLE ... ALTER COLUMN u SET DATA TYPE <type>` promotes it.
    SQE reads a Spark-written `VOID` column as NULLs, and files written
    before a promotion read typed NULLs. As the spec requires, an unknown
    column is never written to a data file, so Spark reads SQE's files.
    An unknown field nested in a struct, list or map is not supported yet:
    SQE refuses to write one.

- Reading shredded Iceberg V3 variants

    Spark with `write.parquet.shred-variants=true` stores a variant with a
    `typed_value` group beside `value`. SQE now folds it back into the
    variant on every read path (coordinator direct read, the iceberg-rust
    reader and workers), so a shredded table reads the same JSON text as an
    unshredded one. Variant manifest bounds no longer fail manifest loading.
    SQE still writes unshredded variants.
