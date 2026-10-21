### Added

- Shredded variant writes with `write.parquet.shred-variants` (#858)

    A table that sets `write.parquet.shred-variants = true` now gets
    shredded variant columns, as Spark and Iceberg Java write them. Each
    data file infers its shredding from its first batch: object fields with
    one integer, double, string, boolean or date type in every row (also in
    nested objects, at most 64), or the whole value when every row is one
    such type, go to `typed_value`. A row that is not an object, or whose
    field has another type, is written to the residual `value`, so no value
    changes type. Spark 4.1 + Iceberg
    1.12 reads the files, and `rewrite_data_files` keeps the shredding.
    Only top-level variant columns are shredded, and reads still unshred.
