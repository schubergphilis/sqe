### Added

- Variants nested in a struct, a list or a map are shredded on write (#870)

    With `write.parquet.shred-variants = 'true'`, a `JSON` / `VARIANT` field
    inside a `STRUCT`, an `ARRAY` element or a `MAP` value now gets its own
    `typed_value`, inferred per data file like a top-level variant. Spark 4.1 +
    Iceberg 1.12 reads the result. JSON text in such a nested field (for
    example `named_struct('v', '{"a": 1}')`) is parsed on INSERT; invalid JSON
    fails the write.

### Fixed

- Nested shredded variants read back every value (#870)

    A shredded variant inside a struct, list or map lost its `typed_value` on
    read, so the shredded fields came back missing. The coordinator and the
    workers now fold the nested `typed_value` back into the variant, as they
    already did for a top-level column.
