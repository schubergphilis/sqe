### Added

- Reading shredded variant fields skips the other shredded fields (#870)

    A query that reads a shredded variant column only through
    `variant_get` / `try_variant_get` object paths (`$.a`, `$.b.c`,
    `$['d']`) reads `metadata`, the residual `value` and just the
    `typed_value` fields those paths start with, instead of every shredded
    field. This holds on the coordinator's direct read, on the iceberg-rust
    reader and on a worker; the scan ticket goes to version 8 when it names
    fields, so an older worker refuses it rather than reading wrong. Any
    other use of the column (selecting it, `$`, an array path, a `UNION`,
    `DISTINCT` on it) reads it whole. `EXPLAIN` shows the fields as
    `variant_fields={<field id>:[names]}` on the scan.
