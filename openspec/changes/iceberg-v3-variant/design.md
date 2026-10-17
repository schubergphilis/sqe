## Storage

An Iceberg `variant` is stored in Arrow as `Struct { metadata: Binary,
value: Binary }` with the `arrow.parquet.variant` extension on the field
(`sqe_core::variant`). DataFusion drops field metadata through expressions,
so variant storage is also recognised by shape (`is_variant_storage`) and
re-tagged (`variant::tag`) before every Arrow -> Iceberg schema conversion.

```
INSERT text ──json_to_variant──▶ Struct{metadata,value} ──▶ parquet variant
SELECT      ◀──variant::to_json── Struct{metadata,value} ◀── parquet variant
             (Trino json / Flight Utf8 at the protocol edge)
```

## Deciding the format version (CREATE TABLE)

`plan_create_table_schema` (`crates/sqe-write/src/write_handler.rs`):

1. Read a requested version from the options: `format-version` or
   `format_version`, values 1 / 2 / 3; anything else is an error.
2. Build the schema with `JSON` as a string and run `requires_v3_features`
   (variant at any depth, nanosecond timestamps, DEFAULTs).
3. Version = V3 if a v3 feature is present (an explicit lower request is an
   error), else the requested version, else the configured default.
4. On V3, rebuild the schema with `JSON` as a variant
   (`sql_type_to_arrow_for(.., json_as_variant = true)`, threaded through
   STRUCT and ROW fields).

The two passes keep one source of truth for "needs v3": the Iceberg schema
check that already sees nested variants, rather than a second walker over the
SQL types.

## ALTER TABLE ADD COLUMN

`catalog_ops.rs` maps the column with `json_as_variant` set from the table's
current format version, tags nested variant storage, and refuses any variant
(top-level or nested) on a table below v3. A top-level variant becomes
`Type::Variant`; a struct holding one goes through
`iceberg::arrow::arrow_type_to_type`, which reads the extension on nested
fields.

`ALTER COLUMN ... SET DATA TYPE` keeps `JSON` as a string: Iceberg does not
allow evolving `string` to `variant`.

## JSON functions over a variant

`crates/sqe-coordinator/src/variant_json_args.rs` re-registers every scalar
function whose name starts with `json` behind `VariantJsonArgs`:

- The signature becomes user-defined, so planning calls `coerce_types`,
  which coerces the other arguments as the wrapped function would
  (`fields_with_udf` on the text types) and keeps a variant argument as is
  (a cast would render the struct, not its JSON).
- `return_type` / `return_field_from_args` see a variant argument as `Utf8`.
- `invoke_with_args` renders variant arrays and scalars as JSON text and
  calls the wrapped function.

The wrapper registers under the same name and aliases as the original, so
the `shadowed_keys` invariant of `overrides.rs` holds, and it is idempotent.

## Compatibility

| Reader                     | v3 table with variant |
|----------------------------|-----------------------|
| Spark 4.0 + Iceberg 1.10+  | yes                   |
| Trino 481+                 | yes                   |
| Spark 3.x, Trino < 481, Iceberg < 1.10 | no (v3 unsupported) |

New tables default to v3 (`default_table_format_version = 3`). Where older
readers share the tables, the default is set to 2 or a table is created
`WITH ('format-version' = '2')`; its `JSON` columns are then strings.
