**STATUS 2026-10-21: type promotion checked against the spec; unknown lists/maps measured against Iceberg Java; matrix unknown-type to full (`fix/iceberg-unknown-list-map-859`, #859; stacked on `feat/iceberg-shredded-variant-write-858`).**

The matrix cell had two caveats. Both were probed against Spark 4.1 +
Iceberg 1.12 before changing anything:

- `ARRAY<VOID>`, `MAP<STRING, VOID>` and `STRUCT<u: VOID>`: Spark creates the
  tables, but every INSERT fails ("Cannot convert element Parquet: unknown",
  "Cannot convert value Parquet: unknown", "Cannot write a schema with an
  empty group"). SQE refuses the same writes, so this is a format limit, not
  an SQE gap. New test
  `unknown_type_spark_interop::unknown_list_map_and_struct_are_declared_not_written`
  asserts both engines read the empty table and both refuse the row.
- information_schema names Arrow types for every column (`Int32`, `Utf8`),
  so unknown shows as `Null`. Kept as a caveat; cosmetic.

The rubric asks that an unknown column can be "promoted to a concrete type
later". Iceberg Java 1.12 refuses `ALTER COLUMN u TYPE INT` for unknown at
top level and nested (`Cannot change column type: u: unknown -> int`); SQE
promotes a top-level unknown (existing test).

Finding, fixed here: `ALTER COLUMN ... SET DATA TYPE` committed any type
change. A probe showed `VARCHAR -> INT`, `BIGINT -> INT` and
`DECIMAL(10,2) -> DECIMAL(8,2)` accepted, after which every SELECT failed
with "failed to conform scan batch". `check_type_promotion`
(`crates/sqe-coordinator/src/catalog_ops.rs`) allows the same type, int ->
long, float -> double, decimal precision widening with the same scale, and
unknown -> any primitive. Struct/list/map columns and nested targets get a
message instead of "Field id not found in metadata". date -> timestamp (a V3
spec promotion) is refused: the reader path for it is untested. Unit test
`type_promotion_follows_the_iceberg_spec`; integration test
`v3_e2e::type_promotion_widens_and_refuses_the_rest`.

Second finding, fixed here: a spec promotion other than int -> long broke
reads. After `f REAL -> DOUBLE` every SELECT failed with "failed to conform
scan batch: expected Float64 but found Float32": the older data file keeps
the old type, and the reader widened int only. `conform_batch_to_projection`
(`crates/sqe-catalog/src/iceberg_scan.rs`) now casts int -> long, float ->
double and decimal precision widening (`is_type_promotion`). Unit test
`a_promoted_column_reads_widened_from_an_older_file`; the integration test
reads rows written before and after both promotions.

Gates: `make test-integration FILTER=unknown_type_spark_interop` -> 5 passed,
no skips; `FILTER=type_promotion` -> 2 passed.
`cargo test -p sqe-coordinator --lib type_promotion_follows` -> 1 passed.
Matrix 150/162 (92.6%).

Left open:
- `ALTER COLUMN s.u SET DATA TYPE` (nested path) does not parse; a nested
  field cannot be promoted. Next branch.
- date -> timestamp / timestamp_ns promotion (V3) is refused.

**NEXT:** nested `ALTER COLUMN a.b SET DATA TYPE` (pre-parse like
`DropNestedColumn`, rebuild the struct keeping field ids), then retarget the
stack (!1498 -> !1500 -> this MR) to main as each parent merges.
