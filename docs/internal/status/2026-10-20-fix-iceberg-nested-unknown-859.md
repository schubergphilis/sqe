**STATUS 2026-10-20: Iceberg V3 `unknown` struct members read and written (`fix/iceberg-nested-unknown-859`, #859).**

The vendored `ParquetWriter` patch (v3-unknown) used to drop top-level
`unknown` columns and refuse a nested one. It now prunes `unknown` fields
at any depth (`prune_unknown_type` / `prune_unknown_array`): a struct member
is left out of the file, also inside a list, a large or fixed-size list or a
map value. An `unknown` list element, an `unknown` map key or value, and a
struct whose members are all `unknown` cannot be represented without the
field, so they are refused with `FeatureUnsupported`.

On read, the new `sqe_core::unknown::fill_unknown` rebuilds a decoded column
in the declared type, matching struct members by parquet field id (by name
when the file has none) and filling missing nullable members with NULLs. The
coordinator scan (`conform_batch_to_projection`) uses it when a projected
type holds `unknown`. The worker's `ProjectionFill::conform` uses it instead
of a plain `cast`. `projection_evolved` now reports true for any projected
column holding `unknown`, so distributed scans always carry the fill.

Gate: `make test-integration FILTER="unknown_fields_read_as_null_on_a_worker
files_from_before_add_column unknown_type_spark_interop"` -> 6 passed
(`nested_unknown_struct_spark_writes_sqe_reads`,
`nested_unknown_struct_sqe_creates_spark_reads`,
`sqe_declares_an_unknown_column_spark_reads_it`,
`unknown_column_reads_writes_and_promotes`, and in
`distributed_schema_evolution_e2e` the new
`unknown_fields_read_as_null_on_a_worker` plus the #831 test), none skipped.
The worker test reads a nested and a top-level `unknown` field through an
in-process worker, including `SELECT *`. Unit tests:
vendored iceberg `-- nested_unknown skips_unknown prune_unknown` 3 passed;
`sqe-core unknown` 4 new; `sqe-catalog unknown_member` 1; `sqe-worker
projection_fill` 5.

Matrix: `sqe:unknown-type:v3` stays partial (148/162). The remaining
caveats are the refused list-element / map key-value shapes (the spec and
iceberg-rust allow them, so they are real gaps) and
information_schema showing the Arrow name `Null`. That column comes from
DataFusion's built-in provider, so renaming it means wrapping the provider
(see #665).

Finding: DataFusion cannot plan `l[1].b` (dot access after a subscript):
"Dot access not supported for non-string expr". `l[1]['b']` works. Trino and
Spark accept the dot form; this needs a fix or a pre-parse rewrite.

**NEXT:** `l[1].b` field access after a subscript, then #858 (shredded
variant write).
