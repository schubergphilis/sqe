# fix/typeof-variant-849

`typeof(v)` on a variant column returned the Arrow struct storage type; it now returns `variant`, read from the `arrow.parquet.variant` extension tag on the argument field. `sqe-trino-functions` checks the tag through `Field::extension_type_name` rather than `sqe_core::variant`, because `sqe-core` is only its dev-dependency. This was the last open item of #849 (VARIANT, JSON as variant, `variant_get`, shredded reads and the Spark interop shipped earlier).

Gate: `make test-integration FILTER=variant` -> 11 passed, 2 failed. The two failures are `variant_distributed::*`, which need the worker rig ("Failed to connect to worker localhost:50052"); they are untouched by this change. `variant_e2e::variant_column_stores_json_and_reads_it_back` failed before the fix with the struct type name and passes after it.

Finding, not fixed: `typeof` returns Arrow type names (`Int32`, `Utf8`), not Trino names (`integer`, `varchar`). Changing that is a Trino-compat change across every type, not part of #849.

**NEXT:** close #849 after merge.
