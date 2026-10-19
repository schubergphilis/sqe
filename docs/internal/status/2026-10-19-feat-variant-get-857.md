**STATUS 2026-10-19: variant_get / try_variant_get and a variant compaction test (`feat/variant-get-857`, #857).**

`crates/sqe-core/src/variant_get.rs` registers `variant_get` and `try_variant_get` as scalar UDFs over arrow 59.3's `parquet_variant_compute::variant_get` kernel, at the same three sites that wrap the json functions (both session builders in `session_context.rs`, and `sqe-cli` embedded). The path and type must be literals: the return type comes from the type argument in `return_field_from_args`. A Spark path's leading `$` / `$.` is stripped to arrow's `VariantPath` syntax. Type names are Spark/SQL names, with Arrow's own names as a fallback. `variant_get` casts strictly (`CastOptions { safe: false }`), `try_variant_get` safely. Without a type the result is a variant in unshredded storage, so the encoders render it as JSON.

Tests: five unit tests in `variant_get.rs`, plus `variant_e2e::variant_get_reads_typed_fields` and `variant_e2e::rewrite_data_files_preserves_variant_values` (six one-row files merge into one, values and SHOW CREATE TABLE unchanged) against the local Polaris stack. Matrix cell `sqe:variant-type:v3` is full: 148/162.

Limits: no pushdown; the function runs on the coordinator after the scan, and shredded files are unshredded on read, so `typed_value` is not used for direct access.

**NEXT:** unchanged
