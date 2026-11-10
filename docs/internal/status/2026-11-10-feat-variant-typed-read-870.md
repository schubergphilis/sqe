**STATUS 2026-11-10: #870 typed field access on shredded variants (`feat/variant-typed-read-870`).**

A shredded variant column was read whole and unshredded: every
`typed_value` field was decoded even when the query read one of them.

Now `VariantReadHints::of` (`sqe-catalog/src/variant_read.rs`) walks the
final logical plan in `EngineQueryPlanner` and, per variant column name,
collects the first object field of every `variant_get` / `try_variant_get`
path. Any other use of the column (a bare column reference, `$`, an array
path, a non-literal path, a node that consumes rows whole such as `UNION` /
`DISTINCT` / a write) marks it whole. The hints ride a `SessionConfig`
extension to `scan_impl_inner`, which resolves names to field ids
(`IcebergScanExec::with_variant_fields`).

`iceberg::arrow::variant_projection::narrow_variant_projection` (vendor,
SQE PATCH) drops the `typed_value.<f>` subtrees no hint names. It always keeps
`metadata` and the residual `value`: SQE's writer puts a whole object in
`value` (with `typed_value` NULL) for a row whose field type does not fit
the shredding, and the shredding spec allows that. A column narrows only when
its `typed_value` is an object group holding at least one hinted name, so the
narrowed column still reads as shredded storage. Wired on the direct read,
the vendored reader (`SqeReadOptions::variant_fields`) and the worker
(`ScanTask::variant_fields`, ticket v8).

`EXPLAIN` shows `variant_fields={id:[names]}` on `IcebergScanExec`.

Tests: `variant_projection` (6), `variant_read` (6), scan_task v8 round trip,
IT `variant_e2e::reads_only_the_shredded_fields_a_query_uses` (direct and
vendored reader) and `variant_distributed::reads_only_the_shredded_fields_a_query_uses_on_a_worker`.

**NEXT:** #870 can close. Further reads could narrow on nested paths
(`$.b.c` reads all of `b`) and through `json_get_*`, which still decode the
whole column.
