**STATUS 2026-10-21: nested `ALTER COLUMN s.a SET DATA TYPE` (`feat/iceberg-nested-type-promotion-859`, #859; stacked on `fix/iceberg-unknown-list-map-859`, !1501).**

The matrix cells `type-promotion:v3` and `unknown-type:v3` were already full
and stay full (150/162); this branch removes their "top-level columns only"
and "nested unknown member cannot be promoted" caveats.

- `sqe-sql/src/classifier.rs`: `StatementKind::AlterNestedColumnType`,
  detected before sqlparser (which rejects the dotted name) for
  `ALTER COLUMN a.b [SET DATA] TYPE T`; the type is parsed with sqlparser and
  trailing text is a syntax error. Listed with `DropNestedColumn` in the
  ALTER TABLE privilege mapping and the MCP write gate.
- `sqe-coordinator/src/catalog_ops.rs`: `alter_nested_column_type` +
  `set_struct_member_type`. The member and every ancestor are cloned with
  only the type swapped (ids, nullability, doc and defaults kept); the leaf
  goes through `check_type_promotion`.
- `sqe-catalog/src/iceberg_scan.rs`: a struct/list/map column whose decoded
  type differs from the table's is rebuilt member by member with
  `fill_unknown` (widening promoted members). The worker path
  (`sqe-worker/src/projection_fill.rs`) already called `fill_unknown` on
  every column, and a type change attaches the projection fill.

Decision: `fill_unknown` still falls back to an Arrow cast for a primitive
leaf mismatch rather than refusing it. Writes are gated by
`check_type_promotion`, and the worker calls `fill_unknown` on every column,
so making it strict risks refusing representational differences
(Utf8/Utf8View and similar) that read today.

Tests: unit `a_dotted_alter_column_type_classifies_to_alternestedcolumntype`,
`a_nested_member_is_promoted_in_place_keeping_every_id`,
`a_promoted_struct_member_reads_widened_from_an_older_file`; integration
`unknown_type_spark_interop::nested_type_promotion_reads_in_sqe_and_spark`
(promotes `s.a`, `s.f`, `s.u`, `s.d`, refuses four illegal changes, SQE and
Spark read both rows).

Gate: `make test-integration FILTER=nested_type_promotion` -> 1 passed, no skips.

Left open: a list element or map value (`l.element`, `m.value`) cannot be
promoted.

**NEXT:** retarget the stack (!1498 -> !1500 -> !1501 -> this MR) to main as
each parent merges; then the next matrix gap.
