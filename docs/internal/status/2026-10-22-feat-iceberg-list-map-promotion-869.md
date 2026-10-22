**STATUS 2026-10-22: `ALTER COLUMN ... SET DATA TYPE` promotes list elements and map values (`feat/iceberg-list-map-promotion-869`, #869).**

#869 tracked the two caveats left on `sqe:type-promotion:v3` after #859:
list elements / map values could not be promoted, and date -> timestamp was
refused.

- `crates/sqe-coordinator/src/catalog_ops.rs`: `set_nested_type` replaces
  `set_struct_member_type`. It walks a dotted path through structs (member
  name), lists (`element`) and maps (`value`), keeps every field id and
  nullability, and runs `check_type_promotion` on the primitive at the end.
  A map key, a wrong segment (`l.item`), a path through a primitive, or a
  non-primitive target is refused with the column named. Unit test
  `a_list_element_and_a_map_value_are_promoted_keeping_every_id`.
- No read-path change was needed: the coordinator's `fill_unknown`, the
  iceberg-rust `PromotePlan` and the worker already cast through List and
  Map.
- `crates/sqe-coordinator/tests/it/unknown_type_spark_interop.rs`:
  `list_element_and_map_value_promotion_reads_in_sqe_and_spark` promotes
  `l.element` int -> bigint and `m.value` real -> double, writes a row on
  each side of the change, and reads both in SQE and Spark 4.1
  (`array<bigint>`, `map<int,double>`).
- date -> timestamp stays refused: Iceberg Java 1.12 (Spark 4.1) refuses
  "Cannot change column type: d: date -> timestamp", so a table SQE promoted
  would not be readable by any other engine. Caveat reworded; split to
  #871 (`blocked::upstream`).
- Matrix, `ddl.md`, `limitations.md` and `roadmap.md` updated. Score
  unchanged (cell was already full).

Gate:
- `cargo test -p sqe-coordinator --lib -- promoted type_promotion`: 3 passed.
- `make test-integration FILTER=type_promotion`: 3 passed;
  `FILTER=map_value_promotion`: 1 passed, Spark leg ran.

**NEXT:** re-render `iceberg-matrix.md` and sync the site with #855;
date -> timestamp waits on Iceberg Java (#871).
