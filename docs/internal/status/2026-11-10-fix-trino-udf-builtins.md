**STATUS 2026-11-10: `arbitrary()` is an alias of DataFusion's `any_value` aggregate (`fix/trino-udf-builtins`).**

The scalar `Arbitrary` stub in `trino_functions_ext.rs` is gone. `arbitrary` is
now registered next to the other Trino aggregate aliases in
`trino_functions.rs`, as `any_value_udaf().with_aliases(["arbitrary"])`.
`GROUP BY` queries now plan and return one value per group. The new test is
`trino_udf_test::test_arbitrary_group_by`. The scalar call `SELECT
arbitrary(42)` still works.

The other Trino scalars that look like DataFusion built-ins (`sign`,
`to_base64`, `from_base64`, `bitwise_*`) stay: their return types follow
Trino, not DataFusion's `signum` / `encode`.

**NEXT:** unchanged
