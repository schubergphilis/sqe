# 703: A dozen extended Trino UDFs only implement the scalar case and silently return NULL for every row when given a column

- Scope: sqe-trino-functions (trino_functions_ext.rs)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open
- Issue: #623

## Problem

Several UDFs in `crates/sqe-trino-functions/src/trino_functions_ext.rs` match only `ColumnarValue::Scalar` inputs and collapse everything else to a NULL scalar. When DataFusion passes a real column (`ColumnarValue::Array`), the function returns one NULL which is broadcast to the whole batch.

`at_timezone`, `trino_functions_ext.rs:1096-1126`:

```rust
match (&args.args[0], &args.args[1]) {
    (
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), _)),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str))),
    ) => { ... }
    _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, None))),
}
```

Same shape, verified by reading each `invoke_with_args`:

| Function | Fallback line | Returns |
|---|---|---|
| `hamming_distance` | 370 | `Int64(None)` |
| `from_base` / `to_base` | 396-485 | NULL |
| `from_iso8601_timestamp` | 537-579 | NULL |
| `to_iso8601` | 606 | `Utf8(None)` |
| `with_timezone` | 1038-1075 | NULL |
| `at_timezone` | 1123 | NULL |
| `format_datetime` | 1157 | `Utf8(None)` |
| `parse_datetime` | 1166-1215 | NULL |
| `json_array_contains` | 251 | `Boolean(None)` |
| `json_size` | 1500-1547 | NULL |
| `json_array_get` | 1574 | `Utf8(None)` |

The signatures are `Signature::any(2, Volatility::Immutable)` (`:1085-1086`, `:1139-1140`), so DataFusion does not reject column input at plan time. The test suite never feeds a column: every test is `SELECT f('literal')` (`crates/sqe-trino-functions/tests/trino_udf_test.rs:276`, `:400`, `:490`, `:741`), which the const evaluator folds to a scalar before `invoke_with_args` ever sees an array. The tests pass and the bug is invisible.

A second defect in the same block: `at_timezone::return_type` declares `Timestamp(Microsecond, None)` (`:1089-1094`) but the scalar arm returns `TimestampMicrosecond(Some(us), Some(tz))` (`:1118-1121`), so the runtime array type does not match the declared schema.

## Why it matters

`SELECT format_datetime(created_at, 'yyyy-MM-dd') FROM events` returns a column of NULLs with no error. dbt models and BI dashboards built on Trino idioms produce wrong numbers rather than failing. This is a silent-wrong-result class of bug on the read path, the worst class for an analytics engine. It has been present since these functions were added and is undetectable by the current tests.

## Proposed change

1. Add one helper in `crates/sqe-trino-functions/src/helpers.rs` that lifts a per-row closure over any combination of scalar and array inputs for the common arities, following the pattern `str_transform` already uses for strings (`helpers.rs:22-46`) but generic over Arrow primitive types. Rewrite the eleven functions above on top of it.
2. Where an input type is genuinely unsupported, return `DataFusionError::Plan` with the Trino message, never NULL.
3. Fix `at_timezone`/`with_timezone` return types to `Timestamp(Microsecond, Some(tz))` when the tz argument is a literal, else error at plan time (Trino requires a constant zone).
4. Make the test harness defeat constant folding: register a one-column `MemTable` and call every UDF once via `SELECT f(col) FROM t` in addition to the literal form. Add a `#[test]` that iterates all registered UDFs and asserts none returns a `Scalar` when handed an `Array` (invoke `invoke_with_args` directly with a 3-row array and check `ColumnarValue::Array` with 3 rows).

## Acceptance criteria

- Every UDF in `register_extended_trino_functions` passes a column-fed test with at least one non-null row and the expected value.
- `cargo test -p sqe-trino-functions` includes the array-shape invariant test and it fails if any UDF returns a scalar for array input.
- `crates/sqe-trino-functions/src/trino_functions_ext.rs` has zero `_ => Ok(ColumnarValue::Scalar(ScalarValue::*(None` arms.

## Related

- 709 (UDF error classification).
- `docs/internal/reviews/2026-06-26-codebase-analysis.md` item on UDF executors lacking precondition docs still applies.
- Memory: Puffin NDV consumer notes Utf8View gaps in UDF paths; `helpers.rs:36-40` and `trino_functions.rs:2059-2062` downcast to `StringArray` only, so `Utf8View` columns error with `Internal("Expected StringArray")` on the same functions.
