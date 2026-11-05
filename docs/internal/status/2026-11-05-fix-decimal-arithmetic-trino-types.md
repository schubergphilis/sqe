**STATUS 2026-11-05: decimal arithmetic and `sum` return Trino's result types (`fix/decimal-arithmetic-trino-types`, #785).**

Follows !1656 (`avg(decimal(p,s))` returns `decimal(p,s)`).

- `DecimalArithmeticRule` (`session_tuning/decimal_arithmetic.rs`) runs before DataFusion's type coercion and replaces decimal `+ - * /` with `__sqe_decimal_arith(a, b, op, type)` when Trino's type differs from arrow's, or when the result has 38 digits (arrow checks the i128 range, not the precision). Running before coercion means comparisons, CASE and UNION coerce against the new type. The UDF computes exactly (Decimal256 when needed, a quotient to one guard digit) and rounds half away from zero. Workers register the UDF too, for pushed-down filters.
- `TrinoSum` wraps DataFusion's `sum` and returns `decimal(38,s)`; the accumulators take their type from the return type.
- Prepared statements now advertise the analyzed schema, like GetFlightInfo already did.
- Every type rule is checked against Trino 481 `typeof` probes (`result_types_match_trino`).
- Known gap: DataFusion types a CASE over `decimal(7,2)` and an integer literal as `decimal(22,2)` (Trino: `decimal(12,2)`), so arithmetic over it has a wider precision than Trino's. The scale and the value match.

TPC-DS q58, q61 and q78 differed from Trino only in decimal digits. They need a rerun of the SF10 compare on this branch (main now carries the `avg` and rollup fixes too).

**NEXT:** rerun the SF10 TPC-DS compare on this branch; run `make test-integration` for golden decimal strings.
