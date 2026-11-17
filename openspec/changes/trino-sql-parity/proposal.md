## Why

SQE speaks the Trino HTTP protocol and Flight SQL, and its users write Trino SQL: BI tools through the Trino JDBC driver, dbt models and notebooks moved over from Trino. A query that runs on both engines but returns `integer` where Trino returns `bigint`, `7.0` where Trino returns `7`, or an error where Trino has a function, breaks those users in ways a "lenient" comparison hides. The parity harness scored 153 of 153 with numbers rounded to two places and types ignored; read strictly, SQE matches Trino on 137 of 153 queries over HTTP and 129 over Flight, and covers 121 of 296 Trino functions.

The goal is 100% against a denominator Trino defines, not one we pick.

## What Changes

- **Harness (done in the first MR).** `tests/parity/` compares strictly (column types, values canonicalised by Trino's type, order under `ORDER BY`) over the Trino HTTP protocol and Flight SQL. `function_coverage.py` takes its function list from Trino's own `SHOW FUNCTIONS`. `scripts/parity-test.sh --all --protocol both` runs the three suites.
- **Result types at the protocol boundary.** Trino has no unsigned types: a `UInt64` result (`row_number`, `rank`, `cardinality`, `approx_distinct`) is `bigint` over both protocols, not only over HTTP. The HTTP type label of a JSON value, a NaN and a timestamp with time zone matches what Trino sends.
- **Function return types.** `length`, `strpos`, `position` and `extract` return `bigint`; `mod`, `sign`, `ceil` and `floor` keep an integer argument's type; `abs` and `avg` keep a decimal's precision and scale; aggregates over `real` return `real`; bitwise shifts keep the argument's width.
- **Trino type syntax.** `CAST(x AS array(double))`, `map(K, V)`, `row(a T, ...)` and `time(p)` parse.
- **Function semantics.** Each `DIFF` in the baseline is fixed to Trino's result (for example `array_remove`, `concat` over arrays, `date_trunc` on a date, `format_datetime` patterns, `checksum`).
- **Missing functions.** The 92 functions (or signatures) that do not resolve on SQE are added, DataFusion first.
- **Integer literals as `integer`.** Trino types `1` as `integer`; DataFusion types it as `bigint`. This touches every plan, so it comes last and on its own.

## Capabilities

### New Capabilities
- `trino-sql-parity`: SQE results match Trino 484's on the parity query set, the Iceberg scenario set and Trino's function list, over the Trino HTTP protocol and Flight SQL.

### Modified Capabilities
<!-- None: no existing spec covers result parity with Trino. -->

## Impact

- `tests/parity/`, `scripts/parity-test.sh`: the harness.
- `sqe-sql` (front door, Trino rewrites, type parsing), `sqe-trino-functions` or wherever Trino-compat UDFs live, the Trino HTTP result encoder and the Flight result schema.
- Policy: none of the rewrites may add expressions the leakproof classifier (`crates/sqe-policy/src/leakproof.rs`) treats as fallible, or row-filtered scans silently switch to the guarded read.
- Docs: the function reference in the book lists what is Trino-compatible.
