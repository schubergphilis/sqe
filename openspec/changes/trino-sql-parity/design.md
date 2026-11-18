## Context

The parity stack (`scripts/parity-test.sh`) runs SQE and Trino 484 against one Polaris catalog, so both engines read the same Iceberg files. Before this change the comparator rounded numbers to two places, read numeric strings as floats and ignored column types; it only spoke the Trino HTTP protocol.

### Baseline, 2026-11-17 (SQE at origin/main f3c5ac427)

| Suite | Denominator | HTTP | Flight |
|---|---|---|---|
| Queries (`queries.json`) | 153 | 137 MATCH (14 TYPE, 2 DIFF) | 129 MATCH (22 TYPE, 2 DIFF) |
| Iceberg scenarios | 20 | 20 | 20 |
| Trino functions (`SHOW FUNCTIONS`, Iceberg-typed) | 296 | 121 | 115 |

Function verdicts (worst signature per function, HTTP): 92 MISSING, 29 SQE-ERR, 31 DIFF, 18 TYPE, 5 UNTESTED. The full table is in `docs/evidence/trino-parity/2026-11-17-functions.md`.

What the gaps have in common:

| Root cause | Rows it explains |
|---|---|
| Integer literals are `bigint` in DataFusion, `integer` in Trino | 8 query TYPE rows (`abs(-5)`, `greatest(size, 10)`, `CASE ... THEN 1`, `cast('42' AS integer) + 1`, ...) |
| `UInt64` results reach Flight clients unsigned | 8 Flight-only query TYPE rows (`row_number`, `rank`, `dense_rank`, `ntile`, `cardinality`) |
| Built-in return types differ (`length` Int32, `extract` Int32, `mod`/`sign` Float64) | 5 query TYPE rows, 2 query DIFF rows |
| Trino type syntax (`array(T)`, `time(p)`) does not parse | 17 function SQE-ERR signatures |
| HTTP encoder: NaN sent as NULL; `json` label on a varchar | `asin`/`acos` HTTP-only DIFF; `json_cast` HTTP-only TYPE |
| Timestamp precision is fixed at 6 (HTTP) or the Arrow unit (Flight); zone dropped from `TIMESTAMP '... UTC'` and `current_timestamp` | `at_timezone`, `with_timezone` and every timestamp-returning function's TYPE row |

The harness sends `X-Trino-Client-Capabilities: PARAMETRIC_DATETIME`, as the JDBC driver does. Without it Trino reports `timestamp with time zone` with no precision, which hid the precision gap.

## Goals / Non-Goals

**Goals:**
- Every query MATCH (or BOTH-ERR) on both protocols.
- Every Iceberg scenario MATCH on both protocols.
- Every function in Trino's list whose signature uses Iceberg-representable types covered on both protocols, with values (not only typed NULLs).

**Non-Goals:**
- Trino-only types (HyperLogLog, Geometry, IPADDRESS, SetDigest, ...). They are listed in the report as scope `trino` and not counted.
- Error message text. A statement both engines reject is parity.
- Trino's default Iceberg format version. SQE creates v3 tables, Trino 484 v2; neither allows a downgrade, so the scenario sets the version explicitly. Changing SQE's default is a product decision, not a parity fix.

## Decisions

**Denominator from Trino.** The function list is Trino's `SHOW FUNCTIONS`, filtered to signatures whose types Iceberg (or plain SQL: interval, json) can carry. A signature the generator cannot call on Trino counts as UNTESTED, against the function, until the generator learns it; it never leaves the denominator.

**Fix at the narrowest shared point.** The Trino rewrites run in the shared front door (`crates/sqe-sql/src/front_door.rs`, `apply_trino_compat`), so a rewrite there fixes both protocols. Result-type coercion (unsigned to signed) goes at the result boundary both protocols share, not into each function.

**Upstream first.** Before adding or shadowing a function, check DataFusion. A Trino-compat UDF that shadows a DataFusion built-in states why (the Trino semantic it needs) in its doc comment.

**Integer literals last, on their own.** Typing literals as `integer` changes every plan that mentions a number. It goes in its own MR after the function work, so a regression there is not mixed with function fixes.

### Timestamps (tasks 2.4 and 2.5)

Trino types a timestamp by its precision `p` (0 to 12) and an optional zone. SQE holds timestamps in Arrow, which has four units: second, millisecond, microsecond, nanosecond. Iceberg stores `timestamp`/`timestamptz` as microseconds and, in v3, `timestamp_ns`/`timestamptz_ns` as nanoseconds, always as UTC instants. The engine runs with `datafusion.execution.time_zone = "+00:00"` and has no session zone.

**Precision label comes from the Arrow unit.** Second is `timestamp(0)`, millisecond `timestamp(3)`, microsecond `timestamp(6)`, nanosecond `timestamp(9)`, on both protocols. HTTP renders that many fraction digits. The fixed 6 in `normalize_timestamp_fraction` and `trino_type_name` goes away; the reason for it (#5: a nanosecond `date_trunc` and a microsecond CAST rendered differently under one label) no longer holds once label and digits both follow the unit. Every remaining nanosecond producer flips from 6 to 9, so the MR lists them from a `typeof` probe before and after, and the parity TYPE count must not rise.

**Literals and casts get Trino's precision in the front door.** `apply_trino_compat` rewrites:
- `TIMESTAMP '<text>'` to `CAST('<text>' AS TIMESTAMP(p))`, with `p` the literal's fraction digits rounded up to 0, 3, 6 or 9. A literal with a zone becomes `TIMESTAMP(p) WITH TIME ZONE` and keeps its zone (below).
- `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` without a precision, inside `CAST`/`TRY_CAST` only, to `TIMESTAMP(3)`. DDL column types are not touched: `CREATE TABLE t (ts timestamp)` stays microseconds, which is what Iceberg stores and what Trino's Iceberg connector reports (`timestamp(6)`).
- `TIMESTAMP(p)` with `p` outside 0/3/6/9 to the next of those. DataFusion refuses those precisions today; after the rewrite they work, and the label differs from Trino (`timestamp(2)` reports `timestamp(3)`). Recorded as a known DIFF.

**Precision reduction rounds.** Trino rounds half up when a cast drops fraction digits; Arrow's unit cast truncates. The MR confirms Trino's result for `CAST(TIMESTAMP '2020-01-01 00:00:00.7896' AS timestamp(3))` on the parity stack and, if it rounds, adds the rounding to the narrowing cast (an analyzer rule over `Cast` between timestamp units, not a per-call rewrite).

**Zone.** A literal keeps its zone, as Trino does: `TIMESTAMP '2024-01-01 10:00 Europe/Amsterdam'` renders in that zone. A zone spelled `+00:00`, `Z` or empty becomes `UTC`, the string the datetime functions use. Two consequences are handled where they bite:
- Iceberg only accepts UTC zones. CTAS (`write_schema_for_format_version`) normalises any zoned timestamp to `+00:00`; the stored instant is the same.
- A cast between two zoned timestamps changes metadata only, never the stored value, so `cast_is_exact` accepts any zoned to zoned cast at one unit. A predicate with the literal on the left (`TIMESTAMP '... Europe/Amsterdam' < ts`) then still pushes down.

**`now()` and friends.** `current_timestamp` and `now()` return `timestamp(3) with time zone` in the session zone (`UTC`); `localtimestamp` returns `timestamp(3)`. `at_timezone` and `with_timezone` keep the input's unit instead of forcing microseconds. The common filter `naive_ts > now() - INTERVAL '7' DAY` coerces the column to the zoned type; `cast_is_exact` already treats a no-zone to UTC cast as exact under the fixed `+00:00` engine zone, and the MR pins both operand orders on a `timestamp` and a `timestamptz` column.

**Pushdown of coarse literals.** `scalar_value_to_datum` maps only microsecond and nanosecond scalars. Second and millisecond literals now reach paths that skip coercion (DELETE, UPDATE and MERGE predicates, distributed scan filters), so it scales them to microseconds. Narrowing casts are still not unwrapped: a filter on `CAST(ts_col AS TIMESTAMP)` now narrows the column to milliseconds and loses pruning, as in Trino's own Iceberg connector.

**Write path.** CTAS of `CAST(x AS TIMESTAMP)` now writes a microsecond `timestamp` (milliseconds widened) on v2 and v3, where a v3 table previously got `timestamp_ns`. That matches Trino. DDL `timestamp(0..3)` columns, which fail today because DDL has no widening step, are widened to microseconds the way CTAS widens.

**Out of scope.** The session zone stays unwired: `SET TIME ZONE` is logged only and `X-Trino-Time-Zone` is not read. `to_iso8601` belongs to the datetime-functions change.

**Split.** 2.4 (precision) and 2.5 (zone) are separate commits; each runs the full integration suite, the distributed suite and a parity run, because every timestamp literal changes type.

## Risks / Trade-offs

- **Policy guard.** A rewrite that adds a cast or arithmetic around a column can turn a leakproof predicate into a fallible one, which reads row-filtered scans through the `CASE WHEN` guard: slower, and a silent change. Each rewrite MR runs `make test-access-control`.
- **Pushdown.** A cast on a column side of a predicate defeats Iceberg pruning. Literal typing and return-type changes must keep predicates on raw columns; the MR that changes literals compares a TPC-H run before and after.
- **Overflow.** `integer` arithmetic overflows at 2^31 in Trino (an error). Typing literals as `integer` brings that behaviour; the MR states it and tests it against Trino. DataFusion's integer operators wrap and have no setting to check, so a narrow literal alone made `2147483647 + 1` wrap to `-2147483648` where it used to be a correct `bigint`. `CheckedIntegerArithmeticRule` runs after type coercion and sends `tinyint`, `smallint` and `integer` `+ - *` through arrow's checked kernels. A follow-up change extends the rule to `bigint` and to unary minus (`-x` on the smallest value fails with `integer negation overflow`, as in Trino); its TPC-H SF10 compare showed no measurable cost (0.97 of main's total). `-2147483648` stays `bigint`, as in Trino: its parser negates the literal `2147483648`, which is outside the `integer` range.
- **Recursive CTEs.** The work table of `WITH RECURSIVE` is typed when the SQL is planned, before any analyzer rule runs, so a narrowed anchor term no longer matches it. Plans holding a `RecursiveQuery` keep `bigint` literals.
- **Flight clients that relied on unsigned types.** Few, and Trino never sent them; noted in the changelog.
