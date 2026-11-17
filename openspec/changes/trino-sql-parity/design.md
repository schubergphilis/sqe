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

## Risks / Trade-offs

- **Policy guard.** A rewrite that adds a cast or arithmetic around a column can turn a leakproof predicate into a fallible one, which reads row-filtered scans through the `CASE WHEN` guard: slower, and a silent change. Each rewrite MR runs `make test-access-control`.
- **Pushdown.** A cast on a column side of a predicate defeats Iceberg pruning. Literal typing and return-type changes must keep predicates on raw columns; the MR that changes literals compares a TPC-H run before and after.
- **Overflow.** `integer` arithmetic overflows at 2^31 in Trino (an error). Typing literals as `integer` brings that behaviour; the MR states it and tests it against Trino.
- **Flight clients that relied on unsigned types.** Few, and Trino never sent them; noted in the changelog.
