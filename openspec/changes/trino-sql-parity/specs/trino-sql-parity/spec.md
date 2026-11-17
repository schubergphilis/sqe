## ADDED Requirements

### Requirement: Parity is measured strictly against Trino
The parity harness SHALL compare SQE with Trino on column types, on values canonicalised by Trino's column type, and on row order when the query has a top-level `ORDER BY`. It SHALL read SQE over the Trino HTTP protocol and over Flight SQL. A rejection by both engines SHALL count as parity.

#### Scenario: A type difference fails the run
- **WHEN** SQE returns `length(name)` as `integer` and Trino as `bigint`, with the same values
- **THEN** the query's verdict is TYPE and `parity_compare.py` exits non-zero

#### Scenario: Both protocols are measured
- **WHEN** `scripts/parity-test.sh --all --protocol both` runs
- **THEN** each suite reports a separate count for `http` and `flight`

### Requirement: The function denominator is Trino's
Function coverage SHALL be measured over every scalar, aggregate and window signature Trino's `SHOW FUNCTIONS` lists whose types Iceberg or plain SQL can carry. A signature the generator cannot call on Trino SHALL count against its function. A function SHALL count as covered only when every counted signature matches, and a function matched only through typed-NULL arguments SHALL be reported apart.

#### Scenario: A generator gap is not hidden
- **WHEN** no generated call to `listagg` is accepted by Trino
- **THEN** `listagg` is reported UNTESTED and is not covered

### Requirement: SQE matches Trino on the parity sets
SQE SHALL return Trino's column types and values for every query in `tests/parity/queries.json`, every scenario in `tests/parity/iceberg_scenarios.json` and every counted function signature, over both protocols.

#### Scenario: Window ranks over Flight
- **WHEN** a Flight SQL client runs `SELECT row_number() OVER (ORDER BY 1)`
- **THEN** the column's Arrow type is `Int64`, which Trino calls `bigint`

#### Scenario: Integer-preserving math
- **WHEN** a client runs `SELECT mod(size, 3), sign(-3), ceil(BIGINT '7') FROM part`
- **THEN** the results are integers of Trino's types, not doubles

#### Scenario: Trino type syntax
- **WHEN** a client runs `SELECT CAST(NULL AS array(double))`
- **THEN** the statement parses and returns one NULL of type `array(double)`

### Requirement: Compatibility work keeps policy and pushdown behaviour
A rewrite added for Trino compatibility SHALL NOT make a leakproof predicate fallible under `crates/sqe-policy/src/leakproof.rs`, and SHALL NOT wrap the column side of a pushed-down predicate in a cast or function.

#### Scenario: Row-filtered scan stays unguarded
- **WHEN** a user with a row filter on `orders` runs `SELECT count(*) FROM orders WHERE orderkey > 10`
- **THEN** the scan reads the filter as before, without the `CASE WHEN` guard, and the access-control suite passes
