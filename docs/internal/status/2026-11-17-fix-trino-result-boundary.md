**STATUS 2026-11-17: Trino result boundary: unsigned, NaN/Infinity, json->varchar (`fix/trino-result-boundary`, openspec trino-sql-parity 2.1-2.3).**

Three result-boundary gaps the parity harness (!1830) found against Trino 484:

- **Unsigned integers.** Flight SQL's client schema (`client_schema` / `conform_batch` in `sqe-coordinator/src/flight_sql.rs`) now sends `UInt64`/`UInt32` as `Int64`, `UInt16` as `Int32` and `UInt8` as `Int16`. The cast runs with `safe: false`, so a `UInt64` above `i64::MAX` fails the query instead of turning into NULL. The conversion is at the boundary, not in an analyzer rule, because internal plans read `UInt64` (DELETE/UPDATE row positions in `write_handler.rs`). The Trino HTTP type labels in `trino_type_name` follow the same widening.
- **NaN and Infinity over HTTP.** `encode.rs` writes `"NaN"`, `"Infinity"` and `"-Infinity"` for Float32 and Float64, including inside lists; they were `null`. The decoder already reads them back.
- **`CAST(json AS varchar)`.** A new analyzer rule `sqe_json_cast` (`sqe-trino-functions/src/json_cast.rs`) runs first in `engine_analyzer_rules()`, before type coercion. It rewrites a cast of JSON-marked text to a string type into `__sqe_json_to_varchar` (or the `TRY_CAST` variant): JSON string unquoted, number/boolean as text, JSON `null` as SQL NULL, array/object an error (NULL under `TRY_CAST`), matching Trino's "Cannot cast ... to varchar" message.

Convention introduced: a cast with a **named** target field is engine-built and keeps the JSON text; the rule only rewrites casts with an unnamed field (what the SQL planner produces). The policy `PartialMask` cast in `sqe-policy/src/plan_rewriter.rs` now uses a named field so a masked JSON column is not converted.

Gate: unit tests for the five crates pass (`cargo test -p sqe-trino-protocol -p sqe-trino-functions -p sqe-catalog -p sqe-policy --lib`, `cargo test -p sqe-coordinator --lib client_schema`). `column_input_test::every_udf_returns_an_array_for_array_input` fails on `cosine_similarity` (no `arg_specs()` recipe); it fails the same way on main and gets its own branch. Integration and parity on the branch:

- `make test-integration`: 411 passed, 0 failed.
- `make test-distributed`: 419 passed, 0 failed (the first attempt timed out waiting for the coordinator port; rerun after `make test-integration-down`).
- `make test-access-control`: did not complete. Polaris answered every Keycloak token with 401 ("Failed to resolve principal"), a stack setup failure unrelated to this change; not compared against main.
- Parity (MATCH / total): queries 138/153 on HTTP and Flight (baseline 137 / 129), iceberg 20/20 on both, functions 125/296 HTTP and 121/296 Flight (baseline 121 / 115). The two query DIFFs left are `mod` and `sign` returning double, fixed in the math functions branch.

Tasks 2.1-2.3 of `openspec/changes/trino-sql-parity/tasks.md` are ticked.

Book: `features/trino-compatibility.md` (unsigned, non-finite doubles) and `sql-reference/json.md` (CAST json AS VARCHAR) describe the new behaviour.

**NEXT:** trino-sql-parity 2.4/2.5 (timestamp precision and zone), 3 (return types), 4 (type syntax).
