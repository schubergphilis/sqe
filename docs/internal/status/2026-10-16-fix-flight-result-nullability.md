**STATUS 2026-10-16: Flight SQL results agree on nullability, and SHOW / DESCRIBE / EXPLAIN advertise their columns (`fix/flight-result-nullability`).**

Follow-up to !1450, which made GetFlightInfo plan through the same path as DoGet. A JDBC and ADBC check against a debug `sqe-server` (`flight_compression = "none"`, passthrough policy) found two more problems:

1. **Nested and constant nullability.** The logical plan behind FlightInfo and the physical plan behind DoGet disagree on nullability for list items, struct children and folded constants. `flight_sql.rs` `client_schema` now describes every result with all fields nullable, nested ones included; map entries and keys stay non-null, as the Arrow spec requires. `advertised_schema_bytes` applies it, and `client_batch_stream` conforms DoGet batches to it. A column is cast only when its nested type differs, and the values are unchanged.
2. **Arrow JDBC read SHOW / DESCRIBE / EXPLAIN as updates.** A plain `Statement` failed with "Statement did not return a result set", because the driver treats a `dataset_schema` with no fields as an update. `get_schema` now runs the read-only result-set kinds (`is_read_only_result_set`: the SHOW family, DESCRIBE, EXPLAIN without ANALYZE) and returns their result's schema. The statement runs twice, at GetFlightInfo and at DoGet, and the audit log records both runs. DDL, DML, EXPLAIN ANALYZE and SET still advertise no schema.

Limits:
- GetFlightInfo describes the plan before policy enforcement, and DoGet streams the enforced one. Built-in masks (nullify, redact, hash, partial) cast back to the column's type (`plan_rewriter.rs` `apply_mask`), and restricted columns are nullified in place, so the schemas still agree. A custom mask expression whose type differs from the column's would still mismatch.
- The GetFlightInfo run has no client IP in its audit row; the DoGet run does.
- `SHOW STATS` and `SHOW PARTITIONS` are on the list too, so their file listing or Puffin read runs twice.

The wire test now also covers a timestamp, a list, a struct, a decimal, `DESCRIBE`, `SHOW CATALOGS` and the prepared path (`dataset_schema` plus the prepared FlightInfo), and it requires a schema to be advertised. It went red first. A new unit test, `client_schema_relaxes_nested_fields_but_map_keys`, checks the map rule.

`make test-integration` passes: 260 passed, 0 failed.

Three-driver matrix: 31 type queries, plus SHOW / DESCRIBE / EXPLAIN, `now()`, list, struct, map and an Iceberg table with every column type. Each runs plain and prepared:
- ADBC: everything passes except `SELECT now()`. That failure is a separate bug: the per-query quota context rebuild (`query_memory.rs` `with_quota_over`, `SessionStateBuilder::from(ctx.state())`) re-registers UDFs in HashMap order, so DataFusion's built-in `now` (aliased `current_timestamp`) sometimes overwrites SQE's Trino `now`, and the type flips between Microsecond and Nanosecond. That is why the wire test uses a timestamp literal.
- Arrow Flight SQL JDBC 19.0.0 (on Java 25 with `--add-opens` / `--sun-misc-unsafe-memory-access=allow`): everything passes, plain `Statement` SHOW / DESCRIBE / EXPLAIN included.
- Arrow Flight SQL JDBC 18.3.0: fails only where the driver itself lacks support. It cannot read Utf8View ("Unsupported", from `CAST AS VARCHAR`), and `getObject` on a time-zoned timestamp is "not implemented".
- Trino JDBC 476 over HTTPS: everything passes except `INTERVAL '2' MONTH`: "Invalid day-time interval: 0-2".
- With the default `flight_compression = "lz4"`, neither Arrow JDBC 18.3.0 nor 19.0.0 can read any result ("Please add arrow-compression module ... LZ4_FRAME").

**NEXT:**
- Fix the `now()` registration race: give `TrinoNow` the `current_timestamp` alias, and check the other Trino overrides of aliased built-ins.
- Decide how JDBC clients get uncompressed Flight results: document it, change the default, or add a per-connection override.
