**STATUS 2026-10-16: SQE function overrides survive a session rebuild (`fix/udf-override-rebuild-race`).**

Found while working on !1452: `SELECT now()` flipped between `Timestamp(µs)` and `Timestamp(ns)` from run to run. Every query with a memory quota rebuilds its session (`query_memory.rs` `with_quota_over`, also `join_fallback.rs` and `sqe-catalog` `scoped_table.rs`) through `SessionStateBuilder::from(ctx.state())`. The rebuild registers each function again in `HashMap` order, and `register_udf` files a function under its name and every alias. A DataFusion built-in that still claimed a key SQE had overridden took it back whenever it came last. The order depends on each map's hash seed, so the outcome varied by connection.

Exactly three such keys, all scalar (no aggregate or window function collides):

- `now`: DataFusion's `now` still held its alias `current_timestamp`. `TrinoNow` now claims `current_timestamp` as an alias, so DataFusion's `now` holds no key and drops out. `current_timestamp` therefore changes from `Timestamp(ns, "+00:00")` to `Timestamp(µs)`, the same as `now()`. In Trino the two are also the same function.
- `date_format`: an alias of DataFusion's `to_char`.
- `element_at`: an alias of DataFusion's `map_extract`.

For the last two, `sqe_trino_functions::overrides::release_alias` re-registers the built-in through a delegating wrapper without the alias. `to_char` and `map_extract` keep their own names.

Tests:

- `overrides::shadowed_keys` lists every function that claims a key it does not hold. It must be empty for the trino-functions set and for the coordinator's full registration mirror (`custom_udfs_plan_under_duckdb_dialect`).
- `a_rebuild_resolves_every_key_to_the_same_function` builds a fresh session 50 times, rebuilds each, and compares every key's function.
- `rebuilt_sessions_run_the_trino_functions` checks the types and values at the SQL level.

Both go red with the fix disabled. `make test-integration`: 260 passed, 0 failed. The first version of the rebuild test rebuilt one map 50 times, which reuses its iteration order, and passed without the fix.

**NEXT:** Trino-aligned date/time semantics for `now()` / `current_timestamp` / `localtimestamp`. They should return `timestamp(3) with time zone` in the session zone. They should also be constant within a query: `TrinoNow` and `LocalTimestamp` read the clock on every batch, while DataFusion and Trino fold the query start time. After !1452 merges, put `SELECT now()` back into `flight_sql_smoke::advertised_schema_matches_the_streamed_one`.
