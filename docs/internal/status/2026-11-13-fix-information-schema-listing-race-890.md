**STATUS 2026-11-13: `information_schema.columns` keeps its answer under concurrent DDL (`fix/information-schema-listing-race-890`, #890).**

A downstream dbt test project saw `information_schema.columns` return no rows for an existing table while other models ran `CREATE OR REPLACE`; dbt's merge strategy then rendered empty `UPDATE SET` / `INSERT ()` lists.

Cause: `SqeSchemaProvider::table_names()` serves the process-wide `TABLE_NAMES_SNAPSHOTS`, and every commit deleted the namespace's entry. A commit between the pre-plan refresh and execution left no names, so DataFusion's `make_columns` produced zero rows.

Changed (`crates/sqe-catalog/src/schema_provider.rs`):

- Invalidation marks the snapshot `invalidated` (names kept, `needs_refresh` true, `table_exist` treats it as unknown as before) instead of deleting it.
- `LISTING_EPOCH` fence: a refresh that overlapped an invalidation is stored marked, so a listing taken before a commit never passes as fresh for the TTL.
- `table()`: a listed name that is not found as table or view is retried 3 times (25/50/100 ms); a Unity-style not-Iceberg name is not retried. A `SqeTableProvider::try_new` failure (schema SQE cannot convert) is now returned as the error instead of falling through to "not found". Blast radius: an `information_schema.columns` read over a namespace holding such a table fails with that table's error, as a 403 or 5xx on it already did.
- dbt-sqe: `sqe__get_columns_in_relation` raises when it finds no columns for a relation `information_schema.tables` lists (a missing relation still returns `[]`, which dbt-core relies on); the merge strategy refuses an empty column list.

Tests: unit `ddl_invalidation_marks_every_tokens_listing_of_the_namespace`, `refresh_outcome_decides_whether_the_mark_stays`; integration `information_schema_listing_race_e2e::information_schema_columns_survive_concurrent_replace_890` (4 sessions replacing tables and views, one with schema-changing replaces, while every read of an untouched table must return its 2 columns; it reads at least 300 times and until at least 100 DDL statements have committed).

Without the fix (base `schema_provider.rs`, same test) 321 of 568 reads were short.

Left open (#892, so this MR is `Part of #890`): a schema-changing `CREATE OR REPLACE` stages `<name>__sqe_replace_<uuid>` and then drops and renames (`write_handler.rs`). Between the two catalog calls the table does not exist, so a listing or lookup in that window cannot see it. With the fix, reads of the table being replaced were still short 2-4 times in 300 when the test read it; the retry covers short windows only. The fix is an atomic replace (one metadata commit on the existing table).

Gate: `make test-integration FILTER=survive_concurrent_replace_890` -> 1 passed (gate=0). Neighbouring suites green on this branch: `unknown_type` 7, `trino_metadata` 3, `view_replace` 1, `ctas_write_modes` 12, `information_schema` 5. `cargo test -p sqe-catalog --lib schema_provider` 11 passed; clippy `-p sqe-catalog --all-targets -D warnings` clean; budgets ok.

Not run: a live dbt run of the dbt-sqe guard (seed, snapshot, docs generate on a missing relation); the guard only raises when `information_schema.tables` lists the relation, so a missing relation keeps the `[]` dbt-core expects.

**NEXT:** #892 (atomic schema-changing replace). The downstream project can drop its `get_columns_in_relation` stopgap once it runs an SQE build carrying this commit, provided its models do not read a table another model is replacing with a new schema at the same moment; the dbt-sqe guard turns that remaining case into an error instead of an empty MERGE.
