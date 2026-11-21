**STATUS 2026-11-21: dbt-sqe 1.1.1: metadata reads name the profile's catalog; a seed re-run keeps its rows (`fix/dbt-sqe-catalog-qualified-relations`).**

A downstream project on dbt-sqe 1.1.0 still saw a second `dbt seed` double the rows (3 became 6). The TRUNCATE fix of 1.1.0 (#891) was never reached: dbt did not find the seed table, so it created it (`CREATE TABLE IF NOT EXISTS`, a no-op) and inserted the rows again.

Cause: every metadata read in the adapter used a bare `information_schema.<view>`. The SQL front door qualifies that with the session catalog (`engine_front_door`), and dbt-sqe never sets one. The pre-planning refresh (#664) loads a namespace's tables for an information_schema read only in the catalogs the statement names. A profile whose catalog is not the server default (a workspace catalog) therefore listed none of its relations.

Changed (all under `adapters/dbt-sqe/`):

- `macros/adapters.sql`: `sqe__information_schema_view(database, view)` renders `"<database>".information_schema.<view>`. `list_relations_without_caching`, `get_columns_in_relation` (both reads), `list_schemas` and `check_schema_exists` use it. The `table_catalog` / `catalog_name` filters stay, because the listing is global.
- `macros/catalog.sql`: `get_catalog` reads both views through it.
- `macros/materializations/seed.sql`: `create_csv_table` runs `DROP TABLE IF EXISTS` then `CREATE TABLE`, so a seed whose lookup misses an existing table replaces it instead of appending. With the lookup working, dbt's default path truncates and loads, as before.
- `__version__.py`: 1.1.1. No dependency change; the lock is unchanged.

Tests: `tests/test_relation_lookup.py` runs real dbt commands (`dbtRunner`) against an in-process fake of the Flight SQL connection that answers a bare `information_schema` from the session catalog only, as SQE does. It covers: seed twice keeps 3 rows (and truncates), seed with a missed lookup keeps 3 rows, an incremental model's second run appends instead of rebuilding, a table model drops a view in the way, and no metadata read is unqualified. All five fail on the 1.1.0 macros (seed: 6 rows) and pass now. `pytest adapters/dbt-sqe/tests`: 34 passed.

Not run here: a live run against a coordinator. The downstream live seed-twice test is the proof once 1.1.1 is vendored.

**NEXT:** vendor dbt-sqe 1.1.1 downstream and run its live seed-twice test.
