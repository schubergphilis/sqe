**STATUS 2026-11-13: dbt-sqe 1.1.0: table properties, snapshots, seed re-run (`fix/dbt-sqe-gaps-891`, #891).**

A downstream dbt test project found these gaps in dbt-sqe 1.0.0.

Changed (all under `adapters/dbt-sqe/`):

- `macros/adapters.sql`:
  - `sqe__tblproperties_clause` renders the `table_properties` / `tblproperties` config as `TBLPROPERTIES (...)`. Quotes are escaped and YAML booleans are lowercased. A non-mapping value is a compile error.
  - `sqe__create_table_as` emits the clause unless `temporary`.
  - `sqe__post_snapshot` drops the snapshot staging table, because SQE has no temporary tables.
- `macros/materializations/incremental.sql`:
  - `--full-refresh` / `full_refresh: true` rebuilds the table with CREATE OR REPLACE. Before, it appended.
  - A view in the way is dropped.
  - The delete+insert staging table is created as `temporary`, so it takes no properties.
- `macros/catalog.sql`: `get_catalog` joins `information_schema.tables` for `table_type`; `columns` has none, so every `dbt docs generate` failed. It is also scoped to the target catalog.
- `connections.py`:
  - `truncate` is in `_DML_SQL`, so its result is read on execute. A seed re-run's TRUNCATE was never read, so it never ran, and every re-seed appended.
  - `data_type_code_to_name` maps the Arrow type in `cursor.description` to an SQE DDL type. dbt's snapshot and contract macros read column types through it.
- `__version__.py`: 1.1.0.
- `requirements-lock.txt`: regenerated with `make dbt-sqe-lock`. A Renovate bump of `setup.cfg` (adbc-driver-flightsql 1.13) had left the lock on 1.12.0.

Tests: unit `test_dml_is_read_on_execute_so_a_failure_raises` (truncate case) and `test_data_type_code_to_name_renders_sqe_ddl_types`. `pytest adapters/dbt-sqe/tests` passes.

Live proof: dbt-core 1.12.6 with the adapter installed editable, against a coordinator built from main with !1777 (#890), !1778 (#893) and !1779 (#894) applied, 4 threads, on a schema emptied first. Every check passed (probe exit 0):

- **Seed.** `dbt seed` run twice, then `--full-refresh`: 3 rows each time.
- **Table properties.**
  - A `table` model's properties are set (escaped quote, lowercased boolean).
  - A re-run with a changed value updates it. This needs !1779.
  - An incremental model with the `tblproperties` alias keeps them after two runs, with 2 rows.
- **Snapshots.** `snap_ts` (timestamp) and `snap_check` (check), both with `hard_deletes: invalidate`, over four passes. Checked as row counts, current rows, versions and invalidation:
  - pass 1 (initial): 3 rows;
  - pass 2 (update id 1, delete id 3, insert id 4): 5 rows, 3 current, 2 versions of id 1, id 3 closed;
  - pass 3 (no change): still 5;
  - pass 4 (update id 2): 6 rows, 3 current.
  - No `__dbt_tmp` table is left.
- **Docs.** `dbt docs generate` passes.

Dependencies on SQE:

- Snapshots need !1778 (#893). dbt's snapshot MERGE failed with "Ambiguous reference to unqualified field id" without it.
- Parallel snapshots need !1777 (#890). Without it, a snapshot reading its columns while another runs CREATE OR REPLACE of its staging table saw no columns ("Snapshot target is missing configured columns"). It passed with 1 thread or on retry.
- Changed properties on a same-schema `table` re-run need !1779 (#894).

Found, not in this MR: `DROP SCHEMA ... CASCADE` against Polaris returns Polaris's 409 "Namespace is not empty". The coordinator does not implement CASCADE. dbt-sqe's `drop_schema` does not send CASCADE, so dbt is not affected.

**NEXT:** after !1777, !1778, !1779 and this MR merge, the downstream dbt project moves to dbt-sqe 1.1.0, drops its workarounds (bigint casts, CTE-free append model, columns macro, `create_table_as` override) and reruns its live corpus.
