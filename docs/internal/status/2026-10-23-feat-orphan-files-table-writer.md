**STATUS 2026-10-23: deleting `remove_orphan_files` runs as a table writer (`feat/orphan-files-table-writer`).**

Portal request: owners of a table should clean its orphan files without an SQE admin role.

- `procedure_tier`: `remove_orphan_files` (non-dry) moved from Admin to TableWrite. The write proof is a catalog commit made as the caller before any delete: `SetProperties { sqe.maintenance.remove-orphan-files.last-run-ms }`. The action then runs on the committed metadata. One metadata version per deleting run, admins included; no snapshot.
- `expire_snapshots` and `remove_orphan_files` evict the table cache entry after their commit (`invalidate_table_cache`).
- `SHOW CREATE TABLE` loaded `TBLPROPERTIES` from `session_catalog`, which is the config default's `SessionCatalog` (first `[catalogs.*]` entry) even under `USE other.ns`. It now loads through `show_catalog` with the name's qualifier. `show_catalog_target` answered `Session` for a session-default override, so `SHOW SCHEMAS` without `FROM` under `USE sales_wh` read ops_wh as well; it now resolves the override by name (unit test `a_session_default_override_is_resolved_by_name`). Same effect on every `show_catalog` caller: SHOW TABLES/VIEWS/STATS, ANALYZE, column comments, and `resolve_reference_catalog` (time travel, whose #317 fix assumed `Session` meant the session's own catalog). A session default that neither config nor Polaris builds (Hive root, ATTACHed catalog) falls back to the session catalog, the pre-change answer, instead of a new "does not exist" error.
- Docs: `sql-reference/procedures.md` permissions table.

Gates (all green on the final tree):
- `cargo test -p sqe-coordinator --features test-sqlite --lib -- maintenance show_ catalog_target` -> 180 passed; `-- query_handler::tests::` -> 86 passed
- `make test-integration FILTER=remove_orphan_files` -> 3 passed; `FILTER=show_` -> 10 passed; `FILTER=time_travel` -> 1 passed
- `scripts/access-control-test.sh` `remove_orphan_files_runs_as_writer_and_refuses_reader`, `expire_snapshots_runs_as_writer_and_refuses_reader`, `show_schemas_describes_the_catalog_it_names` -> 1 passed each

Caveat: a property-only commit may be authorized by Polaris `TABLE_WRITE_PROPERTIES` rather than `TABLE_WRITE_DATA`. A principal holding only the former could delete orphans. Orphans are by definition unreferenced, so the blast radius is files no snapshot reads; a still-running writer's uncommitted files are protected by `older_than`.

Gotcha: `scripts/access-control-test.sh` mints user tokens before `cargo test` compiles; a rebuild longer than the token lifetime fails the fixture with Polaris 401 "JWT is no longer valid". Pre-build with `cargo test -p sqe-coordinator --test it --no-run`.

**NEXT:** unchanged
