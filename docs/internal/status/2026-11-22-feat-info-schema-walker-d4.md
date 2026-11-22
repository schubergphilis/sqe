**STATUS 2026-11-22: Iceberg-side information_schema walk scoped (`feat/info-schema-walker-d4`, OpenSpec `information-schema-visible-catalogs` tasks 2.1-2.3, #939).**

- Problem: DataFusion's built-in `information_schema` provider answered every reference, whatever its qualifier. It walked every catalog, listed every schema and loaded every table, then filtered. It also stopped at the first table that failed to load, a 403 included. This is the follow-up left open by !1922.
- 2.1 decision: option (a), applied to the plan (`crates/sqe-catalog/src/info_schema_scope.rs`), recorded in design.md D4. Every planner entry point (`plan_sql`, `plan_sql_without_policy`, `plan_parsed`, via `finish_plan`) swaps the source of the `tables`, `columns`, `views` and `schemata` scans for a scoped provider. At scan time it narrows by the qualifier and the pushed filters (`MetaScope`, which now holds value sets: equality, exact LIKE, a literal IN list, or a same-column OR). It then runs DataFusion's own provider over a narrowed catalog list. Shapes are unchanged; filters are `Inexact`.
- 2.2: `sqe_sql::pinned_catalog(sql, session_catalog)` and `pinned_table_schemas_from_sql` (IN lists). The coordinator's pre-plan refresh uses them, so its scope covers at least what the walk reads.
- 2.3 (D6): `EnumerationSchemaProvider` maps AccessDenied from `table()` / `table_type()` to `Ok(None)`. It looks for a typed `SqeError` under `External`, `Context` and `Shared`. A 5xx or a conversion failure still fails the read (#890).
- Behaviour change: a qualified `cat.information_schema` read returns only `cat`'s rows, and a bare read under a session catalog only that catalog's rows (Trino's rule). A qualifier that contradicts the `table_catalog` filter returns no rows. Recorded in the changelog fragment.
- Tests: `cargo test -p sqe-catalog --lib` 920 passed. The new `info_schema_scope` tests:
  - run the dbt-sqe `get_catalog`, `get_columns_in_relation`, `list_schemas` and `list_relations` texts scoped and unscoped, with identical rows and a second catalog never listed or loaded;
  - cover IN lists of 1, 2, 3 and 5 elements;
  - cover D6: 403 skipped, 503 and conversion failure fail the read.

  `cargo test -p sqe-sql --lib` (696) passed. `cargo test -p sqe-coordinator --lib` filtered to `information_schema`/`info_schema`/`dbt` passed. `cargo check-server` is clean, `cargo clippy -p sqe-sql -p sqe-catalog --lib --tests` has no new warnings, and `cargo xtask budgets --check` passes.
- Budget raise: owner decision 22 Nov 2026 (#938). `query_handler.rs` 10400 -> 10450, `sqe-catalog` 69500 -> 70500, `sqe-sql` 24000 -> 24400.
- Not run: `make test-integration`, the live dbt probe (#897), and tasks 1.1/1.2 live proofs.

**NEXT:** section 3 of `information-schema-visible-catalogs` (visible catalogs, management list) as its own MR, then the live dbt `docs generate` across two catalogs (task 4.2).
