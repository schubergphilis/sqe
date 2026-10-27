**STATUS 2026-10-27: two distributed-scan failures from the Databricks UC full run fixed (`fix/worker-predicate-coercion`).**

Found rerunning the full playground3 Unity Catalog suite (coordinator + one worker) on main 3f74359a.

- **Worker predicate types (regression from !1560).** Predicates now bind by field id on Databricks `col-N` files, but were then evaluated against the raw file types: `Timestamp(µs,"UTC") > Timestamp(µs,"+00:00")` (timestamptz filters) and `Int32 > Int64` (`WHERE id > 2` after `int -> bigint` promotion). `build_physical_predicate` (`crates/sqe-worker/src/executor.rs`) now runs `TypeCoercionRewriter` against the file schema before `create_physical_expr`; on a coercion error the predicate is dropped (the coordinator still filters). Unit test `build_physical_predicate_coerces_to_file_types`.
- **Coordinator restart window.** After a restart the coordinator has a new ephemeral ticket key; the worker re-pins it from the next heartbeat, but `health_check` already marked it healthy, so the first scans failed with "Invalid scan task signature". The worker's `health_check` reply is now `ok <pinned key b64>` / `ok unpinned` / `ok` (no pin configured); `WorkerRegistry::with_ticket_public_key` makes `check_all_workers` wait (neither healthy nor failed) until the reply names this coordinator's key. A bare `ok` from an older worker still counts as ready. Tests `health_check_names_the_pinned_ticket_key`, `health_check_reply_must_name_this_coordinators_key`.

- **Quiet wait.** A worker that keeps answering with another key (never heartbeats to this coordinator) would stay out of the pool for good, logged only at debug. A wait longer than 60 s is now logged once at warn (`a_long_ticket_key_wait_is_reported_once`).
- **Distributed gate aborted.** `make test-distributed` without FILTER SIGABRTed in `in_subquery_or_stack_overflow::prod_stack_16k` before any distributed test ran: `scripts/integration-test.sh` skipped the debug-only `prod_stack_*` repros only when `DISTRIBUTED != 1`. Those skips now apply to every unfiltered run.
- The worker's `return None` on a failed coercion is safe: `SqeTableProvider` reports pushed filters as `Inexact`, so the coordinator keeps a `FilterExec` above the scan.

Live: the full suite part 2 (49 statements) ran with 0 errors directly after a coordinator restart; `sqe_scheduler_decisions_total{decision="distributed"} 26`; results correct.

Gates:
- `cargo clippy -p sqe-worker -p sqe-coordinator -p sqe-server --all-targets -- -D warnings` -> clean.
- `cargo test -p sqe-coordinator --lib worker_registry` -> 32 passed; the three new unit tests pass.
- `make test-distributed FILTER=worker` -> 14 passed, 0 failed (the `distributed_schema_evolution_e2e` worker scans, `distributed_failure_e2e`, variant and geo worker scans), plus `dispatch_and_collect_groups_against_a_live_worker`.
- `make test-distributed FILTER=distributed` -> 18 passed, 1 failed: `compaction_distributed_benchmark::distributed_rewrite_wall_clock_ratio` needs a third worker on :50054 (environmental; the rig ran two).
- Filtered runs, not the full suite: the unfiltered run aborted on the stack-overflow repro, which this branch fixes in the script; the script change was checked with `bash -n` only.

Open, next branch: `information_schema.columns`, `SHOW CREATE TABLE`, `SHOW COLUMNS` and `DESCRIBE` fail catalog-wide on Unity Catalog. DataFusion's built-in `make_columns` loads every listed table in every catalog (no filter pushdown) and aborts on the first error; UC lists Delta tables whose loadTable returns 400 "not an Iceberg compatible table". `information_schema.tables` works (`SqeSchemaProvider::table_type` short-circuits). dbt-sqe reads raw `information_schema.columns` (`adapters.sql`, `catalog.sql`), so fixing the SQE commands alone does not cover dbt.

**NEXT:** make SQE's filter-aware `columns` provider answer `information_schema.columns` (it tolerates a per-table load failure), keeping restricted column names visible as documented.
