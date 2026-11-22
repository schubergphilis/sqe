**STATUS 2026-11-22: stock dbt-trino `on_table_exists` modes and `views_enabled = false` checked; CTAS `IF NOT EXISTS` fixed (`fix/ctas-if-not-exists-897`, #897, OpenSpec `trino-table-properties` task 4.2).**

- Run: dbt-core 1.12.7 + dbt-trino 1.12.0 against a local `sqe-server` (`cargo dev`) over the Trino HTTPS endpoint, Polaris + RustFS test stack. Models: one table per `on_table_exists` mode (`rename`, `drop`, `replace`, `skip`), a `view`, and two incrementals with `views_enabled = false` (merge on `unique_key`, append). Three runs with `run` = 1, 2, 3.
- Gap found: `skip` sends `create table if not exists ... as (select ...)`. The streaming CTAS handler ignored `if_not_exists`, so the second run failed with `DUPLICATE_TABLE`. Plain `CREATE TABLE` and `CREATE TABLE LIKE` already skipped.
- Fix: `handle_ctas_streaming_on` checks `table_exists` before planning the SELECT and returns 0 affected rows (Trino does not analyze the query either). Hive external CTAS is not changed.
- After the fix: all runs pass. `t_drop`, `t_rename`, `t_replace` hold run 3; `t_skip` keeps run 1; the merge incremental holds ids 1-3; the append incremental holds 1, 2, 2, 3 (run 2 ran twice); no `__dbt_tmp` / `__dbt_backup` table is left.
- Tests: unit `ctas_if_not_exists_skips_an_existing_table` (sqe-write, `test-sqlite`) and integration `trino_ctas_if_not_exists_skip_897`.

**NEXT:** archive the `trino-table-properties` OpenSpec change once this merges (all tasks done).
