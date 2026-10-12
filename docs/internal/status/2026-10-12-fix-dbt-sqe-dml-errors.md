**STATUS 2026-10-12: dbt-sqe incremental models actually increment, and failed DML fails the model (`fix/dbt-sqe-dml-errors`).**

Before this change, a live two-run `dbt run` against a coordinator passed every model while doing the wrong thing. Five adapter defects hid each other:

- **Relation discovery.** `sqe__list_relations_without_caching` selected `(database, schema, name)`, but dbt-adapters unpacks `(database, name, schema)`. No existing relation was ever found, so every incremental run was a `CREATE OR REPLACE`: append kept only the last run, and the DML strategies never ran.
- **DML errors.** Flight SQL `ExecuteQuery` reports a failure only when the result stream is read, and closing an unread DML cursor cancels the write. dbt never fetches DML, so a failing INSERT passed as OK and a good one could be dropped. `_Cursor` now reads INSERT / DELETE / UPDATE / MERGE results on execute. `rowcount` comes from `rows_affected`, so dbt reports real row counts instead of -1.
- **Swallowed metadata errors.** `get_result_from_cursor` returned an empty table on any read error. When `information_schema.columns` failed, MERGE rendered `UPDATE SET` with no columns. Errors now propagate. A DDL statement's fetch is an explicit empty result, and an empty result keeps its column names and types.
- **DDL macros that never ran.** `sqe__drop_relation`, `rename_relation`, `create_schema` and `drop_schema` rendered SQL without running a `statement`, so dbt discarded it. The delete+insert temp table was never dropped, and schemas were never created. The table materialization now drops an existing relation only if it is not a table. `CREATE OR REPLACE` swaps a table in one commit, and a drop-then-create would lose the table when the CTAS fails.
- **delete+insert never completed.** Its INSERT was `statement('insert')`, and dbt fails a model that never runs `statement('main')`. The temp relation also had no type, so `drop_relation` refused it.

Also: `SQERelation` uses `BaseRelation.render`, so `list_schemas` gets the catalog alone, and `list_schemas` strips the quotes.

Verified live on a fresh schema over three runs:
- append accumulates
- delete+insert replaces by key
- merge upserts
- a failing incremental reports ERROR and leaves its table unchanged
- a table model is replaced in place
- the temp table is gone

20 adapter unit tests pass.

Found on the way, both server-side:
- `CREATE OR REPLACE VIEW` over an existing view fails at commit with "Cannot set last added schema: no schema has been added". #770 regressed with a new message.
- A view that selects from another view fails to plan ("table not found").

dbt view models therefore fail from their second run.

**NEXT:** dbt-sqe delete+insert in one scoped transaction (openspec `single-table-transactions` 5.4), then #816 and #803.
