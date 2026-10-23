### Fixed

- dbt-sqe reads result columns with their Arrow types and takes a list `unique_key` (portal #42)

    `run_query` and every other result dbt reads went through agate's type inference, which read a 0/1 integer column as Boolean (a model rendering it got `True`/`False`) and a string column of digits such as `"001"` as Number (`1`). The adapter now maps each column's agate type from the Arrow schema, and a repeated column name no longer drops a column. The `merge` and `delete+insert` incremental strategies took only a single-column `unique_key`: a list rendered as `['id', 'k']` into the SQL and failed to parse. Both now match on every key; a multi-key `delete+insert` deletes with `MERGE ... WHEN MATCHED THEN DELETE` inside the same transaction.
