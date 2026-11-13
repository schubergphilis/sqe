### Fixed

- `information_schema.columns` no longer answers zero rows while other sessions run DDL (#890)

    Every commit deleted the namespace's table-name snapshot, so a reader that
    planned between its pre-plan refresh and a concurrent commit found no names
    and got zero rows for tables nobody touched (dbt then rendered a MERGE with
    an empty column list). A commit now marks the snapshot for a re-list and
    keeps serving its names; a refresh that raced a commit is stored marked.
    A listed name the catalog briefly does not find (a `CREATE OR REPLACE` that
    changes the schema swaps the table) is retried for up to 175 ms before it
    is left out. The table being swapped can still be missed when the swap
    takes longer than that; an atomic replace is tracked in #892. A table whose schema SQE cannot read is now an error with the
    reason instead of "table not found" and a silent gap in `information_schema`.
    dbt-sqe: `get_columns_in_relation` raises when it gets no columns for a
    relation `information_schema.tables` lists, and the merge strategy refuses
    an empty column list.
