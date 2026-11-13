### Fixed

- MERGE: a bare column in a `WHEN NOT MATCHED` clause is a source column (#893)

    `WHEN NOT MATCHED THEN INSERT (id, name) VALUES (id, name)` failed with
    "Ambiguous reference to unqualified field id" when target and source share the
    column name. A NOT MATCHED clause has no target row, so its bare names now
    resolve to the source, in the INSERT VALUES and in the clause's `AND`
    predicate, on the copy-on-write and merge-on-read paths. dbt-core's snapshot
    MERGE uses this form, so every dbt snapshot failed on its second run.
