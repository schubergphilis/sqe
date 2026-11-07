### Changed

- SQL reference states the default NULL order and the Spark migration path (#876)

    A bare `ORDER BY x DESC` puts NULLs first, as in Trino; Spark and Databricks
    put them last. Migrated SQL keeps Spark's order with an explicit `NULLS LAST`.
    Unit tests pin the default, the explicit clauses and the window `ORDER BY`
    behind `ROW_NUMBER` top-N. The session-control table no longer claims that
    `SET <variable>` changes DataFusion settings; SQE refuses it.
