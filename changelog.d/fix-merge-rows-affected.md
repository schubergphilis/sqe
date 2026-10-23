### Fixed

- Copy-on-write MERGE reported the size of the rewritten table as `rows_affected`

    A copy-on-write MERGE returned the number of rows it wrote, which is every row that survives the rewrite. A `WHEN MATCHED THEN DELETE` of 1 row in a 3-row table reported 2, and an update plus an insert into a 3-row table reported 4. MERGE now counts the rows its clauses claim (updated, deleted or inserted), as merge-on-read MERGE and the documentation already did. The count costs one extra COUNT pass over the merge join.
