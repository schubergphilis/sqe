### Fixed

- ADBC Flight SQL clients no longer refuse results with lists, structs
  or constant columns with "inconsistent schema"

    The schema a Flight SQL result announces and the one it streams
    disagreed on nullability: for list items, for struct fields, and for
    constants the planner had folded. Every column of a result is now
    described as nullable on both sides, nested fields included. Map
    entries and keys stay non-null, as the Arrow spec requires. The ADBC
    driver compares the two schemas and refused the result when they
    differed.

- Arrow Flight SQL JDBC can now run `SHOW`, `DESCRIBE` and `EXPLAIN`
  through a plain `Statement`

    These statements used to announce no columns, and the driver read
    that as an update with no result set ("Statement did not return a
    result set"). They now announce their real columns. To learn the
    columns, SQE runs the statement when the client asks for its
    description, then runs it again when the client fetches the rows.
    Only read-only statements are run this way, and the audit log
    records both runs.
