### Fixed

- Flight SQL prepared INSERT, UPDATE and DELETE advertise their parameter types

    A prepared DML statement has no stored plan, so its `parameter_schema`
    advertised every parameter as `Utf8` and a driver bound a BIGINT column
    as a string. The statement is now planned once at prepare time to read
    the types: an INSERT's `VALUES` placeholders take the target columns'
    types, and a `WHERE col = ?` takes the column's. A statement that does
    not plan there (a MERGE) stays `Utf8`, as before. The bind of a
    prepared DML statement is still not checked against these types, so a
    client that sends `"10"` for a BIGINT column keeps working: the value is
    coerced when the statement runs.

    String and binary parameters are advertised as `Utf8` / `Binary`, never
    as the `Utf8View` / `BinaryView` the planner uses, for every prepared
    statement.
