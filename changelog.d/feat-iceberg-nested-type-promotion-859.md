### Added

- `ALTER COLUMN s.a SET DATA TYPE` promotes a struct member (#859)

    A dotted column path now changes the type of a field inside a struct,
    with the same promotions as a top-level column (`INT` to `BIGINT`,
    `REAL` to `DOUBLE`, a wider `DECIMAL` precision, `UNKNOWN` to any
    primitive type). The member keeps its Iceberg field id, so rows written
    before the change read as the new type in SQE and in Spark. A list
    element or map value cannot be changed yet.
