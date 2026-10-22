### Added

- `ALTER COLUMN ... SET DATA TYPE` promotes a list element or a map value (#869)

    A nested field is named with a dotted path: `l.element` for a list
    element, `m.value` for a map value, and the path can continue into a
    struct inside them (`m.value.f`). Field ids are kept, files written
    before the change read as the new type in SQE and in Spark, and a map
    key is refused because Iceberg never lets it change type.
