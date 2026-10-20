### Fixed

- `typeof` on a variant column returns `variant` (#849)

    It returned the Arrow storage type,
    `Struct([Field { name: "metadata", ... }, Field { name: "value", ... }])`,
    because a variant is tagged on the field, not in the data type.
