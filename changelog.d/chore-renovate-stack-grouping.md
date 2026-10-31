### Changed

- Renovate groups the whole DataFusion/Arrow stack into one MR

    The `parquet-*` crates (variant, geospatial) and the sqlparser 0.x
    minor arrived as MRs of their own, apart from the Arrow major. None of
    them can build without the DataFusion release that moves to the same
    versions, so they stayed open and red. The stack rule now matches
    `parquet-*` and keeps major and minor updates of the group together.
