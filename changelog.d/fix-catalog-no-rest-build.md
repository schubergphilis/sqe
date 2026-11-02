### Fixed

- `sqe-catalog` compiles without the `rest` feature again

    The writable embedded catalog needs the REST session catalog and table
    provider, so it is now built only with `rest`, like them.
