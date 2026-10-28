### Added

- Embedded `UPDATE`, `DELETE` and `MERGE INTO` on Iceberg catalogs

    `sqe-cli --embedded` used to hand these statements to DataFusion, which
    refused them ("not supported for Base table"). They now run through the
    coordinator's `sqe-write` handlers, on the `--warehouse` catalog and on
    attached catalogs such as Unity. A table's `write.delete.mode` picks
    copy-on-write or merge-on-read, as on a coordinator. Embedded mode has no
    access policy. `sqe-cli` now depends on `sqe-write`, which adds 11
    crates to its build.
