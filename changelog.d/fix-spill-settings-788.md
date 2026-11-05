### Fixed

- `coordinator.spill_compression` now applies to spill files

    The setting was logged at startup but never passed to DataFusion, so
    the coordinator wrote every spill file uncompressed. Each coordinator
    session now sets `datafusion.execution.spill_compression` from it:
    `lz4` (the default) writes `lz4_frame`, `zstd` writes `zstd`, `none`
    writes uncompressed.

### Added

- `coordinator.spill_max_bytes` caps the disk used by spill files

    Default `100GB`, the DataFusion default that applied before. A query
    that would spill past it fails with a resources-exhausted error that
    names `coordinator.spill_max_bytes`; before, the error named
    `datafusion.runtime.max_temp_directory_size`, which no SQE config sets.
