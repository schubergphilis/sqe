### Fixed

- Embedded mode docs list what runs in-process

    The embedded mode page claimed the full cluster write path (UPDATE,
    DELETE, MERGE, ALTER, branches, tags, time travel) and described
    `--warehouse` as a catalog-free path scan. It now describes the SQLite
    warehouse that `--warehouse` really creates, adds a statement support
    table for embedded versus cluster mode, and points REST catalogs
    (Polaris, Nessie, Unity) at `ATTACH`.
