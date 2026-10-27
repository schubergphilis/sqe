### Changed

- Workers scan merge-on-read tables with position deletes and deletion vectors

    The scan ticket (version 6) carries the Iceberg delete tasks of each data
    file it lists. Before a data file opens, the worker loads its
    position-delete files and deletion vector and skips those rows with a
    Parquet row selection, computed after the morsel and row-group pruning.
    A data file with deletes is never split into byte-range morsels, so its
    deletes load once. Such snapshots used to be scanned on the coordinator
    until they were compacted. A snapshot with equality deletes still is,
    and a worker refuses a ticket that carries one. A version 5 worker
    refuses a version 6 ticket rather than read without the deletes.
