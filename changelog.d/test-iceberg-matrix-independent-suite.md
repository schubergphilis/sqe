### Added

- Black-box Iceberg matrix feature suite (`tests/matrix/`)

    Runs SQE beside a Polaris + RustFS stack (`tests/matrix/docker/start-sqe.sh`, any `SQE_IMAGE`) and measures each cell of the iceberg-matrix rubric over SQE's client protocols (Trino HTTP and Flight SQL), against the rubric's feature description read literally. Write cells are confirmed by an independent reader (PyIceberg plus raw manifest, Parquet and Puffin reads). The report compares measured with claimed levels, states how many of the 54 cells were measured, lists discrepancies as proposed state-file edits (it never changes the state file), and lists features that work on one protocol but not the other as protocol gaps. Covered so far: table creation, row-level operations, schema, partitioning, time travel, branching and tagging, and table maintenance.
