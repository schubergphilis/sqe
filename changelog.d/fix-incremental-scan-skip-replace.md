### Fixed

- `FOR INCREMENTAL BETWEEN SNAPSHOT` no longer counts compacted rows twice

    The incremental planner walked every snapshot in the range, including
    `replace` snapshots from compaction (`rewrite_data_files`), and emitted
    their rewritten files as new inserts. A range covering an append and its
    compaction returned those rows twice. `replace` snapshots are now left
    out of the change set: their added files are not emitted and their
    removed files do not retract rows. The rows still come back once,
    through the files of the snapshot that first added them when that
    snapshot is in range. `snapshots_in_range` still lists the compaction.
