### Changed

- OpenSpec change `write-snapshot-properties`: the plan for user keys in the snapshot summary of every write (`SET` / `RESET SNAPSHOT_PROPERTIES`, scoped to the transaction or the session, plus the `snapshot-property.` Flight SQL ingest option), so a copy job can write its completion marker in the same commit as the data. No code change yet.
