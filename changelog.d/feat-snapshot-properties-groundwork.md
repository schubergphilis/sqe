### Fixed

- `t$snapshots`: the `summary` column is valid JSON for any key and value

    The column was built by hand and escaped only double quotes, so a backslash or control character in a summary value produced JSON that parsers rejected. It is now serialized with `serde_json`, with keys sorted.

### Added

- `SnapshotProperties` validation type in `sqe-core` (groundwork for `SET SNAPSHOT_PROPERTIES`)

    It enforces the `write-snapshot-properties` rules: key charset and length, reserved Iceberg and engine keys and prefixes (case-insensitive), value length and control characters, 32 pairs and 8 KiB per set. Nothing calls it yet.
