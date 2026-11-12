### Changed

- OpenSpec change `ingest-by-column-name`: the plan for Flight SQL bulk ingest to match uploaded columns to the table by name, fill omitted columns with their `write-default` or NULL, keep positional mapping for uploads with generic names, and refuse partly matching uploads. No code change yet.
