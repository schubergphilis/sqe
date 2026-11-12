### Added

- `SET SNAPSHOT_PROPERTIES ('k' = 'v', ...)` and `RESET SNAPSHOT_PROPERTIES` (statement and state only)

    Inside a transaction the properties belong to that transaction and end with it. Outside one they stay on the session until reset. A session the server does not keep (Trino HTTP outside a transaction) refuses the statement instead of ignoring it. Writes do not stamp the properties into their commits yet.
