### Changed

- `write-snapshot-properties` spec: only Flight SQL `BeginTransaction` transactions keep parallel loaders on one bearer token apart

    A SQL `BEGIN` is stored on the session, and clients on one bearer token share that session, so they share its transaction and its snapshot properties. The design, the parallel-loader scenario and the docs task now say so.
