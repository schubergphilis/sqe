### Fixed

- `CALL system.expire_snapshots` / `remove_orphan_files` accept
  `older_than => TIMESTAMP '...'` again

    The Trino rewrite turns the literal into `CAST('...' AS TIMESTAMP(0))`
    before the CALL is parsed, and the procedure parser refused the cast
    as `Unsupported SQL statement`. A timestamp cast of a literal is now
    read like the literal.
