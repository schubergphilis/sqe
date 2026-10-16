### Fixed

- ADBC Flight SQL clients no longer refuse `SELECT 1 + 1`, `SHOW`,
  `DESCRIBE` and `EXPLAIN` with "inconsistent schema"

    The schema a Flight SQL result announces now matches the one it
    streams. An unaliased expression column was announced as, for
    example, `count(*)` but streamed as `_col0`, and `SHOW`, `DESCRIBE`
    and `EXPLAIN` announced no columns while sending some. The ADBC
    driver checks the two and refused the result. Aliased queries were
    not affected. A statement whose columns are only known when it runs
    now announces no schema, and the client takes the stream's.
