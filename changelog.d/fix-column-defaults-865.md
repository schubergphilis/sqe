### Fixed

- A quoted DEFAULT on a non-string column no longer crashes CREATE TABLE (#865)

    `CREATE TABLE t (d DATE DEFAULT '2024-01-02')`, and the same on
    TIMESTAMP, DECIMAL, BINARY or BIGINT columns or through
    `ALTER TABLE ... ADD COLUMN`, stored the default as a string literal and
    panicked in the metadata writer. The text is now parsed as the column's
    type: timestamps take a space or `T`, timestamptz takes `Z` or no offset
    as UTC, and a decimal must fit the column's precision and scale exactly.
    A value that does not parse is an error. `DECIMAL DEFAULT 2` and
    `BINARY DEFAULT X'01ab'` are accepted.
- Binary, fixed and UUID column defaults are written on INSERT and MERGE
  INSERT instead of being refused; a MERGE on such a table no longer fails
  (#865)
