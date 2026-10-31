### Fixed

- `COMMENT ON` writes to the catalog the name qualifies

    `COMMENT ON COLUMN cat.ns.t.c` and `COMMENT ON TABLE cat.ns.t` committed
    to the session's default catalog whatever `cat` named, so on any other
    catalog they failed with "Table does not exist" on a table a `SELECT` of
    the same name reads. The comment now lands in the named catalog (an
    unqualified name still resolves in the session catalog), and `DESCRIBE`
    shows it on the next statement.
