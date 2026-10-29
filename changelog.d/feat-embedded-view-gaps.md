### Added

- Embedded `SHOW TABLES`, `information_schema` and `SHOW CREATE VIEW` see Iceberg views

    On a catalog attached with `TYPE iceberg_rest`, `sqe-cli --embedded`
    lists the catalog's views with type `VIEW`. The list is read at attach
    and after each `CREATE VIEW` / `DROP VIEW` in the session.
    `SHOW CREATE VIEW` renders `CREATE VIEW <name> AS <body>`. `SHOW CREATE
    TABLE` on a view, and `SHOW CREATE VIEW` on a table, fail with a message
    that says which one it is.

### Fixed

- Embedded view bodies are stored under the warehouse name

    `CREATE VIEW pol.ns.v AS SELECT * FROM pol.ns.t` stored `pol.ns.t`
    verbatim, so a coordinator or another attach that names the catalog
    differently could not read the view. The body now names the warehouse
    (`<warehouse>.ns.t`), which every reader resolves, and `SHOW CREATE VIEW`
    maps it back to the attach name.

- A view that references itself fails instead of recursing

    `CREATE OR REPLACE VIEW v AS SELECT * FROM v`, or a longer cycle through
    other views, recursed on read on both the coordinator and the embedded
    engine. Planning a view now tracks the views already being planned and
    fails with `view w.ns.v references itself through w.ns.v -> w.ns.v`, and
    a chain deeper than 32 views fails too.
