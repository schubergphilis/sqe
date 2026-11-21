### Fixed

- `SHOW CREATE TABLE cat.ns.t` finds a table in a catalog the session has not used yet (#921)

    With `catalog_discovery = "polaris-auto"`, a `SHOW CREATE TABLE` (or `SHOW CREATE VIEW`) as the first statement against a sibling warehouse reported "table not found". The parser does not treat the name in `SHOW CREATE` as a table reference, so the pre-flight saw no catalog to discover. The name is now read like any other table reference, for catalog discovery, the namespace refresh and the `system.*` catalog check.
