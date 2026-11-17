### Added

- A Polaris catalog named `system` is reachable next to SQE's virtual `system` schemas (#901)

    With `catalog_discovery = "polaris-auto"`, `system.runtime`, `system.metadata`, `system.jdbc` and `system.information_schema` stay SQE's own schemas. Any other `system.<schema>` resolves to a Polaris catalog named `system`. Tables there can be created, written (including Flight SQL ingest), listed, described, read and dropped like tables in any other catalog. `SHOW SCHEMAS FROM system` lists both parts. `CREATE SCHEMA system.<reserved name>` and `ATTACH ... AS system` are refused, and `CALL system.<procedure>` still runs the built-in procedures. In static catalog mode nothing changes.
