### Added

- `[session.catalog_scope] mode`: `binding` (default, narrows the session to the signed scope), `union` (requires and verifies the scope but does not narrow) or `off`. The mode is logged at startup (#295)

### Changed

- A statement refused by the catalog scope is audited with `error_type` `catalog_not_in_workspace` or `catalog_read_only`; the read-only message names `catalog_read_only`. A hidden catalog still answers "Catalog 'x' not found" (#295)
