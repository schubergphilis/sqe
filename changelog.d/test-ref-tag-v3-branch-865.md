### Fixed

- `SET WRITE_BRANCH` to a tag is refused; a write used to move the tag and
  turn it into a branch (#865)
- `DROP BRANCH` / `DROP TAG` accept Iceberg's `IF EXISTS <name>` order as
  well as `<name> IF EXISTS` (#865)

### Changed

- Integration tests cover Iceberg tags (create, read with FOR VERSION AS OF,
  drop) and branch writes on a format-version 3 table (#865)
