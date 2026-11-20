### Added

- Column tags by Ranger grant: `[access_control] tag_authority` (#911)

    With `tag_authority = "ranger-delegate"`, a user without an admin role may `SET` / `UNSET TAGS` (and `SET` / `UNSET MASKING POLICY`) on a column when the Ranger `hive` access policies grant them `alter` on that column. Every column in the statement is checked before anything is written; deny items win. The default `admin-role` is unchanged. A broad `alter` grant becomes the right to remove a column's mask, so review the `alter` grants before switching.

### Fixed

- `ALTER TABLE <bare name> SET / UNSET TAGS` and `SHOW EFFECTIVE POLICY` now resolve a bare table name in the session's catalog (`USE CATALOG`, Trino `catalog=`), as DML does, instead of the configured default warehouse (#911)
