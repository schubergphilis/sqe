### Fixed

- Column tags follow the column's field id, not its name (#920)

    A column another engine renamed kept its name in `sqe.column-tags`, so it
    lost its tags and its tag masks; a column dropped and added again under
    the same name inherited the old column's tags. Every SQE tag write (SET /
    UNSET TAGS, RENAME and DROP COLUMN) now also records the tagged columns'
    field ids in `sqe.column-tag-ids`, bound to the exact tag map by a
    SHA-256 digest, and every reader (scans, time travel, SHOW TAGS,
    `information_schema.column_tags`, SHOW STATS, the Ranger tag projection)
    resolves the map against the schema it reads. `SHOW TAGS ON` gains an
    `applied` column: a tag whose column was dropped is listed with `applied`
    false. A map changed without SQE no longer matches its digest and applies
    by name, as a table written before this change does; raw tag writes are
    checked on what applies after the write. Only an admin may write
    `sqe.column-tag-ids` as a raw property. `CREATE OR REPLACE` removes it and
    keeps the tags under the columns' current names.
