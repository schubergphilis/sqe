### Added

- Databricks column tag syntax: `ALTER TABLE t ALTER COLUMN c SET TAGS ('k' = 'v')`,
  the key-only `SET TAGS ('k')` and `UNSET TAGS ('k')`

    These statements write the same `sqe.column-tags` property as the
    Snowflake and native forms, under the same tag-authority check. Tag
    keys keep their case, dots and quoted commas. The tag value is parsed
    but not stored yet: tags stay key-only until values are kept. `UNSET
    TAGS` with a value, an empty list or an `=` with no value are errors.
    `SET TAGS ('k' = 'v')` on the table itself (no `ALTER COLUMN`) is
    refused with a pointer to the column form, because table-level tags
    have no storage yet.
