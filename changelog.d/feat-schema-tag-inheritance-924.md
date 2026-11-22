### Added

- Columns can inherit their schema's tags (#924)

    With `[policy] inherit_schema_tags = true` (env `SQE_POLICY__INHERIT_SCHEMA_TAGS`) a tag mask or tag row filter on a schema tag covers every column of every table in the schema, including tables created later. A column's own tag value wins over its table's, and its table's over its schema's. A schema whose tags cannot be read refuses reads of its tables. Off by default; catalog tags are still metadata only.

### Fixed

- The configuration reference spelled `policy.mask_precedence` as `mask-precedence`, a key SQE ignores
