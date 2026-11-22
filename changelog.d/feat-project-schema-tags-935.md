### Added

- Schema tags are projected into Ranger for Spark (#935)

    With `[policy] inherit_schema_tags = true`, a schema's tags are written to Ranger's tag store on the schema-wide resources `{database, table: *, column: *}` and `{database, table: *}`, so a Spark tag mask or tag row filter on a schema tag covers every table in the schema, as it does in SQE. `ALTER SCHEMA ... SET|UNSET TAGS` adds before the commit and removes after it, `CALL system.reproject_column_tags` rewrites the schema-wide resource exactly (a row named `<schema>.*`), and loading a table only adds schema keys Ranger does not hold yet. A schema whose tags cannot be read keeps its projection and logs an error. A table-level change that leaves tags is now one replace in Ranger instead of a delete followed by an add, which left Spark unmasked in between. Turning the setting off or dropping a schema leaves the schema-wide projection in Ranger until it is deleted there.
