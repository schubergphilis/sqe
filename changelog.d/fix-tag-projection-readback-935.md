### Fixed

- Ranger tag projection reads back what Ranger holds and cleans up after DROP SCHEMA (#935)

    What Ranger holds for a schema is read back from Ranger before `ALTER SCHEMA ... SET|UNSET TAGS` and `CALL system.reproject_column_tags` write, and by a table load once its 120-second memory of the schema expires, so a second coordinator can no longer skip a write because it remembers a key another coordinator has removed; an unreadable Ranger writes nothing and refuses `ALTER SCHEMA`. Table-level memory older than five minutes is resent. `DROP SCHEMA` deletes the schema-wide projection, and the repair deletes it when `inherit_schema_tags` is off or the schema has no tags. A column whose tags change is now one replace in Ranger instead of a delete followed by an add. Failures are counted in `sqe_tag_projection_failures_total{op="write"|"read"|"source"}`.
