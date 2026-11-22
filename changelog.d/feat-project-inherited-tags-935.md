### Added

- Table tags are projected into Ranger's tag store, so Spark masks and filters with them as SQE does (#935)

    `ALTER TABLE ... SET TAGS`, `CALL system.reproject_column_tags` and the tag load observer put `sqe.table-tags` on two tag service resources: the table with every column (`column: *`, which Kyuubi needs to apply a tag mask) and the bare table (for tag row filters). Additions reach Ranger before the commit, removals after it. A malformed `sqe.table-tags` never removes or shrinks the projection: Spark does not mask a table with nothing projected, so SQE keeps the last projection and logs an error; own column tags still project. Schema tags are not projected yet.
