### Added

- Columns inherit their table's tags, and tag policies match the inherited tags (#924)

    A column's effective tags are its own tags plus every `sqe.table-tags` key it does not carry itself; the column's own value wins. Tag masks and tag row filters match effective tags, so tagging a table `pii` under a mask on `pii` masks every column, and an inherited row filter applies to SELECT, UPDATE, DELETE and the `SHOW STATS` row count. A malformed `sqe.table-tags` refuses every read of the table. Under `mask_precedence = "refuse"` an inherited mask and a different mask on the column's own tag refuse the read. Tag write authority still reads the tags at each level. Schema and catalog tags stay metadata, and the Ranger tag projection does not carry table tags yet, so Spark does not see them.
