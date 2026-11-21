## Why

Object tags (#922) are metadata only: tagging a table `pii` masks nothing. In Unity Catalog a tag on a catalog, schema or table is inherited by every column below it, and a tag policy that matches the tag applies wherever it is inherited (#924, #930). A data service that tags tables, as Unity Catalog users do, gets no masking from SQE today.

## What changes

- A column's **effective tags** are its own tags plus the tags of its table. A column tag with the same key as a table tag overrides the table tag's value. Tag policies (masks and row filters) match effective tags.
- First MR: table tags only. Schema tags (`sqe.schema-tags`) and catalog tags (`sqe.catalog-tags`) follow in a second MR, read through a cache.
- Unreadable table tags (malformed `sqe.table-tags`) leave the table's tag state unknown: every read of the table is refused, as for a malformed `sqe.column-tags`.
- Inherited tags take part in mask conflicts like own tags: under `mask_precedence = "refuse"`, a mask inherited from the table and a different mask on a column tag refuse the read.
- `SHOW EFFECTIVE POLICY` reports the masks and filters matched through inherited tags.
- `SHOW STATS` drops the total row count under an inherited row filter. Masked columns keep their statistics, as they do for own tags; only restricted columns are dropped.
- Write authority is unchanged: who may tag or untag a column, or a table, is decided on the tags written there, not on inherited ones.

## Out of scope

- Schema and catalog inheritance (second MR).
- The Ranger tag projection stays on own column tags, so engines that read Ranger's tag store (Spark) do not see table tags yet.
- View tags: views take no tags; a query of a view is policed on its base tables' tags.
- Hive tables: object tags are refused there (#922).
- A write-time conflict check for table tags; the read-time refusal is the backstop.
