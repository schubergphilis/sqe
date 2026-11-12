## Context

`handle_ingest_streaming` (`sqe-write/src/write_handler/ingest.rs`) derives the write schema from the table's current Iceberg schema. It then sends each uploaded batch through `apply_stamped_schema` (`sqe-compaction/src/writer.rs`). That function checks arity, then casts column `i` to target field `i`. Names are never read. The upload's Arrow schema is known before the first batch (`PeekedUpload.schema`), so the mapping can be decided before any file is written.

INSERT with a column list maps by name and fills omitted columns from `write-default` or NULL (#828, `reorder_insert_select`). MERGE does the same through `merge_sql::omitted_value`.

## Goals / Non-Goals

**Goals:**
- Ingest a subset of a table's columns, in any order.
- Never write a value into a column it was not meant for.
- Decide and refuse before any data file is written.

**Non-Goals:**
- Matching nested struct fields by name. They stay positional inside a matched top-level column, as today.
- Evolving the table schema from the upload (adding columns).
- Changing how a table created from the stream gets its schema.

## Decisions

**1. Name resolution.** An upload field resolves to a table column with the same name exactly. Failing that, it resolves to the one table column whose name matches ignoring ASCII case, the same folding SQE applies to identifiers (#337). Two table columns that differ only in case, or two upload fields that resolve to the same column, fail the request.

**2. Mode selection, from the stream schema once:**
- Every upload name resolves: **by name**.
- No upload name resolves and the counts are equal: **positional**, as today.
- Anything else fails with `InvalidArgument`, naming the unresolved upload columns and the table's columns.

*Why:* positional fallback keeps clients that send generic names working. A mix of resolved and unresolved names is the case where position silently swaps columns, so it is refused.
*Alternative:* an explicit option (`adbc.ingest.match_by_name`). Rejected as the default, because standard ADBC clients do not set SQE-specific options. One may be added later to force a mode.

**3. Filling omitted columns.** The filled value is the column's Iceberg `write-default` as a scalar repeated to the batch length, or a null array. A required column with no `write-default` that the upload omits fails the request before any write. The default literal is converted once, when the mapping is built.

**4. Where it runs.** A `ColumnMapping` built from (upload schema, table schema) turns each upload batch into a batch in table order, before `apply_stamped_schema`. Type casting stays in `apply_stamped_schema`, so by-name ingest casts exactly as positional ingest does. The row-filter check guard (`with_check_guard`) sees the mapped batches, so a filtered column filled with its default is checked like any other value.

**5. Shared default logic.** The per-column default (Iceberg literal to Arrow scalar) moves to one helper used by ingest and by the INSERT/MERGE paths, instead of a third copy.

## Risks / Trade-offs

- [A client relied on positional writes with partly matching names] -> Now an error that names the columns, rather than a silent swap. Called out as BREAKING in the changelog.
- [Case-insensitive match picks the wrong column] -> Only a unique match is accepted. An ambiguous one is refused.
- [Wide default arrays per batch] -> Built per batch from a cached scalar, at the same cost as the null arrays the INSERT path builds.

## Migration Plan

No configuration change. Clients that send full, ordered, correctly named batches see no difference. Rollback is a binary downgrade.

## Open Questions

None.
