## 1. Mapping

- [ ] 1.1 `ColumnMapping::build(upload_schema, table_schema)`: name resolution (exact, then unique ASCII case-insensitive), mode selection (by name, positional, refuse), refusals with the column names; unit tests for every spec scenario
- [ ] 1.2 Shared helper for an omitted column's fill value (`write-default` literal to Arrow scalar, else null), used by ingest and the INSERT/MERGE column-list paths
- [ ] 1.3 `ColumnMapping::apply(batch)`: reorder and fill into table order; unit tests including empty batches and dictionary-encoded inputs

## 2. Wire into ingest

- [ ] 2.1 `handle_ingest_streaming`: build the mapping from the peeked upload schema after the table is loaded, before the check guard and before any file is written; map each batch ahead of `apply_stamped_schema`
- [ ] 2.2 Skip the mapping when the table was created from the stream in this request

## 3. Tests

- [ ] 3.1 Integration: ingest reordered columns, a subset with a nullable column, a subset with a `write-default` column, generic positional names, one unknown name (refused, table unchanged), omitted required column (refused)
- [ ] 3.2 Integration: by-name ingest inside a Flight transaction, and under a row-filter policy (the filled default is checked)

## 4. Docs and close-out

- [ ] 4.1 Connecting page: ingest matches by name, fills omitted columns, positional fallback, refusals
- [ ] 4.2 Changelog fragment with the BREAKING edge case, status fragment, README roadmap
