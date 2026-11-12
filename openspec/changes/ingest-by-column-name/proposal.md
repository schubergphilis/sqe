## Why

Flight SQL bulk ingest maps the uploaded columns to the table by position. A client that loads a subset of the columns, or the right columns in a different order, gets an arity error or, worse, values in the wrong column when two types match. A copy job cannot ingest into a table that has gained a nullable or defaulted column since the source was read. INSERT already handles this: a column list maps by name and fills omitted columns with their `write-default` or NULL (#828). Ingest should do the same.

## What Changes

- When every column name in the upload resolves to a column of the target table, ingest matches by name. Columns may come in any order, and table columns the upload leaves out are filled.
- An omitted column gets its Iceberg `write-default`, or NULL when it has none. An omitted required column with no default fails the request.
- When no upload name resolves to a table column and the column counts are equal, ingest stays positional, as today. A client that sends generic names (`f0`, `column1`) keeps working.
- **BREAKING (edge case):** an upload where some names resolve and others do not now fails, naming the unknown columns. Before, it was written positionally when the counts matched, which could silently swap columns.
- The mapping is decided once from the stream schema, before any data file is written.

## Capabilities

### New Capabilities
- `flight-ingest-column-mapping`: how bulk ingest maps uploaded columns onto an existing table: by name, positional fallback, defaults for omitted columns, and the refusals.

### Modified Capabilities
<!-- None in openspec/specs/. The ingest-table-definition-options change covers create/replace modes, not column mapping. -->

## Impact

- `sqe-write/src/write_handler/ingest.rs`: build the mapping from the upload schema and the table schema. Wrap the batch stream to reorder and fill before `apply_stamped_schema`.
- Shares the omitted-column default logic with the INSERT column-list path (`reorder_insert_select`, `merge_sql::omitted_value`).
- Applies only when the table already exists. A table created from the stream (`if_not_exist = CREATE`) takes the stream's columns as they are.
- Docs: the Flight SQL ingest section of the connecting page.
