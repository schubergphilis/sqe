**STATUS 2026-11-19: JSON path brackets and session-scoped DROP TABLE (`fix/json-path-index-and-drop-session-catalog`, #914, #915).**

- `sqe-trino-functions/src/json_path.rs`: a parser for Trino's JSON path subset (`$`, `.key`, `[n]`, `["key"]`, `['key']`). `json_extract`, `json_extract_scalar` and `json_size` use it. Before, the path was split on `.`, so `$.items[0]` named a key `items[0]` and returned NULL. The legacy forms (`$.items.0`, no leading `$`) still resolve.
- `sqe-write/src/catalog_ops.rs`: `drop_table` and `purge_target` resolve the catalog with `resolve_target_catalog`, as INSERT, CTAS, DELETE, UPDATE and MERGE already do. Before, a one- or two-part name went to the default warehouse, and `IF EXISTS` turned the miss into a silent no-op.
- `sqe-coordinator`: the Hive DROP route qualifies a short name from the session catalog and schema (`HiveDdlHandler::qualify_for_session`). Before, it only matched a three-part name.

Limits:

- A quoted member that contains `]` (`$["a]b"]`) does not parse.
- The other `catalog_ops` statements still take the catalog only from a three-part name: RENAME, CREATE/DROP VIEW, ALTER TABLE (columns, properties, refs, tags, partition and identifier fields). Not fixed here; tracked in #916.

**NEXT:** give the remaining `catalog_ops` statements the session catalog.
