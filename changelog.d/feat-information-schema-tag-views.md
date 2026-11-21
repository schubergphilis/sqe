### Added

- Unity Catalog tag views: `cat.information_schema.column_tags` lists every
  column tag with its value (`catalog_name, schema_name, table_name,
  column_name, tag_name, tag_value`)

    Rows come only from tables the caller can load, and a table the policy
    denies entirely is left out. A key-only tag shows an empty `tag_value`.
    `catalog_tags`, `schema_tags` and `table_tags` answer with their columns
    and no rows until catalogs, schemas and tables can be tagged. Hive tables
    and Iceberg views are not listed. Results are never served from the
    result cache, and MCP tools can read the views.
