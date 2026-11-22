### Fixed

- `system.jdbc` and `system.metadata` tables no longer load every table in the lake (#939)

    The schemas, tables, columns, table_properties, schema_properties and table_comments tables now build their rows at scan time and only for the catalog, schema and table named in the query's equality or exact LIKE filters. An `information_schema` query no longer lists or loads any table through them, and a schema-filtered `system.jdbc` listing no longer lists other schemas, so a listing error there no longer fails it. A table in scope that fails to load is logged and skipped as before; listing errors other than access denied still fail the query. `information_schema.columns` over the Iceberg catalogs can still fail on an unloadable table in another schema; that is fixed separately.
