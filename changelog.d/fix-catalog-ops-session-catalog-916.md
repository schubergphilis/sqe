### Fixed

- RENAME, CREATE/DROP VIEW and ALTER TABLE act on the session catalog for one- and two-part names (#916)

    `catalog_ops` resolves the catalog with `resolve_target_catalog`, as DROP TABLE, INSERT, CTAS, DELETE, UPDATE and MERGE already do. Before, after `USE other_catalog.schema`, these statements went to the default warehouse: they failed with table-not-found, or changed a same-named table there. Covered: RENAME, CREATE VIEW, DROP VIEW, ALTER TABLE (columns, nested columns, properties, branches and tags, partition and identifier fields). Column tags already resolved this way.
