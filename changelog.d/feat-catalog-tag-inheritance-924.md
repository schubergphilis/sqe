### Added

- Columns can inherit their catalog's tags (#924)

    With `[policy] inherit_catalog_tags = true` (env `SQE_POLICY__INHERIT_CATALOG_TAGS`) a tag mask or tag row filter on a catalog tag covers every column of every table in the catalog, including tables created later. A column's effective tags are its own, then its table's, its schema's and its catalog's; the nearest key wins. The flag is independent of `inherit_schema_tags`. SQE reads `sqe.catalog-tags` from the Polaris catalog with the caller's token on each table load, so a change applies from the next statement. A catalog whose tags cannot be read refuses reads of its tables: with the flag on, every principal that reads tables in the catalog needs `CATALOG_READ_PROPERTIES` on it in Polaris, and without it reads are refused, not unmasked. Off by default. Catalog tags are enforced by SQE only: they are not projected to Ranger, so Spark does not see them, and a catalog attached with `ATTACH` carries none.
