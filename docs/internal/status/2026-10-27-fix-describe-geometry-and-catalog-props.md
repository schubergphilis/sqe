**STATUS 2026-10-27: DESCRIBE / SHOW CREATE print geospatial types, SHOW CREATE drops catalog-owned keys, ST_SRID (`fix/describe-geometry-and-catalog-props`).**

Item 2 of the Databricks UC interop gap queue (after !1573, workers apply delete files).

- `sqe_geo::ddl_type` maps a `geoarrow.wkb` field's CRS/edges metadata to `geometry(4326)` / `geography(4326, karney)` / `geometry('ESRI:102100')`; `single_table_columns` uses it, so DESCRIBE, SHOW COLUMNS and SHOW CREATE TABLE all print it.
- SHOW CREATE TABLE: always prints `format-version`; sort order as a `-- sort order:` comment (no DDL clause for it); hides `write.data.path`, `write.metadata.path`, `write.object-storage.*`, `gc.*`, `history.expire.*`, `databricks.*`, `io.unitycatalog.*`.
- `ST_SRID` reads the CRS from the argument field's metadata; computed geometry -> 0.
- Databricks CLUSTER BY has no Iceberg representation (no sort order, no partition spec; checked on live Unity metadata), so there is nothing to render for it.
- compare.tsv: `geo_srid`, `sqe_geo_srid` rows; not yet run against the live workspace.

Gates:
- `cargo test -p sqe-geo --lib` -> 7 passed.
- `cargo test -p sqe-coordinator --lib show_create` -> 2 passed.
- `make test-integration FILTER=geospatial_types_round_trip` -> 1 passed (DESCRIBE types, SHOW CREATE round trip into a copy, ST_SRID).

**NEXT:** item 3a, embedded UPDATE / DELETE / MERGE through the `sqe-write` WriteHandler; rerun the Databricks matrix to pick up the two ST_SRID rows.
