**STATUS 2026-10-29: embedded INSERT VALUES with ARRAY columns, and filters on ARRAY columns (`fix/insert-values-array-field-ids`).**

Two embedded bugs found in the Databricks interop probe:

- `INSERT ... VALUES` with an `ARRAY` value failed in DataFusion's Values
  plan: the rows were typed against the Iceberg arrow schema, whose list
  element carries `PARQUET:field_id` metadata, and `RecordBatch::try_new`
  refused the plain list. MAP and STRUCT values were not affected.
  Embedded `INSERT` (and the insert step of CTAS) now runs on
  `WriteHandler::handle_insert_streaming_on`, split out of
  `handle_insert_streaming` so the caller passes its own `SessionCatalog`.
  The source is planned as a standalone SELECT, as on the coordinator.
- `WHERE tags IS NOT NULL` on an Iceberg table failed with "Accessor for
  Field tags not found": iceberg-rust builds no accessor for list or map
  fields. Sixth vendor patch in `datafusion-iceberg`: the scan keeps only the
  predicates that bind; the filters are `Inexact`, so DataFusion still
  applies the rest.

Found on the way, not fixed here: embedded `CREATE TABLE` with a `MAP`
column fails with "Unsupported SQL type MAP(VARCHAR, INT)", although the
book page says MAP columns reach the table metadata.

**NEXT:** `MAP` columns in `CREATE TABLE`, then embedded `ALTER TABLE` and
embedded Iceberg `SHOW CREATE TABLE`, then the live Databricks rerun.
