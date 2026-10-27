### Added

- Databricks Unity Catalog interop harness (`scripts/interop/databricks-uc/`)

    Databricks and SQE each write Iceberg tables into one Unity Catalog
    schema, run DELETE, UPDATE and MERGE on the other's tables, and compare
    25 reads value by value. The run is manual and needs a workspace token.
    On 2026-10-27 all 25 reads matched in embedded, coordinator and
    distributed mode. The Iceberg matrix now rates Unity Catalog full on V2
    and V3 (156/162).
