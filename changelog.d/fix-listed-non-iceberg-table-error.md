### Fixed

- A query on a Delta table in Databricks Unity Catalog says the table is not an Iceberg table

    Unity Catalog lists its Delta tables in the Iceberg REST namespace and
    refuses to load them as Iceberg. Since column metadata learned to skip
    those tables, a `SELECT` from one failed with "table not found" while
    `SHOW TABLES` still listed it. The error now names the table and says
    the catalog lists it but does not serve it as Iceberg. A name the
    catalog does not know still fails with "not found".
