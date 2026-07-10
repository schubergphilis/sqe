-- Demo queries for the Unity Catalog OSS quickstart.
--
-- Unity Catalog OSS exposes a READ-ONLY Iceberg REST adapter (create/drop/
-- commit are not supported, and the bundled table is not served as a loadable
-- Iceberg table at this version; see unitycatalog#3). So this quickstart
-- browses the catalog: SQE connects to Unity over Iceberg REST and enumerates
-- its namespaces and tables.

-- 1. List the namespaces Unity exposes (the bundled catalog has `default`).
SHOW SCHEMAS;

-- 2. List the tables in the default namespace (the image ships
--    `marksheet_uniform`). SELECT on it does NOT work against Unity OSS at this
--    version; use the Polaris or Nessie quickstarts for full read/write.
SHOW TABLES IN unity.default;
