-- Demo queries against the four Hive external tables.
--
-- Nothing here names a file, a format, or a credential. The manifests in
-- s3://lake/wh/ carry all of that, which is the point: the SQL is the same SQL
-- you would write against an Iceberg table.

-- What the root holds. SHOW SCHEMAS has no Hive path yet, so
-- information_schema is the reliable way to list a root's databases.
SELECT catalog_name, schema_name
FROM information_schema.schemata
WHERE catalog_name = 'lake'
ORDER BY schema_name;

-- The tables in it. `SHOW TABLES FROM lake.demo` works in server mode but is
-- not parsed in embedded mode at all (a pre-existing embedded gap, not a Hive
-- one), so information_schema is what an embedded session uses.
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'lake' AND table_schema = 'demo'
ORDER BY table_name;

-- Row counts, one per format, from four different readers.
SELECT 'parquet' AS format, COUNT(*) AS rows FROM lake.demo.events_parquet
UNION ALL SELECT 'csv',     COUNT(*) FROM lake.demo.events_csv
UNION ALL SELECT 'json',    COUNT(*) FROM lake.demo.events_json
UNION ALL SELECT 'ndjson',  COUNT(*) FROM lake.demo.events_ndjson
ORDER BY format;

-- Aggregation over the partitioned Parquet table. Refunds are the negative
-- kind, so the totals are not a uniform blob.
SELECT kind, COUNT(*) AS events, ROUND(SUM(amount), 2) AS total
FROM lake.demo.events_parquet
GROUP BY kind
ORDER BY total DESC;

-- Partition pruning. Pinning the LEADING partition columns with equality is
-- what narrows the listing to one prefix; a range or an IN list falls back to
-- listing the whole table.
SELECT dt, region, COUNT(*) AS events, ROUND(AVG(amount), 2) AS avg_amount
FROM lake.demo.events_parquet
WHERE dt = '2026-01-02' AND region = 'eu'
GROUP BY dt, region;

-- Partition columns behave like ordinary columns everywhere else.
SELECT dt, COUNT(*) AS events
FROM lake.demo.events_csv
GROUP BY dt
ORDER BY dt;

-- One query across two formats. The join is Parquet against CSV, which bind
-- their columns differently (Parquet by name, CSV by position) and still meet
-- as the same schema.
SELECT p.kind, COUNT(*) AS matched
FROM lake.demo.events_parquet p
JOIN lake.demo.events_csv c ON p.event_id = c.event_id
WHERE p.dt = '2026-01-01' AND c.dt = '2026-01-01'
GROUP BY p.kind
ORDER BY p.kind;

-- The schema SQE resolved from the manifest, partition columns last.
DESCRIBE lake.demo.events_parquet;
