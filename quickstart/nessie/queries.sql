-- Demo queries for the Nessie quickstart.
-- Nessie starts empty, so we create the namespace via SQL (SQE maps
-- CREATE SCHEMA to an Iceberg create_namespace call).

-- 1. Create the namespace and a table, write rows, read them back.
CREATE SCHEMA IF NOT EXISTS nessie.demo;

DROP TABLE IF EXISTS nessie.demo.events;
CREATE TABLE nessie.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO nessie.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

-- 2. The namespace is now visible.
SHOW SCHEMAS;

-- 3. Read + aggregate.
SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM nessie.demo.events
GROUP BY kind
ORDER BY total DESC;
