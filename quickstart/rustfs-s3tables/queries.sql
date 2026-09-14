-- Demo queries for the RustFS S3 Tables quickstart.
-- The table bucket starts empty, so the namespace is created via SQL (SQE
-- maps CREATE SCHEMA to an Iceberg create_namespace call).

-- 1. Create the namespace and a table, write rows, read them back.
CREATE SCHEMA IF NOT EXISTS rustfs.demo;

DROP TABLE IF EXISTS rustfs.demo.events;
CREATE TABLE rustfs.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO rustfs.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

-- 2. The namespace is now visible.
SHOW SCHEMAS FROM rustfs;

-- 3. Read + aggregate.
SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM rustfs.demo.events
GROUP BY kind
ORDER BY total DESC;

-- 4. CTAS goes through the same signed commit path.
DROP TABLE IF EXISTS rustfs.demo.purchases;
CREATE TABLE rustfs.demo.purchases AS
SELECT id, amount FROM rustfs.demo.events WHERE kind = 'purchase';

SELECT COUNT(*) AS purchases, ROUND(SUM(amount), 2) AS total FROM rustfs.demo.purchases;

SHOW TABLES FROM rustfs.demo;
