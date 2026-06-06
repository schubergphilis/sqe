-- Demo queries for the AWS S3 Tables quickstart.
-- The s3tables backend registers under the SQL catalog name `iceberg`; SQE
-- creates the namespace `demo` inside the table bucket. Tables are addressed
-- iceberg.<namespace>.<table>.

-- 1. Create the namespace (SQE -> S3 Tables CreateNamespace), then a table,
--    write rows, read them back -- the full managed-Iceberg write path.
CREATE SCHEMA IF NOT EXISTS iceberg.demo;

DROP TABLE IF EXISTS iceberg.demo.events;
CREATE TABLE iceberg.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO iceberg.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

-- 2. Read + aggregate.
SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM iceberg.demo.events
GROUP BY kind
ORDER BY total DESC;
