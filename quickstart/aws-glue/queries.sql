-- Demo queries for the AWS Glue quickstart.
-- The glue backend registers under the SQL catalog name `iceberg`; the CDK
-- stack created the Glue database `sqe_glue_quickstart`. Tables are addressed
-- iceberg.<glue_database>.<table>.

-- 1. Create the Glue database via SQE (CREATE SCHEMA -> Glue CreateDatabase).
--    SQE creating it makes the caller the database owner, which is what grants
--    Create Table in a Lake-Formation-enabled account (see README).
CREATE SCHEMA IF NOT EXISTS iceberg.sqe_glue_quickstart;

-- 2. Create a table, write rows, read them back -- the full write path through
--    the Glue catalog (CreateTable/UpdateTable) and S3 (data + metadata files).
--    This is what the format-version fix (MR !286) unblocked: before it, CREATE
--    TABLE on Glue failed with "reserved properties [format-version]".
DROP TABLE IF EXISTS iceberg.sqe_glue_quickstart.events;
CREATE TABLE iceberg.sqe_glue_quickstart.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO iceberg.sqe_glue_quickstart.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

-- 2. Read + aggregate.
SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM iceberg.sqe_glue_quickstart.events
GROUP BY kind
ORDER BY total DESC;
