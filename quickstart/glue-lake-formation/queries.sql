-- Demo queries for the glue-lake-formation quickstart.
-- The glue backend registers under the SQL catalog name `iceberg`; the CDK
-- stack created the LF-governed Glue database `sqe_lf_quickstart`. Tables are
-- addressed iceberg.<glue_database>.<table>.
--
-- run.sh runs this file TWICE:
--   Phase A (before the LF grant): CREATE TABLE is denied by Lake Formation.
--   Phase B (after the LF grant):  the same statements succeed.
-- There is no CREATE SCHEMA: the database already exists (CloudFormation made
-- it), which is exactly why it is LF-governed.

-- Create a table, write rows, read them back -- the full write path through the
-- Glue catalog (CreateTable/UpdateTable) and S3 (data + metadata files).
CREATE TABLE iceberg.sqe_lf_quickstart.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO iceberg.sqe_lf_quickstart.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM iceberg.sqe_lf_quickstart.events
GROUP BY kind
ORDER BY total DESC;
