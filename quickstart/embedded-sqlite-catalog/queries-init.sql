-- Create + populate a table in the persistent embedded catalog (process 1).
-- The embedded engine with --warehouse attaches a SQLite-backed Iceberg catalog
-- named `iceberg` at that path; everything here persists on disk.
CREATE SCHEMA IF NOT EXISTS iceberg.demo;
CREATE TABLE iceberg.demo.events (id BIGINT, kind VARCHAR, amount DOUBLE);
INSERT INTO iceberg.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);
