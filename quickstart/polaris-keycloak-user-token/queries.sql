-- Demo queries for the Polaris + Keycloak quickstart.
-- Run as adminuser (catalog_admin + data_writer + table_reader).
-- The catalog is `quickstart`, the namespace `demo` was created by bootstrap.

-- 1. The catalog and namespace are visible.
SHOW SCHEMAS;

-- 2. Create a table, write rows, read them back -- the full write path
--    through Polaris (commit) and RustFS (data + metadata files).
--    DROP IF EXISTS keeps this script re-runnable without a teardown.
DROP TABLE IF EXISTS quickstart.demo.events;
CREATE TABLE quickstart.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO quickstart.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

-- 3. Read + aggregate (filter pushdown + GROUP BY).
SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM quickstart.demo.events
GROUP BY kind
ORDER BY total DESC;
