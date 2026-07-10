-- name: Important Stock Identification
-- timeout: 60s
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    -- TPC-H spec: FRACTION = 0.0001 / SF. Supplier has 10,000 * SF rows,
    -- so 1.0 / COUNT(supplier) equals the spec fraction at every scale.
    -- A hardcoded 0.0001 is only correct at SF1 and returns 0 rows at SF10.
    SUM(ps_supplycost * ps_availqty) > (
        SELECT SUM(ps_supplycost * ps_availqty)
               * (SELECT 1.0 / COUNT(*) FROM supplier)
        FROM partsupp, supplier, nation
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY'
    )
ORDER BY
    value DESC;
