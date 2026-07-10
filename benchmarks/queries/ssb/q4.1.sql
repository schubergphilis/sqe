-- name: SSB Q4.1 — Profit by year and nation (America region, MFGR#1 or MFGR#2)
-- timeout: 60s
SELECT
    d_year,
    c_nation,
    SUM(lo_revenue - lo_supplycost) AS profit
FROM
    lineorder,
    dim_date,
    customer,
    supplier,
    part
WHERE
    lo_custkey    = c_custkey
    AND lo_suppkey    = s_suppkey
    AND lo_partkey    = p_partkey
    AND lo_orderdate  = d_datekey
    AND c_region      = 'AMERICA'
    AND s_region      = 'AMERICA'
    AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
GROUP BY
    d_year,
    c_nation
ORDER BY
    d_year,
    c_nation;
