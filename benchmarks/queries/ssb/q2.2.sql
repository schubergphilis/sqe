-- name: SSB Q2.2 — Revenue by year and brand (Asia region, MFGR#2221-MFGR#2228)
-- timeout: 60s
SELECT
    SUM(lo_revenue) AS lo_revenue,
    d_year,
    p_brand
FROM
    lineorder,
    dim_date,
    part,
    supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey  = p_partkey
    AND lo_suppkey  = s_suppkey
    AND p_brand     BETWEEN 'MFGR#2221' AND 'MFGR#2228'
    AND s_region    = 'ASIA'
GROUP BY
    d_year,
    p_brand
ORDER BY
    d_year,
    p_brand;
