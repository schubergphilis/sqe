-- name: SSB Q2.1 — Revenue by year and brand (America region, MFGR#12)
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
    AND p_category  = 'MFGR#12'
    AND s_region    = 'AMERICA'
GROUP BY
    d_year,
    p_brand
ORDER BY
    d_year,
    p_brand;
