-- name: SSB Q3.4 — Revenue by customer and supplier city (specific cities, December 1997)
-- timeout: 60s
SELECT
    c_city,
    s_city,
    d_year,
    SUM(lo_revenue) AS lo_revenue
FROM
    lineorder,
    dim_date,
    customer,
    supplier
WHERE
    lo_custkey     = c_custkey
    AND lo_suppkey     = s_suppkey
    AND lo_orderdate   = d_datekey
    AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
    AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
    AND d_yearmonth    = 'Dec1997'
GROUP BY
    c_city,
    s_city,
    d_year
ORDER BY
    d_year ASC,
    lo_revenue DESC;
