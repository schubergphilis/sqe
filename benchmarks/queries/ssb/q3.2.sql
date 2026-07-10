-- name: SSB Q3.2 — Revenue by customer and supplier city (United States, 1992-1997)
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
    lo_custkey    = c_custkey
    AND lo_suppkey    = s_suppkey
    AND lo_orderdate  = d_datekey
    AND c_nation      = 'UNITED STATES'
    AND s_nation      = 'UNITED STATES'
    AND d_year        BETWEEN 1992 AND 1997
GROUP BY
    c_city,
    s_city,
    d_year
ORDER BY
    d_year ASC,
    lo_revenue DESC;
