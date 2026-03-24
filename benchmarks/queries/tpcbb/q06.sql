-- name: Revenue by marketing channel
-- description: Break down total net revenue and number of transactions by
--              channel (store, catalog, web) per calendar year and quarter,
--              to compare channel performance over time.
-- timeout: 120s
SELECT
    d.d_year,
    d.d_qoy                         AS quarter,
    'store'                         AS channel,
    COUNT(ss.ss_ticket_number)      AS transactions,
    SUM(ss.ss_net_paid)             AS net_revenue,
    AVG(ss.ss_net_paid)             AS avg_sale
FROM
    store_sales ss
    JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
GROUP BY d.d_year, d.d_qoy

UNION ALL

SELECT
    d.d_year,
    d.d_qoy,
    'catalog',
    COUNT(cs.cs_order_number),
    SUM(cs.cs_net_paid),
    AVG(cs.cs_net_paid)
FROM
    catalog_sales cs
    JOIN date_dim d ON d.d_date_sk = cs.cs_sold_date_sk
GROUP BY d.d_year, d.d_qoy

UNION ALL

SELECT
    d.d_year,
    d.d_qoy,
    'web',
    COUNT(ws.ws_order_number),
    SUM(ws.ws_net_paid),
    AVG(ws.ws_net_paid)
FROM
    web_sales ws
    JOIN date_dim d ON d.d_date_sk = ws.ws_sold_date_sk
GROUP BY d.d_year, d.d_qoy

ORDER BY
    d_year,
    quarter,
    channel;
