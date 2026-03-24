-- name: Top products by revenue from web clickstreams
-- description: Identify the top 20 items generating the most web-click-driven
--              revenue by joining clickstream events (where a sale occurred)
--              with store_sales and item dimension.
-- timeout: 120s
SELECT
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    COUNT(wcs.wcs_sales_sk)          AS click_sales_count,
    SUM(ss.ss_net_paid)              AS total_net_revenue,
    AVG(ss.ss_net_paid)              AS avg_sale_amount
FROM
    web_clickstreams  wcs
    JOIN store_sales  ss  ON ss.ss_item_sk  = wcs.wcs_item_sk
    JOIN item         i   ON i.i_item_sk    = wcs.wcs_item_sk
WHERE
    wcs.wcs_sales_sk IS NOT NULL
GROUP BY
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class
ORDER BY
    total_net_revenue DESC
LIMIT 20;
