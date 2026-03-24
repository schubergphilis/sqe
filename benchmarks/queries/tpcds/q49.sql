-- name: Return rate by channel for items with high return-to-sale ratio
-- timeout: 60s
WITH in_store AS (
    SELECT 'store' AS channel, ss_item_sk AS item,
           SUM(ss_quantity * ss_sales_price)    AS sales,
           SUM(COALESCE(sr_return_quantity, 0) * COALESCE(sr_return_amt, 0)) AS returns,
           SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
    FROM store_sales
    LEFT OUTER JOIN store_returns ON ss_item_sk = sr_item_sk
                                  AND ss_ticket_number = sr_ticket_number
    JOIN date_dim ON d_date_sk = ss_sold_date_sk
    WHERE d_year    = 2001
      AND d_moy     = 12
    GROUP BY ss_item_sk
),
in_catalog AS (
    SELECT 'catalog' AS channel, cs_item_sk AS item,
           SUM(cs_quantity * cs_sales_price)    AS sales,
           SUM(COALESCE(cr_return_quantity, 0) * COALESCE(cr_return_amount, 0)) AS returns,
           SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
    FROM catalog_sales
    LEFT OUTER JOIN catalog_returns ON cs_item_sk = cr_item_sk
                                    AND cs_order_number = cr_order_number
    JOIN date_dim ON d_date_sk = cs_sold_date_sk
    WHERE d_year    = 2001
      AND d_moy     = 12
    GROUP BY cs_item_sk
),
in_web AS (
    SELECT 'web' AS channel, ws_item_sk AS item,
           SUM(ws_quantity * ws_sales_price)    AS sales,
           SUM(COALESCE(wr_return_quantity, 0) * COALESCE(wr_return_amt, 0)) AS returns,
           SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
    FROM web_sales
    LEFT OUTER JOIN web_returns ON ws_item_sk = wr_item_sk
                                AND ws_order_number = wr_order_number
    JOIN date_dim ON d_date_sk = ws_sold_date_sk
    WHERE d_year    = 2001
      AND d_moy     = 12
    GROUP BY ws_item_sk
)
SELECT channel, item,
       SUM(sales)   AS sales,
       SUM(returns) AS returns,
       SUM(profit)  AS profit
FROM (
    SELECT * FROM in_store
    UNION ALL
    SELECT * FROM in_catalog
    UNION ALL
    SELECT * FROM in_web
) x
GROUP BY channel, item
ORDER BY channel, item
LIMIT 100;
