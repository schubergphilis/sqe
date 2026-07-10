-- name: Net profit and returns by sales channel
-- timeout: 60s
WITH ss AS (
    SELECT s_store_sk, SUM(ss_ext_sales_price) AS sales,
           SUM(ss_net_profit) AS profit
    FROM store_sales, date_dim, store
    WHERE ss_sold_date_sk = d_date_sk
      AND d_date BETWEEN DATE '2000-08-23' AND DATE '2000-09-06'
      AND ss_store_sk = s_store_sk
    GROUP BY s_store_sk
),
sr AS (
    SELECT s_store_sk, SUM(sr_return_amt) AS returns,
           SUM(sr_net_loss) AS profit_loss
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk
      AND d_date BETWEEN DATE '2000-08-23' AND DATE '2000-09-06'
      AND sr_store_sk = s_store_sk
    GROUP BY s_store_sk
),
cs AS (
    SELECT cs_call_center_sk, SUM(cs_ext_sales_price) AS sales,
           SUM(cs_net_profit) AS profit
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
      AND d_date BETWEEN DATE '2000-08-23' AND DATE '2000-09-06'
    GROUP BY cs_call_center_sk
),
cr AS (
    SELECT SUM(cr_return_amount) AS returns,
           SUM(cr_net_loss) AS profit_loss
    FROM catalog_returns, date_dim
    WHERE cr_returned_date_sk = d_date_sk
      AND d_date BETWEEN DATE '2000-08-23' AND DATE '2000-09-06'
),
ws AS (
    SELECT wp_web_page_sk, SUM(ws_ext_sales_price) AS sales,
           SUM(ws_net_profit) AS profit
    FROM web_sales, date_dim, web_page
    WHERE ws_sold_date_sk = d_date_sk
      AND d_date BETWEEN DATE '2000-08-23' AND DATE '2000-09-06'
      AND ws_web_page_sk = wp_web_page_sk
    GROUP BY wp_web_page_sk
),
wr AS (
    SELECT wp_web_page_sk, SUM(wr_return_amt) AS returns,
           SUM(wr_net_loss) AS profit_loss
    FROM web_returns, date_dim, web_page
    WHERE wr_returned_date_sk = d_date_sk
      AND d_date BETWEEN DATE '2000-08-23' AND DATE '2000-09-06'
      AND wr_web_page_sk = wp_web_page_sk
    GROUP BY wp_web_page_sk
)
SELECT channel, id,
       SUM(sales)   AS sales,
       SUM(returns) AS returns,
       SUM(profit)  AS profit
FROM (
    SELECT 'store channel' AS channel,
           ss.s_store_sk AS id,
           sales, COALESCE(returns, 0) AS returns,
           (profit - COALESCE(profit_loss, 0)) AS profit
    FROM ss LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
    UNION ALL
    SELECT 'catalog channel', cs_call_center_sk,
           sales, returns,
           (profit - profit_loss)
    FROM cs, cr
    UNION ALL
    SELECT 'web channel', ws.wp_web_page_sk,
           sales, COALESCE(returns, 0),
           (profit - COALESCE(profit_loss, 0))
    FROM ws LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk
) x
GROUP BY channel, id
ORDER BY channel, id
LIMIT 100;
