-- name: Cumulative store and web sales with running total window
-- timeout: 60s
WITH web_v1 AS (
    SELECT ws_item_sk AS item_sk, d_date,
           SUM(ws_sales_price) AS sales_price,
           SUM(SUM(ws_sales_price)) OVER (
               PARTITION BY ws_item_sk ORDER BY d_date
               ROWS UNBOUNDED PRECEDING
           ) AS cume_sales
    FROM web_sales, date_dim
    WHERE ws_sold_date_sk = d_date_sk
      AND d_month_seq     BETWEEN 1200 AND 1200 + 11
      AND ws_item_sk      IS NOT NULL
    GROUP BY ws_item_sk, d_date
),
store_v1 AS (
    SELECT ss_item_sk AS item_sk, d_date,
           SUM(ss_sales_price) AS sales_price,
           SUM(SUM(ss_sales_price)) OVER (
               PARTITION BY ss_item_sk ORDER BY d_date
               ROWS UNBOUNDED PRECEDING
           ) AS cume_sales
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk
      AND d_month_seq     BETWEEN 1200 AND 1200 + 11
      AND ss_item_sk      IS NOT NULL
    GROUP BY ss_item_sk, d_date
)
SELECT *
FROM (
    SELECT item_sk, d_date,
           web_sales, store_sales,
           MAX(web_sales) OVER (
               PARTITION BY item_sk ORDER BY d_date
               ROWS UNBOUNDED PRECEDING
           ) AS web_cumulative,
           MAX(store_sales) OVER (
               PARTITION BY item_sk ORDER BY d_date
               ROWS UNBOUNDED PRECEDING
           ) AS store_cumulative
    FROM (
        SELECT COALESCE(web.item_sk, store.item_sk)   AS item_sk,
               COALESCE(web.d_date, store.d_date)     AS d_date,
               web.cume_sales   AS web_sales,
               store.cume_sales AS store_sales
        FROM web_v1 web
        FULL OUTER JOIN store_v1 store
            ON web.item_sk = store.item_sk
           AND web.d_date  = store.d_date
    ) x
) y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk, d_date
LIMIT 100;
