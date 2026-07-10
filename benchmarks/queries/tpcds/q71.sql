-- name: Sales by item and channel for promotional items in specific month
-- timeout: 60s
SELECT i_brand_id brand_id, i_brand brand, t_hour, t_minute,
       SUM(ext_price) AS ext_price
FROM item,
(
    SELECT ws_ext_sales_price AS ext_price, ws_sold_time_sk AS sold_time_sk,
           ws_item_sk AS sold_item_sk
    FROM web_sales, date_dim
    WHERE d_date_sk       = ws_sold_date_sk
      AND d_moy           = 11
      AND d_year          = 1999
    UNION ALL
    SELECT cs_ext_sales_price, cs_sold_time_sk, cs_item_sk
    FROM catalog_sales, date_dim
    WHERE d_date_sk       = cs_sold_date_sk
      AND d_moy           = 11
      AND d_year          = 1999
    UNION ALL
    SELECT ss_ext_sales_price, ss_sold_time_sk, ss_item_sk
    FROM store_sales, date_dim
    WHERE d_date_sk       = ss_sold_date_sk
      AND d_moy           = 11
      AND d_year          = 1999
) tmp, time_dim
WHERE sold_time_sk     = t_time_sk
  AND i_item_sk        = sold_item_sk
  AND i_manager_id     = 1
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id
LIMIT 100;
