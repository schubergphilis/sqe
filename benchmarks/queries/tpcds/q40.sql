-- name: Catalog sales revenue impact of items losing promotion
-- timeout: 60s
SELECT w_state, i_item_id,
       SUM(CASE WHEN d_date < DATE '2000-03-11'
                THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
                ELSE 0 END) AS sales_before,
       SUM(CASE WHEN d_date >= DATE '2000-03-11'
                THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
                ELSE 0 END) AS sales_after
FROM catalog_sales
LEFT OUTER JOIN catalog_returns
    ON cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk,
     warehouse, item, date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND i_item_sk       = cs_item_sk
  AND cs_warehouse_sk = w_warehouse_sk
  AND cs_sold_date_sk = d_date_sk
  AND d_date          BETWEEN DATE '2000-02-10' AND DATE '2000-04-10'
GROUP BY w_state, i_item_id
ORDER BY w_state, i_item_id
LIMIT 100;
