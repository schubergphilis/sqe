-- name: Top store sales items by category and class with revenue ratio
-- timeout: 60s
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ss_ext_sales_price)            AS itemrevenue,
       SUM(ss_ext_sales_price) * 100.0 / SUM(SUM(ss_ext_sales_price)) OVER
           (PARTITION BY i_class)         AS revenueratio
FROM store_sales, item, date_dim
WHERE ss_item_sk    = i_item_sk
  AND i_category    IN ('Sports', 'Books', 'Home')
  AND ss_sold_date_sk = d_date_sk
  AND d_date BETWEEN DATE '1999-02-22' AND DATE '1999-03-24'
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100;
