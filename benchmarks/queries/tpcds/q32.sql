-- name: Catalog sales discount exceeding 30% of average for item class
-- timeout: 60s
SELECT SUM(cs_ext_discount_amt) AS excess_discount_amount
FROM catalog_sales, item, date_dim
WHERE i_manufact_id = 977
  AND i_item_sk     = cs_item_sk
  AND d_date        BETWEEN DATE '2000-01-27' AND DATE '2000-04-26'
  AND d_date_sk     = cs_sold_date_sk
  AND cs_ext_discount_amt > (
      SELECT 1.3 * AVG(cs_ext_discount_amt)
      FROM catalog_sales, date_dim
      WHERE cs_item_sk  = i_item_sk
        AND d_date       BETWEEN DATE '2000-01-27' AND DATE '2000-04-26'
        AND d_date_sk    = cs_sold_date_sk
  )
LIMIT 100;
