-- name: Items with low inventory but high catalog purchases
-- timeout: 60s
SELECT i_item_id, i_item_desc, i_current_price
FROM item, inventory, date_dim, store_sales
WHERE i_current_price BETWEEN 62 AND 62 + 30
  AND inv_item_sk     = i_item_sk
  AND d_date_sk       = inv_date_sk
  AND d_date          BETWEEN DATE '1998-04-01' AND DATE '1998-06-01'
  AND i_manufact_id   IN (437, 129, 727, 663)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND ss_item_sk      = i_item_sk
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100;
