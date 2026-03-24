-- name: Inventory variance vs prior year by warehouse and item
-- timeout: 60s
SELECT *
FROM (
    SELECT w_warehouse_name, i_item_id,
           SUM(CASE WHEN d_date < DATE '2000-03-11'
                    THEN inv_quantity_on_hand
                    ELSE 0 END)          AS inv_before,
           SUM(CASE WHEN d_date >= DATE '2000-03-11'
                    THEN inv_quantity_on_hand
                    ELSE 0 END)          AS inv_after
    FROM inventory, warehouse, item, date_dim
    WHERE i_item_sk   = inv_item_sk
      AND w_warehouse_sk = inv_warehouse_sk
      AND inv_date_sk = d_date_sk
      AND i_current_price BETWEEN 0.99 AND 1.49
      AND d_date BETWEEN DATE '2000-02-10' AND DATE '2000-04-10'
    GROUP BY w_warehouse_name, i_item_id
) x
WHERE (CASE WHEN inv_before > 0
            THEN inv_after / inv_before
            ELSE NULL END) BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100;
