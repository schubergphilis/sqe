-- name: Delivery — Update customer balance
-- requires: write_via_benchmark
-- description: Step 4 of Delivery transaction: credit customer balance and increment delivery count
-- type: write
-- timeout: 30s

-- Add the total order-line amount to the customer's balance and increment
-- their delivery count. Uses a subquery to compute the total from order_line.
UPDATE customer
SET c_balance = c_balance + (
        SELECT COALESCE(SUM(ol.ol_amount), 0)
        FROM order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_d_id = 1
          AND ol.ol_o_id = (
              SELECT MIN(no_o_id)
              FROM new_order
              WHERE no_w_id = 1 AND no_d_id = 1
          )
    ),
    c_delivery_cnt = c_delivery_cnt + 1
WHERE c_w_id = 1
  AND c_d_id = 1
  AND c_id = (
      SELECT o.o_c_id
      FROM orders o
      WHERE o.o_w_id = 1
        AND o.o_d_id = 1
        AND o.o_id = (
            SELECT MIN(no_o_id)
            FROM new_order
            WHERE no_w_id = 1 AND no_d_id = 1
        )
  );
