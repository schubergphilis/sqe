-- name: Delivery — Set delivery date on order lines
-- requires: write_via_benchmark
-- description: Step 3 of Delivery transaction: stamp delivery_d on order_line rows
-- type: write
-- timeout: 30s

-- Set the delivery date on all order lines for orders being delivered.
-- CURRENT_TIMESTAMP is used as the delivery date per the TPC-C spec.
UPDATE order_line
SET ol_delivery_d = CURRENT_TIMESTAMP
WHERE ol_w_id = 1
  AND ol_d_id = 1
  AND ol_delivery_d IS NULL
  AND ol_o_id = (
      SELECT MIN(no_o_id)
      FROM new_order
      WHERE no_w_id = 1 AND no_d_id = 1
  );
