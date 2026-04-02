-- name: Delivery — Update carrier on orders
-- requires: write_via_benchmark
-- description: Step 2 of Delivery transaction: set carrier_id on delivered orders
-- type: write
-- timeout: 30s

-- Set carrier_id on orders that have been processed for delivery.
-- In the full TPC-C profile the carrier_id is a random value 1-10.
UPDATE orders
SET o_carrier_id = 7
WHERE o_w_id = 1
  AND o_d_id = 1
  AND o_carrier_id IS NULL
  AND o_id = (
      SELECT MIN(no_o_id)
      FROM new_order
      WHERE no_w_id = 1 AND no_d_id = 1
  );
