-- name: Delivery — Delete fulfilled new_order rows
-- requires: full_schema
-- description: Step 1 of Delivery transaction: remove the oldest pending new_order per district
-- type: write
-- timeout: 30s

-- Delete the oldest new_order row for district 1, warehouse 1.
-- In the full TPC-C profile this runs once per district (1-10) in a loop.
DELETE FROM new_order
WHERE no_w_id = 1
  AND no_d_id = 1
  AND no_o_id = (
      SELECT MIN(no_o_id)
      FROM new_order
      WHERE no_w_id = 1 AND no_d_id = 1
  );
