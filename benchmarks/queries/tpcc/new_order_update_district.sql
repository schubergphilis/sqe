-- name: New Order — Increment district next order ID
-- description: Step 1 of New Order transaction: bump d_next_o_id for the district
-- type: write
-- timeout: 30s

-- Increment the district's next order ID counter. In the full TPC-C profile,
-- the old d_next_o_id is captured first and used as the new order's o_id.
UPDATE district
SET d_next_o_id = d_next_o_id + 1
WHERE d_w_id = 1
  AND d_id = 1;
