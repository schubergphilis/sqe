-- name: New Order — Decrement stock quantity
-- requires: write_via_benchmark
-- description: Step 2 of New Order transaction: decrement stock for ordered items
-- type: write
-- timeout: 30s

-- Decrement stock quantity for item being ordered. If s_quantity drops below
-- threshold (10), add 91 to replenish per TPC-C spec. Also increment
-- s_order_cnt. Remote warehouse orders would also increment s_remote_cnt.
UPDATE stock
SET s_quantity = CASE
        WHEN s_quantity - 5 < 10 THEN s_quantity - 5 + 91
        ELSE s_quantity - 5
    END,
    s_order_cnt = s_order_cnt + 1
WHERE s_w_id = 1
  AND s_i_id = 1;
