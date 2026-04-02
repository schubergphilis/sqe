-- name: Payment — Update warehouse year-to-date
-- requires: write_via_benchmark
-- description: Step 1 of Payment transaction: increment warehouse w_ytd by payment amount
-- type: write
-- timeout: 30s

-- Increment the warehouse's year-to-date total by the payment amount.
-- TPC-C spec: h_amount is a random value between 1.00 and 5000.00.
UPDATE warehouse
SET w_ytd = w_ytd + 2500.00
WHERE w_id = 1;
