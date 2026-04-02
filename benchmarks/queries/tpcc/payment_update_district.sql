-- name: Payment — Update district year-to-date
-- requires: full_schema
-- description: Step 2 of Payment transaction: increment district d_ytd by payment amount
-- type: write
-- timeout: 30s

-- Increment the district's year-to-date total by the payment amount.
UPDATE district
SET d_ytd = d_ytd + 2500.00
WHERE d_w_id = 1
  AND d_id = 1;
