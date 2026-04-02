-- name: Payment — Update customer balance and payment counters
-- requires: write_via_benchmark
-- description: Step 3 of Payment transaction: debit customer balance, increment counters
-- type: write
-- timeout: 30s

-- Debit the payment amount from the customer's balance and update payment
-- tracking fields. The TPC-C spec decrements c_balance and increments
-- c_ytd_payment and c_payment_cnt.
UPDATE customer
SET c_balance = c_balance - 2500.00,
    c_ytd_payment = c_ytd_payment + 2500.00,
    c_payment_cnt = c_payment_cnt + 1
WHERE c_w_id = 1
  AND c_d_id = 1
  AND c_id = 1;
