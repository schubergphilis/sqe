-- name: Payment Transaction
-- description: Process a customer payment (write transaction)
-- type: write
-- requires: update
-- timeout: 10s

-- This is a write transaction that cannot be expressed as a read-only SQL query.
-- The Payment transaction:
--   1. Updates warehouse.w_ytd (increment by payment amount)
--   2. Updates district.d_ytd (increment by payment amount)
--   3. Reads or selects the customer (by id or by last name)
--   4. Updates customer balance, ytd_payment, payment_cnt; sets credit data for BC customers
--   5. Inserts a row into hist
--
-- Read-only equivalent: inspect recent payment hist per district

SELECT
    h.h_c_id,
    h.h_c_d_id,
    h.h_c_w_id,
    h.h_d_id,
    h.h_w_id,
    h.h_date,
    h.h_amount,
    c.c_first,
    c.c_middle,
    c.c_last,
    c.c_credit,
    c.c_balance,
    c.c_ytd_payment,
    c.c_payment_cnt,
    w.w_name,
    d.d_name
FROM
    hist h
    JOIN customer c
        ON c.c_id = h.h_c_id
        AND c.c_d_id = h.h_c_d_id
        AND c.c_w_id = h.h_c_w_id
    JOIN warehouse w
        ON w.w_id = h.h_w_id
    JOIN district d
        ON d.d_w_id = h.h_w_id
        AND d.d_id = h.h_d_id
WHERE
    h.h_w_id = 1
    AND h.h_d_id = 1
ORDER BY
    h.h_date DESC
LIMIT 50;
