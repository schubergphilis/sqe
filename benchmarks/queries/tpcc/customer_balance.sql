-- name: Customer Balance Report
-- description: Top customers ranked by balance (most negative = most in debt)
-- type: read-only
-- timeout: 60s

SELECT
    c.c_w_id,
    c.c_d_id,
    c.c_id,
    c.c_first,
    c.c_middle,
    c.c_last,
    c.c_credit,
    c.c_credit_lim,
    c.c_discount,
    c.c_balance,
    c.c_ytd_payment,
    c.c_payment_cnt,
    c.c_delivery_cnt,
    w.w_name  AS warehouse_name,
    d.d_name  AS district_name
FROM
    customer c
    JOIN warehouse w
        ON w.w_id = c.c_w_id
    JOIN district d
        ON d.d_w_id = c.c_w_id
        AND d.d_id = c.c_d_id
ORDER BY
    c.c_balance ASC
LIMIT 100;
