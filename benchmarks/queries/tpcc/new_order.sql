-- name: New Order Transaction
-- description: Create a new order for a customer (write transaction)
-- type: write
-- requires: insert, update
-- timeout: 10s

-- This is a write transaction that cannot be expressed as a read-only SQL query.
-- The New Order transaction:
--   1. Reads warehouse and district tax rates
--   2. Increments district.d_next_o_id
--   3. Inserts a new row into orders
--   4. Inserts a row into new_order
--   5. For each order line:
--      a. Reads item price and stock quantity
--      b. Updates stock (decrements quantity, increments order/remote counts)
--      c. Inserts a row into order_line
--
-- Read-only equivalent: inspect recent new orders awaiting delivery

SELECT
    o.o_id,
    o.o_d_id,
    o.o_w_id,
    o.o_c_id,
    o.o_entry_d,
    o.o_ol_cnt,
    ol.ol_i_id,
    ol.ol_quantity,
    ol.ol_amount,
    i.i_price,
    s.s_quantity
FROM
    new_order no_
    JOIN orders o
        ON o.o_id = no_.no_o_id
        AND o.o_d_id = no_.no_d_id
        AND o.o_w_id = no_.no_w_id
    JOIN order_line ol
        ON ol.ol_o_id = o.o_id
        AND ol.ol_d_id = o.o_d_id
        AND ol.ol_w_id = o.o_w_id
    JOIN item i
        ON i.i_id = ol.ol_i_id
    JOIN stock s
        ON s.s_i_id = ol.ol_i_id
        AND s.s_w_id = ol.ol_supply_w_id
WHERE
    no_.no_w_id = 1
    AND no_.no_d_id = 1
ORDER BY
    o.o_id DESC,
    ol.ol_number
LIMIT 100;
