-- name: Delivery Transaction
-- description: Process delivery of pending new orders (write transaction)
-- type: write
-- timeout: 10s

-- This is a write transaction that cannot be expressed as a read-only SQL query.
-- The Delivery transaction:
--   1. For each of the 10 districts in a warehouse:
--      a. Selects and deletes the oldest row from new_order
--      b. Updates the corresponding orders row (set carrier_id)
--      c. Updates all order_line rows (set delivery_d to now)
--      d. Updates customer balance and delivery_cnt
--
-- Read-only equivalent: inspect new orders pending delivery per district

SELECT
    no_.no_w_id,
    no_.no_d_id,
    no_.no_o_id,
    o.o_c_id,
    o.o_entry_d,
    o.o_carrier_id,
    o.o_ol_cnt,
    SUM(ol.ol_amount) AS total_amount
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
WHERE
    no_.no_w_id = 1
GROUP BY
    no_.no_w_id,
    no_.no_d_id,
    no_.no_o_id,
    o.o_c_id,
    o.o_entry_d,
    o.o_carrier_id,
    o.o_ol_cnt
ORDER BY
    no_.no_d_id,
    no_.no_o_id
LIMIT 10;
