-- name: Order Status Query
-- description: Look up a customer's most recent order and its line items
-- type: read-only
-- timeout: 30s

SELECT
    c.c_id,
    c.c_first,
    c.c_middle,
    c.c_last,
    c.c_balance,
    o.o_id,
    o.o_entry_d,
    o.o_carrier_id,
    ol.ol_i_id,
    ol.ol_supply_w_id,
    ol.ol_quantity,
    ol.ol_amount,
    ol.ol_delivery_d
FROM
    customer c
    JOIN orders o
        ON o.o_c_id = c.c_id
        AND o.o_d_id = c.c_d_id
        AND o.o_w_id = c.c_w_id
    JOIN order_line ol
        ON ol.ol_o_id = o.o_id
        AND ol.ol_d_id = o.o_d_id
        AND ol.ol_w_id = o.o_w_id
WHERE
    c.c_w_id = 1
    AND c.c_d_id = 1
    AND c.c_last = 'BARBARBAR'
    AND o.o_id = (
        SELECT MAX(o2.o_id)
        FROM orders o2
        WHERE
            o2.o_c_id = c.c_id
            AND o2.o_d_id = c.c_d_id
            AND o2.o_w_id = c.c_w_id
    )
ORDER BY
    ol.ol_number;
